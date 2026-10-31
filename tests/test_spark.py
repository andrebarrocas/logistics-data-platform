"""Distributed adapter, history, deterministic identity and conflict tests."""

# ruff: noqa: E402

import json
import os
import sys
from pathlib import Path

import pytest

pyspark = pytest.importorskip("pyspark")
from pyspark.sql import SparkSession

from logistics_platform.contracts import compact, normalize
from logistics_platform.generator import generate, synthetic_records
from logistics_platform.spark_job import EVIDENCE_SCHEMA, normalize_json, prepare_evidence, run_job

pytestmark = pytest.mark.spark


@pytest.fixture(scope="module")
def spark():
    os.environ.setdefault("SPARK_LOCAL_IP", "127.0.0.1")
    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
    session = (
        SparkSession.builder.master("local[2]")
        .appName("logistics-tests")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .getOrCreate()
    )
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()


def test_spark_matches_shared_adapter_and_preserves_ambiguous_candidates(spark):
    records = synthetic_records(2, 42)
    aliases = [
        r
        for r in records["reference_master"]
        if r["entity_type"] == "alias"
        and r["attributes"]["source"] == "carrier_delivery"
        and r["attributes"]["alias"] == "ASTER"
    ]
    assert aliases
    alias = aliases[0]
    conflicting = dict(
        alias,
        source_id="conflict",
        business_key="ALIAS-CONFLICT",
        attributes=dict(alias["attributes"], target_key="CAR-MF"),
    )
    rows = [
        normalize_json(compact(r), "reference_master", 1, "a" * 64) for r in [alias, conflicting]
    ]
    event = dict(records["carrier_delivery"][0], carrier_alias="ASTER")
    rows.append(normalize_json(compact(event), "carrier_delivery", 1, "b" * 64))
    rows.append(rows[-1])
    prepared = prepare_evidence(spark.createDataFrame(rows, EVIDENCE_SCHEMA))
    assert prepared["evidence"].count() == 3
    assert prepared["alias_conflicts"].count() == 1
    decision = prepared["resolution_decisions"].first()
    assert decision.outcome == "ambiguous"
    assert decision.carrier_id is None
    assert decision.candidates == ["CAR-AP", "CAR-MF"]
    assert rows[-1]["payload_hash"] == normalize("carrier_delivery", event).payload_hash


def test_spark_full_and_increment_with_prior_evidence(spark, tmp_path: Path):
    first, second = generate(tmp_path / "input", shipments=3)
    report = run_job(spark, str(first), str(tmp_path / "full"))
    assert report["status"] == "prepared"
    report2 = run_job(
        spark,
        str(second),
        str(tmp_path / "increment"),
        history_uri=str(tmp_path / "full" / "evidence"),
    )
    assert report2["status"] == "prepared"
    assert report2["evidence_rows"] > report["evidence_rows"]
    current = spark.read.parquet(str(tmp_path / "increment" / "current"))
    assert current.filter("entity_type='cost' AND revision=2").count() == 1
    assert current.filter("entity_type='facility' AND business_key='FAC-MAD'").count() >= 2
    quality_file = next((tmp_path / "increment" / "quality").glob("part-*"))
    assert json.loads(quality_file.read_text())["status"] == "prepared"
