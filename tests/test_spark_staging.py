"""The distributed canonical stage agrees with the audited local implementation."""

from __future__ import annotations

import os
import sys
from pathlib import Path

import pytest

from logistics_platform.contracts import compact
from logistics_platform.generator import (
    generate,
    incremental_records,
    synthetic_records,
    write_batch,
)
from logistics_platform.pipeline import Pipeline, connect

pytestmark = pytest.mark.spark


@pytest.fixture(scope="module")
def spark():
    pytest.importorskip("pyspark")
    from pyspark.sql import SparkSession

    os.environ.setdefault("SPARK_LOCAL_IP", "127.0.0.1")
    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
    session = (
        SparkSession.builder.master("local[2]")
        .appName("canonical-staging-parity")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .getOrCreate()
    )
    session.conf.set("spark.sql.session.timeZone", "UTC")
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()


def test_all_six_typed_stages_equal_local_after_corrections(spark, tmp_path: Path):
    from logistics_platform.spark_job import EVIDENCE_SCHEMA, normalize_json, prepare_evidence
    from logistics_platform.spark_staging import build_staging

    manifests = generate(tmp_path / "input", shipments=3)
    db = tmp_path / "warehouse.duckdb"
    for manifest in manifests:
        report = Pipeline(db).run(manifest)
        assert report["status"] == "published", report
    full = synthetic_records(3)
    increment = incremental_records(full)
    normalized = [
        normalize_json(compact(record), source, 1, "a" * 64)
        for batch in [full, increment]
        for source, records in batch.items()
        for record in records
    ]
    prepared = prepare_evidence(spark.createDataFrame(normalized, EVIDENCE_SCHEMA))
    stages, decisions, checks = build_staging(prepared, "parity")
    assert all(c["passed"] for c in checks), checks
    con = connect(db)
    try:
        for table, frame in stages.items():
            columns = [c for c in frame.columns if c not in {"raw_hash", "run_id"}]
            expected = con.execute(f"SELECT {','.join(columns)} FROM {table}").fetchall()
            actual = [tuple(row) for row in frame.select(*columns).collect()]
            assert sorted(actual, key=str) == sorted(expected, key=str), table
    finally:
        con.close()
        decisions.unpersist()


def test_missing_operational_state_uses_shared_created_default(spark, tmp_path: Path):
    from logistics_platform.spark_job import EVIDENCE_SCHEMA, normalize_json, prepare_evidence
    from logistics_platform.spark_staging import build_staging

    records = synthetic_records(1)
    for record in records["shipment_ops"]:
        if record["entity_type"] == "shipment":
            record["attributes"].pop("state", None)
    manifest = write_batch(tmp_path / "input", "missing-state", 1, records)
    database = tmp_path / "warehouse.duckdb"
    report = Pipeline(database).run(manifest)
    assert report["status"] == "published", report
    normalized = [
        normalize_json(compact(record), source, 1, "a" * 64)
        for source, rows in records.items()
        for record in rows
    ]
    prepared = prepare_evidence(spark.createDataFrame(normalized, EVIDENCE_SCHEMA))
    stages, decisions, checks = build_staging(prepared, "missing-state")
    try:
        assert all(check["passed"] for check in checks), checks
        actual = [
            tuple(row) for row in stages["stg_shipment"].select("business_key", "state").collect()
        ]
        connection = connect(database)
        try:
            expected = connection.execute("SELECT business_key,state FROM stg_shipment").fetchall()
        finally:
            connection.close()
        assert actual == expected
        assert actual[0][1] == "created"
    finally:
        decisions.unpersist()
