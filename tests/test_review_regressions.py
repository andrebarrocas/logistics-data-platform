"""Regression cases found by independent review of contracts and alias lifecycle."""

from __future__ import annotations

import json
import os
import sys
from pathlib import Path
from typing import Any

import duckdb
import pytest

from logistics_platform.contracts import compact, normalize
from logistics_platform.generator import empty_records, envelope, synthetic_records, write_batch
from logistics_platform.keys import canonical_id, iso
from logistics_platform.pipeline import Pipeline
from logistics_platform.resolution import Resolver


def alias_lifecycle() -> list[dict[str, Any]]:
    values = []
    for revision, target, start, operation in [
        (1, "CAR-AP", "2026-01-01T00:00:00Z", "upsert"),
        (2, "CAR-MF", "2026-01-03T00:00:00Z", "upsert"),
        (3, "CAR-MF", "2026-01-05T00:00:00Z", "delete"),
    ]:
        values.append(
            envelope(
                "alias",
                "REF-LIFECYCLE",
                "LIFECYCLE",
                {
                    "target_type": "carrier",
                    "source": "carrier_delivery",
                    "alias": "BOUNDARY",
                    "target_key": target,
                    "effective_to": "9999-12-31T00:00:00Z",
                    "rule_id": f"LIFECYCLE-{revision}",
                },
                revision=revision,
                operation=operation,
                effective_at=start,
            )
        )
    return values


def test_alias_successor_and_tombstone_bound_prior_source_assertions() -> None:
    resolver = Resolver([(normalize("reference_master", row), {}) for row in alias_lifecycle()])
    assert resolver.conflicts() == []
    for instant, expected in [
        ("2026-01-02T23:59:59Z", canonical_id("carrier", "CAR-AP")),
        ("2026-01-03T00:00:00Z", canonical_id("carrier", "CAR-MF")),
        ("2026-01-05T00:00:00Z", None),
        ("2026-01-06T00:00:00Z", None),
    ]:
        assert (
            resolver.resolve("carrier", "carrier_delivery", "BOUNDARY", iso(instant), "event")
            == expected
        )


@pytest.mark.parametrize(
    "attribute",
    [
        "carrier_alias",
        "origin_facility_alias",
        "destination_facility_alias",
        "service_alias",
    ],
)
def test_nonscalar_alias_is_structured_schema_failure(tmp_path: Path, attribute: str) -> None:
    records = synthetic_records(2)
    for row in records["shipment_ops"]:
        if row["entity_type"] == "shipment":
            row["attributes"][attribute] = 123
    manifest = write_batch(tmp_path / "input", "bad-alias-type", 1, records)
    database = tmp_path / "warehouse.duckdb"
    result = Pipeline(database).run(manifest)
    assert result["status"] == "failed" and result["watermark"] == 0
    assert any(
        check["rule_id"] == "ING_SCHEMA" and check["status"] == "fail"
        for check in result["quality"]
    )
    with duckdb.connect(str(database), read_only=True) as connection:
        assert connection.execute("SELECT count(*) FROM pipeline_runs").fetchone()[0] == 1
        assert connection.execute("SELECT count(*) FROM evidence").fetchone()[0] == 0


@pytest.fixture(scope="module")
def review_spark():
    pytest.importorskip("pyspark")
    from pyspark.sql import SparkSession

    os.environ.setdefault("SPARK_LOCAL_IP", "127.0.0.1")
    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
    session = (
        SparkSession.builder.master("local[2]")
        .appName("logistics-review-regressions")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .getOrCreate()
    )
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()


@pytest.mark.spark
def test_spark_alias_lifecycle_and_distinct_source_conflicts(review_spark) -> None:
    from logistics_platform.spark_job import EVIDENCE_SCHEMA, normalize_json, prepare_evidence

    aliases = alias_lifecycle()
    # Shipment dimensional identity is resolved at creation; its state observation
    # may have a later business-effective instant.
    aliases.extend(
        [
            dict(
                row,
                source_id="REF-SHIP-LIFECYCLE",
                business_key="SHIP-LIFECYCLE",
                attributes=dict(row["attributes"], source="shipment_ops"),
            )
            for row in alias_lifecycle()
        ]
    )
    for suffix, target in [("A", "CAR-AP"), ("B", "CAR-MF")]:
        aliases.append(
            envelope(
                "alias",
                f"REF-CONFLICT-{suffix}",
                f"CONFLICT-{suffix}",
                {
                    "target_type": "carrier",
                    "source": "carrier_delivery",
                    "alias": "CONFLICT",
                    "target_key": target,
                    "effective_to": "9999-12-31T00:00:00Z",
                    "rule_id": f"CONFLICT-{suffix}",
                },
            )
        )
    evidence = [normalize_json(compact(row), "reference_master", 1, "a" * 64) for row in aliases]
    for identity, instant, alias in [
        ("before", "2026-01-02T00:00:00Z", "BOUNDARY"),
        ("successor", "2026-01-03T00:00:00Z", "BOUNDARY"),
        ("deleted", "2026-01-05T00:00:00Z", "BOUNDARY"),
        ("conflict", "2026-01-03T00:00:00Z", "CONFLICT"),
    ]:
        event = {
            "event_id": identity,
            "shipment_key": "SHP-1",
            "package_key": "",
            "carrier_alias": alias,
            "state": "in_transit",
            "occurred_at": instant,
            "updated_at": instant,
            "revision": 1,
            "verified": False,
        }
        evidence.append(normalize_json(compact(event), "carrier_delivery", 1, "b" * 64))
    shipment = envelope(
        "shipment",
        "ops-shipment",
        "SHP-1",
        {
            "order_key": "ORD-1",
            "route_key": "RTE-1",
            "country_code": "PT",
            "created_at": "2026-01-02T00:00:00Z",
            "promised_at": "2026-01-06T00:00:00Z",
            "carrier_alias": "BOUNDARY",
            "state": "in_transit",
        },
        effective_at="2026-01-04T00:00:00Z",
    )
    evidence.append(normalize_json(compact(shipment), "shipment_ops", 1, "c" * 64))
    prepared = prepare_evidence(review_spark.createDataFrame(evidence, EVIDENCE_SCHEMA))
    actual = {row.source_id: row for row in prepared["resolution_decisions"].collect()}
    assert actual["ops-shipment"].carrier_id == canonical_id("carrier", "CAR-AP")
    assert actual["before"].carrier_id == canonical_id("carrier", "CAR-AP")
    assert actual["successor"].carrier_id == canonical_id("carrier", "CAR-MF")
    assert actual["deleted"].outcome == "unresolved" and actual["deleted"].carrier_id is None
    assert actual["conflict"].outcome == "ambiguous" and actual["conflict"].carrier_id is None
    assert prepared["alias_conflicts"].count() == 1


@pytest.mark.spark
def test_missing_spark_object_leaves_structured_failure_report(
    review_spark, tmp_path: Path
) -> None:
    from logistics_platform.spark_job import run_job

    manifest = write_batch(tmp_path / "input", "missing-object", 1, empty_records())
    declared = json.loads(manifest.read_text())
    (manifest.parent / declared["files"][0]["path"]).unlink()
    output = tmp_path / "failed-output"
    with pytest.raises(Exception):
        run_job(review_spark, str(manifest), str(output))
    quality_files = list((output / "quality").glob("part-*"))
    assert quality_files, "A missing source object must still leave a structured run result"
    report = json.loads(quality_files[0].read_text())
    assert report["status"] == "failed"
    assert any(check["severity"] == "error" and not check["passed"] for check in report["quality"])


@pytest.mark.parametrize(
    ("entity_type", "business_key"),
    [
        ("country", "GB"),
        ("region", "REG-WEST"),
    ],
)
def test_reference_tombstone_does_not_disappear_during_island_compression(
    tmp_path: Path,
    entity_type: str,
    business_key: str,
) -> None:
    records = synthetic_records(2)
    database = tmp_path / "reference-history.duckdb"
    pipeline = Pipeline(database)
    first = write_batch(tmp_path / "inputs", "reference-full", 1, records)
    assert pipeline.run(first)["status"] == "published"
    original = next(
        row
        for row in records["reference_master"]
        if row["entity_type"] == entity_type and row["business_key"] == business_key
    )
    correction = empty_records()
    correction["reference_master"] = [
        dict(
            original,
            revision=2,
            operation="delete",
            effective_at="2026-01-05T00:00:00Z",
            updated_at="2026-01-05T00:00:00Z",
        )
    ]
    second = write_batch(tmp_path / "inputs", "reference-delete", 2, correction)
    assert pipeline.run(second)["status"] == "published"
    with duckdb.connect(str(database), read_only=True) as connection:
        assert connection.execute(
            "SELECT status FROM dim_reference WHERE entity_type=? AND business_key=? AND is_current",
            [entity_type, business_key],
        ).fetchall() == [("INACTIVE",)]
        assert (
            connection.execute(
                "SELECT count(*) FROM evidence WHERE entity_type=? AND business_key=?",
                [entity_type, business_key],
            ).fetchone()[0]
            == 2
        )
