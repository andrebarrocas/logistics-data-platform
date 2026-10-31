"""Small shared fixtures prove Spark/local business semantics, not scale claims."""

# ruff: noqa: E402
from __future__ import annotations

import json
import os
import sys
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import duckdb
import pytest

pytest.importorskip("pyspark")
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from test_warehouse import event, insert, reference, seed

from logistics_platform.cloud import PRODUCT_KEYS
from logistics_platform.keys import canonical_id, surrogate_key
from logistics_platform.spark_warehouse import STAGING_SCHEMAS, project_spark, quality_checks_spark
from logistics_platform.warehouse import create_staging, project

pytestmark = pytest.mark.spark


@pytest.fixture(scope="module")
def spark() -> Iterator[SparkSession]:
    os.environ.setdefault("SPARK_LOCAL_IP", "127.0.0.1")
    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
    session = (
        SparkSession.builder.master("local[2]")
        .appName("warehouse-parity")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.catalog.clearCache()
    session.stop()


def canonical_rows(columns: list[str], rows: list[Any]) -> list[str]:
    normalized = []
    for row in rows:
        result = dict(zip(columns, row))
        for key in ("attributes",):
            if result.get(key) is not None:
                result[key] = json.loads(result[key])
        normalized.append(json.dumps(result, sort_keys=True, default=str))
    return sorted(normalized)


def test_spark_projects_all_products_with_historical_and_label_parity(
    spark: SparkSession, tmp_path: Path
) -> None:
    local = duckdb.connect()
    local.create_function("canonical_id", canonical_id, ["VARCHAR", "VARCHAR"], "VARCHAR")
    local.create_function(
        "surrogate_key", surrogate_key, ["VARCHAR", "VARCHAR", "VARCHAR"], "BIGINT"
    )
    create_staging(local)
    ids = seed(local)
    event(local, ids)
    reference(
        local,
        "carrier",
        "CAR-ASTER",
        "Aster Parcel corrected",
        "2025-01-04",
        updated="2025-02-01",
        operating_status="active",
    )
    reference(local, "region", "REG-EAST", "Eastern region")
    reference(
        local,
        "facility",
        "FAC-A",
        "Synthetic A",
        "2025-01-05",
        updated="2025-02-01",
        country_key="PT",
        region_key="REG-EAST",
    )
    local.execute("UPDATE stg_shipment SET state_occurred_at=TIMESTAMP '2025-01-04'")
    for key, amount in (("charge", 2400), ("credit", -200)):
        insert(
            local,
            "cost",
            cost_id=key,
            cost_canonical_id=canonical_id("cost", key),
            shipment_id=ids["shipment"],
            carrier_id=ids["carrier"],
            amount_minor=amount,
            currency="EUR",
            cost_type="linehaul",
            incurred_at="2025-01-05",
            revision=1,
            source_updated_at="2025-01-07",
            is_deleted=False,
        )
    stages = {}
    for name, ddl in STAGING_SCHEMAS.items():
        columns = [field.split()[0] for field in ddl.split(",")]
        rows = local.execute(f"SELECT {','.join(columns)} FROM {name}").fetchall()
        stages[name] = spark.createDataFrame(rows, ddl)
    project(local, "shared-fixture")
    projected = project_spark(spark, stages, "shared-fixture", str(tmp_path / "warehouse"))
    assert set(PRODUCT_KEYS) <= set(projected)
    for table in PRODUCT_KEYS:
        ignored = {"run_id", "source_run_id", "recorded_at"}
        columns = [
            row[0] for row in local.execute(f"DESCRIBE {table}").fetchall() if row[0] not in ignored
        ]
        assert set(columns) == set(projected[table].columns) - ignored, table
        expected = canonical_rows(
            columns, local.execute(f"SELECT {','.join(columns)} FROM {table}").fetchall()
        )
        # Spark Row deserialization uses the driver's OS timezone for TimestampType.
        # Compare SQL-rendered UTC strings, including the 1900 sentinel whose
        # Europe/Lisbon historical offset is not today's offset.
        types = {field.name: field.dataType.simpleString() for field in projected[table].schema}
        selected = [
            F.col(name).cast("string").alias(name)
            if types[name].startswith("timestamp")
            else F.col(name)
            for name in columns
        ]
        actual = canonical_rows(columns, projected[table].select(*selected).collect())
        assert actual == expected, table
    checks = quality_checks_spark(spark)
    assert len(checks) >= 80
    assert all(check["passed"] for check in checks), [c for c in checks if not c["passed"]]
    local.close()
