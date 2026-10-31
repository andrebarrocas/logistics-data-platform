"""Five raw sources -> Spark canonical/history/marts -> typed Redshift release."""

from __future__ import annotations

import csv
import hashlib
import json
import os
import sys
from pathlib import Path

import pytest

from logistics_platform.cloud import PRODUCT_KEYS, publication_sql, validate_manifest
from logistics_platform.generator import generate
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
        .appName("complete-cloud-path")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .getOrCreate()
    )
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()


def test_full_then_increment_publish_complete_spark_products(spark, tmp_path: Path):
    from logistics_platform.spark_job import run_job

    manifests = generate(tmp_path / "input", shipments=3)
    local_db = tmp_path / "local.duckdb"
    history = None
    total_before = None
    for index, source_manifest in enumerate(manifests, 1):
        local = Pipeline(local_db).run(source_manifest)
        assert local["status"] == "published", local
        output = tmp_path / f"normalized-{index}"
        release = tmp_path / f"release-{index}"
        report = run_job(
            spark,
            str(source_manifest),
            str(output),
            history_uri=history,
            build_products=True,
            release_uri=str(release),
            release_id=f"spark-{index}",
        )
        assert report["status"] == "ready", report
        assert all(
            q["passed"] for q in report["quality"] if q["severity"].lower() in {"error", "critical"}
        )
        manifest = json.loads((release / "release.json").read_text())
        validate_manifest(manifest)
        assert manifest["profile"] == "validated-spark-snapshot"
        assert manifest["contract_version"] == 2
        assert {t["name"] for t in manifest["tables"]} == set(PRODUCT_KEYS)
        marker = json.loads((release / "COMMITTED.json").read_text())
        assert (
            marker["sha256"] == hashlib.sha256((release / "release.json").read_bytes()).hexdigest()
        )
        con = connect(local_db)
        try:
            for table in manifest["tables"]:
                name = table["name"]
                assert (
                    table["row_count"] == con.execute(f"SELECT count(*) FROM {name}").fetchone()[0]
                ), name
                physical_count = 0
                for shard in table["files"]:
                    path = release / shard["file"]
                    assert hashlib.sha256(path.read_bytes()).hexdigest() == shard["sha256"]
                    with path.open(newline="") as stream:
                        rows = list(csv.DictReader(stream))
                    assert len(rows) == shard["row_count"]
                    physical_count += len(rows)
                assert physical_count == table["row_count"]
            cost = next(t for t in manifest["tables"] if t["name"] == "fact_transportation_cost")
            total = con.execute(
                "SELECT sum(amount_minor) FROM fact_transportation_cost"
            ).fetchone()[0]
            assert cost["amount_minor_by_currency"] == {"EUR": total}
            if total_before is not None:
                assert total == total_before + 125
            total_before = total
        finally:
            con.close()
        sql = publication_sql(
            manifest,
            marker["sha256"],
            "s3://synthetic-bucket/releases/spark-1/release.json",
            "arn:aws:iam::123456789012:role/synthetic-copy",
        )
        assert "MERGE INTO" in sql[0] and "Cost reconciliation" in sql[0]
        history = str(output / "evidence")
