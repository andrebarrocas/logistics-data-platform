"""Offline cloud contract tests: these are not AWS execution evidence."""

from __future__ import annotations

import copy
import io
import json
from pathlib import Path
from typing import Any

import duckdb
import pytest
import sqlglot

from logistics_platform.cloud import (
    PublicationError,
    bootstrap_statements,
    export_release,
    load_release,
    publication_sql,
    read_release,
    redshift_type,
    upload_release,
    validate_manifest,
    wait_statement,
)

pytestmark = pytest.mark.cloud
ROLE = "arn:aws:iam::123456789012:role/portfolio-copy"
CHECKS = [{"check_id": "complete", "severity": "ERROR", "passed": True}]


class PreconditionFailed(Exception):
    response = {"Error": {"Code": "PreconditionFailed"}}


class FakeS3:
    def __init__(self) -> None:
        self.objects: dict[tuple[str, str], bytes] = {}
        self.writes: list[str] = []

    def put_object(self, **kwargs: Any) -> None:
        assert kwargs["IfNoneMatch"] == "*"
        key = (kwargs["Bucket"], kwargs["Key"])
        if key in self.objects:
            raise PreconditionFailed()
        self.objects[key] = kwargs["Body"]
        self.writes.append(kwargs["Key"])

    def get_object(self, **kwargs: Any) -> dict[str, Any]:
        return {"Body": io.BytesIO(self.objects[(kwargs["Bucket"], kwargs["Key"])])}


class FakeData:
    def __init__(self, status: str = "FINISHED") -> None:
        self.calls: list[dict[str, Any]] = []
        self.status = status

    def batch_execute_statement(self, **kwargs: Any) -> dict[str, str]:
        self.calls.append(kwargs)
        return {"Id": str(len(self.calls))}

    def describe_statement(self, **kwargs: Any) -> dict[str, str]:
        return {"Id": kwargs["Id"], "Status": self.status, "Error": "deliberate test failure"}


@pytest.fixture
def release(tmp_path: Path) -> tuple[Path, dict[str, Any]]:
    with duckdb.connect() as connection:
        connection.execute("CREATE TABLE dim_carrier (carrier_sk BIGINT, name VARCHAR)")
        connection.execute("INSERT INTO dim_carrier VALUES (0, 'Unknown'), (1, 'Aster')")
        connection.execute(
            "CREATE TABLE fact_transportation_cost (cost_canonical_id VARCHAR, carrier_sk BIGINT, amount_minor BIGINT, currency VARCHAR, incurred_at TIMESTAMP)"
        )
        connection.execute(
            "INSERT INTO fact_transportation_cost VALUES ('cost-1',1,1025,'EUR','2026-01-01'),('cost-2',0,-25,'EUR','2026-01-02')"
        )
        path = export_release(
            connection,
            tmp_path / "release",
            "release_1",
            1,
            {"dim_carrier": "carrier_sk", "fact_transportation_cost": "cost_canonical_id"},
            CHECKS,
            foreign_keys={
                "fact_transportation_cost": {"carrier_sk": ("dim_carrier", "carrier_sk")}
            },
        )
    return path.parent, json.loads(path.read_text())


def test_export_is_typed_and_exact(release: tuple[Path, dict[str, Any]]) -> None:
    directory, manifest = release
    cost = manifest["tables"][1]
    assert cost["row_count"] == 2
    assert cost["amount_minor_by_currency"] == {"EUR": 1000}
    assert cost["columns"][2] == {"name": "amount_minor", "type": "BIGINT"}
    assert cost["bytes"] == (directory / cost["file"]).stat().st_size


def test_publish_and_identical_rerun_are_immutable(release: tuple[Path, dict[str, Any]]) -> None:
    directory, expected = release
    s3 = FakeS3()
    uri = upload_release(directory, "synthetic", "releases", s3)
    assert s3.writes[-1] == "releases/release_1/COMMITTED.json"
    actual, digest = read_release(uri, s3)
    assert actual == expected
    assert len(digest) == 64
    original_writes = list(s3.writes)
    assert upload_release(directory, "synthetic", "releases", s3) == uri
    assert s3.writes == original_writes


def test_corrupted_local_object_cannot_publish_marker(release: tuple[Path, dict[str, Any]]) -> None:
    directory, _ = release
    (directory / "dim_carrier.csv").write_text("wrong")
    s3 = FakeS3()
    with pytest.raises(PublicationError, match="checksum"):
        upload_release(directory, "synthetic", "releases", s3)
    assert not any("COMMITTED" in key for key in s3.writes)


def test_uploaded_corruption_and_extra_copy_file_block_load(
    release: tuple[Path, dict[str, Any]],
) -> None:
    directory, _ = release
    s3 = FakeS3()
    uri = upload_release(directory, "synthetic", "releases", s3)
    key = ("synthetic", "releases/release_1/dim_carrier.manifest.json")
    s3.objects[key] = b'{"entries": []}'
    with pytest.raises(PublicationError, match="COPY manifest"):
        read_release(uri, s3)
    key = ("synthetic", "releases/release_1/dim_carrier.csv")
    s3.objects[key] = b"tampered data"
    with pytest.raises(PublicationError, match="checksum"):
        read_release(uri, s3)


def test_same_release_with_changed_bytes_rejected(release: tuple[Path, dict[str, Any]]) -> None:
    directory, _ = release
    s3 = FakeS3()
    upload_release(directory, "synthetic", "releases", s3)
    s3.objects[("synthetic", "releases/release_1/dim_carrier.csv")] = b"already changed"
    with pytest.raises(PublicationError, match="collision"):
        upload_release(directory, "synthetic", "releases", s3)


@pytest.mark.parametrize(
    "check",
    [
        {"severity": "ERROR", "passed": False},
        {"severity": "critical", "status": "fail"},
        {"severity": "error", "status": "skipped"},
        {"severity": "Error", "status": "unknown"},
        {"severity": "ERROR", "passed": "true"},
        {"severity": "ERROR", "passed": False, "status": "pass"},
        {"severity": "ERROR", "passed": True, "status": "fail"},
        {"severity": "whatever", "status": "pass"},
        {"severity": "warning", "passed": True},
    ],
)
def test_quality_fails_closed(check: dict[str, Any], release: tuple[Path, dict[str, Any]]) -> None:
    _, manifest = release
    manifest["quality_results"] = [check]
    with pytest.raises(PublicationError):
        validate_manifest(manifest)


def test_public_and_internal_quality_schemas_accepted(release: tuple[Path, dict[str, Any]]) -> None:
    _, manifest = release
    for check in [CHECKS[0], {"severity": "error", "status": "pass"}]:
        manifest["quality_results"] = [check]
        validate_manifest(manifest)


def test_duplicate_or_null_key_rejected(tmp_path: Path) -> None:
    with duckdb.connect() as connection:
        connection.execute("CREATE TABLE dim_carrier (carrier_sk INTEGER)")
        connection.execute("INSERT INTO dim_carrier VALUES (NULL), (1), (1)")
        with pytest.raises(PublicationError, match="Null or duplicate"):
            export_release(
                connection, tmp_path / "bad", "r1", 1, {"dim_carrier": "carrier_sk"}, CHECKS
            )


@pytest.mark.parametrize(
    "type_name", ["INTEGER; DROP TABLE x", "HUGEINT", "DECIMAL(39,0)", "DECIMAL(10,20)"]
)
def test_unsafe_or_unsupported_types_fail(type_name: str) -> None:
    with pytest.raises(PublicationError):
        redshift_type(type_name)


def test_atomic_sql_guards_controls_and_merges_facts(release: tuple[Path, dict[str, Any]]) -> None:
    _, manifest = release
    create, call, drop = publication_sql(
        manifest, "a" * 64, "s3://synthetic/releases/release_1/release.json", ROLE
    )
    assert "LOCK TABLE ssot.publication_runs" in create
    assert "Release identity collision" in create
    assert "Stale release sequence" in create
    assert "Schema change requires explicit migration" in create
    assert "COPY row-count mismatch" in create
    assert "Cost reconciliation failed" in create
    assert "Unsupported or missing cost currency/amount" in create
    assert "Orphan reference" in create
    assert 'MERGE INTO ssot."fact_transportation_cost"' in create
    assert "RETURN;" in create
    assert "TRUNCATE" not in create  # TRUNCATE would commit a Redshift transaction.
    assert call.startswith("CALL ssot.publish_") and drop.startswith("DROP PROCEDURE")
    for statement in bootstrap_statements(manifest):
        assert sqlglot.parse_one(statement, dialect="redshift") is not None
    for statement in create.splitlines():
        if statement.strip().startswith(("MERGE INTO", "COPY ")):
            assert sqlglot.parse_one(statement.strip(), dialect="redshift") is not None


def test_critical_transaction_submitted_as_single_batch(
    release: tuple[Path, dict[str, Any]],
) -> None:
    directory, _ = release
    s3, data = FakeS3(), FakeData()
    uri = upload_release(directory, "synthetic", "releases", s3)
    result = load_release(uri, "workgroup", "logistics", ROLE, s3_client=s3, data_client=data)
    assert result["status"] == "FINISHED"
    assert len(data.calls[-1]["Sqls"]) == 3
    assert data.calls[-1]["Sqls"][1].startswith("CALL")
    assert "ExecutionMode" not in data.calls[-1]
    assert "ClientToken" not in data.calls[-1]  # SDK owns the per-invocation token.


def test_failed_statement_and_unknown_timeout_are_distinct() -> None:
    with pytest.raises(PublicationError, match="FAILED"):
        wait_statement(FakeData("FAILED"), "failure", poll_seconds=0)
    with pytest.raises(TimeoutError, match="outcome unknown"):
        wait_statement(FakeData("STARTED"), "uncertain", timeout=0, poll_seconds=0)


def test_amount_controls_and_unsafe_identifiers_rejected(
    release: tuple[Path, dict[str, Any]],
) -> None:
    _, manifest = release
    for controls in [None, {}, {"USD": 1000}]:
        bad = copy.deepcopy(manifest)
        bad["tables"][1]["amount_minor_by_currency"] = controls
        with pytest.raises(PublicationError):
            validate_manifest(bad)
    manifest["tables"][0]["name"] = 'x"; DROP SCHEMA ssot;--'
    with pytest.raises(PublicationError):
        validate_manifest(manifest)


def test_real_pipeline_exports_all_products_and_temporal_foreign_keys(tmp_path: Path) -> None:
    from logistics_platform.cloud import PRODUCT_KEYS, product_foreign_keys
    from logistics_platform.generator import generate
    from logistics_platform.pipeline import Pipeline, connect

    paths = generate(tmp_path / "sources", shipments=8)
    database = tmp_path / "warehouse.duckdb"
    pipeline = Pipeline(database)
    reports = [pipeline.run(path) for path in paths]
    assert all(report["status"] == "published" for report in reports)
    with connect(database) as connection:
        connection.execute("BEGIN TRANSACTION")
        links = product_foreign_keys(connection)
        assert links["fact_shipment"]["destination_region_sk"] == ("dim_region", "region_sk")
        path = export_release(
            connection,
            tmp_path / "full-release",
            "integration_2",
            2,
            PRODUCT_KEYS,
            reports[-1]["quality"],
            foreign_keys=links,
        )
        connection.execute("COMMIT")
    manifest = json.loads(path.read_text())
    assert len(manifest["tables"]) == 24
    s3, client = FakeS3(), FakeData()
    uri = upload_release(path.parent, "synthetic", "releases", s3)
    assert (
        load_release(uri, "workgroup", "logistics", ROLE, s3_client=s3, data_client=client)[
            "status"
        ]
        == "FINISHED"
    )
    assert len(client.calls[0]["Sqls"]) == 26
    for line in client.calls[-1]["Sqls"][0].splitlines():
        if line.strip().startswith(("COPY ", "MERGE INTO")):
            assert sqlglot.parse_one(line.strip(), dialect="redshift") is not None


def test_partition_writer_bounds_multiline_csv_and_idempotent_files(tmp_path: Path) -> None:
    import csv
    import hashlib

    from logistics_platform.spark_release import _write_partition

    rows = [(1, 'Quoted "carrier", with\nnewline'), (2, None), (3, "tail")]
    kwargs = dict(
        table="dim_carrier",
        column_names=["carrier_sk", "carrier_name"],
        output_uri=str(tmp_path),
        max_shard_bytes=1024,
        max_rows_per_shard=2,
    )
    files = list(_write_partition(7, rows, **kwargs))
    assert len(files) == 2
    assert [item["row_count"] for item in files] == [2, 1]
    assert list(_write_partition(7, rows, **kwargs)) == files
    restored = []
    for item in files:
        body = (tmp_path / item["file"]).read_bytes()
        assert len(body) <= 1024
        assert hashlib.sha256(body).hexdigest() == item["sha256"]
        restored.extend(list(csv.reader(io.StringIO(body.decode(), newline="")))[1:])
    assert restored[0] == ["1", rows[0][1]]
    with pytest.raises(PublicationError, match="collision"):
        list(_write_partition(7, [(1, "changed")], **kwargs))


def test_partition_writer_rejects_unrepresentable_rows(tmp_path: Path) -> None:
    from logistics_platform.cloud import _NULL
    from logistics_platform.spark_release import _write_partition

    kwargs = dict(
        table="dim_carrier",
        column_names=["carrier_sk", "carrier_name"],
        output_uri=str(tmp_path),
        max_shard_bytes=1024,
        max_rows_per_shard=2,
    )
    for bad in [_NULL, "x" * 2000, "contains\x00nul", float("inf")]:
        with pytest.raises(PublicationError):
            list(_write_partition(0, [(1, bad)], **kwargs))


def test_v2_manifest_rejects_missing_shards_and_inconsistent_controls(
    release: tuple[Path, dict[str, Any]],
) -> None:
    _, manifest = release
    manifest["contract_version"] = 2
    manifest["profile"] = "validated-spark-snapshot"
    for table in manifest["tables"]:
        table["files"] = [
            {
                "file": table["name"] + "/part-00000-00000.csv",
                "sha256": table.pop("sha256"),
                "row_count": table["row_count"],
                "bytes": table["bytes"],
            }
        ]
        table.pop("file")
    validate_manifest(manifest)
    for mutate in [
        lambda m: m["tables"][0]["files"].clear(),
        lambda m: m["tables"][0]["files"][0].update(row_count=999),
        lambda m: m["tables"][0]["files"][0].update(file="../escape.csv"),
        lambda m: m["tables"][0]["files"].append(dict(m["tables"][0]["files"][0])),
    ]:
        bad = copy.deepcopy(manifest)
        mutate(bad)
        with pytest.raises(PublicationError):
            validate_manifest(bad)


@pytest.mark.spark
def test_distributed_spark_shards_flow_through_existing_loader(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    import os
    import sys
    import time

    pytest.importorskip("pyspark")
    from pyspark.sql import SparkSession
    from pyspark.sql import functions as F

    from logistics_platform import spark_release

    original_tz = os.environ.get("TZ")
    os.environ["TZ"] = "Europe/Lisbon"
    time.tzset()
    os.environ.setdefault("SPARK_LOCAL_IP", "127.0.0.1")
    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
    spark = (
        SparkSession.builder.master("local[2]")
        .appName("spark-release-test")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", "2")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")
    monkeypatch.setattr(
        spark_release,
        "PRODUCT_KEYS",
        {"dim_carrier": "carrier_sk", "fact_transportation_cost": "cost_canonical_id"},
    )
    try:
        carriers = spark.range(16, numPartitions=4).select(
            F.col("id").alias("carrier_sk"),
            F.lit('Aster,"quoted"\nline').alias("carrier_name"),
            F.lit("1900-01-01 00:00:00").cast("timestamp").alias("effective_from"),
            F.lit("1900-01-01").cast("date").alias("reference_date"),
        )
        costs = spark.range(16, numPartitions=4).select(
            F.col("id").cast("string").alias("cost_canonical_id"),
            F.col("id").alias("carrier_sk"),
            (F.col("id") + 1).alias("amount_minor"),
            F.lit("EUR").alias("currency"),
        )
        path = Path(
            spark_release.export_spark_release(
                {"dim_carrier": carriers, "fact_transportation_cost": costs},
                str(tmp_path / "spark-release"),
                "spark_1",
                1,
                CHECKS,
                lineage={
                    "source_manifest_uri": "s3://synthetic/raw/manifest.json",
                    "source_manifest_sha256": "a" * 64,
                    "previous_evidence_uri": None,
                },
                max_rows_per_shard=2,
                max_shard_bytes=4096,
            )
        )
        manifest = json.loads(path.read_text())
        assert manifest["contract_version"] == 2
        assert manifest["lineage"]["source_manifest_sha256"] == "a" * 64
        import csv

        with (path.parent / manifest["tables"][0]["files"][0]["file"]).open(newline="") as stream:
            first_exported = list(csv.DictReader(stream))[0]
        assert first_exported["effective_from"] == "1900-01-01 00:00:00.000000"
        assert first_exported["reference_date"] == "1900-01-01"
        assert all(len(table["files"]) >= 8 for table in manifest["tables"])
        assert manifest["tables"][1]["amount_minor_by_currency"] == {"EUR": 136}
        s3, client = FakeS3(), FakeData()
        uri = upload_release(path.parent, "synthetic", "releases", s3)
        assert read_release(uri, s3)[0] == manifest
        assert (
            load_release(uri, "workgroup", "logistics", ROLE, s3_client=s3, data_client=client)[
                "status"
            ]
            == "FINISHED"
        )
        # Re-evaluate the distributed plan: deterministic partitioning preserves
        # immutable shard bytes and the completed manifest on an exact rerun.
        assert spark_release.export_spark_release(
            {"dim_carrier": carriers, "fact_transportation_cost": costs},
            str(path.parent),
            "spark_1",
            1,
            CHECKS,
            lineage=manifest["lineage"],
            max_rows_per_shard=2,
            max_shard_bytes=4096,
        ) == str(path)
        assert json.loads(path.read_text()) == manifest
        missing = copy.deepcopy(manifest)
        missing["tables"][0]["files"].pop()
        with pytest.raises(PublicationError, match="reconcile"):
            validate_manifest(missing)
    finally:
        spark.stop()
        if original_tz is None:
            os.environ.pop("TZ", None)
        else:
            os.environ["TZ"] = original_tz
        time.tzset()


def test_executor_s3_upload_checks_bytes_checksum_and_conditional_replay() -> None:
    import base64
    import hashlib

    from logistics_platform.spark_release import _s3_put_shard

    class StreamingFakeS3(FakeS3):
        def put_object(self, **kwargs: Any) -> None:
            body = kwargs["Body"].read()
            assert kwargs["ContentLength"] == len(body)
            assert (
                kwargs["ChecksumSHA256"] == base64.b64encode(hashlib.sha256(body).digest()).decode()
            )
            super().put_object(**(kwargs | {"Body": body}))

    client = StreamingFakeS3()
    body = b"id,name\n1,Aster\n"
    digest = hashlib.sha256(body).hexdigest()
    _s3_put_shard(client, "synthetic", "releases/r1/part.csv", io.BytesIO(body), digest, len(body))
    _s3_put_shard(client, "synthetic", "releases/r1/part.csv", io.BytesIO(body), digest, len(body))
    assert len(client.writes) == 1
    other = b"id,name\n1,Changed\n"
    with pytest.raises(PublicationError, match="collision"):
        _s3_put_shard(
            client,
            "synthetic",
            "releases/r1/part.csv",
            io.BytesIO(other),
            hashlib.sha256(other).hexdigest(),
            len(other),
        )


class InputS3(FakeS3):
    """Model the service checksum, including file-like upload bodies."""

    def put_object(self, **kwargs: Any) -> None:
        import base64
        import hashlib

        body = kwargs["Body"]
        if hasattr(body, "read"):
            body = body.read()
            assert kwargs["ContentLength"] == len(body)
            assert (
                kwargs["ChecksumSHA256"] == base64.b64encode(hashlib.sha256(body).digest()).decode()
            )
        super().put_object(**(kwargs | {"Body": body}))


def test_raw_inputs_upload_manifest_last_and_replay_identically(tmp_path: Path) -> None:
    from logistics_platform.cloud import upload_inputs
    from logistics_platform.generator import generate

    manifest_path = generate(tmp_path / "input", shipments=3)[0]
    source_manifest = json.loads(manifest_path.read_text())
    client = InputS3()
    uri = upload_inputs(manifest_path, "synthetic", "inputs", client)
    assert uri == "s3://synthetic/inputs/batch001/manifest.json"
    assert client.writes[-1] == "inputs/batch001/manifest.json"
    assert len(client.writes) == len(source_manifest["files"]) + 1
    before = list(client.writes)
    assert upload_inputs(manifest_path, "synthetic", "inputs", client) == uri
    assert client.writes == before
    assert (
        client.objects[("synthetic", "inputs/batch001/manifest.json")] == manifest_path.read_bytes()
    )


def test_raw_inputs_changed_bytes_never_publish_manifest(tmp_path: Path) -> None:
    from logistics_platform.cloud import upload_inputs
    from logistics_platform.generator import generate

    manifest_path = generate(tmp_path / "input", shipments=3)[0]
    manifest = json.loads(manifest_path.read_text())
    path = manifest_path.parent / manifest["files"][-1]["path"]
    path.write_bytes(path.read_bytes() + b"tampered")
    client = InputS3()
    with pytest.raises(PublicationError, match="checksum"):
        upload_inputs(manifest_path, "synthetic", "inputs", client)
    assert not any(key.endswith("manifest.json") for key in client.writes)


@pytest.mark.parametrize(
    "problem", ["traversal", "duplicate", "symlink", "missing", "controls", "version"]
)
def test_raw_inputs_reject_unsafe_paths_and_incomplete_contract(
    tmp_path: Path, problem: str
) -> None:
    from logistics_platform.cloud import upload_inputs
    from logistics_platform.generator import generate

    manifest_path = generate(tmp_path / "input", shipments=3)[0]
    manifest = json.loads(manifest_path.read_text())
    first = manifest["files"][0]
    if problem == "traversal":
        first["path"] = "../outside.jsonl"
    elif problem == "duplicate":
        manifest["files"].append(dict(first))
    elif problem == "symlink":
        outside = tmp_path / "outside.jsonl"
        outside.write_bytes((manifest_path.parent / first["path"]).read_bytes())
        (manifest_path.parent / first["path"]).unlink()
        (manifest_path.parent / first["path"]).symlink_to(outside)
    elif problem == "missing":
        (manifest_path.parent / first["path"]).unlink()
    elif problem == "version":
        first["schema_version"] = 999
    else:
        manifest["controls"][first["source"]]["files"] += 1
    manifest_path.write_text(json.dumps(manifest))
    client = InputS3()
    with pytest.raises(PublicationError):
        upload_inputs(manifest_path, "synthetic", "inputs", client)
    assert not client.writes


def test_raw_inputs_cannot_replace_existing_published_object(tmp_path: Path) -> None:
    import hashlib

    from logistics_platform.cloud import upload_inputs
    from logistics_platform.generator import generate

    manifest_path = generate(tmp_path / "input", shipments=3)[0]
    client = InputS3()
    upload_inputs(manifest_path, "synthetic", "inputs", client)
    manifest = json.loads(manifest_path.read_text())
    first = manifest["files"][0]
    path = manifest_path.parent / first["path"]
    new_body = path.read_bytes() + b"\n"
    path.write_bytes(new_body)
    first["sha256"] = hashlib.sha256(new_body).hexdigest()
    manifest_path.write_text(json.dumps(manifest))
    with pytest.raises(PublicationError, match="collision"):
        upload_inputs(manifest_path, "synthetic", "inputs", client)
    original = json.loads(client.objects[("synthetic", "inputs/batch001/manifest.json")])
    assert original["files"][0]["sha256"] != first["sha256"]


def test_raw_upload_snapshots_bytes_before_transport_to_avoid_local_race(tmp_path: Path) -> None:
    from logistics_platform.cloud import upload_inputs
    from logistics_platform.generator import generate

    manifest_path = generate(tmp_path / "input", shipments=3)[0]
    first = json.loads(manifest_path.read_text())["files"][0]
    source = manifest_path.parent / first["path"]
    original = source.read_bytes()

    class ConcurrentEditS3(InputS3):
        def put_object(self, **kwargs: Any) -> None:
            if kwargs["Key"].endswith("/" + first["path"]):
                # Simulate a producer editing the file after checksum validation
                # and before the transport consumes the uploaded stream.
                source.write_bytes(b"changed between verification and upload")
            super().put_object(**kwargs)

    client = ConcurrentEditS3()
    upload_inputs(manifest_path, "synthetic", "inputs", client)
    assert source.read_bytes() != original
    assert client.objects[("synthetic", "inputs/batch001/" + first["path"])] == original


def test_failed_warehouse_load_can_retry_same_manifest_after_repair(
    release: tuple[Path, dict[str, Any]],
) -> None:
    """A content-derived API token must not replay a cached failed execution."""

    class CachingData(FakeData):
        def __init__(self) -> None:
            super().__init__()
            self.repaired = False
            self.cached: dict[str, str] = {}
            self.statuses: dict[str, str] = {}
            self.publication_ids: list[str] = []

        def batch_execute_statement(self, **kwargs: Any) -> dict[str, str]:
            self.calls.append(kwargs)
            statement_id = str(len(self.calls))
            # The actual SDK generates a unique token when not explicitly set.
            token = kwargs.get("ClientToken", "sdk-generated-" + statement_id)
            if token in self.cached:
                return {"Id": self.cached[token]}
            self.cached[token] = statement_id
            publication = any(sql.startswith("CALL ") for sql in kwargs["Sqls"])
            self.statuses[statement_id] = (
                "FAILED" if publication and not self.repaired else "FINISHED"
            )
            if publication:
                self.publication_ids.append(statement_id)
            return {"Id": statement_id}

        def describe_statement(self, **kwargs: Any) -> dict[str, str]:
            return {
                "Id": kwargs["Id"],
                "Status": self.statuses[kwargs["Id"]],
                "Error": "temporary COPY permission failure",
            }

    directory, _ = release
    s3, client = FakeS3(), CachingData()
    uri = upload_release(directory, "synthetic", "releases", s3)
    with pytest.raises(PublicationError, match="temporary COPY permission failure"):
        load_release(uri, "workgroup", "logistics", ROLE, s3_client=s3, data_client=client)
    client.repaired = True
    result = load_release(uri, "workgroup", "logistics", ROLE, s3_client=s3, data_client=client)
    assert result["status"] == "FINISHED"
    assert len(client.publication_ids) == len(set(client.publication_ids)) == 2
