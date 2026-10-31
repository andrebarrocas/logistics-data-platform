"""Manifest-bound snapshot publication; AWS APIs are injected for offline tests.

Version 1 publishes validated local warehouse snapshots. Version 2 publishes
partitioned Spark product snapshots through the same atomic warehouse loader.
"""

from __future__ import annotations

import base64
import csv
import hashlib
import json
import re
import tempfile
import time
from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

_IDENTIFIER = re.compile(r"^[a-z][a-z0-9_]{0,62}$")
_RELEASE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_-]{0,99}$")
_HASH = re.compile(r"^[0-9a-f]{64}$")
_NULL = "__LOGISTICS_SQL_NULL_7A8F__"
_TYPE_MAP = {
    "VARCHAR": "VARCHAR(65535)",
    "UUID": "VARCHAR(36)",
    "JSON": "VARCHAR(65535)",
    "BIGINT": "BIGINT",
    "UBIGINT": "DECIMAL(20,0)",
    "INTEGER": "INTEGER",
    "SMALLINT": "SMALLINT",
    "TINYINT": "SMALLINT",
    "BOOLEAN": "BOOLEAN",
    "DATE": "DATE",
    "TIMESTAMP": "TIMESTAMP",
    "TIMESTAMP WITH TIME ZONE": "TIMESTAMPTZ",
    "DOUBLE": "DOUBLE PRECISION",
    "FLOAT": "REAL",
}

PRODUCT_KEYS = {
    "canonical_order": "canonical_id",
    "canonical_package": "canonical_id",
    "canonical_shipment": "canonical_id",
    "canonical_delivery_event": "event_canonical_id",
    "canonical_transportation_cost": "cost_canonical_id",
    "fact_shipment": "shipment_id",
    "fact_delivery_event": "event_canonical_id",
    "fact_transportation_cost": "cost_canonical_id",
    "ssot_shipments": "shipment_id",
    "ssot_delivery_events": "event_canonical_id",
    "ssot_transportation_costs": "cost_canonical_id",
    **{
        f"dim_{entity}": f"{entity}_sk"
        for entity in ["carrier", "facility", "route", "country", "region", "service_level", "date"]
    },
    **{
        f"{entity}_reference": "surrogate_key"
        for entity in ["carrier", "facility", "route", "country", "region", "service_level"]
    },
}


def product_foreign_keys(
    connection: Any, tables: Mapping[str, str] | None = None
) -> dict[str, dict[str, tuple[str, str]]]:
    """Declare all warehouse surrogate-key joins, including unknown member zero."""
    selected = tables or PRODUCT_KEYS
    result: dict[str, dict[str, tuple[str, str]]] = {}
    for table in selected:
        identifier(table)
        if not table.startswith(("fact_", "ssot_")):
            continue
        links: dict[str, tuple[str, str]] = {}
        for row in connection.execute(f'DESCRIBE SELECT * FROM "{table}"').fetchall():
            column = row[0]
            if not column.endswith("_sk"):
                continue
            for dimension in [
                "service_level",
                "facility",
                "carrier",
                "country",
                "region",
                "route",
                "date",
            ]:
                if column == f"{dimension}_sk" or column.endswith(f"_{dimension}_sk"):
                    if f"dim_{dimension}" in selected:
                        links[column] = (f"dim_{dimension}", f"{dimension}_sk")
                    break
        result[table] = links
    return result


class PublicationError(ValueError):
    """A release violated an integrity, ordering, or publication contract."""


def identifier(value: str) -> str:
    if not _IDENTIFIER.fullmatch(value):
        raise PublicationError(f"Unsafe SQL identifier: {value!r}")
    return value


def literal(value: str) -> str:
    """Quote SQL text; not a shell quoting utility."""
    if "\x00" in value:
        raise PublicationError("NUL in SQL literal")
    return "'" + value.replace("'", "''") + "'"


def redshift_type(value: str) -> str:
    if value in _TYPE_MAP:
        return _TYPE_MAP[value]
    if re.fullmatch(r"DECIMAL\(([1-9]|[12][0-9]|3[0-8]),([0-9]|[12][0-9]|3[0-8])\)", value):
        precision, scale = map(int, re.findall(r"\d+", value))
        if scale <= precision:
            return value
    raise PublicationError(f"Unsupported export type: {value}")


def _json_bytes(value: Any) -> bytes:
    return (json.dumps(value, sort_keys=True, separators=(",", ":"), default=str) + "\n").encode()


def _sha(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def _file_sha(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _s3_parts(uri: str) -> tuple[str, str]:
    parsed = urlparse(uri)
    if parsed.scheme != "s3" or not parsed.netloc or not parsed.path.lstrip("/"):
        raise PublicationError("Expected an s3://bucket/key URI")
    if parsed.query or parsed.fragment or any(c in uri for c in "\r\n\x00"):
        raise PublicationError("Invalid S3 URI")
    if not re.fullmatch(r"[a-z0-9][a-z0-9.-]+", parsed.netloc) or not re.fullmatch(
        r"[A-Za-z0-9_./-]+", parsed.path
    ):
        raise PublicationError("S3 publication paths must use safe application identifiers")
    return parsed.netloc, parsed.path.lstrip("/")


def _validate_quality(checks: Sequence[Mapping[str, Any]]) -> None:
    if not checks:
        raise PublicationError("Quality evidence is required")
    critical_count = 0
    for check in checks:
        severity = str(check.get("severity", "")).lower()
        if severity not in {"error", "critical", "warning", "warn", "info"}:
            raise PublicationError("Unknown quality severity")
        if severity in {"error", "critical"}:
            critical_count += 1
            passed = check.get("passed") is True or str(check.get("status", "")).lower() == "pass"
            # Contradictory representations also fail closed.
            if "passed" in check and check["passed"] is not True:
                passed = False
            if "status" in check and str(check["status"]).lower() != "pass":
                passed = False
            if not passed:
                raise PublicationError("Critical quality failures prohibit export")
    if not critical_count:
        raise PublicationError("At least one passing critical quality check is required")


def export_release(
    connection: Any,
    directory: Path,
    release_id: str,
    sequence: int,
    tables: Mapping[str, str],
    quality_results: Sequence[Mapping[str, Any]],
    *,
    foreign_keys: Mapping[str, Mapping[str, tuple[str, str]]] | None = None,
) -> Path:
    """Export a consistent caller-owned snapshot to deterministic typed CSV.

    The caller must hold a read transaction and supply the full product table/key
    map. The directory must be empty. Critical quality results are mandatory.
    """
    if not _RELEASE.fullmatch(release_id) or sequence < 1:
        raise PublicationError("Invalid release identity or sequence")
    if not tables or not quality_results:
        raise PublicationError("A release needs tables and quality evidence")
    _validate_quality(quality_results)
    directory.mkdir(parents=True, exist_ok=True)
    if any(directory.iterdir()):
        raise PublicationError("Export directory must be empty")
    descriptors: list[dict[str, Any]] = []
    for table, primary_key in sorted(tables.items()):
        identifier(table)
        identifier(primary_key)
        described = connection.execute(f'DESCRIBE SELECT * FROM "{table}"').fetchall()
        columns = [{"name": identifier(row[0]), "type": redshift_type(row[1])} for row in described]
        if primary_key not in {column["name"] for column in columns}:
            raise PublicationError(f"Missing primary key in {table}")
        duplicate = connection.execute(
            f'SELECT COUNT(*) - COUNT(DISTINCT "{primary_key}") FROM "{table}"'
        ).fetchone()[0]
        if duplicate:
            raise PublicationError(f"Null or duplicate primary key in {table}")
        path = directory / f"{table}.csv"
        row_count = 0
        with path.open("w", newline="", encoding="utf-8") as stream:
            writer = csv.writer(stream, lineterminator="\n")
            writer.writerow([column["name"] for column in columns])
            cursor = connection.execute(f'SELECT * FROM "{table}" ORDER BY "{primary_key}"')
            while rows := cursor.fetchmany(10000):
                for row in rows:
                    if any(value is not None and str(value) == _NULL for value in row):
                        raise PublicationError("Source value collides with reserved CSV NULL token")
                    if any(isinstance(value, str) and len(value.encode()) > 65535 for value in row):
                        raise PublicationError("Value exceeds Redshift VARCHAR byte limit")
                    writer.writerow([_NULL if value is None else value for value in row])
                row_count += len(rows)
        descriptor: dict[str, Any] = {
            "name": table,
            "primary_key": primary_key,
            "columns": columns,
            "row_count": row_count,
            "file": path.name,
            "sha256": _file_sha(path),
            "bytes": path.stat().st_size,
            "foreign_keys": dict((foreign_keys or {}).get(table, {})),
        }
        if table == "fact_transportation_cost":
            descriptor["amount_minor_by_currency"] = dict(
                connection.execute(
                    f'SELECT currency, SUM(amount_minor)::BIGINT FROM "{table}" GROUP BY currency'
                ).fetchall()
            )
        descriptors.append(descriptor)
    manifest = {
        "contract_version": 1,
        "release_id": release_id,
        "sequence": sequence,
        "profile": "validated-local-snapshot",
        "tables": descriptors,
        "quality_results": list(quality_results),
    }
    validate_manifest(manifest)
    path = directory / "release.json"
    path.write_bytes(_json_bytes(manifest))
    return path


def validate_manifest(manifest: Mapping[str, Any]) -> None:
    if type(manifest.get("contract_version")) is not int or (
        manifest.get("contract_version"),
        manifest.get("profile"),
    ) not in {
        (1, "validated-local-snapshot"),
        (2, "validated-spark-snapshot"),
    }:
        raise PublicationError("Unsupported release contract")
    if (
        not _RELEASE.fullmatch(str(manifest.get("release_id", "")))
        or type(manifest.get("sequence")) is not int
        or manifest["sequence"] < 1
    ):
        raise PublicationError("Invalid release identity")
    tables = manifest.get("tables", [])
    if not tables or len({table["name"] for table in tables}) != len(tables):
        raise PublicationError("Missing or duplicate table descriptors")
    checks = manifest.get("quality_results", [])
    _validate_quality(checks)
    table_names = {table["name"] for table in tables}
    lineage = manifest.get("lineage")
    if lineage is not None:
        allowed = {
            "source_manifest_uri",
            "source_manifest_sha256",
            "evidence_uri",
            "quality_uri",
            "previous_evidence_uri",
        }
        if (
            not isinstance(lineage, dict)
            or not set(lineage).issubset(allowed)
            or any(
                value is not None and (not isinstance(value, str) or "\x00" in value)
                for value in lineage.values()
            )
        ):
            raise PublicationError("Invalid release lineage")
        source_hash = lineage.get("source_manifest_sha256")
        if source_hash is not None and not _HASH.fullmatch(source_hash):
            raise PublicationError("Invalid source manifest hash")
    for table in tables:
        identifier(table["name"])
        identifier(table["primary_key"])
        if manifest["contract_version"] == 1:
            if table["file"] != f"{table['name']}.csv" or not _HASH.fullmatch(table["sha256"]):
                raise PublicationError("Invalid release file identity")
        else:
            files = table.get("files", [])
            if not files or len({item["file"] for item in files}) != len(files):
                raise PublicationError("Missing or duplicate release shards")
            for item in files:
                pattern = re.escape(table["name"]) + r"/part-[0-9]{5,}-[0-9]{5,}\.csv"
                if not re.fullmatch(pattern, item["file"]) or not _HASH.fullmatch(item["sha256"]):
                    raise PublicationError("Invalid release shard identity")
                if (
                    type(item["bytes"]) is not int
                    or item["bytes"] < 1
                    or type(item["row_count"]) is not int
                    or item["row_count"] < 0
                ):
                    raise PublicationError("Invalid release shard controls")
            if (
                sum(item["row_count"] for item in files) != table["row_count"]
                or sum(item["bytes"] for item in files) != table["bytes"]
            ):
                raise PublicationError("Shard controls do not reconcile to table controls")
        if (
            type(table["row_count"]) is not int
            or table["row_count"] < 0
            or type(table["bytes"]) is not int
            or table["bytes"] < 1
        ):
            raise PublicationError("Invalid release controls")
        columns = table["columns"]
        names = {column["name"] for column in columns}
        if len(names) != len(columns) or table["primary_key"] not in names:
            raise PublicationError("Invalid release column/key contract")
        for column in columns:
            identifier(column["name"])
            if column["type"] not in set(_TYPE_MAP.values()):
                redshift_type(column["type"])
        for column, (target, target_key) in table.get("foreign_keys", {}).items():
            identifier(column)
            identifier(target_key)
            if column not in names or target not in table_names:
                raise PublicationError("Invalid foreign-key contract")
            target_table = next(item for item in tables if item["name"] == target)
            if target_key not in {item["name"] for item in target_table["columns"]}:
                raise PublicationError("Invalid foreign-key target")
        if table["name"] == "fact_transportation_cost":
            controls = table.get("amount_minor_by_currency")
            if not isinstance(controls, dict) or any(
                currency != "EUR" or not isinstance(amount, int)
                for currency, amount in controls.items()
            ):
                raise PublicationError("Missing or invalid cost controls")
            if table["row_count"] > 0 and set(controls) != {"EUR"}:
                raise PublicationError("A nonempty cost fact needs EUR controls")


def release_files(table: Mapping[str, Any]) -> list[dict[str, Any]]:
    """Normalize v1 single-file and v2 partitioned table descriptors."""
    if "files" in table:
        return list(table["files"])
    return [{key: table[key] for key in ("file", "sha256", "bytes", "row_count")}]


def _put_immutable(s3: Any, bucket: str, key: str, body: bytes) -> None:
    try:
        s3.put_object(
            Bucket=bucket, Key=key, Body=body, IfNoneMatch="*", Metadata={"sha256": _sha(body)}
        )
    except Exception as exc:
        code = getattr(exc, "response", {}).get("Error", {}).get("Code")
        if code not in {"PreconditionFailed", "412"}:
            raise
        existing = s3.get_object(Bucket=bucket, Key=key)["Body"].read()
        if existing != body:
            raise PublicationError(f"Immutable object collision: {key}") from exc


def upload_inputs(manifest_path: Path, bucket: str, prefix: str, s3_client: Any) -> str:
    """Publish an immutable producer delivery, with its original manifest last.

    The base prefix receives a batch_id child. Raw records are not normalized:
    schema and business validation remain the pipeline's responsibility. Each
    source file is snapshotted into a bounded-memory spillable spool while its
    hash is checked, so concurrent local edits cannot change transmitted bytes
    between verification and upload. S3 independently checks the SHA-256.
    """
    from logistics_platform.contracts import SOURCES, integer
    from logistics_platform.contracts import validate_manifest as validate_input_manifest

    manifest_path = Path(manifest_path).resolve()
    manifest_bytes = manifest_path.read_bytes()
    manifest = json.loads(manifest_bytes)
    validate_input_manifest(manifest)
    if not _RELEASE.fullmatch(manifest["batch_id"]) or not re.fullmatch(
        r"[A-Za-z0-9_-]+(?:/[A-Za-z0-9_-]+)*", prefix
    ):
        raise PublicationError("Input publication needs safe prefix and batch identifiers")
    key_prefix = f"{prefix}/{manifest['batch_id']}"
    uri = f"s3://{bucket}/{key_prefix}/manifest.json"
    _s3_parts(uri)
    root = manifest_path.parent
    paths: list[tuple[dict[str, Any], Path]] = []
    seen: set[str] = set()
    declared = {source: {"files": 0, "rows": 0} for source in SOURCES}
    for entry in manifest["files"]:
        relative = entry["path"]
        if (
            not re.fullmatch(r"[A-Za-z0-9_.-]+(?:/[A-Za-z0-9_.-]+)*", relative)
            or any(part in {".", ".."} for part in relative.split("/"))
            or relative == "manifest.json"
            or relative in seen
        ):
            raise PublicationError("Unsafe, reserved, or duplicate input path")
        path = (root / relative).resolve()
        if not path.is_relative_to(root) or not path.is_file():
            raise PublicationError("Input path escapes the delivery or does not exist")
        version = integer(entry["schema_version"], "schema_version")
        if version not in ({1, 2} if entry["source"] == "carrier_delivery" else {1}):
            raise PublicationError("Unsupported input schema version")
        seen.add(relative)
        paths.append((entry, path))
        declared[entry["source"]]["files"] += 1
        declared[entry["source"]]["rows"] += integer(entry["rows"], "rows")
    for source, controls in declared.items():
        if any(
            value != integer(manifest["controls"][source][name], name)
            for name, value in controls.items()
        ):
            raise PublicationError(f"Source manifest control mismatch: {source}")
    for entry, path in paths:
        # A spillable immutable upload snapshot bounds RAM and prevents a local
        # hash-then-open race. The temporary file is private and never published.
        with tempfile.SpooledTemporaryFile(max_size=8 * 1024 * 1024, mode="w+b") as snapshot:
            digest = hashlib.sha256()
            size = 0
            with path.open("rb") as source_stream:
                while chunk := source_stream.read(1024 * 1024):
                    size += len(chunk)
                    if size > 5 * 1024 * 1024 * 1024:
                        raise PublicationError(
                            "Raw object exceeds single-PutObject bound; producer must shard below 5 GiB"
                        )
                    snapshot.write(chunk)
                    digest.update(chunk)
            sha = digest.hexdigest()
            if sha != entry["sha256"]:
                raise PublicationError(f"Input checksum mismatch: {entry['path']}")
            snapshot.seek(0)
            key = f"{key_prefix}/{entry['path']}"
            try:
                s3_client.put_object(
                    Bucket=bucket,
                    Key=key,
                    Body=snapshot,
                    ContentLength=size,
                    IfNoneMatch="*",
                    ChecksumSHA256=base64.b64encode(digest.digest()).decode(),
                    Metadata={"sha256": sha},
                )
            except Exception as exc:
                code = getattr(exc, "response", {}).get("Error", {}).get("Code")
                if code not in {"PreconditionFailed", "412"}:
                    raise
                existing = s3_client.get_object(Bucket=bucket, Key=key)["Body"]
                existing_digest = hashlib.sha256()
                existing_size = 0
                while chunk := existing.read(1024 * 1024):
                    existing_digest.update(chunk)
                    existing_size += len(chunk)
                if existing_digest.hexdigest() != sha or existing_size != size:
                    raise PublicationError(
                        f"Immutable input object collision: {entry['path']}"
                    ) from exc
    _put_immutable(s3_client, bucket, f"{key_prefix}/manifest.json", manifest_bytes)
    return uri


def upload_release(directory: Path, bucket: str, prefix: str, s3_client: Any) -> str:
    """Conditionally publish exact objects, then a release marker as commit boundary."""
    manifest_bytes = (directory / "release.json").read_bytes()
    manifest = json.loads(manifest_bytes)
    validate_manifest(manifest)
    if not re.fullmatch(r"releases(?:/[a-zA-Z0-9_-]+)*", prefix):
        raise PublicationError("Release prefix must be releases[/safe-path]")
    key_prefix = f"{prefix}/{manifest['release_id']}"
    for table in manifest["tables"]:
        files = release_files(table)
        for item in files:
            body = (directory / item["file"]).read_bytes()
            if len(body) != item["bytes"] or _sha(body) != item["sha256"]:
                raise PublicationError(f"Export checksum mismatch: {table['name']}")
            _put_immutable(s3_client, bucket, f"{key_prefix}/{item['file']}", body)
        copy_manifest = {
            "entries": [
                {"url": f"s3://{bucket}/{key_prefix}/{item['file']}", "mandatory": True}
                for item in files
            ]
        }
        _put_immutable(
            s3_client,
            bucket,
            f"{key_prefix}/{table['name']}.manifest.json",
            _json_bytes(copy_manifest),
        )
    _put_immutable(s3_client, bucket, f"{key_prefix}/release.json", manifest_bytes)
    _put_immutable(
        s3_client,
        bucket,
        f"{key_prefix}/COMMITTED.json",
        _json_bytes({"sha256": _sha(manifest_bytes)}),
    )
    return f"s3://{bucket}/{key_prefix}/release.json"


def read_release(uri: str, s3_client: Any) -> tuple[dict[str, Any], str]:
    """Verify ready marker, all data, and COPY manifests before submitting SQL."""
    bucket, key = _s3_parts(uri)
    if not key.endswith("/release.json"):
        raise PublicationError("Expected a release.json object")
    prefix = key.rsplit("/", 1)[0]
    body = s3_client.get_object(Bucket=bucket, Key=key)["Body"].read()
    digest = _sha(body)
    marker = json.loads(
        s3_client.get_object(Bucket=bucket, Key=f"{prefix}/COMMITTED.json")["Body"].read()
    )
    if marker != {"sha256": digest}:
        raise PublicationError("Release marker checksum mismatch")
    manifest: dict[str, Any] = json.loads(body)
    validate_manifest(manifest)
    for table in manifest["tables"]:
        files = release_files(table)
        for item in files:
            response = s3_client.get_object(Bucket=bucket, Key=f"{prefix}/{item['file']}")
            stream = response["Body"]
            sha = hashlib.sha256()
            length = 0
            while chunk := stream.read(1024 * 1024):
                sha.update(chunk)
                length += len(chunk)
            if sha.hexdigest() != item["sha256"] or length != item["bytes"]:
                raise PublicationError(f"Published data checksum mismatch: {table['name']}")
        copied = json.loads(
            s3_client.get_object(Bucket=bucket, Key=f"{prefix}/{table['name']}.manifest.json")[
                "Body"
            ].read()
        )
        expected = {
            "entries": [
                {"url": f"s3://{bucket}/{prefix}/{item['file']}", "mandatory": True}
                for item in files
            ]
        }
        if copied != expected:
            raise PublicationError("COPY manifest does not match release object identity")
    return manifest, digest


def bootstrap_statements(manifest: Mapping[str, Any]) -> list[str]:
    validate_manifest(manifest)
    statements = [
        "CREATE SCHEMA IF NOT EXISTS ssot",
        "CREATE TABLE IF NOT EXISTS ssot.publication_runs (release_id VARCHAR(100) NOT NULL, manifest_sha256 VARCHAR(64) NOT NULL, sequence BIGINT NOT NULL, schema_sha256 VARCHAR(64) NOT NULL, committed_at TIMESTAMP DEFAULT GETDATE()) DISTSTYLE ALL",
    ]
    for table in manifest["tables"]:
        columns = ", ".join(f'"{column["name"]}" {column["type"]}' for column in table["columns"])
        statements.append(
            f'CREATE TABLE IF NOT EXISTS ssot."{table["name"]}" ({columns}) DISTSTYLE AUTO'
        )
    return statements


def publication_sql(
    manifest: Mapping[str, Any], manifest_sha: str, release_uri: str, copy_role_arn: str
) -> tuple[str, str, str]:
    """An atomic stored procedure avoids Data API's 40-statement batch limit.

    Fact MERGE source uniqueness, counts, cost controls, and FKs are checked inside
    the same transaction. The durable ledger protects reruns beyond API tokens.
    """
    validate_manifest(manifest)
    if not _HASH.fullmatch(manifest_sha) or not re.fullmatch(
        r"arn:aws(?:-us-gov|-cn)?:iam::[0-9]{12}:role/[A-Za-z0-9+=,.@_/-]+", copy_role_arn
    ):
        raise PublicationError("Invalid manifest hash or COPY role ARN")
    bucket, key = _s3_parts(release_uri)
    prefix = key.rsplit("/", 1)[0]
    schema_hash = _sha(
        _json_bytes(
            [
                {
                    **{key: table[key] for key in ["name", "primary_key", "columns"]},
                    "foreign_keys": table.get("foreign_keys", {}),
                }
                for table in manifest["tables"]
            ]
        )
    )
    name = "publish_" + manifest_sha[:24]
    release = literal(manifest["release_id"])
    sequence = manifest["sequence"]
    lines = [
        "LOCK TABLE ssot.publication_runs;",
        f"SELECT COUNT(*) INTO n FROM ssot.publication_runs WHERE release_id = {release};",
        "IF n > 0 THEN",
        f"  SELECT COUNT(*) INTO n FROM ssot.publication_runs WHERE release_id = {release} AND manifest_sha256 = {literal(manifest_sha)};",
        "  IF n <> 1 THEN RAISE EXCEPTION 'Release identity collision'; END IF;",
        "  RETURN;",
        "END IF;",
        f"SELECT COUNT(*) INTO n FROM ssot.publication_runs WHERE sequence >= {sequence};",
        "IF n > 0 THEN RAISE EXCEPTION 'Stale release sequence'; END IF;",
        f"SELECT COUNT(*) INTO n FROM ssot.publication_runs WHERE schema_sha256 <> {literal(schema_hash)};",
        "IF n > 0 THEN RAISE EXCEPTION 'Schema change requires explicit migration'; END IF;",
    ]
    for table in manifest["tables"]:
        table_name, pk = table["name"], table["primary_key"]
        staging = f"stage_{table_name}"
        identifier(staging)
        lines.extend(
            [
                f'CREATE TEMP TABLE "{staging}" (LIKE ssot."{table_name}");',
                f"COPY \"{staging}\" FROM {literal(f's3://{bucket}/{prefix}/{table_name}.manifest.json')} IAM_ROLE {literal(copy_role_arn)} MANIFEST CSV IGNOREHEADER 1 NULL AS {literal(_NULL)} TIMEFORMAT 'auto' DATEFORMAT 'auto';",
                f'SELECT COUNT(*) INTO n FROM "{staging}";',
                f"IF n <> {table['row_count']} THEN RAISE EXCEPTION 'COPY row-count mismatch: {table_name}'; END IF;",
                f'SELECT COUNT(*) - COUNT(DISTINCT "{pk}") INTO n FROM "{staging}";',
                f"IF n <> 0 THEN RAISE EXCEPTION 'Null/duplicate key: {table_name}'; END IF;",
            ]
        )
        for currency, amount in table.get("amount_minor_by_currency", {}).items():
            if currency != "EUR" or not isinstance(amount, int):
                raise PublicationError("Invalid monetary control")
            lines.extend(
                [
                    f'SELECT COALESCE(SUM(amount_minor),0) INTO n FROM "{staging}" WHERE currency = {literal(currency)};',
                    f"IF n <> {amount} THEN RAISE EXCEPTION 'Cost reconciliation failed'; END IF;",
                ]
            )
        if table_name == "fact_transportation_cost":
            lines.extend(
                [
                    f"SELECT COUNT(*) INTO n FROM \"{staging}\" WHERE currency IS NULL OR currency <> 'EUR' OR amount_minor IS NULL;",
                    "IF n <> 0 THEN RAISE EXCEPTION 'Unsupported or missing cost currency/amount'; END IF;",
                ]
            )
    for table in manifest["tables"]:
        for column, (target, target_key) in table.get("foreign_keys", {}).items():
            lines.extend(
                [
                    f'SELECT COUNT(*) INTO n FROM "stage_{table["name"]}" f LEFT JOIN "stage_{target}" d ON f."{column}" = d."{target_key}" WHERE f."{column}" IS NOT NULL AND d."{target_key}" IS NULL;',
                    f"IF n <> 0 THEN RAISE EXCEPTION 'Orphan reference: {table['name']}.{column}'; END IF;",
                ]
            )
    for table in manifest["tables"]:
        table_name, pk = table["name"], table["primary_key"]
        staging = f'"stage_{table_name}"'
        target = f'ssot."{table_name}"'
        columns = [f'"{column["name"]}"' for column in table["columns"]]
        if table_name.startswith("fact_"):
            updates = ", ".join(
                f"{column} = s.{column}" for column in columns if column != f'"{pk}"'
            )
            lines.append(
                f'MERGE INTO {target} USING {staging} AS s ON {target}."{pk}" = s."{pk}" WHEN MATCHED THEN UPDATE SET {updates} WHEN NOT MATCHED THEN INSERT ({", ".join(columns)}) VALUES ({", ".join("s." + column for column in columns)});'
            )
            lines.append(
                f'DELETE FROM {target} WHERE "{pk}" NOT IN (SELECT "{pk}" FROM {staging});'
            )
        else:
            # A validated full timeline snapshot includes backdated corrections.
            lines.extend(
                [f"DELETE FROM {target};", f"INSERT INTO {target} SELECT * FROM {staging};"]
            )
    lines.append(
        f"INSERT INTO ssot.publication_runs (release_id,manifest_sha256,sequence,schema_sha256) VALUES ({release},{literal(manifest_sha)},{sequence},{literal(schema_hash)});"
    )
    body = "\n  ".join(lines)
    create = f"CREATE OR REPLACE PROCEDURE ssot.{name}() AS $$\nDECLARE n BIGINT;\nBEGIN\n  {body}\nEND;\n$$ LANGUAGE plpgsql"
    return create, f"CALL ssot.{name}()", f"DROP PROCEDURE ssot.{name}()"


def wait_statement(
    client: Any, statement_id: str, *, timeout: float = 900, poll_seconds: float = 1
) -> dict[str, Any]:
    deadline = time.monotonic() + timeout
    while True:
        result: dict[str, Any] = client.describe_statement(Id=statement_id)
        if result["Status"] == "FINISHED":
            return result
        if result["Status"] in {"FAILED", "ABORTED"}:
            raise PublicationError(
                f"Redshift {result['Status']}: {result.get('Error', 'unknown error')} (statement {statement_id})"
            )
        if time.monotonic() >= deadline:
            # The server may still commit. Never silently retry an unknown outcome.
            raise TimeoutError(
                f"Redshift outcome unknown; inspect statement {statement_id} and publication_runs before retry"
            )
        time.sleep(poll_seconds)


def load_release(
    release_uri: str,
    workgroup: str,
    database: str,
    copy_role_arn: str,
    *,
    s3_client: Any,
    data_client: Any,
) -> dict[str, Any]:
    manifest, digest = read_release(release_uri, s3_client)
    bootstrap = bootstrap_statements(manifest)
    for offset in range(0, len(bootstrap), 40):
        response = data_client.batch_execute_statement(
            WorkgroupName=workgroup, Database=database, Sqls=bootstrap[offset : offset + 40]
        )
        wait_statement(data_client, response["Id"])
    create, call, drop = publication_sql(manifest, digest, release_uri, copy_role_arn)
    # Batch default is one transaction. Never set AUTO_COMMIT. Let the SDK
    # generate its per-invocation retry token: a manifest-derived token would
    # replay a failed execution for hours after an operator repaired its cause.
    # The locked durable ledger provides idempotency across fresh invocations.
    response = data_client.batch_execute_statement(
        WorkgroupName=workgroup,
        Database=database,
        Sqls=[create, call, drop],
    )
    result = wait_statement(data_client, response["Id"])
    return {
        "release_id": manifest["release_id"],
        "manifest_sha256": digest,
        "statement_id": response["Id"],
        "status": result["Status"],
    }
