"""Distributed bounded CSV shards for the manifest-driven Redshift publisher.

Executors write immutable shards and return only file controls. The driver never
collects product rows. Every shard has its own header and exact row/hash controls.
The local filesystem target is for local Spark integration tests; a distributed
Glue job must use S3 so all executor outputs share the same object namespace.
"""

from __future__ import annotations

import base64
import csv
import hashlib
import io
import math
import os
import re
import tempfile
from collections.abc import Iterable, Iterator, Mapping, Sequence
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

from logistics_platform.cloud import (
    _NULL,
    _RELEASE,
    PRODUCT_KEYS,
    PublicationError,
    _json_bytes,
    _put_immutable,
    _s3_parts,
    _sha,
    _validate_quality,
    identifier,
    redshift_type,
    validate_manifest,
)


def spark_redshift_type(data_type: Any) -> str:
    """Reject complex types rather than inventing an implicit warehouse encoding."""
    name = data_type.typeName()
    types = {
        "string": "VARCHAR",
        "long": "BIGINT",
        "integer": "INTEGER",
        "short": "SMALLINT",
        "byte": "TINYINT",
        "boolean": "BOOLEAN",
        "date": "DATE",
        "timestamp": "TIMESTAMP",
        "timestamp_ntz": "TIMESTAMP",
        "double": "DOUBLE",
        "float": "FLOAT",
    }
    if name == "decimal":
        return redshift_type(f"DECIMAL({data_type.precision},{data_type.scale})")
    if name not in types:
        raise PublicationError(f"Unsupported Spark product type: {data_type.simpleString()}")
    return redshift_type(types[name])


def _local_directory(uri: str) -> Path:
    parsed = urlparse(uri)
    if parsed.scheme == "file" and parsed.netloc in {"", "localhost"}:
        return Path(parsed.path).resolve()
    if parsed.scheme:
        raise PublicationError("Product output must be local or s3://")
    return Path(uri).resolve()


def _stream_controls(stream: Any) -> tuple[str, int]:
    digest = hashlib.sha256()
    size = 0
    while chunk := stream.read(1024 * 1024):
        digest.update(chunk)
        size += len(chunk)
    return digest.hexdigest(), size


def _local_put_immutable(path: Path, stream: Any, sha: str, size: int) -> None:
    """Link a fully written temporary file atomically; no partially visible shard."""
    path.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile(
        dir=path.parent, prefix=".uncommitted-", mode="w+b"
    ) as temporary:
        stream.seek(0)
        while chunk := stream.read(1024 * 1024):
            temporary.write(chunk)
        temporary.flush()
        os.fsync(temporary.fileno())
        try:
            os.link(temporary.name, path)
        except FileExistsError as exc:
            with path.open("rb") as existing:
                actual = _stream_controls(existing)
            if actual != (sha, size):
                raise PublicationError(f"Immutable local shard collision: {path.name}") from exc


def _s3_put_shard(client: Any, bucket: str, key: str, stream: Any, sha: str, size: int) -> None:
    stream.seek(0)
    try:
        client.put_object(
            Bucket=bucket,
            Key=key,
            Body=stream,
            ContentLength=size,
            IfNoneMatch="*",
            ChecksumSHA256=base64.b64encode(bytes.fromhex(sha)).decode(),
            Metadata={"sha256": sha},
        )
    except Exception as exc:
        if getattr(exc, "response", {}).get("Error", {}).get("Code") not in {
            "412",
            "PreconditionFailed",
        }:
            raise
        existing = client.get_object(Bucket=bucket, Key=key)["Body"]
        if _stream_controls(existing) != (sha, size):
            raise PublicationError(f"Immutable S3 shard collision: {key}") from exc


def _csv_bytes(values: Sequence[Any]) -> bytes:
    with io.StringIO(newline="") as stream:
        csv.writer(stream, lineterminator="\n").writerow(values)
        return stream.getvalue().encode("utf-8")


def _write_partition(
    partition: int,
    rows: Iterable[Any],
    *,
    table: str,
    column_names: list[str],
    output_uri: str,
    max_shard_bytes: int,
    max_rows_per_shard: int,
) -> Iterator[dict[str, Any]]:
    """Bound memory with a spillable file; conditional uploads tolerate retries."""
    client = None
    bucket = prefix = ""
    directory = None
    if output_uri.startswith("s3://"):
        import boto3

        bucket, prefix = _s3_parts(output_uri)
        client = boto3.client("s3")
    else:
        directory = _local_directory(output_uri)
    header = _csv_bytes(column_names)
    if len(header) > max_shard_bytes:
        raise PublicationError("CSV header exceeds the configured shard-byte bound")
    shard_number = 0
    row_count = 0
    size = len(header)
    digest = hashlib.sha256(header)
    spool = tempfile.SpooledTemporaryFile(
        max_size=min(max_shard_bytes, 8 * 1024 * 1024), mode="w+b"
    )
    spool.write(header)

    def finish() -> dict[str, Any]:
        filename = f"{table}/part-{partition:05d}-{shard_number:05d}.csv"
        sha = digest.hexdigest()
        if directory is not None:
            _local_put_immutable(directory / filename, spool, sha, size)
        else:
            _s3_put_shard(client, bucket, f"{prefix}/{filename}", spool, sha, size)
        return {"file": filename, "sha256": sha, "bytes": size, "row_count": row_count}

    try:
        for row in rows:
            values = list(row)
            for value in values:
                if isinstance(value, str) and (
                    value == _NULL or len(value.encode()) > 65535 or "\x00" in value
                ):
                    raise PublicationError("String violates Redshift byte/null-token contract")
                if isinstance(value, float) and not math.isfinite(value):
                    raise PublicationError("Non-finite product measure")
            encoded = _csv_bytes([_NULL if value is None else value for value in values])
            if len(encoded) >= 4 * 1024 * 1024:
                raise PublicationError("CSV row exceeds the Redshift COPY row-size limit")
            if len(header) + len(encoded) > max_shard_bytes:
                raise PublicationError("A row exceeds the configured shard-byte bound")
            if row_count and (
                size + len(encoded) > max_shard_bytes or row_count >= max_rows_per_shard
            ):
                yield finish()
                spool.close()
                shard_number += 1
                row_count = 0
                size = len(header)
                digest = hashlib.sha256(header)
                spool = tempfile.SpooledTemporaryFile(
                    max_size=min(max_shard_bytes, 8 * 1024 * 1024), mode="w+b"
                )
                spool.write(header)
            spool.write(encoded)
            digest.update(encoded)
            size += len(encoded)
            row_count += 1
        yield finish()  # Empty partitions produce a valid header-only COPY shard.
    finally:
        spool.close()


def _frame_links(table: str, columns: Sequence[str]) -> dict[str, tuple[str, str]]:
    links = {}
    if table.startswith(("fact_", "ssot_")):
        for column in columns:
            for entity in [
                "service_level",
                "facility",
                "carrier",
                "country",
                "region",
                "route",
                "date",
            ]:
                if column == f"{entity}_sk" or column.endswith(f"_{entity}_sk"):
                    links[column] = (f"dim_{entity}", f"{entity}_sk")
                    break
    return links


def export_spark_release(
    frames: Mapping[str, Any],
    output_uri: str,
    release_id: str,
    sequence: int,
    quality_results: Sequence[Mapping[str, Any]],
    s3_client: Any = None,
    *,
    lineage: Mapping[str, Any] | None = None,
    max_shard_bytes: int = 64 * 1024 * 1024,
    max_rows_per_shard: int = 100_000,
) -> str:
    """Export all 24 products, then publish a v2 manifest and ready marker.

    ``s3_client`` is used by the driver for metadata only. Executors initialize
    their own role-authenticated boto3 clients, so no credentials are serialized.
    Unknown helper frames are ignored; missing required products are an error.
    Hash repartitioning by the primary key derives its count from total rows and
    the configured row bound, followed by per-partition sorting. Exact reruns use
    the same data, Spark version, and shard configuration. Changed data under an
    existing release identity fails instead of overwriting evidence.
    """
    from pyspark import StorageLevel
    from pyspark.sql import functions as F

    _validate_quality(quality_results)
    if not _RELEASE.fullmatch(release_id) or type(sequence) is not int or sequence < 1:
        raise PublicationError("Invalid Spark release identity")
    if max_shard_bytes < 1024 or max_shard_bytes > 256 * 1024 * 1024 or max_rows_per_shard < 1:
        raise PublicationError("Use shard bounds between 1 KiB and 256 MiB and positive row count")
    missing = set(PRODUCT_KEYS) - set(frames)
    if missing:
        raise PublicationError(f"Missing required Spark product frames: {sorted(missing)}")
    if output_uri.startswith("s3://"):
        bucket, prefix = _s3_parts(output_uri.rstrip("/"))
        if (
            not re.fullmatch(r"releases(?:/[a-zA-Z0-9_-]+)+", prefix)
            or prefix.rsplit("/", 1)[-1] != release_id
        ):
            raise PublicationError("S3 output must be releases/.../<release_id>")
        output_uri = f"s3://{bucket}/{prefix}"
        if s3_client is None:
            import boto3

            s3_client = boto3.client("s3")
    else:
        output_uri = str(_local_directory(output_uri))
    descriptors: list[dict[str, Any]] = []
    for table, primary_key in sorted(PRODUCT_KEYS.items()):
        frame = frames[table]
        frame.sparkSession.conf.set("spark.sql.session.timeZone", "UTC")
        columns = [
            {"name": identifier(field.name), "type": spark_redshift_type(field.dataType)}
            for field in frame.schema.fields
        ]
        if primary_key not in frame.columns:
            raise PublicationError(f"Missing Spark primary key: {table}.{primary_key}")
        # Persist only one export table at a time; observations are never collected.
        already_cached = frame.is_cached
        if not already_cached:
            frame = frame.persist(StorageLevel.MEMORY_AND_DISK)
        try:
            controls = frame.agg(
                F.count(F.lit(1)).alias("rows"), F.countDistinct(primary_key).alias("keys")
            ).first()
            row_count = int(controls["rows"])
            if row_count != int(controls["keys"]):
                raise PublicationError(f"Null or duplicate Spark product key: {table}")
            prepared = frame.select(
                *[
                    F.date_format(F.col(field.name), "yyyy-MM-dd HH:mm:ss.SSSSSS").alias(field.name)
                    if field.dataType.typeName() in {"timestamp", "timestamp_ntz"}
                    else F.date_format(F.col(field.name), "yyyy-MM-dd").alias(field.name)
                    if field.dataType.typeName() == "date"
                    else F.col(field.name)
                    for field in frame.schema.fields
                ]
            )
            partition_count = max(1, (row_count + max_rows_per_shard - 1) // max_rows_per_shard)
            prepared = prepared.repartition(
                partition_count, F.col(primary_key)
            ).sortWithinPartitions(primary_key)
            # Only O(number of shards) metadata crosses to the driver.
            files = prepared.rdd.mapPartitionsWithIndex(
                lambda partition, rows: _write_partition(
                    partition,
                    rows,
                    table=table,
                    column_names=[column["name"] for column in columns],
                    output_uri=output_uri,
                    max_shard_bytes=max_shard_bytes,
                    max_rows_per_shard=max_rows_per_shard,
                )
            ).collect()
            if not files:
                files = list(
                    _write_partition(
                        0,
                        [],
                        table=table,
                        column_names=[column["name"] for column in columns],
                        output_uri=output_uri,
                        max_shard_bytes=max_shard_bytes,
                        max_rows_per_shard=max_rows_per_shard,
                    )
                )
            files.sort(key=lambda item: item["file"])
            if sum(item["row_count"] for item in files) != row_count:
                raise PublicationError(f"Spark export reconciliation failed: {table}")
            descriptor: dict[str, Any] = {
                "name": table,
                "primary_key": primary_key,
                "columns": columns,
                "row_count": row_count,
                "bytes": sum(item["bytes"] for item in files),
                "files": files,
                "foreign_keys": _frame_links(table, frame.columns),
            }
            if table == "fact_transportation_cost":
                invalid = (
                    frame.where(
                        F.col("currency").isNull()
                        | (F.col("currency") != "EUR")
                        | F.col("amount_minor").isNull()
                    )
                    .limit(1)
                    .count()
                )
                if invalid:
                    raise PublicationError("Unsupported or missing Spark cost currency/amount")
                # At most one currency exists after the EUR check.
                money = (
                    frame.groupBy("currency").agg(F.sum("amount_minor").alias("total")).collect()
                )
                descriptor["amount_minor_by_currency"] = {
                    row["currency"]: int(row["total"]) for row in money
                }
            descriptors.append(descriptor)
        finally:
            if not already_cached:
                frame.unpersist()
    manifest = {
        "contract_version": 2,
        "profile": "validated-spark-snapshot",
        "release_id": release_id,
        "sequence": sequence,
        "tables": descriptors,
        "quality_results": list(quality_results),
    }
    if lineage is not None:
        manifest["lineage"] = dict(lineage)
    validate_manifest(manifest)
    body = _json_bytes(manifest)
    if output_uri.startswith("s3://"):
        bucket, prefix = _s3_parts(output_uri)
        for descriptor_item in descriptors:
            copy_manifest = {
                "entries": [
                    {"url": f"{output_uri}/{item['file']}", "mandatory": True}
                    for item in descriptor_item["files"]
                ]
            }
            _put_immutable(
                s3_client,
                bucket,
                f"{prefix}/{descriptor_item['name']}.manifest.json",
                _json_bytes(copy_manifest),
            )
        _put_immutable(s3_client, bucket, f"{prefix}/release.json", body)
        _put_immutable(
            s3_client, bucket, f"{prefix}/COMMITTED.json", _json_bytes({"sha256": _sha(body)})
        )
        return f"{output_uri}/release.json"
    directory = Path(output_uri)
    _local_put_immutable(directory / "release.json", io.BytesIO(body), _sha(body), len(body))
    marker = _json_bytes({"sha256": _sha(body)})
    _local_put_immutable(
        directory / "COMMITTED.json", io.BytesIO(marker), _sha(marker), len(marker)
    )
    return str(directory / "release.json")
