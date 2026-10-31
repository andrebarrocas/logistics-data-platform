"""Glue 5.0 ingestion, evidence preparation, and optional product release build.

The job may create a quality-gated immutable snapshot. Warehouse visibility still
requires the separate transactional Redshift publisher.
"""

from __future__ import annotations

import argparse
import hashlib
import json
from dataclasses import asdict
from functools import reduce
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql import types as T

from logistics_platform.contracts import (
    CARRIER_COLUMNS,
    COST_COLUMNS,
    REFERENCE_TYPES,
    SOURCES,
    ContractError,
    compact,
    normalize,
    validate_manifest,
)
from logistics_platform.keys import canonical_id

EVIDENCE_SCHEMA = T.StructType(
    [
        T.StructField(name, T.LongType() if name == "revision" else T.StringType(), True)
        for name in [
            "source_system",
            "entity_type",
            "source_id",
            "business_key",
            "revision",
            "operation",
            "effective_at",
            "updated_at",
            "attributes",
            "payload_hash",
            "raw_hash",
            "raw_record",
            "error_rule",
            "error_message",
        ]
    ]
)


def normalize_json(raw: str, source: str, version: int, raw_hash: str) -> dict[str, Any]:
    """Executor-safe shared adapter; errors become structured quarantine rows."""
    try:
        record = normalize(source, json.loads(raw), version)
        result = asdict(record)
        result["attributes"] = compact(record.attributes)
        result.update(
            payload_hash=record.payload_hash,
            raw_hash=raw_hash,
            raw_record=raw,
            error_rule=None,
            error_message=None,
        )
        return result
    except (ContractError, ValueError, TypeError, KeyError) as exc:
        return {
            "source_system": source,
            "raw_hash": raw_hash,
            "raw_record": raw,
            "error_rule": exc.rule if isinstance(exc, ContractError) else "ING_SCHEMA",
            "error_message": str(exc),
        }


def prepare_evidence(frame: DataFrame) -> dict[str, DataFrame]:
    """Use windows and grouped candidate sets; ambiguous aliases never pick a row."""
    good = frame.filter(F.col("error_rule").isNull())
    identity = ["source_system", "entity_type", "source_id", "revision"]
    conflicts = (
        good.groupBy(*identity)
        .agg(F.countDistinct("payload_hash").alias("payloads"))
        .filter("payloads > 1")
    )
    # Hash + evidence key are deterministic; physical receipts stay in the source manifest.
    dedup_window = Window.partitionBy(*identity, "payload_hash").orderBy("raw_hash", "raw_record")
    evidence = (
        good.withColumn("_rank", F.row_number().over(dedup_window)).filter("_rank=1").drop("_rank")
    )
    changed_keys = (
        evidence.groupBy("source_system", "entity_type", "source_id")
        .agg(F.countDistinct("business_key").alias("keys"))
        .filter("keys > 1")
    )
    scoped = evidence.withColumn(
        "_boundary",
        F.when(
            F.col("entity_type").isin(sorted(REFERENCE_TYPES | {"alias"})), F.col("effective_at")
        ).otherwise(F.lit("")),
    )
    revision_window = Window.partitionBy(
        "source_system", "entity_type", "source_id", "_boundary"
    ).orderBy(F.desc("revision"), F.desc("updated_at"), "payload_hash")
    current = (
        scoped.withColumn("_rank", F.row_number().over(revision_window))
        .filter("_rank=1")
        .drop("_rank", "_boundary")
    )
    cid = F.udf(canonical_id, T.StringType())
    current = current.withColumn(
        "canonical_id", F.when(F.col("entity_type") != "alias", cid("entity_type", "business_key"))
    )
    alias_window = Window.partitionBy("source_system", "source_id").orderBy("effective_at")
    alias_timeline = current.filter("entity_type='alias'").withColumn(
        "_next_boundary", F.lead("effective_at").over(alias_window)
    )
    aliases = alias_timeline.filter("operation='upsert'").select(
        F.get_json_object("attributes", "$.target_type").alias("target_type"),
        F.get_json_object("attributes", "$.source").alias("alias_source"),
        F.upper(F.trim(F.get_json_object("attributes", "$.alias"))).alias("alias_value"),
        F.get_json_object("attributes", "$.target_key").alias("target_key"),
        F.least(F.get_json_object("attributes", "$.effective_to"), F.col("_next_boundary")).alias(
            "effective_to"
        ),
        F.get_json_object("attributes", "$.rule_id").alias("rule_id"),
        F.col("effective_at").alias("effective_from"),
        F.col("source_id").alias("alias_id"),
    )
    left, right = aliases.alias("l"), aliases.alias("r")
    alias_conflicts = left.join(
        right,
        (
            F.struct(F.col("l.alias_id"), F.col("l.effective_from"))
            < F.struct(F.col("r.alias_id"), F.col("r.effective_from"))
        )
        & (F.col("l.target_type") == F.col("r.target_type"))
        & (F.col("l.alias_source") == F.col("r.alias_source"))
        & (F.col("l.alias_value") == F.col("r.alias_value"))
        & (F.col("l.target_key") != F.col("r.target_key"))
        & (F.col("l.effective_from") < F.col("r.effective_to"))
        & (F.col("r.effective_from") < F.col("l.effective_to")),
        "inner",
    ).select("l.*")
    observations = current.filter(
        "entity_type IN ('shipment','event','cost') AND operation='upsert'"
    ).select(
        "source_system",
        "entity_type",
        "source_id",
        F.when(F.col("entity_type") == "shipment", F.get_json_object("attributes", "$.created_at"))
        .otherwise(F.col("effective_at"))
        .alias("effective_at"),
        "payload_hash",
        F.upper(F.trim(F.get_json_object("attributes", "$.carrier_alias"))).alias("carrier_alias"),
    )
    o, a = observations.alias("o"), aliases.filter("target_type='carrier'").alias("a")
    matches = o.join(
        F.broadcast(a),
        (F.col("o.source_system") == F.col("a.alias_source"))
        & (F.col("o.carrier_alias") == F.col("a.alias_value"))
        & (F.col("o.effective_at") >= F.col("a.effective_from"))
        & (F.col("o.effective_at") < F.col("a.effective_to")),
        "left",
    )
    decisions = matches.groupBy(
        "o.source_system", "o.entity_type", "o.source_id", "o.effective_at", "o.payload_hash"
    ).agg(
        F.sort_array(F.collect_set("a.target_key")).alias("candidates"),
        F.sort_array(F.collect_set("a.rule_id")).alias("rules"),
    )
    decisions = decisions.withColumn(
        "outcome",
        F.when(F.size("candidates") == 1, "resolved")
        .when(F.size("candidates") > 1, "ambiguous")
        .otherwise("unresolved"),
    )
    # No call to identity generation is made for an ambiguous candidate set.
    safe_id = F.udf(
        lambda targets: canonical_id("carrier", targets[0]) if len(targets) == 1 else None,
        T.StringType(),
    )
    decisions = decisions.withColumn("carrier_id", safe_id("candidates"))
    return {
        "aliases": aliases,
        "evidence": evidence,
        "current": current,
        "resolution_decisions": decisions,
        "quarantine": frame.filter(F.col("error_rule").isNotNull()),
        "version_conflicts": conflicts,
        "identity_conflicts": changed_keys,
        "alias_conflicts": alias_conflicts,
    }


def _read_bytes(uri: str, s3: Any) -> bytes:
    if uri.startswith("s3://"):
        part = urlparse(uri)
        return s3.get_object(Bucket=part.netloc, Key=part.path.lstrip("/"))["Body"].read()
    return Path(uri).read_bytes()


def _verify_sha(uri: str, expected: str, s3: Any) -> None:
    sha = hashlib.sha256()
    if uri.startswith("s3://"):
        part = urlparse(uri)
        stream = s3.get_object(Bucket=part.netloc, Key=part.path.lstrip("/"))["Body"]
    else:
        stream = Path(uri).open("rb")
    try:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            sha.update(chunk)
    finally:
        stream.close()
    if sha.hexdigest() != expected:
        raise ContractError("ING_CHECKSUM", f"Checksum mismatch for {uri}")


def _history_report(history_uri: str, s3: Any) -> dict[str, Any]:
    """A directory of Parquet files alone is not accepted prior state."""
    prefix = history_uri.rstrip("/").rsplit("/", 1)[0] + "/quality/"
    if prefix.startswith("s3://"):
        part = urlparse(prefix)
        objects = s3.list_objects_v2(Bucket=part.netloc, Prefix=part.path.lstrip("/"))
        matches = [
            item["Key"]
            for item in objects.get("Contents", [])
            if item["Key"].rsplit("/", 1)[-1].startswith("part-")
        ]
        if len(matches) != 1:
            raise ContractError(
                "INC_CHECKPOINT", "Prior Spark state must have exactly one completed quality report"
            )
        content = _read_bytes(f"s3://{part.netloc}/{matches[0]}", s3)
    else:
        matches = list(Path(prefix).glob("part-*"))
        if len(matches) != 1:
            raise ContractError(
                "INC_CHECKPOINT", "Prior Spark state must have exactly one completed quality report"
            )
        content = matches[0].read_bytes()
    return json.loads(content)


def run_job(
    spark: SparkSession,
    manifest_uri: str,
    output_uri: str,
    *,
    history_uri: str | None = None,
    s3_client: Any = None,
    build_products: bool = False,
    release_uri: str | None = None,
    release_id: str | None = None,
) -> dict[str, Any]:
    """Prepare immutable evidence and optionally build a gated product release.

    With history_uri, union the previous accepted evidence snapshot before window
    selection. No event-time watermark is applied. Product releases are ready for
    the separate transactional Redshift publisher; this job does not advance its
    warehouse checkpoint.
    """
    if s3_client is None and manifest_uri.startswith("s3://"):
        import boto3

        s3_client = boto3.client("s3")
    spark.conf.set("spark.sql.session.timeZone", "UTC")
    manifest: dict[str, Any] = {}
    quality: list[dict[str, Any]] = []
    cached: list[DataFrame] = []
    try:
        manifest_bytes = _read_bytes(manifest_uri, s3_client)
        parsed_manifest = json.loads(manifest_bytes)
        validate_manifest(parsed_manifest)
        manifest = parsed_manifest
        if build_products and not history_uri and manifest["ingest_sequence"] != 1:
            raise ContractError(
                "INC_CHECKPOINT", "Incremental Spark products require prior accepted evidence"
            )
        if history_uri:
            previous = _history_report(history_uri, s3_client)
            accepted = {"ready"} if build_products else {"prepared", "ready"}
            if (
                previous.get("status") not in accepted
                or previous.get("ingest_sequence") != manifest["ingest_sequence"] - 1
            ):
                raise ContractError(
                    "INC_CHECKPOINT", "History must be a successful immediately preceding Spark run"
                )
        if (
            set(manifest["required_sources"]) != SOURCES
            or {f["source"] for f in manifest["files"]} != SOURCES
        ):
            raise ContractError("ING_REQUIRED_SOURCES", "Missing explicit required feeds")
        if len({f["path"] for f in manifest["files"]}) != len(manifest["files"]):
            raise ContractError("ING_EXPECTED_FILES", "Repeated manifest path")
        frames = []
        source_rows: dict[str, int] = dict.fromkeys(SOURCES, 0)
        source_files: dict[str, int] = dict.fromkeys(SOURCES, 0)
        raw_money: dict[str, int] = {}
        parent = (
            manifest_uri.rsplit("/", 1)[0]
            if manifest_uri.startswith("s3://")
            else str(Path(manifest_uri).parent)
        )
        normalizer = F.udf(normalize_json, EVIDENCE_SCHEMA)
        for entry in manifest["files"]:
            relative = Path(entry["path"])
            if relative.is_absolute() or ".." in relative.parts:
                raise ContractError("ING_EXPECTED_FILES", "Unsafe manifest path")
            uri = parent + "/" + entry["path"]
            source, version = entry["source"], int(entry["schema_version"])
            if version not in ({1, 2} if source == "carrier_delivery" else {1}):
                raise ContractError("ING_SCHEMA", f"Unsupported {source} version {version}")
            _verify_sha(uri, entry["sha256"], s3_client)
            if source in {"carrier_delivery", "transport_costs"}:
                data = (
                    spark.read.option("header", True)
                    .option("mode", "FAILFAST")
                    .option("multiLine", True)
                    .option("escape", '"')
                    .csv(uri)
                )
                expected = CARRIER_COLUMNS if source == "carrier_delivery" else COST_COLUMNS
                if source == "carrier_delivery" and version == 2:
                    expected = [
                        {"state": "delivery_state", "occurred_at": "event_time"}.get(c, c)
                        for c in expected
                    ]
                if data.columns != expected:
                    raise ContractError(
                        "ING_SCHEMA", f"CSV schema mismatch for {source} version {version}"
                    )
                # Spark defaults empty CSV strings to NULL; the source contract keeps blanks.
                data = data.fillna("")
                if source == "transport_costs":
                    if data.filter(~F.col("amount_minor").rlike(r"^-?[0-9]+$")).limit(1).count():
                        raise ContractError("REC_COST_RAW", "Invalid integer cost amount")
                    for row in (
                        data.groupBy("currency")
                        .agg(F.sum(F.col("amount_minor").cast("long")).alias("total"))
                        .collect()
                    ):
                        raw_money[row.currency] = raw_money.get(row.currency, 0) + row.total
                raw = data.select(F.to_json(F.struct(*data.columns)).alias("value"))
            else:
                raw = spark.read.text(uri).filter(F.length(F.trim("value")) > 0)
            count = raw.count()
            if count != entry["rows"]:
                raise ContractError("ING_ROW_COUNT", f"Row count mismatch {uri}")
            source_rows[source] += count
            source_files[source] += 1
            frames.append(
                raw.select(
                    normalizer(
                        "value", F.lit(source), F.lit(version), F.lit(entry["sha256"])
                    ).alias("r")
                ).select("r.*")
            )
        for source in SOURCES:
            controls = manifest["controls"][source]
            if controls["files"] != source_files[source] or controls["rows"] != source_rows[source]:
                raise ContractError("ING_EXPECTED_FILES", f"Control mismatch for {source}")
        expected_money = manifest["controls"]["transport_costs"]["amount_minor"]
        if any(
            raw_money.get(k, 0) != expected_money.get(k, 0)
            for k in raw_money.keys() | expected_money.keys()
        ):
            raise ContractError("REC_COST_RAW", "Signed raw currency controls failed")
        incoming = reduce(DataFrame.unionByName, frames).cache()
        cached.append(incoming)
        combined = incoming
        if history_uri:
            history = spark.read.schema(EVIDENCE_SCHEMA).parquet(history_uri)
            combined = history.unionByName(incoming)
        outputs = prepare_evidence(combined)
        # Materialize once: window-heavy descendants reuse accepted source scans.
        for key in ("evidence", "current", "resolution_decisions"):
            outputs[key] = outputs[key].cache()
            cached.append(outputs[key])
        quality.append(
            {
                "rule_id": "ING_MANIFEST_CONTROLS",
                "severity": "error",
                "passed": True,
                "observed": source_rows,
                "expected": {s: manifest["controls"][s]["rows"] for s in SOURCES},
            }
        )
        for key, rule in [
            ("quarantine", "ING_SCHEMA"),
            ("version_conflicts", "SRC_VERSION_CONFLICT"),
            ("identity_conflicts", "CAN_RELATION_CONFLICT"),
            ("alias_conflicts", "REF_AMBIGUOUS_ALIAS"),
        ]:
            count = outputs[key].count()
            quality.append(
                {
                    "rule_id": rule,
                    "severity": "error",
                    "passed": count == 0,
                    "observed": count,
                    "expected": 0,
                }
            )
        unresolved = outputs["resolution_decisions"].filter("outcome='unresolved'").count()
        quality.append(
            {
                "rule_id": "REF_MISSING_VALUE",
                "severity": "warning",
                "passed": unresolved == 0,
                "observed": unresolved,
                "expected": 0,
            }
        )
        failed = any(not q["passed"] and q["severity"] == "error" for q in quality)
        if failed:
            outputs["quarantine"].write.mode("errorifexists").parquet(output_uri + "/quarantine")
            raise ContractError("PUBLICATION_GATE", "Spark evidence checks failed")
        for key in ("evidence", "current", "resolution_decisions"):
            outputs[key].write.mode("errorifexists").parquet(output_uri + "/" + key)
        product_manifest = None
        if build_products:
            from logistics_platform.spark_release import export_spark_release
            from logistics_platform.spark_staging import build_staging
            from logistics_platform.spark_warehouse import project_spark, quality_checks_spark

            if not release_uri or not release_id:
                raise ContractError(
                    "PUBLICATION_GATE", "Product build requires release_uri and release_id"
                )
            staging, all_decisions, staging_checks = build_staging(outputs, release_id)
            quality.extend(staging_checks)
            if any(
                not q["passed"] and q["severity"].lower() in {"error", "critical"} for q in quality
            ):
                raise ContractError("PUBLICATION_GATE", "Canonical staging quality checks failed")
            products = project_spark(
                spark, staging, release_id, materialization_uri=output_uri + "/_warehouse"
            )
            quality.extend(quality_checks_spark(spark))
            pending = products["pending_fact_reference"].count()
            slots = products["fact_attribution"].count() * 8
            quality.append(
                {
                    "check_id": "REF_UNKNOWN_RATE",
                    "severity": "error",
                    "passed": pending / max(1, slots) <= 0.25,
                    "observed": pending / max(1, slots),
                    "expected": 0.25,
                }
            )
            if any(
                not q["passed"] and q["severity"].lower() in {"error", "critical"} for q in quality
            ):
                raise ContractError("PUBLICATION_GATE", "Spark warehouse quality checks failed")
            all_decisions.write.mode("errorifexists").parquet(
                output_uri + "/canonical_resolution_decisions"
            )
            spark.createDataFrame(
                [(compact({"status": "passed", "quality": quality}),)], ["value"]
            ).coalesce(1).write.mode("errorifexists").text(output_uri + "/product_quality")
            product_manifest = export_spark_release(
                products,
                release_uri,
                release_id,
                manifest["ingest_sequence"],
                quality,
                s3_client=s3_client,
                lineage={
                    "source_manifest_uri": manifest_uri,
                    "source_manifest_sha256": hashlib.sha256(manifest_bytes).hexdigest(),
                    "evidence_uri": output_uri + "/evidence",
                    "quality_uri": output_uri + "/product_quality",
                    "previous_evidence_uri": history_uri,
                },
            )
        report = {
            "batch_id": manifest["batch_id"],
            "ingest_sequence": manifest["ingest_sequence"],
            "status": "ready" if build_products else "prepared",
            "product_manifest": product_manifest,
            "quality": quality,
            "input_manifest_sha256": hashlib.sha256(manifest_bytes).hexdigest(),
            "evidence_rows": outputs["evidence"].count(),
        }
        spark.createDataFrame([(compact(report),)], ["value"]).coalesce(1).write.mode(
            "errorifexists"
        ).text(output_uri + "/quality")
        return report
    except Exception as exc:
        quality.append(
            {
                "rule_id": exc.rule
                if isinstance(exc, ContractError)
                else "SPARK_EXECUTION_FAILURE",
                "severity": "error",
                "passed": False,
                "observed": str(exc),
                "expected": "valid complete input",
            }
        )
        report = {"batch_id": manifest.get("batch_id"), "status": "failed", "quality": quality}
        # CloudWatch/stdout remains available even if the output bucket or JVM fails.
        print(compact(report))
        try:
            spark.createDataFrame([(compact(report),)], ["value"]).coalesce(1).write.mode(
                "errorifexists"
            ).text(output_uri + "/quality")
        except Exception as report_error:
            print(compact({"status": "quality_output_failed", "error": str(report_error)}))
        raise
    finally:
        for frame in reversed(cached):
            frame.unpersist()
        if build_products:
            spark.catalog.clearCache()


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--manifest_uri", required=True)
    parser.add_argument("--output_uri", required=True)
    parser.add_argument("--history_uri")
    parser.add_argument("--build_products", action="store_true")
    parser.add_argument("--release_uri")
    parser.add_argument("--release_id")
    args, _ = parser.parse_known_args()  # Glue supplies service-owned job arguments.
    spark = SparkSession.builder.appName("multi-carrier-canonical-evidence").getOrCreate()
    try:
        print(
            json.dumps(
                run_job(
                    spark,
                    args.manifest_uri,
                    args.output_uri,
                    history_uri=args.history_uri,
                    build_products=args.build_products,
                    release_uri=args.release_uri,
                    release_id=args.release_id,
                ),
                sort_keys=True,
            )
        )
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
