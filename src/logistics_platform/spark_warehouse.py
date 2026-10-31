"""Spark-native canonical, dimensional and self-service projections.

This module does not import DuckDB or collect business datasets to the driver.
All joins, window histories and quality scans execute through Spark SQL. Only
bounded scalar quality results cross the driver boundary. The caller controls
publication: any failing ERROR result must prevent a release manifest.
"""

from __future__ import annotations

import re
from datetime import datetime, timezone
from importlib.resources import files
from typing import Any

from pyspark import StorageLevel
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import LongType, StringType

from logistics_platform.keys import canonical_id, surrogate_key

# Kept explicit so Glue does not need the local DuckDB runtime. Parity tests
# compare this contract against the local staging schema and model results.
STAGING_SCHEMAS = {
    "stg_reference": "entity_type STRING,business_key STRING,canonical_id STRING,canonical_value STRING,attributes STRING,effective_from TIMESTAMP,revision BIGINT,source_updated_at TIMESTAMP,status STRING,source_id STRING,raw_hash STRING,run_id STRING",
    "stg_order": "canonical_id STRING,business_key STRING,created_at TIMESTAMP,country_id STRING,is_deleted BOOLEAN,source_id STRING,revision BIGINT,raw_hash STRING,run_id STRING",
    "stg_package": "canonical_id STRING,business_key STRING,shipment_id STRING,weight_grams BIGINT,is_deleted BOOLEAN,source_id STRING,revision BIGINT,raw_hash STRING,run_id STRING",
    "stg_shipment": "canonical_id STRING,business_key STRING,order_id STRING,carrier_id STRING,origin_facility_id STRING,destination_facility_id STRING,route_id STRING,country_id STRING,service_level_id STRING,created_at TIMESTAMP,promised_at TIMESTAMP,state STRING,is_deleted BOOLEAN,source_id STRING,revision BIGINT,updated_at TIMESTAMP,state_occurred_at TIMESTAMP,raw_hash STRING,run_id STRING",
    "stg_event": "source_system STRING,event_id STRING,event_canonical_id STRING,shipment_id STRING,package_id STRING,carrier_id STRING,state STRING,occurred_at TIMESTAMP,source_updated_at TIMESTAMP,verified BOOLEAN,revision BIGINT,raw_hash STRING,run_id STRING",
    "stg_cost": "cost_id STRING,cost_canonical_id STRING,shipment_id STRING,carrier_id STRING,amount_minor BIGINT,currency STRING,cost_type STRING,incurred_at TIMESTAMP,revision BIGINT,source_updated_at TIMESTAMP,is_deleted BOOLEAN,raw_hash STRING,run_id STRING",
}


def _canonical_nullable(entity: str | None, key: str | None) -> str | None:
    return canonical_id(entity, key) if entity is not None and key is not None else None


def _surrogate_nullable(entity: str | None, identity: str | None, start: str | None) -> int | None:
    if entity is None or identity is None or start is None:
        return None
    return surrogate_key(entity, identity, start)


def project_spark(
    spark: SparkSession,
    staging: dict[str, DataFrame],
    run_id: str,
    materialization_uri: str | None = None,
) -> dict[str, DataFrame]:
    """Project typed staging into named Spark temp views and return DataFrames.

    Staging keys may use either ``stg_shipment`` or ``shipment`` spelling. Every
    other field follows execution-contract.md. Persisted intermediate views avoid
    repeatedly evaluating Python identity UDFs and multiway temporal joins; Spark
    owns their distributed memory/disk cache, not the driver. The session must be
    dedicated to this one publication while its views are in use. Production
    callers supply a run-scoped S3 materialization_uri to truncate source/temporal
    lineage with durable Parquet boundaries before quality and release writes.
    """
    spark.conf.set("spark.sql.session.timeZone", "UTC")
    spark.conf.set("spark.sql.parquet.outputTimestampType", "TIMESTAMP_MICROS")
    spark.udf.register("canonical_id", _canonical_nullable, StringType())
    spark.udf.register("surrogate_key", _surrogate_nullable, LongType())
    for table, ddl in STAGING_SCHEMAS.items():
        frame = staging.get(table)
        if frame is None:
            frame = staging.get(table.removeprefix("stg_"))
        if frame is None:
            raise ValueError(f"Missing required typed staging DataFrame: {table}")
        expected = spark.createDataFrame([], ddl).schema
        missing = set(expected.fieldNames()) - set(frame.columns)
        if missing == {"state_occurred_at"} and table == "stg_shipment":
            frame = frame.withColumn("state_occurred_at", F.lit(None).cast("timestamp"))
            missing.clear()
        if missing:
            raise ValueError(f"Missing {table} fields: {sorted(missing)}")
        if spark.catalog.tableExists(table) and spark.catalog.isCached(table):
            spark.catalog.uncacheTable(table)
        typed = frame.select(*(F.col(f.name).cast(f.dataType).alias(f.name) for f in expected))
        if materialization_uri:
            destination = f"{materialization_uri.rstrip('/')}/staging/{table}"
            typed.write.mode("errorifexists").parquet(destination)
            typed = spark.read.parquet(destination)
        typed.persist(StorageLevel.MEMORY_AND_DISK)
        typed.count()  # Distributed stage boundary; only the scalar count reaches the driver.
        typed.createOrReplaceTempView(table)
    now = datetime.now(timezone.utc)
    spark.createDataFrame(
        [(run_id, now)], "run_id STRING,recorded_at TIMESTAMP"
    ).createOrReplaceTempView("projection_context")
    outputs: dict[str, DataFrame] = {}
    directory = files("logistics_platform").joinpath("spark_sql")
    cached = {"dim_reference", "canonical_shipment", "shipment_state_evidence", "fact_attribution"}
    for filename in ("001_reference.sql", "002_canonical.sql", "003_marts.sql", "004_products.sql"):
        # These checked-in SQL files contain no semicolons inside strings.
        for statement in directory.joinpath(filename).read_text(encoding="utf-8").split(";"):
            match = re.search(r"CREATE OR REPLACE TEMP VIEW\s+(\w+)\s+AS", statement, re.I)
            if not match:
                continue
            name = match.group(1)
            if spark.catalog.tableExists(name) and spark.catalog.isCached(name):
                spark.catalog.uncacheTable(name)
            spark.sql(statement)
            if materialization_uri and name in {
                "dim_reference",
                "canonical_shipment",
                "fact_attribution",
            }:
                # Cache alone retains the complete logical lineage. A durable
                # Parquet boundary bounds plan/task serialization and survives
                # executor loss without collecting records to the driver.
                destination = f"{materialization_uri.rstrip('/')}/models/{name}"
                spark.table(name).write.mode("errorifexists").parquet(destination)
                spark.read.parquet(destination).createOrReplaceTempView(name)
            if name in cached or name.startswith("dim_") or name.startswith("fact_"):
                spark.catalog.cacheTable(name)
            outputs[name] = spark.table(name)
    return outputs


def quality_checks_spark(spark: SparkSession) -> list[dict[str, Any]]:
    """Execute distributed gates and return only one scalar per declared check."""
    specs: list[tuple[str, str, str, str]] = []

    def add(key: str, sql: str, details: str, severity: str = "ERROR") -> None:
        specs.append((key, sql, details, severity))

    add(
        "DIM_INTERVAL_OVERLAP",
        """SELECT count(*) FROM (
        SELECT canonical_id,effective_from,lag(effective_to) OVER (
          PARTITION BY entity_type,canonical_id ORDER BY effective_from) previous_end
        FROM dim_reference WHERE surrogate_key<>0) q WHERE previous_end>effective_from""",
        "Reference intervals cannot overlap.",
    )
    add(
        "DIM_SK_COLLISION",
        "SELECT count(*) FROM (SELECT surrogate_key FROM dim_reference WHERE surrogate_key<>0 GROUP BY surrogate_key HAVING count(*)>1) q",
        "Surrogate collisions block publication.",
    )
    add(
        "DIM_INVALID_INTERVAL",
        "SELECT count(*) FROM dim_reference WHERE effective_from>=effective_to OR effective_from IS NULL OR effective_to IS NULL",
        "Nonempty half-open intervals are mandatory.",
    )
    facts = (
        ("shipment", "fact_shipment", "shipment_id"),
        ("event", "fact_delivery_event", "event_canonical_id"),
        ("cost", "fact_transportation_cost", "cost_canonical_id"),
    )
    roles = (
        ("carrier_sk", "carrier_id", "dim_carrier", "carrier_sk"),
        ("origin_facility_sk", "origin_facility_id", "dim_facility", "facility_sk"),
        ("destination_facility_sk", "destination_facility_id", "dim_facility", "facility_sk"),
        ("route_sk", "route_id", "dim_route", "route_sk"),
        ("country_sk", "country_id", "dim_country", "country_sk"),
        ("region_sk", "origin_region_id", "dim_region", "region_sk"),
        ("destination_region_sk", "destination_region_id", "dim_region", "region_sk"),
        ("service_level_sk", "service_level_id", "dim_service_level", "service_level_sk"),
        ("origin_country_sk", "origin_country_id", "dim_country", "country_sk"),
        ("destination_country_sk", "destination_country_id", "dim_country", "country_sk"),
    )
    for fact_type, table, identity in facts:
        add(
            f"GRAIN_{table.upper()}",
            f"SELECT count(*) FROM (SELECT {identity} FROM {table} GROUP BY {identity} HAVING count(*)>1) q",
            "Fact grain is unique.",
        )
        for column, canonical, dimension, key in roles:
            add(
                f"FK_{table.upper()}_{column.upper()}",
                f"SELECT count(*) FROM {table} f LEFT JOIN {dimension} d ON f.{column}=d.{key} WHERE d.{key} IS NULL",
                "All role-playing keys resolve, including member 0.",
            )
            add(
                f"ASOF_{fact_type.upper()}_{column.upper()}",
                f"""SELECT count(*) FROM {table} f
                JOIN fact_attribution a ON a.fact_type='{fact_type}' AND a.fact_id=f.{identity}
                JOIN {dimension} d ON f.{column}=d.{key}
                WHERE f.{column}<>0 AND (NOT (d.canonical_id <=> a.{canonical})
                  OR a.business_at<d.effective_from OR a.business_at>=d.effective_to)""",
                "Fact version matches identity and business time.",
            )
    for table, columns in (
        ("fact_shipment", ("created_date_sk", "promised_date_sk", "delivered_date_sk")),
        ("fact_delivery_event", ("event_date_sk",)),
        ("fact_transportation_cost", ("incurred_date_sk",)),
    ):
        for column in columns:
            add(
                f"DATE_FK_{table.upper()}_{column.upper()}",
                f"SELECT count(*) FROM {table} f LEFT JOIN dim_date d ON f.{column}=d.date_sk WHERE d.date_sk IS NULL",
                "Date keys resolve to the UTC calendar.",
            )
    add(
        "REFERENCE_ATTRIBUTE_VALUE",
        """SELECT count(*) FROM stg_reference WHERE
      (entity_type='service_level' AND (
        NOT coalesce(get_json_object(attributes,'$.target_business_days'),'') RLIKE '^[0-9]+$'
        OR try_cast(get_json_object(attributes,'$.target_business_days') AS BIGINT)<=0))
      OR (entity_type='route' AND get_json_object(attributes,'$.distance_km') IS NOT NULL AND (
        try_cast(get_json_object(attributes,'$.distance_km') AS DOUBLE) IS NULL
        OR isnan(try_cast(get_json_object(attributes,'$.distance_km') AS DOUBLE))
        OR abs(try_cast(get_json_object(attributes,'$.distance_km') AS DOUBLE))=cast('Infinity' AS DOUBLE)
        OR try_cast(get_json_object(attributes,'$.distance_km') AS DOUBLE)<0))
      OR (entity_type='facility' AND (nullif(get_json_object(attributes,'$.country_key'),'') IS NULL
        OR nullif(get_json_object(attributes,'$.region_key'),'') IS NULL))
      OR (entity_type='route' AND (nullif(get_json_object(attributes,'$.origin_facility_key'),'') IS NULL
        OR nullif(get_json_object(attributes,'$.destination_facility_key'),'') IS NULL))""",
        "Typed reference values and relationships are required.",
    )
    for entity, table, key in (
        ("COUNTRY", "dim_country", "country_sk"),
        ("REGION", "dim_region", "region_sk"),
    ):
        identity = entity.lower() + "_id"
        add(
            f"REFERENCE_{entity}_ORPHAN",
            f"""SELECT count(*) FROM dim_facility f LEFT JOIN {table} d
            ON f.{identity}=d.canonical_id AND f.effective_from>=d.effective_from AND f.effective_from<d.effective_to
            WHERE f.facility_sk<>0 AND d.{key} IS NULL""",
            "Facility reference parent must exist at the business boundary.",
        )
    add(
        "ROUTE_ENDPOINT_DISAGREEMENT",
        """SELECT count(*) FROM canonical_shipment s JOIN dim_route r
        ON s.route_id=r.canonical_id AND s.created_at>=r.effective_from AND s.created_at<r.effective_to
        WHERE NOT (r.origin_facility_id <=> s.origin_facility_id)
          OR NOT (r.destination_facility_id <=> s.destination_facility_id)""",
        "Directed route endpoints must agree.",
    )
    add(
        "STATE_AUTHORITY_TIE",
        "SELECT count(*) FROM shipment_state_conflict",
        "Equal-authority contradictions block publication.",
    )
    add(
        "INVALID_STATE_VALUE",
        "SELECT count(*) FROM shipment_state_evidence WHERE state IS NULL OR state NOT IN ('created','collected','in_transit','out_for_delivery','delivered','delivery_failed','returned','cancelled')",
        "State vocabulary is controlled.",
    )
    add(
        "INVALID_STATE_TRANSITION",
        "SELECT count(*) FROM invalid_state_transition",
        "Terminal regressions are not silently accepted.",
    )
    add(
        "SHIPMENT_IMPOSSIBLE_TIME",
        "SELECT count(*) FROM canonical_shipment WHERE promised_at<created_at OR actual_delivery_at<created_at",
        "Milestones cannot precede creation.",
    )
    add(
        "EVENT_BEFORE_CREATION",
        "SELECT count(*) FROM canonical_delivery_event e JOIN canonical_shipment s ON e.shipment_id=s.canonical_id WHERE e.occurred_at<s.created_at",
        "Business chronology differs from arrival chronology.",
    )
    add(
        "PACKAGE_NEGATIVE_WEIGHT",
        "SELECT count(*) FROM canonical_package WHERE weight_grams<0",
        "Weights are nonnegative.",
    )
    add(
        "PACKAGE_SHIPMENT_ORPHAN",
        "SELECT count(*) FROM canonical_package p LEFT JOIN canonical_shipment s ON p.shipment_id=s.canonical_id WHERE s.canonical_id IS NULL AND NOT p.is_deleted",
        "Package membership must resolve.",
    )
    add(
        "SHIPMENT_ORDER_ORPHAN",
        "SELECT count(*) FROM canonical_shipment s LEFT JOIN canonical_order o ON s.order_id=o.canonical_id WHERE o.canonical_id IS NULL AND NOT s.is_deleted",
        "Shipment order must resolve.",
    )
    add(
        "EVENT_SHIPMENT_ORPHAN",
        "SELECT count(*) FROM canonical_delivery_event e LEFT JOIN canonical_shipment s ON e.shipment_id=s.canonical_id WHERE s.canonical_id IS NULL",
        "Event shipment must resolve.",
    )
    add(
        "COST_SHIPMENT_ORPHAN",
        "SELECT count(*) FROM canonical_transportation_cost c LEFT JOIN canonical_shipment s ON c.shipment_id=s.canonical_id WHERE s.canonical_id IS NULL AND NOT c.is_deleted",
        "Cost shipment must resolve.",
    )
    add(
        "COST_CURRENCY",
        "SELECT count(*) FROM fact_transportation_cost WHERE currency<>'EUR' OR currency IS NULL",
        "No implicit exchange conversion.",
    )
    add(
        "COST_CANONICAL_RECONCILIATION",
        "SELECT abs((SELECT coalesce(sum(amount_minor),0) FROM canonical_transportation_cost WHERE NOT is_deleted)-(SELECT coalesce(sum(amount_minor),0) FROM fact_transportation_cost))",
        "Signed accepted and fact totals reconcile.",
    )
    add(
        "SHIPMENT_COUNT_RECONCILIATION",
        "SELECT abs((SELECT count(*) FROM canonical_shipment)-(SELECT count(*) FROM fact_shipment))",
        "Shipment projection preserves counts.",
    )
    add(
        "EVENT_COUNT_RECONCILIATION",
        "SELECT abs((SELECT count(*) FROM canonical_delivery_event)-(SELECT count(*) FROM fact_delivery_event))",
        "Event projection preserves counts.",
    )
    add(
        "PENDING_REFERENCE",
        "SELECT count(*) FROM pending_fact_reference",
        "Missing descriptive references remain explicit.",
        "WARN",
    )

    results: list[dict[str, Any]] = []
    # Do not UNION complex gates: duplicating temporal plans can exhaust the
    # driver's task-serialization heap even when the input dataset is small.
    # Each action returns exactly one scalar; distributed caches serve scans.
    for key, query, details, severity in specs:
        row = spark.sql(query).first()
        if row is None:
            raise RuntimeError(f"Quality query returned no aggregate: {key}")
        value = int(row[0])
        results.append(
            {
                "check_id": key,
                "severity": severity,
                "observed": value,
                "expected": 0,
                "passed": value == 0,
                "details": details,
            }
        )
    return results
