"""Distributed typed canonical staging from accepted versioned evidence.

Only bounded aggregate quality statistics return to the driver. Fact records and
resolution candidate sets remain distributed DataFrames throughout this stage.
"""

from __future__ import annotations

from typing import Any

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F
from pyspark.sql import types as T

from logistics_platform.contracts import COUNTRIES, REFERENCE_TYPES
from logistics_platform.keys import canonical_id


def build_staging(
    prepared: dict[str, DataFrame], run_id: str, max_unknown_ratio: float = 0.25
) -> tuple[dict[str, DataFrame], DataFrame, list[dict[str, Any]]]:
    current = prepared["current"]
    aliases = prepared["aliases"]
    cid = F.udf(lambda entity, key: canonical_id(entity, key) if key else None, T.StringType())

    def attr(name: str):
        return F.get_json_object("attributes", f"$.{name}")

    def request(field: str, target: str, name: str):
        return F.struct(
            F.lit(field).alias("field"),
            F.lit(target).alias("target_type"),
            attr(name).alias("source_value"),
        )

    # Equal-authority duplicate business identity cannot be settled by row order.
    business = current.filter("entity_type IN ('shipment','order','package')")
    newest = Window.partitionBy("entity_type", "business_key").orderBy(F.desc("updated_at"))
    tied = business.withColumn("_tier", F.dense_rank().over(newest)).filter("_tier=1")
    structural_conflict = (
        tied.groupBy("entity_type", "business_key")
        .agg(F.countDistinct(F.struct("attributes", "operation")).alias("values"))
        .filter("values>1")
    )
    deterministic = Window.partitionBy("entity_type", "business_key").orderBy(
        F.desc("updated_at"), F.desc("source_id")
    )
    business = (
        business.withColumn("_rank", F.row_number().over(deterministic))
        .filter("_rank=1")
        .drop("_rank")
    )
    selected = current.filter(
        ~F.col("entity_type").isin("shipment", "order", "package")
    ).unionByName(business)
    # Every lookup has its own source, entity, field and business-time clock.
    lookup_base = selected.filter("entity_type IN ('shipment','order','event','cost')").withColumn(
        "lookup_at",
        F.when(F.col("entity_type").isin("shipment", "order"), attr("created_at")).otherwise(
            F.col("effective_at")
        ),
    )
    requests = lookup_base.withColumn(
        "requests",
        F.when(
            F.col("entity_type") == "shipment",
            F.array(
                request("carrier_id", "carrier", "carrier_alias"),
                request("origin_facility_id", "facility", "origin_facility_alias"),
                request("destination_facility_id", "facility", "destination_facility_alias"),
                request("service_level_id", "service_level", "service_alias"),
                request("country_id", "country", "country_code"),
            ),
        )
        .when(
            F.col("entity_type") == "order",
            F.array(request("country_id", "country", "country_code")),
        )
        .otherwise(F.array(request("carrier_id", "carrier", "carrier_alias"))),
    )
    requests = requests.select(
        "source_system",
        "entity_type",
        "source_id",
        "lookup_at",
        F.explode("requests").alias("request"),
    ).select("source_system", "entity_type", "source_id", "lookup_at", "request.*")
    o, a = requests.alias("o"), aliases.alias("a")
    joined = o.join(
        F.broadcast(a),
        (F.col("o.target_type") == F.col("a.target_type"))
        & (F.col("o.source_system") == F.col("a.alias_source"))
        & (F.upper(F.trim(F.col("o.source_value"))) == F.col("a.alias_value"))
        & (F.col("o.lookup_at") >= F.col("a.effective_from"))
        & (F.col("o.lookup_at") < F.col("a.effective_to")),
        "left",
    )
    decisions = joined.groupBy(
        "o.source_system",
        "o.entity_type",
        "o.source_id",
        "o.lookup_at",
        "o.field",
        "o.target_type",
        "o.source_value",
    ).agg(
        F.sort_array(F.collect_set("a.target_key")).alias("candidates"),
        F.sort_array(F.collect_set("a.rule_id")).alias("rules"),
    )
    explicit_country = (F.col("target_type") == "country") & F.col("source_value").isin(
        sorted(COUNTRIES)
    )
    decisions = decisions.withColumn(
        "candidates",
        F.when(explicit_country, F.array(F.col("source_value"))).otherwise(F.col("candidates")),
    ).withColumn(
        "rules",
        F.when(explicit_country, F.array(F.lit("controlled-country-key-v1"))).otherwise(
            F.col("rules")
        ),
    )
    decisions = decisions.withColumn(
        "outcome",
        F.when(F.size("candidates") == 1, "resolved")
        .when(F.size("candidates") > 1, "ambiguous")
        .otherwise("unresolved"),
    )
    candidate_id = F.udf(
        lambda entity, targets: canonical_id(entity, targets[0]) if len(targets) == 1 else None,
        T.StringType(),
    )
    decisions = decisions.withColumn(
        "resolved_id", candidate_id("target_type", "candidates")
    ).cache()
    fields = [
        "carrier_id",
        "origin_facility_id",
        "destination_facility_id",
        "service_level_id",
        "country_id",
    ]
    resolved = decisions.groupBy("source_system", "entity_type", "source_id").agg(
        *[
            F.max(F.when(F.col("field") == field, F.col("resolved_id"))).alias(field)
            for field in fields
        ]
    )
    enriched = (
        selected.join(resolved, ["source_system", "entity_type", "source_id"], "left")
        .withColumn("run_id", F.lit(run_id))
        .withColumn("source_updated_at", F.col("updated_at").cast("timestamp"))
        .withColumn("is_deleted", F.col("operation") == "delete")
    )
    refs = enriched.filter(F.col("entity_type").isin(sorted(REFERENCE_TYPES)))
    stages = {
        "stg_reference": refs.select(
            "entity_type",
            "business_key",
            "canonical_id",
            attr("name").alias("canonical_value"),
            "attributes",
            F.col("effective_at").cast("timestamp").alias("effective_from"),
            "revision",
            "source_updated_at",
            F.when(F.col("is_deleted"), "INACTIVE").otherwise("ACTIVE").alias("status"),
            "source_id",
            "raw_hash",
            "run_id",
        ),
        "stg_order": enriched.filter("entity_type='order'").select(
            "canonical_id",
            "business_key",
            attr("created_at").cast("timestamp").alias("created_at"),
            "country_id",
            "is_deleted",
            "source_id",
            "revision",
            "raw_hash",
            "run_id",
        ),
        "stg_package": enriched.filter("entity_type='package'").select(
            "canonical_id",
            "business_key",
            cid(F.lit("shipment"), attr("shipment_key")).alias("shipment_id"),
            attr("weight_grams").cast("long").alias("weight_grams"),
            "is_deleted",
            "source_id",
            "revision",
            "raw_hash",
            "run_id",
        ),
        "stg_shipment": enriched.filter("entity_type='shipment'").select(
            "canonical_id",
            "business_key",
            cid(F.lit("order"), attr("order_key")).alias("order_id"),
            "carrier_id",
            "origin_facility_id",
            "destination_facility_id",
            cid(F.lit("route"), attr("route_key")).alias("route_id"),
            "country_id",
            "service_level_id",
            attr("created_at").cast("timestamp").alias("created_at"),
            attr("promised_at").cast("timestamp").alias("promised_at"),
            F.coalesce(attr("state"), F.lit("created")).alias("state"),
            "is_deleted",
            "source_id",
            "revision",
            F.col("updated_at").cast("timestamp").alias("updated_at"),
            F.col("effective_at").cast("timestamp").alias("state_occurred_at"),
            "raw_hash",
            "run_id",
        ),
        "stg_event": enriched.filter("entity_type='event' AND NOT is_deleted").select(
            "source_system",
            F.col("source_id").alias("event_id"),
            F.col("canonical_id").alias("event_canonical_id"),
            cid(F.lit("shipment"), attr("shipment_key")).alias("shipment_id"),
            cid(F.lit("package"), attr("package_key")).alias("package_id"),
            "carrier_id",
            attr("state").alias("state"),
            F.col("effective_at").cast("timestamp").alias("occurred_at"),
            "source_updated_at",
            attr("verified").cast("boolean").alias("verified"),
            "revision",
            "raw_hash",
            "run_id",
        ),
        "stg_cost": enriched.filter("entity_type='cost'").select(
            F.col("source_id").alias("cost_id"),
            F.col("canonical_id").alias("cost_canonical_id"),
            cid(F.lit("shipment"), attr("shipment_key")).alias("shipment_id"),
            "carrier_id",
            attr("amount_minor").cast("long").alias("amount_minor"),
            attr("currency").alias("currency"),
            attr("cost_type").alias("cost_type"),
            F.col("effective_at").cast("timestamp").alias("incurred_at"),
            "revision",
            "source_updated_at",
            "is_deleted",
            "raw_hash",
            "run_id",
        ),
    }
    checks: list[dict[str, Any]] = []

    def check(rule: str, count: int, details: str):
        checks.append(
            {
                "check_id": rule,
                "severity": "error",
                "observed": count,
                "expected": 0,
                "passed": count == 0,
                "details": details,
            }
        )

    check(
        "CAN_DUPLICATE_BUSINESS_KEY",
        structural_conflict.count(),
        "Equal-time authority cannot select between different source assertions",
    )
    check(
        "REF_INVALID_COUNTRY",
        decisions.filter("target_type='country' AND outcome<>'resolved'").count()
        + refs.filter(
            (F.col("entity_type") == "country") & ~F.col("business_key").isin(sorted(COUNTRIES))
        ).count(),
        "Country identity must be a controlled code or approved alias",
    )
    check(
        "REF_BOUNDARY_CONFLICT",
        refs.groupBy("entity_type", "canonical_id", "effective_at")
        .agg(F.countDistinct(F.struct("attributes", "operation")).alias("values"))
        .filter("values>1")
        .count(),
        "Authoritative boundary conflicts block publication",
    )
    check(
        "REF_AMBIGUOUS_ALIAS",
        decisions.filter("outcome='ambiguous'").count(),
        "No arbitrary candidate selection",
    )
    alias_decisions = decisions.filter("target_type<>'country'")
    measures = alias_decisions.agg(
        F.count("*").alias("total"),
        F.sum(F.when(F.col("outcome") != "resolved", 1).otherwise(0)).alias("unknown"),
    ).first()
    ratio = (measures.unknown or 0) / max(1, measures.total)
    checks.append(
        {
            "check_id": "CAN_UNRESOLVED_IDENTITY",
            "severity": "error",
            "observed": ratio,
            "expected": max_unknown_ratio,
            "passed": ratio <= max_unknown_ratio,
            "details": "Explicit maximum unresolved alias ratio",
        }
    )
    for name, stage in stages.items():
        stage.createOrReplaceTempView(name)
    membership = (
        current.sparkSession.sql(
            "SELECT count(*) n FROM stg_event e LEFT JOIN stg_package p ON e.package_id=p.canonical_id WHERE e.package_id IS NOT NULL AND (p.canonical_id IS NULL OR p.shipment_id<>e.shipment_id)"
        )
        .first()
        .n
    )
    check(
        "CAN_EVENT_MEMBERSHIP",
        membership,
        "Package-to-shipment identity must match accepted membership",
    )
    return stages, decisions, checks
