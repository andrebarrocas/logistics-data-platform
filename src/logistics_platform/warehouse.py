"""Transactional SQL projections; the caller owns commit and rollback.

All evidence is validated before entering typed staging. Projections rebuild from
accepted current observations and dated references, making late corrections
independent of arrival order. This is deliberately a local correctness harness,
not a distributed warehouse performance claim.
"""

from __future__ import annotations

from importlib.resources import files
from typing import Any

import duckdb

_STAGING = {
    "reference": "entity_type VARCHAR, business_key VARCHAR, canonical_id VARCHAR, canonical_value VARCHAR, attributes JSON, effective_from TIMESTAMP, revision BIGINT, source_updated_at TIMESTAMP, status VARCHAR, source_id VARCHAR",
    "order": "canonical_id VARCHAR, business_key VARCHAR, created_at TIMESTAMP, country_id VARCHAR, is_deleted BOOLEAN, source_id VARCHAR, revision BIGINT",
    "package": "canonical_id VARCHAR, business_key VARCHAR, shipment_id VARCHAR, weight_grams BIGINT, is_deleted BOOLEAN, source_id VARCHAR, revision BIGINT",
    "shipment": "canonical_id VARCHAR, business_key VARCHAR, order_id VARCHAR, carrier_id VARCHAR, origin_facility_id VARCHAR, destination_facility_id VARCHAR, route_id VARCHAR, country_id VARCHAR, service_level_id VARCHAR, created_at TIMESTAMP, promised_at TIMESTAMP, state VARCHAR, is_deleted BOOLEAN, source_id VARCHAR, revision BIGINT, updated_at TIMESTAMP, state_occurred_at TIMESTAMP",
    "event": "source_system VARCHAR, event_id VARCHAR, event_canonical_id VARCHAR, shipment_id VARCHAR, package_id VARCHAR, carrier_id VARCHAR, state VARCHAR, occurred_at TIMESTAMP, source_updated_at TIMESTAMP, verified BOOLEAN, revision BIGINT",
    "cost": "cost_id VARCHAR, cost_canonical_id VARCHAR, shipment_id VARCHAR, carrier_id VARCHAR, amount_minor BIGINT, currency VARCHAR, cost_type VARCHAR, incurred_at TIMESTAMP, revision BIGINT, source_updated_at TIMESTAMP, is_deleted BOOLEAN",
}


def create_staging(connection: duckdb.DuckDBPyConnection) -> None:
    """Create and empty staging inside the caller's transaction."""
    for name, columns in _STAGING.items():
        connection.execute(
            f"CREATE TABLE IF NOT EXISTS stg_{name} ({columns}, raw_hash VARCHAR, run_id VARCHAR)"
        )
        connection.execute(f"DELETE FROM stg_{name}")
    # Additive staging migration for databases created before the state-time field.
    connection.execute(
        "ALTER TABLE stg_shipment ADD COLUMN IF NOT EXISTS state_occurred_at TIMESTAMP"
    )


def project(connection: duckdb.DuckDBPyConnection, run_id: str) -> None:
    """Build SQL projections. Do not commit: quality gates decide publication."""
    connection.execute(
        "CREATE OR REPLACE TEMP TABLE projection_context(run_id VARCHAR, recorded_at TIMESTAMP)"
    )
    connection.execute(
        "INSERT INTO projection_context VALUES (?,current_timestamp AT TIME ZONE 'UTC')", [run_id]
    )
    directory = files("logistics_platform").joinpath("sql")
    for name in ("001_reference.sql", "002_canonical.sql", "003_marts.sql", "004_products.sql"):
        connection.execute(directory.joinpath(name).read_text(encoding="utf-8"))


def quality_checks(connection: duckdb.DuckDBPyConnection) -> list[dict[str, Any]]:
    """Return machine-readable checks, including passing checks on every run."""
    checks: list[dict[str, Any]] = []

    def add(check_id: str, sql: str, detail: str, severity: str = "ERROR") -> None:
        row = connection.execute(sql).fetchone()
        if row is None:
            raise RuntimeError(f"Quality query returned no aggregate: {check_id}")
        observed = int(row[0])
        checks.append(
            {
                "check_id": check_id,
                "severity": severity,
                "observed": observed,
                "expected": 0,
                "passed": observed == 0,
                "details": detail,
            }
        )

    add(
        "DIM_INTERVAL_OVERLAP",
        """SELECT count(*) FROM (
      SELECT canonical_id,effective_from,lag(effective_to) OVER
      (PARTITION BY entity_type,canonical_id ORDER BY effective_from) previous_end
      FROM dim_reference WHERE surrogate_key<>0
    ) WHERE previous_end>effective_from""",
        "Business-valid intervals must not overlap.",
    )
    add(
        "DIM_SK_COLLISION",
        """SELECT count(*) FROM (
      SELECT surrogate_key FROM dim_reference WHERE surrogate_key<>0
      GROUP BY surrogate_key HAVING count(*)>1)""",
        "A deterministic surrogate collision blocks publication.",
    )
    add(
        "DIM_INVALID_INTERVAL",
        "SELECT count(*) FROM dim_reference WHERE effective_from>=effective_to",
        "Every dimension interval is nonempty and half-open.",
    )
    for table, key in (
        ("fact_shipment", "shipment_id"),
        ("fact_delivery_event", "event_canonical_id"),
        ("fact_transportation_cost", "cost_canonical_id"),
    ):
        add(
            f"GRAIN_{table.upper()}",
            f"SELECT count(*) FROM (SELECT {key} FROM {table} GROUP BY {key} HAVING count(*)>1)",
            f"Unique declared grain in {table}.",
        )
        for column, dimension, dimension_key in (
            ("carrier_sk", "dim_carrier", "carrier_sk"),
            ("route_sk", "dim_route", "route_sk"),
            ("service_level_sk", "dim_service_level", "service_level_sk"),
            ("country_sk", "dim_country", "country_sk"),
            ("region_sk", "dim_region", "region_sk"),
            ("origin_facility_sk", "dim_facility", "facility_sk"),
            ("destination_facility_sk", "dim_facility", "facility_sk"),
            ("destination_region_sk", "dim_region", "region_sk"),
            ("origin_country_sk", "dim_country", "country_sk"),
            ("destination_country_sk", "dim_country", "country_sk"),
        ):
            add(
                f"FK_{table.upper()}_{column.upper()}",
                f"SELECT count(*) FROM {table} f LEFT JOIN {dimension} d ON f.{column}=d.{dimension_key} WHERE d.{dimension_key} IS NULL",
                "Key 0 must resolve to the explicit unknown member; all other keys must resolve.",
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
                "Every role-playing date resolves, including explicit unknown key 0.",
            )
    for column, identity, dimension, dimension_key in (
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
    ):
        for fact_type, table, key in (
            ("shipment", "fact_shipment", "shipment_id"),
            ("event", "fact_delivery_event", "event_canonical_id"),
            ("cost", "fact_transportation_cost", "cost_canonical_id"),
        ):
            add(
                f"ASOF_{fact_type.upper()}_{column.upper()}",
                f"""SELECT count(*) FROM {table} f
                JOIN fact_attribution a ON a.fact_type='{fact_type}' AND a.fact_id=f.{key}
                JOIN {dimension} d ON f.{column}=d.{dimension_key}
                WHERE f.{column}<>0 AND (d.canonical_id IS DISTINCT FROM a.{identity}
                    OR a.business_at<d.effective_from OR a.business_at>=d.effective_to)""",
                "Fact attribution must identify the same canonical entity in the applicable half-open interval.",
            )
    add(
        "REFERENCE_ATTRIBUTE_VALUE",
        """SELECT count(*) FROM stg_reference
        WHERE (entity_type='service_level' AND (
            NOT regexp_full_match(coalesce(attributes->>'target_business_days',''),'[0-9]+')
            OR try_cast(attributes->>'target_business_days' AS BIGINT)<=0))
        OR (entity_type='route' AND (attributes->>'distance_km') IS NOT NULL AND (
            try_cast(attributes->>'distance_km' AS DOUBLE) IS NULL
            OR NOT isfinite(try_cast(attributes->>'distance_km' AS DOUBLE))
            OR try_cast(attributes->>'distance_km' AS DOUBLE)<0))
        OR (entity_type='facility' AND (
            nullif(attributes->>'country_key','') IS NULL OR nullif(attributes->>'region_key','') IS NULL))
        OR (entity_type='route' AND (
            nullif(attributes->>'origin_facility_key','') IS NULL OR nullif(attributes->>'destination_facility_key','') IS NULL))""",
        "Reference attributes have required relationship keys, positive integral service days, and finite nonnegative route distance.",
    )
    add(
        "REFERENCE_COUNTRY_ORPHAN",
        """SELECT count(*) FROM dim_facility f
        LEFT JOIN dim_country c ON f.country_id=c.canonical_id
          AND f.effective_from>=c.effective_from AND f.effective_from<c.effective_to
        WHERE f.facility_sk<>0 AND c.country_sk IS NULL""",
        "Facility country must exist at the facility version start.",
    )
    add(
        "REFERENCE_REGION_ORPHAN",
        """SELECT count(*) FROM dim_facility f
        LEFT JOIN dim_region r ON f.region_id=r.canonical_id
          AND f.effective_from>=r.effective_from AND f.effective_from<r.effective_to
        WHERE f.facility_sk<>0 AND r.region_sk IS NULL""",
        "Facility region must exist at the facility version start.",
    )
    add(
        "ROUTE_ENDPOINT_DISAGREEMENT",
        """SELECT count(*) FROM canonical_shipment s
        JOIN dim_route r ON s.route_id=r.canonical_id
          AND s.created_at>=r.effective_from AND s.created_at<r.effective_to
        WHERE r.origin_facility_id IS DISTINCT FROM s.origin_facility_id
           OR r.destination_facility_id IS DISTINCT FROM s.destination_facility_id""",
        "The assigned directed route must agree with shipment origin and destination.",
    )
    add(
        "INVALID_STATE_VALUE",
        """SELECT count(*) FROM shipment_state_evidence
        WHERE state IS NULL OR state NOT IN ('created','collected','in_transit','out_for_delivery',
            'delivered','delivery_failed','returned','cancelled')""",
        "States use the closed normalized vocabulary.",
    )
    add(
        "STATE_AUTHORITY_TIE",
        "SELECT count(*) FROM shipment_state_conflict",
        "Different states at the highest equal authority and occurrence require explicit resolution.",
    )
    add(
        "SHIPMENT_IMPOSSIBLE_TIME",
        "SELECT count(*) FROM canonical_shipment WHERE promised_at<created_at OR actual_delivery_at<created_at",
        "Promised and actual delivery cannot precede shipment creation.",
    )
    add(
        "EVENT_BEFORE_CREATION",
        "SELECT count(*) FROM canonical_delivery_event e JOIN canonical_shipment s ON s.canonical_id=e.shipment_id WHERE e.occurred_at<s.created_at",
        "Late arrival is permitted; impossible business chronology is not.",
    )
    add(
        "PACKAGE_NEGATIVE_WEIGHT",
        "SELECT count(*) FROM canonical_package WHERE weight_grams<0",
        "Optional unknown weight stays null; negative weight is invalid.",
    )
    add(
        "PACKAGE_SHIPMENT_ORPHAN",
        "SELECT count(*) FROM canonical_package p LEFT JOIN canonical_shipment s ON p.shipment_id=s.canonical_id WHERE s.canonical_id IS NULL AND NOT p.is_deleted",
        "Accepted package membership requires a known shipment.",
    )
    add(
        "SHIPMENT_ORDER_ORPHAN",
        "SELECT count(*) FROM canonical_shipment s LEFT JOIN canonical_order o ON s.order_id=o.canonical_id WHERE o.canonical_id IS NULL AND NOT s.is_deleted",
        "Active shipments require a known order.",
    )
    add(
        "EVENT_SHIPMENT_ORPHAN",
        "SELECT count(*) FROM canonical_delivery_event e LEFT JOIN canonical_shipment s ON e.shipment_id=s.canonical_id WHERE s.canonical_id IS NULL",
        "Delivery observations require accepted shipment identity.",
    )
    add(
        "COST_SHIPMENT_ORPHAN",
        "SELECT count(*) FROM canonical_transportation_cost c LEFT JOIN canonical_shipment s ON c.shipment_id=s.canonical_id WHERE s.canonical_id IS NULL AND NOT c.is_deleted",
        "Financial postings require accepted shipment identity.",
    )
    add(
        "COST_CURRENCY",
        "SELECT count(*) FROM fact_transportation_cost WHERE currency<>'EUR' OR currency IS NULL",
        "No implicit conversion or currency substitution.",
    )
    add(
        "COST_CANONICAL_RECONCILIATION",
        "SELECT abs((SELECT coalesce(sum(amount_minor),0) FROM canonical_transportation_cost WHERE NOT is_deleted)-(SELECT coalesce(sum(amount_minor),0) FROM fact_transportation_cost))",
        "Current accepted signed cost amount equals mart amount, in EUR minor units.",
    )
    add(
        "SHIPMENT_COUNT_RECONCILIATION",
        "SELECT abs((SELECT count(*) FROM canonical_shipment)-(SELECT count(*) FROM fact_shipment))",
        "Shipment fact includes tombstones; SSOT filters them explicitly.",
    )
    add(
        "EVENT_COUNT_RECONCILIATION",
        "SELECT abs((SELECT count(*) FROM canonical_delivery_event)-(SELECT count(*) FROM fact_delivery_event))",
        "Event projection must not multiply observations through temporal joins.",
    )
    add(
        "INVALID_STATE_TRANSITION",
        "SELECT count(*) FROM invalid_state_transition",
        "Explicit returned after delivered is allowed; terminal regression is an error.",
    )
    add(
        "PENDING_REFERENCE",
        "SELECT count(*) FROM pending_fact_reference",
        "Identity is known but a descriptive dimension is absent at business time; products disclose degraded quality.",
        "WARN",
    )
    return checks
