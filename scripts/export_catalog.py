#!/usr/bin/env python3
"""Export documented product contracts with schemas read from a published DuckDB.

This writes a portable JSON catalogue. It does not create or update an AWS Glue
Data Catalog registry, and does not claim the local schemas were deployed remotely.
"""

from __future__ import annotations

import argparse
import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import duckdb

from logistics_platform.cloud import PRODUCT_KEYS

REFERENCE_ENTITIES = {"carrier", "facility", "route", "country", "region", "service_level"}
REFERENCE_DIMENSIONS = [f"dim_{entity}" for entity in sorted(REFERENCE_ENTITIES)]
CONTRACTS: dict[str, dict[str, Any]] = {
    "canonical_order": {
        "description": "Current accepted organization order identity and source attributes, including tombstones.",
        "grain": "One current canonical order per organization-scoped order business key.",
        "sources": ["shipment_ops", "reference_master"],
        "dependencies": ["evidence", "resolution_decisions"],
        "example_sql": "SELECT business_key,created_at,is_deleted FROM canonical_order ORDER BY created_at;",
    },
    "canonical_package": {
        "description": "Current accepted package identity, owning shipment, weight, and deletion status.",
        "grain": "One current canonical package per organization-scoped package business key.",
        "sources": ["shipment_ops"],
        "dependencies": ["evidence"],
        "example_sql": "SELECT shipment_id,count(*) AS active_packages FROM canonical_package WHERE NOT is_deleted GROUP BY shipment_id;",
    },
    "canonical_shipment": {
        "description": "Auditable shipment identity and structure with the selected supported state and actual delivery milestone.",
        "grain": "One current canonical shipment per organization-scoped shipment business key, including tombstones.",
        "sources": ["shipment_ops", "carrier_delivery", "package_events", "reference_master"],
        "dependencies": [
            "evidence",
            "resolution_decisions",
            "shipment_state_evidence",
            "shipment_state_selection",
        ],
        "example_sql": "SELECT business_key,state,state_source_system,state_observation_id,state_occurred_at FROM canonical_shipment;",
    },
    "canonical_delivery_event": {
        "description": "Current accepted interpretation of each source event; distinct observers remain separate observations.",
        "grain": "One current revision per (source_system, event_id), excluding withdrawn observations.",
        "sources": ["carrier_delivery", "package_events", "reference_master"],
        "dependencies": ["evidence", "resolution_decisions"],
        "example_sql": "SELECT source_system,state,count(*) AS observations FROM canonical_delivery_event GROUP BY source_system,state;",
    },
    "canonical_transportation_cost": {
        "description": "Current accepted interpretation of each cost posting, including explicit source tombstone flags.",
        "grain": "One current revision per transport_costs cost_id; signed integer EUR minor units.",
        "sources": ["transport_costs", "reference_master"],
        "dependencies": ["evidence", "resolution_decisions"],
        "example_sql": "SELECT currency,sum(amount_minor) AS current_amount_minor FROM canonical_transportation_cost WHERE NOT is_deleted GROUP BY currency;",
    },
    "fact_shipment": {
        "description": "Accumulating shipment snapshot with historical dimension attribution and selected state evidence.",
        "grain": "One row per canonical shipment, including withdrawn shipment identities.",
        "sources": ["shipment_ops", "carrier_delivery", "package_events", "reference_master"],
        "dependencies": [
            "canonical_shipment",
            "canonical_package",
            *REFERENCE_DIMENSIONS,
            "dim_date",
        ],
        "example_sql": "SELECT state,sum(shipment_count) AS shipments FROM fact_shipment WHERE NOT is_deleted GROUP BY state;",
    },
    "fact_delivery_event": {
        "description": "Current source event observations attributed to dimensions at the event's business occurrence time.",
        "grain": "One current accepted revision per namespaced source event ID; observers of one physical milestone remain separate.",
        "sources": ["carrier_delivery", "package_events", "shipment_ops", "reference_master"],
        "dependencies": [
            "canonical_delivery_event",
            "canonical_shipment",
            *REFERENCE_DIMENSIONS,
            "dim_date",
        ],
        "example_sql": "SELECT source_system,max(source_recording_delay_seconds) AS max_source_delay_seconds FROM fact_delivery_event GROUP BY source_system;",
    },
    "fact_transportation_cost": {
        "description": "Current accepted financial postings with temporal attribution; superseded/deleted contributions are excluded.",
        "grain": "One current nondeleted transport_costs cost_id, including separately identified signed credits.",
        "sources": ["transport_costs", "shipment_ops", "reference_master"],
        "dependencies": [
            "canonical_transportation_cost",
            "canonical_shipment",
            *REFERENCE_DIMENSIONS,
            "dim_date",
        ],
        "example_sql": "SELECT currency,sum(amount_minor) AS transportation_amount_minor FROM fact_transportation_cost GROUP BY currency;",
    },
    "ssot_shipments": {
        "description": "Analyst-facing active shipment state, milestones, readable references, and per-row quality status.",
        "grain": "One active canonical shipment; withdrawn shipments remain in the fact and audit history.",
        "sources": ["shipment_ops", "carrier_delivery", "package_events", "reference_master"],
        "dependencies": ["fact_shipment", *REFERENCE_DIMENSIONS],
        "example_sql": "SELECT carrier_name,count(*) AS delivered_shipments FROM ssot_shipments WHERE state='delivered' GROUP BY carrier_name;",
    },
    "ssot_delivery_events": {
        "description": "Analyst-facing source event observations, their recording delay, readable carrier, and quality status.",
        "grain": "One current accepted namespaced source event observation, regardless of receipt lateness.",
        "sources": ["carrier_delivery", "package_events", "shipment_ops", "reference_master"],
        "dependencies": ["fact_delivery_event", "dim_carrier"],
        "example_sql": "SELECT source_system,count(*) AS observations,count(DISTINCT shipment_id) AS shipments FROM ssot_delivery_events GROUP BY source_system;",
    },
    "ssot_transportation_costs": {
        "description": "Analyst-facing current EUR transportation postings with readable references and quality status.",
        "grain": "One current accepted nondeleted cost posting; aggregate costs before joining one-to-many event/package facts.",
        "sources": ["transport_costs", "shipment_ops", "reference_master"],
        "dependencies": [
            "fact_transportation_cost",
            "dim_carrier",
            "dim_route",
            "dim_service_level",
        ],
        "example_sql": "SELECT shipment_id,currency,sum(amount_minor) AS transportation_amount_minor FROM ssot_transportation_costs GROUP BY shipment_id,currency;",
    },
}


def reference_contract(name: str) -> dict[str, Any]:
    is_dimension = name.startswith("dim_")
    entity = name[4:] if is_dimension else name.removesuffix("_reference")
    if entity == "date":
        return {
            "description": "Calendar dates derived from accepted fact business timestamps, plus unknown date key zero.",
            "grain": "One calendar date; an additional surrogate-key-0 unknown member.",
            "sources": ["shipment_ops", "carrier_delivery", "package_events", "transport_costs"],
            "dependencies": ["fact_shipment", "fact_delivery_event", "fact_transportation_cost"],
            "example_sql": "SELECT date_sk,full_date FROM dim_date WHERE date_sk<>0 ORDER BY full_date;",
        }
    if entity not in REFERENCE_ENTITIES:
        raise ValueError(f"Missing catalogue contract for {name}")
    return {
        "description": (
            f"Historical {entity.replace('_', ' ')} attribution with explicit surrogate keys."
            if is_dimension
            else f"Reusable authoritative {entity.replace('_', ' ')} values with effective intervals, source provenance, and status."
        ),
        "grain": (
            f"One business-valid version per canonical {entity} and effective interval; display corrections use Type 1. "
            + (
                "Includes surrogate-key-0 unknown member."
                if is_dimension
                else "Excludes the unknown sentinel row."
            )
        ),
        "sources": ["reference_master"],
        "dependencies": ["evidence", "dim_reference"],
        "example_sql": (
            f'SELECT * FROM "{name}" WHERE "{PRODUCT_KEYS[name]}"<>0 ORDER BY "{PRODUCT_KEYS[name]}";'
        ),
    }


def _utc_text(value: datetime) -> str:
    return value.replace(tzinfo=UTC).isoformat().replace("+00:00", "Z")


def export_catalog(database: Path, output: Path) -> dict[str, Any]:
    """Read a consistent committed warehouse snapshot and export all 24 contracts."""
    database, output = Path(database), Path(output)
    if not database.is_file():
        raise ValueError(f"Published warehouse does not exist: {database}")
    with duckdb.connect(str(database), read_only=True) as connection:
        connection.execute("BEGIN TRANSACTION")
        published = connection.execute(
            "SELECT batch_id,ingest_sequence,run_id,published_at FROM published_batches "
            "ORDER BY ingest_sequence DESC LIMIT 1"
        ).fetchone()
        if published is None:
            raise ValueError(
                "No successfully published batch exists; cannot claim product freshness"
            )
        batch_id, sequence, run_id, published_at = published
        quality_rows = connection.execute(
            "SELECT rule_id,severity,status FROM quality_results WHERE run_id=? ORDER BY rule_id",
            [run_id],
        ).fetchall()
        if not quality_rows:
            raise ValueError("Published run has no quality evidence")
        blocking = [
            rule
            for rule, severity, status in quality_rows
            if severity == "error" and status == "fail"
        ]
        warnings = [
            rule
            for rule, severity, status in quality_rows
            if severity == "warning" and status == "fail"
        ]
        latest_attempt = connection.execute(
            "SELECT run_id,batch_id,status,json_extract_string(report,'$.ended_at') "
            "FROM pipeline_runs ORDER BY json_extract_string(report,'$.ended_at') DESC LIMIT 1"
        ).fetchone()
        freshness = {
            "mode": "complete-batch publication",
            "published_batch_id": batch_id,
            "published_ingest_sequence": sequence,
            "published_run_id": run_id,
            "published_at_utc": _utc_text(published_at),
            "description": "Freshness is the last committed input batch, not a running service SLA or source event time.",
        }
        products = []
        for name, key in PRODUCT_KEYS.items():
            contract = CONTRACTS.get(name) or reference_contract(name)
            physical = connection.execute(
                "SELECT column_name,data_type,is_nullable,ordinal_position "
                "FROM information_schema.columns WHERE table_schema='main' AND table_name=? "
                "ORDER BY ordinal_position",
                [name],
            ).fetchall()
            if not physical:
                raise ValueError(f"Published product is missing: {name}")
            columns = [
                {
                    "name": column,
                    "data_type": data_type,
                    "nullable": nullable == "YES",
                    "ordinal_position": position,
                }
                for column, data_type, nullable, position in physical
            ]
            if key not in {column["name"] for column in columns}:
                raise ValueError(f"Declared product key {key} missing from {name}")
            object_type = connection.execute(
                "SELECT table_type FROM information_schema.tables WHERE table_schema='main' AND table_name=?",
                [name],
            ).fetchone()[0]
            row_count = connection.execute(f'SELECT count(*) FROM "{name}"').fetchone()[0]
            row_quality: dict[str, int] | None = None
            if "quality_status" in {column["name"] for column in columns}:
                row_quality = dict(
                    connection.execute(
                        f'SELECT quality_status,count(*) FROM "{name}" GROUP BY quality_status ORDER BY quality_status'
                    ).fetchall()
                )
            owner = (
                "Reference Steward"
                if name.endswith("_reference") or name.startswith("dim_")
                else "Logistics Data Platform"
            )
            if name == "dim_date":
                owner = "Logistics Data Platform"
            products.append(
                {
                    "name": name,
                    "object_type": object_type,
                    "description": contract["description"],
                    "grain": contract["grain"],
                    "declared_unique_key": [key],
                    "key_enforcement": "Pipeline quality checks; this field does not assert a physical PRIMARY KEY constraint.",
                    "owner": owner,
                    "freshness": freshness,
                    "quality": {
                        "publication_status": "FAILED"
                        if blocking
                        else "PASSED_WITH_WARNINGS"
                        if warnings
                        else "PASSED",
                        "blocking_failures": blocking,
                        "warnings": warnings,
                        "row_status_counts": row_quality,
                        "meaning": "Recorded checks for the committed run; source truth beyond synthetic evidence is not asserted.",
                    },
                    "lineage": {
                        "source_systems": contract["sources"],
                        "raw": "Immutable manifested objects, identified by source and SHA-256 in file_receipts/evidence.",
                        "upstream_objects": contract["dependencies"],
                        "audit_objects": [
                            "evidence",
                            "resolution_decisions",
                            "publication_history",
                            "pipeline_runs",
                        ],
                        "downstream_consumers": [
                            "Operations analysts",
                            "Finance analysts",
                            "Downstream data engineers",
                        ],
                    },
                    "row_count": row_count,
                    "schema": columns,
                    "example_sql": contract["example_sql"],
                }
            )
        connection.execute("COMMIT")
    result = {
        "catalog_version": "1.0",
        "project": "Multi-Carrier Logistics Canonical Data Platform",
        "generated_at_utc": datetime.now(UTC).isoformat().replace("+00:00", "Z"),
        "execution_profile": "local DuckDB published warehouse",
        "registration": "Portable JSON catalogue only; no automatic AWS Glue Data Catalog registration.",
        "physical_nullability_note": "Nullability is read from DuckDB metadata. Nullable CTAS/view columns may have stronger semantic checks in the pipeline; no unobserved NOT NULL constraint is invented.",
        "latest_attempt": dict(
            zip(["run_id", "batch_id", "status", "ended_at_utc"], latest_attempt, strict=True)
        )
        if latest_attempt
        else None,
        "product_count": len(products),
        "products": products,
    }
    output.parent.mkdir(parents=True, exist_ok=True)
    temporary = output.with_name(output.name + ".tmp")
    temporary.write_text(json.dumps(result, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
    temporary.replace(output)
    return result


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--database", required=True, type=Path, help="Existing published local DuckDB file"
    )
    parser.add_argument("--output", type=Path, default=Path("docs/catalog.json"))
    args = parser.parse_args()
    try:
        catalog = export_catalog(args.database, args.output)
    except (ValueError, OSError, duckdb.Error) as exc:
        parser.exit(2, f"Catalog export failed: {exc}\n")
    print(json.dumps({"output": str(args.output), "product_count": catalog["product_count"]}))


if __name__ == "__main__":
    main()
