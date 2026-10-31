"""Deep publication contracts independent of the incident scenario runner."""

from __future__ import annotations

import copy
import json
from pathlib import Path
from typing import Any

import duckdb
import pytest

from logistics_platform.contracts import normalize
from logistics_platform.generator import (
    empty_records,
    envelope,
    incremental_records,
    synthetic_records,
    write_batch,
)
from logistics_platform.keys import canonical_id, iso
from logistics_platform.pipeline import Pipeline
from logistics_platform.resolution import Resolver

BUSINESS_TABLES = (
    "canonical_order",
    "canonical_package",
    "canonical_shipment",
    "canonical_delivery_event",
    "canonical_transportation_cost",
    "fact_shipment",
    "fact_delivery_event",
    "fact_transportation_cost",
    "dim_reference",
)
LINEAGE_COLUMNS = {"run_id", "source_run_id", "raw_hash", "recorded_at"}


def rows(database: Path, query: str) -> list[tuple[Any, ...]]:
    with duckdb.connect(str(database), read_only=True) as connection:
        return connection.execute(query).fetchall()


def business_snapshot(database: Path) -> dict[str, list[tuple[Any, ...]]]:
    with duckdb.connect(str(database), read_only=True) as connection:
        result = {}
        for table in BUSINESS_TABLES:
            columns = [
                column[1]
                for column in connection.execute(f"PRAGMA table_info('{table}')").fetchall()
                if column[1] not in LINEAGE_COLUMNS
            ]
            projection = ",".join(f'"{column}"' for column in columns)
            result[table] = sorted(
                connection.execute(f"SELECT {projection} FROM {table}").fetchall(), key=repr
            )
        return result


def publish_baseline(tmp_path: Path) -> tuple[Pipeline, Path, dict[str, list[dict[str, Any]]]]:
    records = synthetic_records(6, seed=81)
    manifest = write_batch(tmp_path / "inputs", "initial", 1, records)
    database = tmp_path / "warehouse.duckdb"
    pipeline = Pipeline(database)
    result = pipeline.run(manifest)
    assert result["status"] == "published", result.get("error")
    return pipeline, database, records


def test_full_replay_matches_incremental_business_state(tmp_path: Path) -> None:
    full = synthetic_records(6, seed=81)
    incremental = incremental_records(full)
    incremental_database = tmp_path / "incremental.duckdb"
    pipeline = Pipeline(incremental_database)
    first = write_batch(tmp_path / "incremental", "initial", 1, full)
    second = write_batch(
        tmp_path / "incremental", "changes", 2, incremental, {"carrier_delivery": 2}
    )
    assert pipeline.run(first)["status"] == "published"
    assert pipeline.run(second)["status"] == "published"
    all_history = {source: full[source] + incremental[source] for source in full}
    replay_database = tmp_path / "replay.duckdb"
    replay = write_batch(tmp_path / "replay", "complete-history", 1, all_history)
    assert Pipeline(replay_database).run(replay)["status"] == "published"
    assert business_snapshot(replay_database) == business_snapshot(incremental_database)


def test_alias_intervals_are_half_open_source_scoped_and_exact() -> None:
    mappings = []
    for identity, target, start, end in [
        ("OLD", "CAR-AP", "2026-01-01T00:00:00Z", "2026-01-03T00:00:00Z"),
        ("NEW", "CAR-MF", "2026-01-03T00:00:00Z", "9999-12-31T00:00:00Z"),
    ]:
        record = envelope(
            "alias",
            f"REF-{identity}",
            identity,
            {
                "target_type": "carrier",
                "source": "carrier_delivery",
                "alias": "ASTER-EXP",
                "target_key": target,
                "effective_to": end,
                "rule_id": f"RULE-{identity}",
            },
            effective_at=start,
        )
        mappings.append((normalize("reference_master", record), {}))
    resolver = Resolver(mappings)
    assert resolver.conflicts() == []
    assert resolver.resolve(
        "carrier", "carrier_delivery", " aster-exp ", iso("2026-01-02T23:59:59Z"), "event1"
    ) == canonical_id("carrier", "CAR-AP")
    assert resolver.resolve(
        "carrier", "carrier_delivery", "ASTER-EXP", iso("2026-01-03T00:00:00Z"), "event2"
    ) == canonical_id("carrier", "CAR-MF")
    assert (
        resolver.resolve(
            "carrier", "carrier_delivery", "ASTER EXP", iso("2026-01-03T00:00:00Z"), "event3"
        )
        is None
    )
    assert (
        resolver.resolve(
            "carrier", "transport_costs", "ASTER-EXP", iso("2026-01-03T00:00:00Z"), "cost1"
        )
        is None
    )
    assert resolver.decisions[0]["rules"] == ["RULE-OLD"]
    assert resolver.decisions[1]["rules"] == ["RULE-NEW"]
    assert all(decision["outcome"] == "unresolved" for decision in resolver.decisions[2:])


def test_identical_rerun_preserves_publication_history_but_records_attempt(tmp_path: Path) -> None:
    pipeline, database, _ = publish_baseline(tmp_path)
    manifest = tmp_path / "inputs" / "initial" / "manifest.json"
    before = business_snapshot(database)
    history = rows(
        database, "SELECT * FROM publication_history ORDER BY run_id,table_name,row_hash"
    )
    evidence = rows(
        database, "SELECT * FROM evidence ORDER BY source_system,entity_type,source_id,revision"
    )
    result = pipeline.run(manifest)
    assert result["status"] == "noop" and result["watermark"] == 1
    assert business_snapshot(database) == before
    assert (
        rows(database, "SELECT * FROM publication_history ORDER BY run_id,table_name,row_hash")
        == history
    )
    assert (
        rows(
            database, "SELECT * FROM evidence ORDER BY source_system,entity_type,source_id,revision"
        )
        == evidence
    )
    assert rows(database, "SELECT count(*) FROM pipeline_runs") == [(2,)]
    assert rows(database, "SELECT count(*) FROM published_batches") == [(1,)]


@pytest.mark.parametrize(
    ("case", "expected_rule"),
    [
        ("same_revision_payload", "SRC_VERSION_CONFLICT"),
        ("naive_timestamp", "CAN_TIMESTAMP"),
        ("invalid_country", "REF_INVALID_COUNTRY"),
        ("wrong_currency", "REC_COST_RAW"),
        ("wrong_financial_control", "REC_COST_RAW"),
        ("wrong_checksum", "ING_CHECKSUM"),
        ("missing_required_source", "ING_REQUIRED_SOURCES"),
    ],
)
def test_invalid_candidate_preserves_prior_publication(
    tmp_path: Path,
    case: str,
    expected_rule: str,
) -> None:
    pipeline, database, full = publish_baseline(tmp_path)
    before = business_snapshot(database)
    prior_history = rows(database, "SELECT count(*) FROM publication_history")
    prior_evidence = rows(database, "SELECT count(*) FROM evidence")
    changed = empty_records()
    if case == "same_revision_payload":
        changed["transport_costs"] = [dict(full["transport_costs"][0], amount_minor=999999)]
    elif case == "naive_timestamp":
        changed["carrier_delivery"] = [
            dict(
                full["carrier_delivery"][0],
                revision=2,
                occurred_at="2026-01-03T12:00:00",
            )
        ]
    elif case == "invalid_country":
        shipment = copy.deepcopy(
            next(row for row in full["shipment_ops"] if row["entity_type"] == "shipment")
        )
        shipment["revision"] = 2
        shipment["attributes"]["country_code"] = "ZZ"
        changed["shipment_ops"] = [shipment]
    elif case == "wrong_currency":
        changed["transport_costs"] = [dict(full["transport_costs"][0], revision=2, currency="USD")]
    elif case == "wrong_financial_control":
        changed["transport_costs"] = [dict(full["transport_costs"][0], revision=2)]
    manifest_path = write_batch(tmp_path / "candidate", "bad", 2, changed)
    manifest = json.loads(manifest_path.read_text())
    if case == "wrong_financial_control":
        manifest["controls"]["transport_costs"]["amount_minor"]["EUR"] += 1
    elif case == "wrong_checksum":
        manifest["files"][0]["sha256"] = "0" * 64
    elif case == "missing_required_source":
        manifest["required_sources"].remove("package_events")
    manifest_path.write_text(json.dumps(manifest))
    result = pipeline.run(manifest_path)
    assert result["status"] == "failed" and result["watermark"] == 1
    assert any(
        check["rule_id"] == expected_rule and check["status"] == "fail"
        for check in result["quality"]
    )
    assert business_snapshot(database) == before
    assert rows(database, "SELECT count(*) FROM publication_history") == prior_history
    assert rows(database, "SELECT count(*) FROM evidence") == prior_evidence


def test_operational_and_cost_tombstones_have_explicit_financial_effect(tmp_path: Path) -> None:
    pipeline, database, full = publish_baseline(tmp_path)
    prior_total = rows(database, "SELECT sum(amount_minor) FROM fact_transportation_cost")[0][0]
    shipment = next(row for row in full["shipment_ops"] if row["entity_type"] == "shipment")
    deleted_shipment = empty_records()
    deleted_shipment["shipment_ops"] = [
        dict(
            shipment,
            revision=2,
            operation="delete",
            updated_at="2026-01-07T10:00:00Z",
        )
    ]
    manifest = write_batch(tmp_path / "delete", "withdraw-shipment", 2, deleted_shipment)
    assert pipeline.run(manifest)["status"] == "published"
    assert rows(
        database, "SELECT is_deleted FROM fact_shipment WHERE business_key='SHP-000001'"
    ) == [(True,)]
    assert rows(database, "SELECT count(*) FROM fact_shipment") == [(6,)]
    assert rows(database, "SELECT sum(amount_minor) FROM fact_transportation_cost") == [
        (prior_total,)
    ]
    deleted_cost = empty_records()
    cost = full["transport_costs"][0]
    deleted_cost["transport_costs"] = [
        dict(
            cost,
            revision=2,
            operation="delete",
            updated_at="2026-01-08T10:00:00Z",
        )
    ]
    manifest = write_batch(tmp_path / "delete", "withdraw-cost", 3, deleted_cost)
    assert pipeline.run(manifest)["status"] == "published"
    assert rows(database, "SELECT sum(amount_minor) FROM fact_transportation_cost") == [
        (prior_total - cost["amount_minor"],)
    ]
    assert rows(
        database,
        "SELECT revision,operation FROM evidence WHERE source_system='transport_costs' "
        "AND source_id='COST-000001' ORDER BY revision",
    ) == [(1, "upsert"), (2, "delete")]
    assert (
        rows(
            database,
            "SELECT count(*) FROM publication_history WHERE table_name='fact_transportation_cost'",
        )[0][0]
        > 6
    )


def test_early_fact_keeps_known_identity_then_reattributes_late_dimension(tmp_path: Path) -> None:
    full = synthetic_records(6, seed=81)
    absent = next(row for row in full["reference_master"] if row["business_key"] == "CAR-AP")
    full["reference_master"].remove(absent)
    database = tmp_path / "early.duckdb"
    pipeline = Pipeline(database)
    initial = write_batch(tmp_path / "inputs", "early-fact", 1, full)
    result = pipeline.run(initial)
    assert result["status"] == "published", result.get("error")
    early = rows(
        database,
        "SELECT shipment_id,carrier_id,carrier_sk FROM fact_shipment "
        "WHERE business_key='SHP-000001'",
    )[0]
    assert early[1] == canonical_id("carrier", "CAR-AP")
    assert early[2] == 0
    assert rows(database, "SELECT count(*) FROM pending_fact_reference")[0][0] > 0
    late = empty_records()
    late["reference_master"] = [dict(absent, updated_at="2026-01-09T10:00:00Z")]
    delivery = write_batch(tmp_path / "inputs", "late-dimension", 2, late)
    assert pipeline.run(delivery)["status"] == "published"
    recovered = rows(
        database,
        "SELECT shipment_id,carrier_id,carrier_sk FROM fact_shipment "
        "WHERE business_key='SHP-000001'",
    )[0]
    assert recovered[:2] == early[:2] and recovered[2] != 0
    assert rows(database, "SELECT count(*) FROM pending_fact_reference") == [(0,)]
    assert rows(
        database,
        "SELECT count(DISTINCT run_id) FROM publication_history WHERE table_name='fact_shipment'",
    ) == [(2,)]
