"""Business-invariant SQL tests on deterministic synthetic observations."""

from __future__ import annotations

import json
from collections.abc import Iterator
from typing import Any

import duckdb
import pytest

from logistics_platform.keys import canonical_id, surrogate_key
from logistics_platform.warehouse import create_staging, project, quality_checks


@pytest.fixture
def db() -> Iterator[duckdb.DuckDBPyConnection]:
    connection = duckdb.connect()
    connection.create_function("canonical_id", canonical_id, ["VARCHAR", "VARCHAR"], "VARCHAR")
    connection.create_function(
        "surrogate_key", surrogate_key, ["VARCHAR", "VARCHAR", "VARCHAR"], "BIGINT"
    )
    create_staging(connection)
    yield connection
    connection.close()


def insert(db: duckdb.DuckDBPyConnection, table: str, **values: Any) -> None:
    values.setdefault("raw_hash", "synthetic-test-object")
    values.setdefault("run_id", "test-run")
    columns = ",".join(values)
    placeholders = ",".join("?" for _ in values)
    db.execute(
        f"INSERT INTO stg_{table} ({columns}) VALUES ({placeholders})", list(values.values())
    )


def reference(
    db: duckdb.DuckDBPyConnection,
    entity: str,
    key: str,
    name: str,
    start: str = "2025-01-01",
    revision: int = 1,
    updated: str = "2025-01-01",
    **attributes: Any,
) -> str:
    identity = canonical_id(entity, key)
    insert(
        db,
        "reference",
        entity_type=entity,
        business_key=key,
        canonical_id=identity,
        canonical_value=name,
        attributes=json.dumps({"name": name, **attributes}),
        effective_from=start,
        revision=revision,
        source_updated_at=updated,
        status="ACTIVE",
        source_id=f"{entity}:{key}:{start}",
    )
    return identity


def seed(db: duckdb.DuckDBPyConnection) -> dict[str, str]:
    ids = {
        "country": reference(db, "country", "PT", "Portugal"),
        "region": reference(db, "region", "REG-WEST", "Western region"),
        "service": reference(
            db, "service_level", "SVC-STANDARD", "Standard", target_business_days=3
        ),
        "carrier": reference(db, "carrier", "CAR-ASTER", "Aster Parcel", operating_status="active"),
        "origin": reference(
            db, "facility", "FAC-A", "Synthetic A", country_key="PT", region_key="REG-WEST"
        ),
        "destination": reference(
            db, "facility", "FAC-B", "Synthetic B", country_key="PT", region_key="REG-WEST"
        ),
        "route": reference(
            db,
            "route",
            "RTE-AB",
            "A to B",
            origin_facility_key="FAC-A",
            destination_facility_key="FAC-B",
            distance_km=120,
        ),
        "order": canonical_id("order", "ORD-1"),
        "shipment": canonical_id("shipment", "SHP-1"),
        "package": canonical_id("package", "PKG-1"),
    }
    insert(
        db,
        "order",
        canonical_id=ids["order"],
        business_key="ORD-1",
        created_at="2025-01-03",
        country_id=ids["country"],
        is_deleted=False,
        source_id="o1",
        revision=1,
    )
    insert(
        db,
        "shipment",
        canonical_id=ids["shipment"],
        business_key="SHP-1",
        order_id=ids["order"],
        carrier_id=ids["carrier"],
        origin_facility_id=ids["origin"],
        destination_facility_id=ids["destination"],
        route_id=ids["route"],
        country_id=ids["country"],
        service_level_id=ids["service"],
        created_at="2025-01-03",
        promised_at="2025-01-08",
        state="in_transit",
        is_deleted=False,
        source_id="s1",
        revision=1,
        updated_at="2025-01-04",
    )
    insert(
        db,
        "package",
        canonical_id=ids["package"],
        business_key="PKG-1",
        shipment_id=ids["shipment"],
        weight_grams=1000,
        is_deleted=False,
        source_id="p1",
        revision=1,
    )
    return ids


def event(
    db: duckdb.DuckDBPyConnection,
    ids: dict[str, str],
    key: str = "e1",
    state: str = "delivered",
    occurred: str = "2025-01-06",
    source: str = "carrier_delivery",
    verified: bool = True,
    package: bool = False,
) -> None:
    insert(
        db,
        "event",
        source_system=source,
        event_id=key,
        event_canonical_id=canonical_id("event", f"{source}:{key}"),
        shipment_id=ids["shipment"],
        package_id=ids["package"] if package else None,
        carrier_id=ids["carrier"],
        state=state,
        occurred_at=occurred,
        source_updated_at="2025-01-10",
        verified=verified,
        revision=1,
    )


def test_empty_projection_has_unknown_members_and_structured_checks(
    db: duckdb.DuckDBPyConnection,
) -> None:
    project(db, "empty")
    assert db.execute("SELECT count(*) FROM dim_reference WHERE surrogate_key=0").fetchone()[0] == 6
    assert all(result["passed"] for result in quality_checks(db))


def test_complete_projection_reconciles_and_keeps_grains(db: duckdb.DuckDBPyConnection) -> None:
    ids = seed(db)
    event(db, ids)
    for key, amount in (("charge", 2400), ("credit", -200)):
        insert(
            db,
            "cost",
            cost_id=key,
            cost_canonical_id=canonical_id("cost", key),
            shipment_id=ids["shipment"],
            carrier_id=ids["carrier"],
            amount_minor=amount,
            currency="EUR",
            cost_type="linehaul",
            incurred_at="2025-01-05",
            revision=1,
            source_updated_at="2025-01-07",
            is_deleted=False,
        )
    project(db, "complete")
    assert (
        db.execute("SELECT sum(amount_minor) FROM ssot_transportation_costs").fetchone()[0] == 2200
    )
    assert db.execute("SELECT count(*),sum(package_count) FROM ssot_shipments").fetchone() == (1, 1)
    assert all(result["passed"] for result in quality_checks(db))


def test_type1_label_correction_does_not_create_scd_version(db: duckdb.DuckDBPyConnection) -> None:
    seed(db)
    reference(
        db,
        "carrier",
        "CAR-ASTER",
        "Aster Parcel corrected",
        "2025-01-10",
        updated="2025-01-11",
        operating_status="active",
    )
    project(db, "labels")
    assert db.execute(
        "SELECT count(*),min(carrier_name) FROM dim_carrier WHERE carrier_sk<>0"
    ).fetchone() == (1, "Aster Parcel corrected")


def test_scd2_gap_islands_collapse_unchanged_attributes(db: duckdb.DuckDBPyConnection) -> None:
    seed(db)
    reference(db, "carrier", "CAR-ASTER", "Aster Parcel", "2025-01-05", operating_status="active")
    reference(
        db, "carrier", "CAR-ASTER", "Aster Parcel", "2025-01-10", operating_status="suspended"
    )
    reference(
        db, "carrier", "CAR-ASTER", "Aster Parcel", "2025-01-15", operating_status="suspended"
    )
    reference(db, "carrier", "CAR-ASTER", "Aster Parcel", "2025-01-20", operating_status="active")
    project(db, "islands")
    rows = db.execute(
        "SELECT effective_from::DATE,effective_to::DATE,operating_status FROM dim_carrier WHERE carrier_sk<>0 ORDER BY effective_from"
    ).fetchall()
    assert len(rows) == 3
    assert str(rows[0][1]) == "2025-01-10"
    assert str(rows[1][1]) == "2025-01-20"
    assert rows[2][2] == "active"


def test_half_open_boundary_and_backdated_fact_restatement(db: duckdb.DuckDBPyConnection) -> None:
    ids = seed(db)
    event(db, ids, occurred="2025-01-05")
    project(db, "before-correction")
    old_region = db.execute("SELECT region_sk FROM fact_delivery_event").fetchone()[0]
    reference(db, "region", "REG-EAST", "Eastern region")
    reference(
        db,
        "facility",
        "FAC-A",
        "Synthetic A",
        "2025-01-05",
        updated="2025-02-01",
        country_key="PT",
        region_key="REG-EAST",
    )
    project(db, "after-correction")
    event_region = db.execute("SELECT region_sk FROM fact_delivery_event").fetchone()[0]
    shipment_region = db.execute("SELECT region_sk FROM fact_shipment").fetchone()[0]
    assert event_region != old_region
    assert shipment_region == old_region
    assert db.execute("SELECT count(*) FROM fact_delivery_event").fetchone()[0] == 1
    assert all(check["passed"] for check in quality_checks(db))


def test_highest_revision_corrects_same_effective_boundary(db: duckdb.DuckDBPyConnection) -> None:
    seed(db)
    reference(
        db,
        "carrier",
        "CAR-ASTER",
        "Aster Parcel",
        revision=2,
        updated="2025-02-01",
        operating_status="corrected",
    )
    project(db, "corrected")
    assert db.execute(
        "SELECT count(*),min(operating_status) FROM dim_carrier WHERE carrier_sk<>0"
    ).fetchone() == (1, "corrected")


def test_late_reference_replaces_unknown_member_at_business_time(
    db: duckdb.DuckDBPyConnection,
) -> None:
    seed(db)
    db.execute("DELETE FROM stg_reference WHERE entity_type='carrier'")
    project(db, "missing")
    assert db.execute("SELECT carrier_sk FROM fact_shipment").fetchone()[0] == 0
    assert (
        db.execute("SELECT quality_status FROM ssot_shipments").fetchone()[0]
        == "DEGRADED_PENDING_REFERENCE"
    )
    reference(db, "carrier", "CAR-ASTER", "Aster Parcel", "2025-01-01", operating_status="active")
    reference(db, "carrier", "CAR-ASTER", "Aster Parcel", "2025-01-10", operating_status="inactive")
    project(db, "repaired")
    result = db.execute(
        "SELECT c.operating_status FROM fact_shipment f JOIN dim_carrier c USING(carrier_sk)"
    ).fetchone()[0]
    assert result == "active"
    assert db.execute("SELECT count(*) FROM pending_fact_reference").fetchone()[0] == 0


def test_verified_state_wins_and_retraction_removes_delivery_time(
    db: duckdb.DuckDBPyConnection,
) -> None:
    ids = seed(db)
    event(db, ids)
    project(db, "delivered")
    state, milestone = db.execute(
        "SELECT state,actual_delivery_at FROM canonical_shipment"
    ).fetchone()
    assert state == "delivered" and milestone is not None
    db.execute("UPDATE stg_event SET revision=2,state='in_transit'")
    project(db, "retracted")
    state, milestone = db.execute(
        "SELECT state,actual_delivery_at FROM canonical_shipment"
    ).fetchone()
    assert state == "in_transit" and milestone is None
    assert db.execute("SELECT count(*) FROM shipment_state_evidence").fetchone()[0] == 2


def test_equal_authority_conflict_is_blocking_quality_failure(
    db: duckdb.DuckDBPyConnection,
) -> None:
    ids = seed(db)
    event(db, ids, "e1", "delivered")
    event(db, ids, "e2", "in_transit")
    project(db, "conflict")
    check = next(c for c in quality_checks(db) if c["check_id"] == "STATE_AUTHORITY_TIE")
    assert check["observed"] == 1 and not check["passed"] and check["severity"] == "ERROR"


def test_partial_package_delivery_does_not_confirm_whole_shipment(
    db: duckdb.DuckDBPyConnection,
) -> None:
    ids = seed(db)
    event(db, ids, package=True)
    project(db, "partial-package")
    assert db.execute("SELECT state,actual_delivery_at FROM canonical_shipment").fetchone() == (
        "in_transit",
        None,
    )
    assert db.execute("SELECT count(*) FROM fact_delivery_event").fetchone()[0] == 1


def test_source_event_terminal_regression_is_detected(db: duckdb.DuckDBPyConnection) -> None:
    ids = seed(db)
    event(db, ids, "e1", "delivered", "2025-01-05")
    event(db, ids, "e2", "in_transit", "2025-01-06")
    project(db, "bad-transition")
    assert db.execute("SELECT count(*) FROM invalid_state_transition").fetchone()[0] == 1


def test_rerun_preserves_surrogate_keys_and_business_results(db: duckdb.DuckDBPyConnection) -> None:
    ids = seed(db)
    event(db, ids)
    project(db, "first")
    before = db.execute(
        "SELECT shipment_id,state,carrier_sk,origin_facility_sk,route_sk FROM fact_shipment"
    ).fetchall()
    project(db, "second")
    after = db.execute(
        "SELECT shipment_id,state,carrier_sk,origin_facility_sk,route_sk FROM fact_shipment"
    ).fetchall()
    assert before == after


def test_reference_attribute_validation_rejects_fractional_service_days(
    db: duckdb.DuckDBPyConnection,
) -> None:
    seed(db)
    db.execute(
        "UPDATE stg_reference SET attributes=? WHERE entity_type='service_level'",
        [json.dumps({"target_business_days": 1.5})],
    )
    project(db, "invalid-reference")
    check = next(c for c in quality_checks(db) if c["check_id"] == "REFERENCE_ATTRIBUTE_VALUE")
    assert not check["passed"] and check["observed"] == 1


def test_reference_country_and_region_relationships_are_enforced(
    db: duckdb.DuckDBPyConnection,
) -> None:
    seed(db)
    db.execute("DELETE FROM stg_reference WHERE entity_type IN ('country','region')")
    project(db, "missing-reference-parents")
    checks = {c["check_id"]: c for c in quality_checks(db)}
    assert checks["REFERENCE_COUNTRY_ORPHAN"]["observed"] == 2
    assert checks["REFERENCE_REGION_ORPHAN"]["observed"] == 2
    assert checks["PENDING_REFERENCE"]["severity"] == "WARN"


def test_asof_quality_rejects_existing_but_wrong_historical_key(
    db: duckdb.DuckDBPyConnection,
) -> None:
    ids = seed(db)
    event(db, ids)
    reference(db, "carrier", "CAR-ASTER", "Aster Parcel", "2025-01-10", operating_status="inactive")
    project(db, "bad-key")
    future_key = db.execute(
        "SELECT carrier_sk FROM dim_carrier WHERE effective_from=TIMESTAMP '2025-01-10'"
    ).fetchone()[0]
    db.execute("UPDATE fact_delivery_event SET carrier_sk=?", [future_key])
    checks = {c["check_id"]: c for c in quality_checks(db)}
    assert checks["FK_FACT_DELIVERY_EVENT_CARRIER_SK"]["passed"]
    assert not checks["ASOF_EVENT_CARRIER_SK"]["passed"]


def test_role_playing_date_foreign_key_is_checked(db: duckdb.DuckDBPyConnection) -> None:
    seed(db)
    project(db, "bad-date")
    db.execute("UPDATE fact_shipment SET promised_date_sk=123")
    check = next(
        c for c in quality_checks(db) if c["check_id"] == "DATE_FK_FACT_SHIPMENT_PROMISED_DATE_SK"
    )
    assert not check["passed"]


def test_reversed_route_endpoints_are_not_equivalent(db: duckdb.DuckDBPyConnection) -> None:
    seed(db)
    db.execute(
        "UPDATE stg_shipment SET origin_facility_id=destination_facility_id,destination_facility_id=origin_facility_id"
    )
    project(db, "wrong-route")
    check = next(c for c in quality_checks(db) if c["check_id"] == "ROUTE_ENDPOINT_DISAGREEMENT")
    assert not check["passed"]


def test_operational_delivery_milestone_uses_state_effective_time(
    db: duckdb.DuckDBPyConnection,
) -> None:
    seed(db)
    db.execute("""UPDATE stg_shipment SET state='delivered',
        state_occurred_at=TIMESTAMP '2025-01-07 16:30:00',
        updated_at=TIMESTAMP '2025-01-10 09:00:00'""")
    project(db, "operational-delivery")
    result = db.execute("""SELECT created_at,state_occurred_at,actual_delivery_at,
        operational_state_occurred_at,state_source_system FROM canonical_shipment""").fetchone()
    assert result is not None
    assert str(result[0]) == "2025-01-03 00:00:00"
    assert str(result[1]) == "2025-01-07 16:30:00"
    assert result[1] == result[2] == result[3]
    assert result[4] == "shipment_ops"
    assert db.execute("SELECT delivered_date_sk FROM fact_shipment").fetchone()[0] == 20250107
    assert all(check["passed"] for check in quality_checks(db))


def test_staging_additive_migration_preserves_new_state_time_column(
    db: duckdb.DuckDBPyConnection,
) -> None:
    db.execute("ALTER TABLE stg_shipment DROP COLUMN state_occurred_at")
    create_staging(db)
    columns = [row[1] for row in db.execute("PRAGMA table_info('stg_shipment')").fetchall()]
    assert "state_occurred_at" in columns
