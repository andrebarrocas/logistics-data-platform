"""A source-update clock must not change a creation-time identity lookup."""

from pathlib import Path

from logistics_platform.generator import envelope, synthetic_records, write_batch
from logistics_platform.keys import canonical_id
from logistics_platform.pipeline import Pipeline, connect


def test_order_and_shipment_country_alias_uses_creation_time(tmp_path: Path) -> None:
    records = synthetic_records(1)
    for record in records["shipment_ops"]:
        if record["entity_type"] in {"shipment", "order"}:
            record["attributes"]["country_code"] = "LOCAL-COUNTRY"
            # The record's current operational state/update occurs after the alias
            # changes, but its order/shipment identity was established beforehand.
            record["effective_at"] = "2026-01-04T00:00:00Z"
            record["updated_at"] = "2026-01-04T00:00:00Z"
    for revision, target, start, end in (
        (1, "PT", "2026-01-01T00:00:00Z", "2026-01-03T00:00:00Z"),
        (2, "ES", "2026-01-03T00:00:00Z", "9999-12-31T00:00:00Z"),
    ):
        records["reference_master"].append(
            envelope(
                "alias",
                "COUNTRY-TIMELINE",
                "ALIAS-COUNTRY-TIMELINE",
                {
                    "target_type": "country",
                    "source": "shipment_ops",
                    "alias": "LOCAL-COUNTRY",
                    "target_key": target,
                    "effective_to": end,
                    "rule_id": f"COUNTRY-CLOCK-{revision}",
                },
                revision=revision,
                effective_at=start,
            )
        )
    manifest = write_batch(tmp_path / "input", "country-clock", 1, records)
    database = tmp_path / "warehouse.duckdb"
    report = Pipeline(database).run(manifest)
    assert report["status"] == "published", report
    connection = connect(database)
    try:
        for table in ("stg_order", "stg_shipment"):
            assert connection.execute(f"SELECT country_id FROM {table}").fetchall() == [
                (canonical_id("country", "PT"),)
            ]
    finally:
        connection.close()


def test_crosswalk_cannot_redefine_a_controlled_country_key(tmp_path: Path) -> None:
    records = synthetic_records(1)
    records["reference_master"].append(
        envelope(
            "alias",
            "COUNTRY-CONTRADICTION",
            "ALIAS-COUNTRY-CONTRADICTION",
            {
                "target_type": "country",
                "source": "shipment_ops",
                "alias": "PT",
                "target_key": "ES",
                "effective_to": "9999-12-31T00:00:00Z",
                "rule_id": "CONTRADICTORY-COUNTRY",
            },
        )
    )
    manifest = write_batch(tmp_path / "input", "country-conflict", 1, records)
    database = tmp_path / "warehouse.duckdb"
    report = Pipeline(database).run(manifest)
    assert report["status"] == "failed" and report["watermark"] == 0
    assert any(
        result["rule_id"] == "CAN_RELATION_CONFLICT" and result["status"] == "fail"
        for result in report["quality"]
    )
    with connect(database) as connection:
        assert connection.execute("SELECT count(*) FROM evidence").fetchone()[0] == 0
