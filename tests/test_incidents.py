"""Incident tests prove publication isolation, recovery, and safe repeat execution."""

from pathlib import Path
from typing import Any

import duckdb
import pytest

from logistics_platform.incidents import SCENARIOS, create_incident
from logistics_platform.pipeline import Pipeline


def snapshot(database: Path) -> dict[str, list[tuple[Any, ...]]]:
    with duckdb.connect(str(database), read_only=True) as connection:
        return {
            table: sorted(connection.execute(f"SELECT * FROM {table}").fetchall(), key=repr)
            for table in (
                "fact_shipment",
                "fact_delivery_event",
                "fact_transportation_cost",
                "dim_reference",
                "published_batches",
                "evidence",
            )
        }


def measures(database: Path) -> tuple[int, int, int]:
    with duckdb.connect(str(database), read_only=True) as connection:
        return (
            connection.execute("SELECT count(*) FROM fact_shipment").fetchone()[0],
            connection.execute("SELECT count(*) FROM fact_delivery_event").fetchone()[0],
            connection.execute("SELECT sum(amount_minor) FROM fact_transportation_cost").fetchone()[
                0
            ],
        )


@pytest.mark.parametrize("scenario", SCENARIOS)
def test_incident_atomicity_recovery_and_replay(tmp_path: Path, scenario: str) -> None:
    fixture = create_incident(tmp_path, scenario, shipments=6, seed=73)
    database = tmp_path / scenario / "warehouse.duckdb"
    pipeline = Pipeline(database)
    baseline = pipeline.run(fixture.baseline)
    assert baseline["status"] == "published"
    prior = snapshot(database)
    prior_measures = measures(database)
    incident = pipeline.run(fixture.incident)
    assert incident["status"] == fixture.expected_incident_status
    if incident["status"] == "failed":
        assert incident["watermark"] == baseline["watermark"]
        assert snapshot(database) == prior
        assert any(
            check["severity"] == "error" and check["status"] == "fail"
            for check in incident["quality"]
        )
    elif scenario == "duplicate_file":
        assert measures(database) == prior_measures
        assert any(
            check["rule_id"] == "ING_DUPLICATE_FILE" and check["status"] == "fail"
            for check in incident["quality"]
        )
    elif scenario == "historical_correction":
        assert measures(database)[2] == prior_measures[2] + 125
        with duckdb.connect(str(database), read_only=True) as connection:
            revisions = connection.execute(
                "SELECT revision FROM evidence WHERE source_system='transport_costs' "
                "AND source_id='COST-000001' ORDER BY revision"
            ).fetchall()
            assert revisions == [(1,), (2,)]
            assert (
                connection.execute(
                    "SELECT count(*) FROM dim_reference WHERE entity_type='facility' "
                    "AND business_key='FAC-MAD'"
                ).fetchone()[0]
                == 2
            )
    elif scenario == "late_event":
        assert measures(database)[1] == prior_measures[1] + 1
        assert measures(database)[2] == prior_measures[2]
    recovery = pipeline.run(fixture.recovery)
    assert recovery["status"] == "published"
    recovered = snapshot(database)
    repeated = pipeline.run(fixture.recovery)
    assert repeated["status"] == "noop"
    assert repeated["watermark"] == recovery["watermark"]
    assert snapshot(database) == recovered
    with duckdb.connect(str(database), read_only=True) as connection:
        assert connection.execute("SELECT count(*) FROM pipeline_runs").fetchone()[0] == 4
        assert connection.execute("SELECT count(*) FROM quality_results").fetchone()[0] > 0
