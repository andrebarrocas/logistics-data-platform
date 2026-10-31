"""Verify synthetic delivery contracts independently of warehouse behavior."""

import csv
import hashlib
import json
from pathlib import Path

import pytest

from logistics_platform.generator import SOURCES, generate, synthetic_records, write_batch
from logistics_platform.incidents import SCENARIOS, create_incident, create_incidents


def test_reproducible_complete_manifests_and_physical_cost_controls(tmp_path: Path) -> None:
    first = generate(tmp_path / "first", shipments=13, seed=17)
    second = generate(tmp_path / "second", shipments=13, seed=17)
    for left, right in zip(first, second, strict=True):
        assert left.read_bytes() == right.read_bytes()
        manifest = json.loads(left.read_text())
        assert set(manifest["required_sources"]) == set(SOURCES)
        received_amount = 0
        for source in SOURCES:
            files = [item for item in manifest["files"] if item["source"] == source]
            assert len(files) == manifest["controls"][source]["files"]
            assert sum(item["rows"] for item in files) == manifest["controls"][source]["rows"]
            for item in files:
                file = left.parent / item["path"]
                assert hashlib.sha256(file.read_bytes()).hexdigest() == item["sha256"]
                assert file.read_bytes() == (right.parent / item["path"]).read_bytes()
                if file.suffix == ".csv":
                    with file.open(newline="") as stream:
                        rows = list(csv.DictReader(stream))
                else:
                    rows = [json.loads(line) for line in file.read_text().splitlines()]
                assert len(rows) == item["rows"]
                if source == "transport_costs":
                    received_amount += sum(int(row["amount_minor"]) for row in rows)
        assert manifest["controls"]["transport_costs"]["amount_minor"] == {"EUR": received_amount}
        assert manifest["controls"]["transport_costs"]["files"] == 2


def test_noisy_full_and_versioned_increment_have_specific_evidence(tmp_path: Path) -> None:
    full_path, incremental_path = generate(tmp_path, shipments=7)
    full = json.loads(full_path.read_text())
    carrier_files = [item for item in full["files"] if item["source"] == "carrier_delivery"]
    assert len(carrier_files) == 2
    assert carrier_files[0]["sha256"] == carrier_files[1]["sha256"]
    assert full["controls"]["shipment_ops"]["rows"] == 22
    incremental = json.loads(incremental_path.read_text())
    carrier = next(item for item in incremental["files"] if item["source"] == "carrier_delivery")
    assert carrier["schema_version"] == 2
    header = (incremental_path.parent / carrier["path"]).read_text().splitlines()[0]
    assert "delivery_state" in header and "event_time" in header
    events_file = next(item for item in incremental["files"] if item["source"] == "package_events")
    event = json.loads((incremental_path.parent / events_file["path"]).read_text())
    assert event["event"]["occurred_at"] < event["updated_at"]


def test_seed_changes_operational_values_but_not_global_random_state() -> None:
    import random

    random.seed(91)
    before = random.getstate()
    first = synthetic_records(3, seed=1)
    second = synthetic_records(3, seed=2)
    assert random.getstate() == before
    assert first["transport_costs"] != second["transport_costs"]
    assert first["reference_master"] == second["reference_master"]


def test_empty_feeds_are_written_and_batch_overwrite_rejected(tmp_path: Path) -> None:
    path = write_batch(tmp_path, "empty", 1, {})
    manifest = json.loads(path.read_text())
    assert set(manifest["controls"]) == set(SOURCES)
    assert all(control["rows"] == 0 for control in manifest["controls"].values())
    with pytest.raises(FileExistsError):
        write_batch(tmp_path, "empty", 1, {})
    with pytest.raises(ValueError):
        write_batch(tmp_path, "../escape", 1, {})


def test_incident_inventory_and_exact_partial_feed_recovery(tmp_path: Path) -> None:
    fixtures = create_incidents(tmp_path, shipments=4)
    assert {fixture.name for fixture in fixtures} == set(SCENARIOS)
    partial = next(fixture for fixture in fixtures if fixture.name == "partial_cost")
    assert partial.incident.read_bytes() == partial.recovery.read_bytes()
    manifest = json.loads(partial.incident.read_text())
    missing = [
        item for item in manifest["files"] if not (partial.incident.parent / item["path"]).exists()
    ]
    assert len(missing) == 1 and missing[0]["source"] == "transport_costs"
    assert all((partial.recovery.parent / item["path"]).exists() for item in manifest["files"])


def test_schema_incident_preserves_checksum_to_isolate_contract_failure(tmp_path: Path) -> None:
    fixture = create_incident(tmp_path, "schema_drift", shipments=3)
    manifest = json.loads(fixture.incident.read_text())
    carrier = next(item for item in manifest["files"] if item["source"] == "carrier_delivery")
    file = fixture.incident.parent / carrier["path"]
    assert carrier["sha256"] == hashlib.sha256(file.read_bytes()).hexdigest()
    assert "unannounced_status" in file.read_text().splitlines()[0]
    assert carrier["schema_version"] == 1
