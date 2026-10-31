"""Reproducible incident inputs and a real pipeline recovery exercise."""

from __future__ import annotations

import csv
import hashlib
import json
import shutil
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any

from logistics_platform.generator import (
    duplicate_delivery,
    empty_records,
    envelope,
    incremental_records,
    synthetic_records,
    write_batch,
)

SCENARIOS = (
    "schema_drift",
    "duplicate_file",
    "partial_cost",
    "ambiguous_alias",
    "historical_correction",
    "late_event",
    "foreign_key_spike",
    "country_drift",
)


@dataclass(frozen=True)
class IncidentFixture:
    name: str
    baseline: Path
    incident: Path
    recovery: Path
    expected_incident_status: str


def _save_manifest(path: Path, manifest: dict[str, Any]) -> None:
    path.write_text(json.dumps(manifest, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def create_incident(
    output: Path,
    scenario: str,
    shipments: int = 12,
    seed: int = 42,
) -> IncidentFixture:
    """Build baseline, incident, and recovered deliveries without running a warehouse."""
    if scenario not in SCENARIOS:
        raise ValueError(f"Unknown incident {scenario!r}; choose from {SCENARIOS}")
    if shipments < 2:
        raise ValueError("Incident fixtures need at least two shipments for cost shard checks")
    directory = Path(output) / scenario
    full = synthetic_records(shipments, seed)
    baseline = write_batch(directory / "baseline", "batch001", 1, full)
    duplicate_delivery(baseline)
    bad = empty_records()
    fixed = empty_records()
    expected = "failed"
    version = 1
    recovery_version = 1
    if scenario == "schema_drift":
        observation = dict(
            full["carrier_delivery"][0], revision=2, updated_at="2026-01-07T10:00:00Z"
        )
        bad["carrier_delivery"] = [observation]
        fixed["carrier_delivery"] = [observation]
        recovery_version = 2
    elif scenario == "duplicate_file":
        bad["carrier_delivery"] = [dict(full["carrier_delivery"][0])]
        fixed["carrier_delivery"] = [dict(full["carrier_delivery"][0])]
        expected = "published"
    elif scenario == "partial_cost":
        bad["transport_costs"] = [
            dict(
                row,
                revision=2,
                amount_minor=row["amount_minor"] + 100,
                updated_at="2026-01-07T10:00:00Z",
            )
            for row in full["transport_costs"][:2]
        ]
        fixed["transport_costs"] = bad["transport_costs"]
    elif scenario == "ambiguous_alias":
        alias = envelope(
            "alias",
            "REF-CONFLICT",
            "ALIAS-CONFLICT",
            {
                "target_type": "carrier",
                "source": "carrier_delivery",
                "alias": "ASTER",
                "target_key": "CAR-MF",
                "effective_to": "9999-12-31T00:00:00Z",
                "rule_id": "ER-INCIDENT-CONFLICT",
            },
            updated_at="2026-01-07T10:00:00Z",
        )
        bad["reference_master"] = [alias]
        fixed["reference_master"] = [
            dict(alias, revision=2, attributes=dict(alias["attributes"], target_key="CAR-AP"))
        ]
    elif scenario == "historical_correction":
        changed = incremental_records(full)
        for source in ("transport_costs", "carrier_delivery", "reference_master"):
            bad[source] = changed[source]
        fixed = empty_records()  # Recovery is an empty delivery plus an idempotency replay.
        expected = "published"
        version = 2
    elif scenario == "late_event":
        bad["package_events"] = incremental_records(full)["package_events"]
        expected = "published"
    elif scenario in {"foreign_key_spike", "country_drift"}:
        shipments_seen: set[str] = set()
        for row in full["shipment_ops"]:
            if row["entity_type"] != "shipment" or row["business_key"] in shipments_seen:
                continue
            shipments_seen.add(row["business_key"])
            attr = dict(row["attributes"])
            if scenario == "foreign_key_spike":
                attr.update(
                    carrier_alias="UNMAPPED-CARRIER",
                    origin_facility_alias="UNMAPPED-ORIGIN",
                    destination_facility_alias="UNMAPPED-DESTINATION",
                )
            else:
                attr["country_code"] = "UK"
            bad["shipment_ops"].append(
                dict(
                    row,
                    revision=2,
                    attributes=attr,
                    updated_at="2026-01-07T10:00:00Z",
                )
            )
            fixed["shipment_ops"].append(
                dict(
                    row,
                    revision=3,
                    updated_at="2026-01-07T11:00:00Z",
                )
            )
    incident = write_batch(
        directory / "incident", "batch002", 2, bad, {"carrier_delivery": version}
    )
    if scenario == "schema_drift":
        manifest = json.loads(incident.read_text(encoding="utf-8"))
        entry = next(item for item in manifest["files"] if item["source"] == "carrier_delivery")
        path = incident.parent / entry["path"]
        # Keep valid row/hash controls so the schema check, not corruption, detects the fault.
        with path.open(encoding="utf-8", newline="") as stream:
            records = list(csv.reader(stream))
        records[0][records[0].index("state")] = "unannounced_status"
        with path.open("w", encoding="utf-8", newline="") as stream:
            csv.writer(stream, lineterminator="\n").writerows(records)
        entry["sha256"] = hashlib.sha256(path.read_bytes()).hexdigest()
        _save_manifest(incident, manifest)
    elif scenario == "duplicate_file":
        duplicate_delivery(incident)
    if scenario == "partial_cost":
        # Recovery restores the exact declared batch, without changing its identity/hash.
        recovery_directory = directory / "recovery" / "batch002"
        shutil.copytree(incident.parent, recovery_directory)
        recovery = recovery_directory / "manifest.json"
        manifest = json.loads(incident.read_text(encoding="utf-8"))
        costs = [item for item in manifest["files"] if item["source"] == "transport_costs"]
        (incident.parent / costs[-1]["path"]).unlink()
    else:
        recovery = write_batch(
            directory / "recovery",
            "batch003",
            2 if expected == "failed" else 3,
            fixed,
            {"carrier_delivery": recovery_version},
        )
    return IncidentFixture(scenario, baseline, incident, recovery, expected)


def create_incidents(output: Path, shipments: int = 12, seed: int = 42) -> list[IncidentFixture]:
    return [create_incident(output, name, shipments, seed) for name in SCENARIOS]


def run_incidents(output: Path, shipments: int = 12, seed: int = 42) -> list[dict[str, Any]]:
    """Run every failure/recovery against isolated actual Pipeline databases.

    The report records observed results. It raises if an expected outcome or
    recovery fails; it never emits invented passing demonstrations.
    """
    from logistics_platform.pipeline import Pipeline

    reports: list[dict[str, Any]] = []
    for fixture in create_incidents(output, shipments, seed):
        pipeline = Pipeline(Path(output) / fixture.name / "warehouse.duckdb")
        results = {
            "baseline": pipeline.run(fixture.baseline),
            "incident": pipeline.run(fixture.incident),
            "recovery": pipeline.run(fixture.recovery),
            "replay": pipeline.run(fixture.recovery),
        }
        passed = (
            results["baseline"]["status"] == "published"
            and results["incident"]["status"] == fixture.expected_incident_status
            and results["recovery"]["status"] == "published"
            and results["replay"]["status"] == "noop"
        )
        report = {
            "scenario": fixture.name,
            "fixtures": {key: str(value) for key, value in asdict(fixture).items()},
            "expected_incident_status": fixture.expected_incident_status,
            "results": results,
            "passed": passed,
        }
        reports.append(report)
        destination = Path(output) / fixture.name / "incident-result.json"
        destination.write_text(json.dumps(report, indent=2, default=str) + "\n", encoding="utf-8")
    summary = Path(output) / "incident-results.json"
    summary.write_text(json.dumps(reports, indent=2, default=str) + "\n", encoding="utf-8")
    if not all(report["passed"] for report in reports):
        failures = [report["scenario"] for report in reports if not report["passed"]]
        raise AssertionError(f"Incident expectations failed: {failures}; inspect {summary}")
    return reports
