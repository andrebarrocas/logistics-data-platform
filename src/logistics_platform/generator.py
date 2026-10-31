"""Independent, seeded synthetic source generation and immutable batch manifests."""

from __future__ import annotations

import csv
import hashlib
import json
import random
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

SOURCES = (
    "shipment_ops",
    "carrier_delivery",
    "package_events",
    "transport_costs",
    "reference_master",
)
CARRIER_FIELDS = [
    "event_id",
    "shipment_key",
    "package_key",
    "carrier_alias",
    "state",
    "occurred_at",
    "updated_at",
    "revision",
    "verified",
]
COST_FIELDS = [
    "cost_id",
    "shipment_key",
    "carrier_alias",
    "amount_minor",
    "currency",
    "cost_type",
    "incurred_at",
    "updated_at",
    "revision",
    "operation",
]
OPEN_END = "9999-12-31T00:00:00Z"
BASE_TIME = datetime(2026, 1, 1, tzinfo=UTC)
CARRIERS = (
    ("CAR-AP", "Aster Parcel", ["ASTER", "Aster Parcel", "AP-001"]),
    ("CAR-MF", "Meridian Freight", ["MERIDIAN", "Meridian Freight", "MF-002"]),
    ("CAR-VC", "Vale Courier", ["VALE", "Vale Courier", "VC-003"]),
)


def _json(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False)


def _time(value: datetime) -> str:
    return value.isoformat().replace("+00:00", "Z")


def empty_records() -> dict[str, list[dict[str, Any]]]:
    """Explicit empty extracts are represented by files, not absent sources."""
    return {source: [] for source in SOURCES}


def envelope(
    entity_type: str,
    source_id: str,
    business_key: str,
    attributes: dict[str, Any],
    *,
    revision: int = 1,
    operation: str = "upsert",
    effective_at: str = "2026-01-01T00:00:00Z",
    updated_at: str | None = None,
) -> dict[str, Any]:
    return {
        "entity_type": entity_type,
        "source_id": source_id,
        "business_key": business_key,
        "revision": revision,
        "operation": operation,
        "effective_at": effective_at,
        "updated_at": updated_at or effective_at,
        "attributes": attributes,
    }


def _write_file(path: Path, source: str, rows: list[dict[str, Any]], version: int) -> None:
    if source in {"carrier_delivery", "transport_costs"}:
        fields = list(CARRIER_FIELDS if source == "carrier_delivery" else COST_FIELDS)
        if source == "carrier_delivery" and version == 2:
            fields = [
                {"state": "delivery_state", "occurred_at": "event_time"}.get(field, field)
                for field in fields
            ]
        with path.open("w", encoding="utf-8", newline="") as stream:
            writer = csv.DictWriter(stream, fieldnames=fields, lineterminator="\n")
            writer.writeheader()
            for original in rows:
                row = dict(original)
                if source == "carrier_delivery" and version == 2:
                    row["delivery_state"] = row.pop("state")
                    row["event_time"] = row.pop("occurred_at")
                if "verified" in row:
                    row["verified"] = str(row["verified"]).lower()
                writer.writerow(row)
    else:
        path.write_text("".join(_json(row) + "\n" for row in rows), encoding="utf-8")


def write_batch(
    output: Path,
    batch_id: str,
    sequence: int,
    records: dict[str, list[dict[str, Any]]],
    schema_versions: dict[str, int] | None = None,
) -> Path:
    """Write five explicit feeds, two cost shards, and physical delivery controls.

    The returned path is ``output / batch_id / manifest.json``. Rewriting a
    different payload into an existing batch is refused: producer corrections
    need a new batch identity, just as consumer ingestion requires.
    """
    if sequence < 1:
        raise ValueError("ingest_sequence must be positive")
    if not batch_id or Path(batch_id).name != batch_id or batch_id in {".", ".."}:
        raise ValueError("batch_id must be a single safe directory name")
    unknown = set(records) - set(SOURCES)
    if unknown:
        raise ValueError(f"Unsupported source names: {sorted(unknown)}")
    directory = Path(output) / batch_id
    if directory.exists() and any(directory.iterdir()):
        raise FileExistsError(f"Refusing to overwrite immutable batch: {directory}")
    directory.mkdir(parents=True, exist_ok=True)
    versions = schema_versions or {}
    manifest: dict[str, Any] = {
        "batch_id": batch_id,
        "ingest_sequence": sequence,
        "created_at": _time(BASE_TIME + timedelta(days=sequence - 1)),
        "required_sources": list(SOURCES),
        "files": [],
        "controls": {},
    }
    for source in SOURCES:
        rows = records.get(source, [])
        version = versions.get(source, 1)
        if version not in ({1, 2} if source == "carrier_delivery" else {1}):
            raise ValueError(f"Unsupported generated schema {source} v{version}")
        if source == "transport_costs":
            midpoint = (len(rows) + 1) // 2
            shards = [rows[:midpoint], rows[midpoint:]]
        else:
            shards = [rows]
        for index, shard in enumerate(shards, start=1):
            extension = "csv" if source in {"transport_costs", "carrier_delivery"} else "jsonl"
            filename = f"{source}-{index:03d}.{extension}"
            path = directory / filename
            _write_file(path, source, shard, version)
            manifest["files"].append(
                {
                    "source": source,
                    "path": filename,
                    "schema_version": version,
                    "sha256": hashlib.sha256(path.read_bytes()).hexdigest(),
                    "rows": len(shard),
                }
            )
        control: dict[str, Any] = {"files": len(shards), "rows": len(rows)}
        if source == "transport_costs":
            amounts: dict[str, int] = {"EUR": 0}
            for row in rows:
                currency = str(row["currency"])
                amounts[currency] = amounts.get(currency, 0) + int(row["amount_minor"])
            control["amount_minor"] = amounts
        manifest["controls"][source] = control
    path = directory / "manifest.json"
    path.write_text(json.dumps(manifest, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return path


def duplicate_delivery(manifest_path: Path, source: str = "carrier_delivery") -> Path:
    """Add an explicitly inventoried second receipt of identical content."""
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    entry = next(item for item in manifest["files"] if item["source"] == source)
    source_path = manifest_path.parent / entry["path"]
    copy_path = source_path.with_name(source_path.stem + "-duplicate" + source_path.suffix)
    copy_path.write_bytes(source_path.read_bytes())
    duplicate = dict(entry, path=copy_path.name)
    manifest["files"].append(duplicate)
    manifest["controls"][source]["files"] += 1
    manifest["controls"][source]["rows"] += entry["rows"]
    if source == "transport_costs":
        with source_path.open(encoding="utf-8", newline="") as stream:
            for row in csv.DictReader(stream):
                totals = manifest["controls"][source]["amount_minor"]
                currency = row["currency"]
                totals[currency] = totals.get(currency, 0) + int(row["amount_minor"])
    manifest_path.write_text(
        json.dumps(manifest, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )
    return copy_path


def reference_records() -> list[dict[str, Any]]:
    """Synthetic authoritative master records and source-scoped exact mappings."""
    rows: list[dict[str, Any]] = []
    for key, name in [
        ("PT", "Portugal"),
        ("ES", "Spain"),
        ("FR", "France"),
        ("GB", "United Kingdom"),
    ]:
        rows.append(envelope("country", f"REF-{key}", key, {"name": name}))
    for key, name in [
        ("REG-IBERIA", "Iberia reporting group"),
        ("REG-WEST", "Western reporting group"),
    ]:
        rows.append(envelope("region", f"REF-{key}", key, {"name": name}))
    rows.append(
        envelope(
            "service_level",
            "REF-SVC-STD",
            "SVC-STD",
            {
                "name": "Standard",
                "target_business_days": 3,
                "operating_status": "active",
            },
        )
    )
    for key, name, country, region in [
        ("FAC-LIS", "Lisbon synthetic hub", "PT", "REG-IBERIA"),
        ("FAC-MAD", "Madrid synthetic hub", "ES", "REG-IBERIA"),
        ("FAC-PAR", "Paris synthetic hub", "FR", "REG-WEST"),
    ]:
        rows.append(
            envelope(
                "facility",
                f"REF-{key}",
                key,
                {
                    "name": name,
                    "country_key": country,
                    "region_key": region,
                    "operating_status": "active",
                },
            )
        )
    rows.append(
        envelope(
            "route",
            "REF-RTE-LIS-MAD",
            "RTE-LIS-MAD",
            {
                "name": "Lisbon to Madrid synthetic route",
                "origin_facility_key": "FAC-LIS",
                "destination_facility_key": "FAC-MAD",
                "distance_km": 620,
                "operating_status": "active",
            },
        )
    )
    aliases: list[tuple[str, str, str, str]] = []
    for key, name, names in CARRIERS:
        rows.append(
            envelope(
                "carrier",
                f"REF-{key}",
                key,
                {
                    "name": name,
                    "default_service_key": "SVC-STD",
                    "operating_status": "active",
                },
            )
        )
        for source in SOURCES[:-1]:
            for alias in [key, *names]:
                aliases.append(("carrier", source, alias, key))
    for key in ["FAC-LIS", "FAC-MAD", "FAC-PAR"]:
        aliases.append(("facility", "shipment_ops", key, key))
    aliases.extend(
        [
            ("service_level", "shipment_ops", "SVC-STD", "SVC-STD"),
            ("service_level", "shipment_ops", "STD", "SVC-STD"),
            ("route", "shipment_ops", "RTE-LIS-MAD", "RTE-LIS-MAD"),
        ]
    )
    for index, (target_type, source, alias, target_key) in enumerate(aliases, start=1):
        alias_id = f"ALIAS-{index:04d}"
        rows.append(
            envelope(
                "alias",
                f"REF-{alias_id}",
                alias_id,
                {
                    "target_type": target_type,
                    "source": source,
                    "alias": alias,
                    "target_key": target_key,
                    "effective_to": OPEN_END,
                    "rule_id": f"ER-SYN-{index:04d}",
                },
            )
        )
    return rows


def synthetic_records(shipments: int = 100, seed: int = 42) -> dict[str, list[dict[str, Any]]]:
    """Return a reproducible healthy batch containing deliberate safe noise."""
    if shipments < 1:
        raise ValueError("shipments must be at least one")
    randomizer = random.Random(seed)
    rows = empty_records()
    rows["reference_master"] = reference_records()
    for index in range(1, shipments + 1):
        order, shipment, package = (f"{prefix}-{index:06d}" for prefix in ("ORD", "SHP", "PKG"))
        _, _, aliases = CARRIERS[(index - 1) % len(CARRIERS)]
        created = BASE_TIME + timedelta(days=1, hours=8, seconds=index)
        effective = created + timedelta(hours=2)
        rows["shipment_ops"].append(
            envelope(
                "order",
                f"OPS-{order}",
                order,
                {
                    "created_at": _time(created),
                    "country_code": "PT",
                },
                effective_at=_time(created),
            )
        )
        rows["shipment_ops"].append(
            envelope(
                "shipment",
                f"OPS-{shipment}",
                shipment,
                {
                    "order_key": order,
                    "carrier_alias": aliases[0],
                    "origin_facility_alias": "FAC-LIS",
                    "destination_facility_alias": "FAC-MAD",
                    "route_key": "RTE-LIS-MAD",
                    "service_alias": "STD",
                    "country_code": "ES",
                    "created_at": _time(created),
                    "promised_at": _time(created + timedelta(days=3)),
                    "state": "in_transit",
                },
                effective_at=_time(effective),
            )
        )
        rows["shipment_ops"].append(
            envelope(
                "package",
                f"OPS-{package}",
                package,
                {
                    "shipment_key": shipment,
                    "weight_grams": randomizer.randint(200, 20000),
                },
                effective_at=_time(created),
            )
        )
        # Equivalent offset representation intentionally differs between sources.
        delivered = created + timedelta(days=1, hours=5)
        state = "delivered" if index % 5 else "out_for_delivery"
        rows["carrier_delivery"].append(
            {
                "event_id": f"CAR-EVT-{index:06d}",
                "shipment_key": shipment,
                "package_key": "",
                "carrier_alias": f" {aliases[1]} " if index % 2 else aliases[2],
                "state": state,
                "occurred_at": _time(delivered),
                "updated_at": _time(delivered + timedelta(minutes=2)),
                "revision": 1,
                "verified": state == "delivered",
            }
        )
        rows["package_events"].append(
            {
                "event": {
                    "id": f"PKG-EVT-{index:06d}",
                    "state": "collected",
                    "occurred_at": (created + timedelta(hours=1)).isoformat(),
                },
                "shipment_key": shipment,
                "package_key": package,
                "carrier_alias": aliases[0],
                "updated_at": _time(created + timedelta(hours=1, minutes=3)),
                "revision": 1,
            }
        )
        rows["transport_costs"].append(
            {
                "cost_id": f"COST-{index:06d}",
                "shipment_key": shipment,
                "carrier_alias": aliases[2],
                "amount_minor": randomizer.randint(800, 9000),
                "currency": "EUR",
                "cost_type": "linehaul",
                "incurred_at": _time(effective),
                "updated_at": _time(effective + timedelta(minutes=5)),
                "revision": 1,
                "operation": "upsert",
            }
        )
    # Same logical version and exact payload: this is safe duplicate source evidence.
    rows["shipment_ops"].append(dict(rows["shipment_ops"][1]))
    return rows


def incremental_records(full: dict[str, list[dict[str, Any]]]) -> dict[str, list[dict[str, Any]]]:
    """Corrections, backdated reference history, a late event, and a tombstone."""
    rows = empty_records()
    correction = dict(full["transport_costs"][0])
    correction.update(
        revision=2, amount_minor=correction["amount_minor"] + 125, updated_at="2026-01-07T10:00:00Z"
    )
    rows["transport_costs"].append(correction)
    event = dict(full["carrier_delivery"][0])
    event.update(revision=2, occurred_at="2026-01-03T14:00:00Z", updated_at="2026-01-07T10:00:00Z")
    rows["carrier_delivery"].append(event)
    rows["package_events"].append(
        {
            "event": {
                "id": "PKG-EXTREMELY-LATE-000001",
                "state": "created",
                "occurred_at": "2026-01-02T08:00:01Z",
            },
            "shipment_key": "SHP-000001",
            "package_key": "PKG-000001",
            "carrier_alias": "ASTER",
            "updated_at": "2026-01-07T09:00:00Z",
            "revision": 1,
        }
    )
    facility = next(row for row in full["reference_master"] if row["business_key"] == "FAC-MAD")
    rows["reference_master"].append(
        envelope(
            "facility",
            facility["source_id"],
            "FAC-MAD",
            dict(facility["attributes"], region_key="REG-WEST"),
            revision=2,
            effective_at="2026-01-02T09:00:00Z",
            updated_at="2026-01-07T09:00:00Z",
        )
    )
    operations = [row for row in full["shipment_ops"] if row["entity_type"] == "shipment"]
    if len(operations) > 2:
        # Withdraw the last source membership; facts and raw history remain auditable.
        last = max(operations, key=lambda row: row["business_key"])
        tombstone = dict(
            last,
            revision=2,
            operation="delete",
            effective_at="2026-01-07T08:00:00Z",
            updated_at="2026-01-07T08:00:00Z",
        )
        rows["shipment_ops"].append(tombstone)
    return rows


def generate(output: Path, shipments: int = 100, seed: int = 42) -> list[Path]:
    """Generate initial and incremental manifests with all five synthetic sources."""
    full = synthetic_records(shipments, seed)
    initial = write_batch(Path(output), "batch001", 1, full)
    duplicate_delivery(initial)
    incremental = write_batch(
        Path(output), "batch002", 2, incremental_records(full), {"carrier_delivery": 2}
    )
    return [initial, incremental]
