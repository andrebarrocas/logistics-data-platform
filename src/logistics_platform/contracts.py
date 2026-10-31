"""Strict source adapters shared by the local and Spark execution profiles."""

import csv
import io
import json
import re
from dataclasses import asdict, dataclass
from hashlib import sha256
from typing import Any

from logistics_platform.keys import iso, utc

SOURCES = frozenset(
    {"shipment_ops", "carrier_delivery", "package_events", "transport_costs", "reference_master"}
)
STATES = frozenset(
    {
        "created",
        "collected",
        "in_transit",
        "out_for_delivery",
        "delivered",
        "delivery_failed",
        "returned",
        "cancelled",
    }
)
REFERENCE_TYPES = frozenset({"carrier", "facility", "route", "country", "region", "service_level"})
COUNTRIES = frozenset({"PT", "ES", "FR", "DE", "IT", "NL", "BE", "GB", "IE"})
CARRIER_COLUMNS = [
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
COST_COLUMNS = [
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


class ContractError(ValueError):
    def __init__(self, rule: str, message: str):
        super().__init__(message)
        self.rule = rule


def compact(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False)


def digest(value: Any) -> str:
    return sha256(compact(value).encode()).hexdigest()


def validate_manifest(manifest: Any) -> None:
    """Validate control types before using the manifest for IO or SQL."""
    if not isinstance(manifest, dict):
        raise ContractError("ING_SCHEMA", "Manifest must be a JSON object")
    text_field(manifest, "batch_id")
    if integer(manifest.get("ingest_sequence"), "ingest_sequence") < 1:
        raise ContractError("INC_CHECKPOINT", "Ingestion sequence must be positive")
    timestamp(manifest.get("created_at"))
    sources = manifest.get("required_sources")
    if (
        not isinstance(sources, list)
        or not all(isinstance(s, str) for s in sources)
        or set(sources) != SOURCES
        or len(sources) != len(SOURCES)
    ):
        raise ContractError(
            "ING_REQUIRED_SOURCES", "Exactly five required sources must be declared"
        )
    files = manifest.get("files")
    controls = manifest.get("controls")
    if not isinstance(files, list) or not isinstance(controls, dict) or set(controls) != SOURCES:
        raise ContractError("ING_SCHEMA", "Manifest files and source controls are required")
    for entry in files:
        if not isinstance(entry, dict):
            raise ContractError("ING_SCHEMA", "File entries must be objects")
        if text_field(entry, "source") not in SOURCES:
            raise ContractError("ING_SCHEMA", "Unknown source")
        text_field(entry, "path")
        if not re.fullmatch(r"[a-f0-9]{64}", text_field(entry, "sha256")):
            raise ContractError("ING_CHECKSUM", "Invalid SHA256 declaration")
        if (
            integer(entry.get("rows"), "rows") < 0
            or integer(entry.get("schema_version"), "schema_version") < 1
        ):
            raise ContractError("ING_SCHEMA", "Invalid row count or schema version")
    for source, control in controls.items():
        if not isinstance(control, dict):
            raise ContractError("ING_SCHEMA", "Source control must be an object")
        if integer(control.get("files"), "files") < 1 or integer(control.get("rows"), "rows") < 0:
            raise ContractError("ING_SCHEMA", "Invalid source controls")
        if source == "transport_costs":
            money = control.get("amount_minor")
            if not isinstance(money, dict):
                raise ContractError("REC_COST_RAW", "Currency-grouped monetary controls required")
            for currency, amount in money.items():
                if currency != "EUR":
                    raise ContractError("REC_COST_RAW", "Only explicit EUR controls supported")
                integer(amount, "amount_minor")


def integer(value: Any, field: str) -> int:
    if isinstance(value, bool) or not isinstance(value, (str, int)):
        raise ContractError("ING_SCHEMA", f"{field} must be an integer")
    try:
        return int(value)
    except (ValueError, TypeError) as exc:
        raise ContractError("ING_SCHEMA", f"{field} must be an integer") from exc


def text_field(row: dict[str, Any], field: str, *, empty: bool = False) -> str:
    value = row.get(field)
    if not isinstance(value, str) or (not value.strip() and not empty):
        raise ContractError("ING_SCHEMA", f"Missing or invalid string {field}")
    return value.strip()


def timestamp(value: Any) -> str:
    try:
        if not isinstance(value, str):
            raise ValueError("not a string")
        return iso(utc(value))
    except (ValueError, TypeError, OverflowError) as exc:
        raise ContractError("CAN_TIMESTAMP", f"Invalid offset-aware timestamp {value!r}") from exc


@dataclass(frozen=True)
class Record:
    source_system: str
    entity_type: str
    source_id: str
    business_key: str
    revision: int
    operation: str
    effective_at: str
    updated_at: str
    attributes: dict[str, Any]

    @property
    def payload_hash(self) -> str:
        return digest(asdict(self))


def read_rows(content: bytes, source: str, version: int) -> list[dict[str, Any]]:
    if source not in SOURCES or version not in ({1, 2} if source == "carrier_delivery" else {1}):
        raise ContractError("ING_SCHEMA", f"Unsupported source/version: {source}/{version}")
    try:
        text = content.decode("utf-8")
        if source in {"carrier_delivery", "transport_costs"}:
            expected = list(CARRIER_COLUMNS if source == "carrier_delivery" else COST_COLUMNS)
            if source == "carrier_delivery" and version == 2:
                expected = [
                    dict(state="delivery_state", occurred_at="event_time").get(c, c)
                    for c in expected
                ]
            reader = csv.DictReader(io.StringIO(text))
            if reader.fieldnames != expected:
                raise ContractError(
                    "ING_SCHEMA",
                    f"{source} v{version} expected columns {expected}, received {reader.fieldnames}",
                )
            rows = list(reader)
            if any(None in row or any(v is None for v in row.values()) for row in rows):
                raise ContractError("ING_SCHEMA", "CSV row width differs from declared header")
            return rows
        rows = [json.loads(line) for line in text.splitlines() if line.strip()]
        if not all(isinstance(row, dict) for row in rows):
            raise ContractError("ING_SCHEMA", "JSONL rows must be objects")
        return rows
    except (UnicodeDecodeError, json.JSONDecodeError, csv.Error) as exc:
        raise ContractError("ING_SCHEMA", str(exc)) from exc


def normalize(source: str, row: dict[str, Any], version: int = 1) -> Record:
    if not isinstance(row, dict):
        raise ContractError("ING_SCHEMA", "Source records must be objects")
    if source not in SOURCES or version not in ({1, 2} if source == "carrier_delivery" else {1}):
        raise ContractError("ING_SCHEMA", f"Unsupported source/version: {source}/{version}")
    revision = integer(row.get("revision"), "revision")
    if revision < 1:
        raise ContractError("ING_SCHEMA", "revision must be positive")
    updated = timestamp(row.get("updated_at"))
    operation = str(row.get("operation", "upsert"))
    if operation not in {"upsert", "delete"}:
        raise ContractError("ING_SCHEMA", "operation must be upsert/delete")
    if source in {"shipment_ops", "reference_master"}:
        entity = text_field(row, "entity_type")
        allowed = (
            {"order", "shipment", "package"}
            if source == "shipment_ops"
            else REFERENCE_TYPES | {"alias"}
        )
        if entity not in allowed:
            raise ContractError("ING_SCHEMA", f"Invalid {source} entity: {entity}")
        attrs = row.get("attributes")
        if not isinstance(attrs, dict):
            raise ContractError("ING_SCHEMA", "attributes must be an object")
        attrs = dict(attrs)
        key = text_field(row, "business_key")
        effective = timestamp(row.get("effective_at"))
        if entity in {"shipment", "order"}:
            attrs["created_at"] = timestamp(attrs.get("created_at"))
            text_field(attrs, "country_code")
        if entity == "shipment":
            attrs["promised_at"] = timestamp(attrs.get("promised_at"))
            for field in ("order_key", "route_key", "country_code"):
                text_field(attrs, field)
            for field in (
                "carrier_alias",
                "origin_facility_alias",
                "destination_facility_alias",
                "service_alias",
            ):
                if attrs.get(field) is not None and not isinstance(attrs[field], str):
                    raise ContractError("ING_SCHEMA", f"{field} must be a string or null")
            if attrs["promised_at"] < attrs["created_at"]:
                raise ContractError("CAN_TIMESTAMP", "promised_at precedes created_at")
        if entity == "package":
            text_field(attrs, "shipment_key")
            if attrs.get("weight_grams") is not None:
                attrs["weight_grams"] = integer(attrs["weight_grams"], "weight_grams")
                if attrs["weight_grams"] < 0:
                    raise ContractError("CAN_TIMESTAMP", "Negative package weight")
        if entity in REFERENCE_TYPES:
            text_field(attrs, "name")
        if entity == "alias":
            for field in ("target_type", "source", "alias", "target_key", "rule_id"):
                text_field(attrs, field)
            if attrs["target_type"] not in REFERENCE_TYPES or attrs["source"] not in SOURCES:
                raise ContractError("ING_SCHEMA", "Invalid alias scope")
            if attrs["target_type"] == "country" and attrs["target_key"] not in COUNTRIES:
                raise ContractError(
                    "REF_INVALID_COUNTRY", "Country alias target outside controlled country set"
                )
            if (
                attrs["target_type"] == "country"
                and attrs["alias"].strip().upper() in COUNTRIES
                and attrs["alias"].strip().upper() != attrs["target_key"]
            ):
                raise ContractError(
                    "CAN_RELATION_CONFLICT", "Alias contradicts a controlled global country key"
                )
            attrs["effective_to"] = timestamp(attrs.get("effective_to"))
            if attrs["effective_to"] <= effective:
                raise ContractError("REF_INTERVAL_OVERLAP", "Alias interval is empty or negative")
        source_id = text_field(row, "source_id")
    elif source in {"carrier_delivery", "package_events"}:
        entity = "event"
        if source == "carrier_delivery":
            state_field = "state" if version == 1 else "delivery_state"
            time_field = "occurred_at" if version == 1 else "event_time"
            source_id = text_field(row, "event_id")
            state = text_field(row, state_field)
            effective = timestamp(row.get(time_field))
            raw_bool = row.get("verified")
            if str(raw_bool).lower() not in {"true", "false"}:
                raise ContractError("ING_SCHEMA", "verified must be true or false")
            verified = str(raw_bool).lower() == "true"
        else:
            event = row.get("event")
            if not isinstance(event, dict):
                raise ContractError("ING_SCHEMA", "event must be an object")
            source_id = text_field(event, "id")
            state = text_field(event, "state")
            effective = timestamp(event.get("occurred_at"))
            verified = False
        key = f"{source}:{source_id}"
        attrs = {
            "shipment_key": text_field(row, "shipment_key"),
            "package_key": text_field(row, "package_key", empty=True),
            "carrier_alias": text_field(row, "carrier_alias", empty=True),
            "state": state,
            "verified": verified,
        }
    else:
        entity = "cost"
        source_id = text_field(row, "cost_id")
        key = f"{source}:{source_id}"
        effective = timestamp(row.get("incurred_at"))
        attrs = {
            "shipment_key": text_field(row, "shipment_key"),
            "carrier_alias": text_field(row, "carrier_alias", empty=True),
            "amount_minor": integer(row.get("amount_minor"), "amount_minor"),
            "currency": text_field(row, "currency"),
            "cost_type": text_field(row, "cost_type"),
        }
        if attrs["currency"] != "EUR":
            raise ContractError("REC_COST_RAW", "Only explicit EUR entries are supported")
    if "state" in attrs and attrs["state"] not in STATES:
        raise ContractError("CAN_STATE_TRANSITION", f"Unknown state: {attrs['state']!r}")
    return Record(source, entity, source_id, key, revision, operation, effective, updated, attrs)
