"""Identity primitives. Hashing is not evidence of identity."""

from datetime import datetime, timezone
from hashlib import sha256
from uuid import NAMESPACE_URL, uuid5

NAMESPACE = uuid5(NAMESPACE_URL, "https://example.invalid/mcl-platform")
END_OF_TIME = "9999-12-31T00:00:00.000000Z"


def canonical_id(entity_type: str, business_key: str) -> str:
    if not entity_type or not business_key or business_key != business_key.strip():
        raise ValueError("Canonical identity requires an explicit normalized business key")
    return str(uuid5(NAMESPACE, f"{entity_type.lower()}:{business_key}"))


def utc(value: str) -> datetime:
    instant = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if instant.tzinfo is None or instant.utcoffset() is None:
        raise ValueError(f"Timestamp requires an explicit offset: {value!r}")
    return instant.astimezone(timezone.utc).replace(tzinfo=None)


def iso(value: datetime | str) -> str:
    if isinstance(value, str):
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    else:
        parsed = value
    if parsed.tzinfo is not None:
        parsed = parsed.astimezone(timezone.utc).replace(tzinfo=None)
    return parsed.strftime("%Y-%m-%dT%H:%M:%S.%fZ")


def surrogate_key(entity_type: str, entity_id: str, effective_from: str) -> int:
    identity = f"{entity_type}:{entity_id}:{iso(effective_from)}"
    return (int.from_bytes(sha256(identity.encode()).digest()[:8], "big") & ((1 << 63) - 1)) or 1
