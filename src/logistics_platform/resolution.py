"""Evidence-based, effective-dated identity resolution; never fuzzy matching."""

from collections import defaultdict
from dataclasses import replace
from typing import Any

from logistics_platform.contracts import REFERENCE_TYPES, ContractError, Record, digest
from logistics_platform.keys import canonical_id


def latest(records: list[tuple[Record, dict[str, Any]]]) -> list[tuple[Record, dict[str, Any]]]:
    versions: dict[tuple[str, ...], tuple[Record, dict[str, Any]]] = {}
    identities: dict[tuple[str, str, str], str] = {}
    for rec, lineage in records:
        source_key = (rec.source_system, rec.entity_type, rec.source_id)
        if source_key in identities and identities[source_key] != rec.business_key:
            raise ContractError(
                "CAN_RELATION_CONFLICT",
                f"Source identity reused for another business key: {source_key}",
            )
        identities[source_key] = rec.business_key
        key = (
            (*source_key, rec.effective_at)
            if rec.entity_type in REFERENCE_TYPES | {"alias"}
            else source_key
        )
        previous = versions.get(key)
        if previous and previous[0].business_key != rec.business_key:
            raise ContractError(
                "CAN_RELATION_CONFLICT", f"Source identity reused for another business key: {key}"
            )
        if previous is None or rec.revision > previous[0].revision:
            versions[key] = (rec, lineage)
    return [versions[key] for key in sorted(versions)]


class Resolver:
    def __init__(self, records: list[tuple[Record, dict[str, Any]]]):
        self.aliases: dict[tuple[str, str, str], list[Record]] = defaultdict(list)
        self.decisions: list[dict[str, Any]] = []
        alias_timelines: dict[str, list[Record]] = defaultdict(list)
        for rec, _ in latest(records):
            if rec.entity_type == "alias":
                alias_timelines[rec.source_id].append(rec)
        effective_aliases = []
        for timeline in alias_timelines.values():
            ordered = sorted(timeline, key=lambda r: r.effective_at)
            for index, rec in enumerate(ordered):
                end = rec.attributes["effective_to"]
                if index + 1 < len(ordered):
                    end = min(end, ordered[index + 1].effective_at)
                effective_aliases.append(
                    replace(rec, attributes={**rec.attributes, "effective_to": end})
                )
        for rec in effective_aliases:
            if rec.entity_type == "alias" and rec.operation != "delete":
                a = rec.attributes
                self.aliases[(a["target_type"], a["source"], a["alias"].strip().upper())].append(
                    rec
                )

    def conflicts(self) -> list[dict[str, Any]]:
        problems = []
        for key, rows in self.aliases.items():
            for index, left in enumerate(rows):
                for right in rows[index + 1 :]:
                    if (
                        left.attributes["target_key"] != right.attributes["target_key"]
                        and left.effective_at < right.attributes["effective_to"]
                        and right.effective_at < left.attributes["effective_to"]
                    ):
                        problems.append(
                            {"scope": key, "candidates": [left.attributes, right.attributes]}
                        )
        return problems

    def resolve(
        self, entity: str, source: str, alias: str | None, at: str, record_id: str
    ) -> str | None:
        token = (alias or "").strip().upper()
        candidates = [
            rec
            for rec in self.aliases.get((entity, source, token), [])
            if rec.effective_at <= at < rec.attributes["effective_to"]
        ]
        targets = sorted({rec.attributes["target_key"] for rec in candidates})
        outcome = "resolved" if len(targets) == 1 else "ambiguous" if targets else "unresolved"
        selected = canonical_id(entity, targets[0]) if len(targets) == 1 else None
        decision = {
            "entity_type": entity,
            "source_system": source,
            "source_id": record_id,
            "source_value": alias,
            "effective_at": at,
            "outcome": outcome,
            "canonical_id": selected,
            "candidate_business_keys": targets,
            "rules": sorted({rec.attributes["rule_id"] for rec in candidates}),
        }
        decision["decision_id"] = digest(decision)
        self.decisions.append(decision)
        return selected
