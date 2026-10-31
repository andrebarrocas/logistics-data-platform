# ADR 0001: deterministic identity and explicit authority

Status: accepted at Phase 0.

## Context
Labels collide, identifiers are only unique inside a source, and operational systems disagree. Similar strings are insufficient proof of identity.

## Decision
Use a fixed UUID5 namespace for canonical IDs derived from entity type and globally governed business key. Keep source keys, business keys, canonical IDs and dimensional surrogate keys separate. Resolve carrier/facility/service aliases only using explicit effective-dated reference assertions. Preserve all candidates and the rule used. Zero candidates are unresolved; multiple candidates are ambiguous. Neither case is silently merged.

Choose shipment state using documented source authority and deterministic time/revision tie handling. Preserve losing evidence. A tombstone retracts a source assertion; it does not erase prior evidence. Two payloads claiming the same source key and revision block publication.

## Consequences
Some facts legitimately use unknown dimension members until a later accepted reference resolves them. Incorrect reference data must be corrected explicitly, with evidence; fuzzy matching is not used to raise a cosmetic match rate. Canonical IDs are stable across reruns and file order changes.
