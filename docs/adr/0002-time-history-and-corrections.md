# ADR 0002: valid time and recorded time

Status: accepted at Phase 0.

## Context
Late evidence, backdated reference corrections and current labels have different historical meanings. A single updated_at cannot represent them all.

## Decision
Normalize offset-aware input to UTC. Reject ambiguous naive timestamps. Store occurred/effective time, source update time, ingestion identity and publication/run time separately. SCD2 periods are half-open [valid_from, valid_to); the final period ends at 9999-12-31. Store source revisions and resolution decisions append-only. Rebuild affected timelines using highest revision per effective boundary, collapse unchanged adjacent values, and restate impacted fact foreign keys. Retain publication audit snapshots so valid-time restatement does not erase prior knowledge.

Use SCD1 for nonsemantic display corrections. Use SCD2 for operational attributes affecting grouping and interpretation. Unknown surrogate key 0 is reserved in each dimension. Deletes become inactive/tombstoned assertions; history remains queryable.

## Consequences
Late reference arrival can change a previously unknown fact key without changing fact grain. Backdated corrections may split historical periods. This is explicitly a restated valid-time warehouse with recorded publication audit, not a claim of full arbitrary bitemporal querying in every mart.
