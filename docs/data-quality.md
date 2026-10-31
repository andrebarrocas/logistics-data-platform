# Data quality and reconciliation specification

Quality checks are part of publication, not a report after inaccurate products become visible. Each execution attempt writes structured results, including ingestion failures and idempotent reruns. A failed gate leaves the previous published batch and ingestion checkpoint intact.

## Result contract

Each quality result carries `run_id`, `batch_id`, `rule_id`, `dataset`, `severity`, `status`, `observed`, `expected`, `failed_count`, `checked_count`, `details`, and `evaluated_at`. Evidence in `details` includes relevant source file/hash/row, entity or business key, and bounded examples. Results use typed numeric values rather than prose-only pass/fail summaries. `status` is `pass`, `fail`, or `skipped`; skipped is never counted as passed. Severity is `error` (blocks publication), `warning` (publishes with visible degraded status), or `info`.

A run-level summary includes ingestion cursor, start/end timestamps, attempted/accepted/quarantined/duplicate counts, candidate versus published status, rules executed, and publication decision. Records rejected before canonicalization remain in a quarantine/audit store with reason and lineage. Result files and audit tables must be readable without a successful product load.

## Quality stages and gates

1. **Receipt:** validate manifest identity, required sources, expected file set, checksums, physical counts, schema versions, and financial controls.
2. **Parsing:** enforce mandatory types and timestamp contracts; classify malformed records with their original row.
3. **Identity:** deduplicate source versions, detect conflicting same-version records, resolve deterministic keys, and reject ambiguous mappings.
4. **Canonical:** validate state, timestamps, country/reference values, relationship consistency, and reference validity intervals.
5. **Warehouse:** validate grains, SCD intervals, surrogate-key relationships, current totals, and as-of joins.
6. **Publication:** aggregate results; publish canonical and dependent products atomically only if no error rule fails. Advance the ingestion checkpoint only with successful publication.

## Rule catalogue

The table names logical quality requirements. Runtime JSON carries the exact executable IDs; warehouse IDs include `DIM_INTERVAL_OVERLAP`, `GRAIN_FACT_*`, and `FK_FACT_*`. Inspect `quality_results` or a run report for the emitted identifier and observed threshold.

| Rule | Check and expected result | Default severity |
|---|---|---|
| `ING_REQUIRED_SOURCES` | All five required sources are explicitly represented, including zero-row extracts. | error |
| `ING_EXPECTED_FILES` | Exact declared object/shard set is present; no partial feed. | error |
| `ING_CHECKSUM` | SHA-256 equals the manifest checksum for every object. | error |
| `ING_ROW_COUNT` | Physical data rows equal per-file and per-source controls. | error |
| `ING_SCHEMA` | Declared supported schema matches required names and types. | error |
| `ING_BATCH_IDENTITY` | Previously seen batch ID has identical manifest and object identity. | error |
| `ING_DUPLICATE_FILE` | Identical logical file delivery is recorded and processed once. | warning |
| `SRC_DUPLICATE_VERSION` | Exact repeated logical key/revision is removed once with count and lineage. | warning |
| `SRC_VERSION_CONFLICT` | Same logical key/revision has no differing payloads. | error |
| `REF_AMBIGUOUS_ALIAS` | After constructing each source ID's successor/tombstone intervals, no effective source/type/alias resolves to distinct overlapping targets. | error |
| `REF_INTERVAL_OVERLAP` | No conflicting overlapping authoritative versions for a business key. | error |
| `REF_MISSING_VALUE` | Missing noncritical descriptions are explicit unknowns, never invented. | warning |
| `REF_INVALID_COUNTRY` | Country resolves to the approved reference set or an explicit source alias. | error for invalid identity-bearing geography; otherwise quarantine and visible warning |
| `CAN_DUPLICATE_BUSINESS_KEY` | One canonical current entity per entity-type/business-key pair. | error |
| `CAN_RELATION_CONFLICT` | Package/order/shipment relationship evidence is consistent. | error |
| `CAN_TIMESTAMP` | Parseable UTC-normalized timestamps; delivery is not before dispatch where both are known; weights/durations are valid. | error for impossible required values |
| `CAN_STATE_TRANSITION` | Ordered current event interpretations follow the allowed transition policy or carry an explicit correction/exception. | warning with retained evidence; impossible contract states are error |
| `CAN_UNRESOLVED_IDENTITY` | Unresolved records are quarantined and visible; identity-bearing source IDs are not silently dropped. | warning within configured threshold; error above it |
| `WH_FACT_GRAIN` | Shipment, event, and cost business grains are unique. | error |
| `WH_FOREIGN_KEYS` | Every surrogate key exists; allowed unknowns reference the real key `0` row. | error |
| `WH_SCD_INTERVALS` | Type 2 intervals do not overlap, end exceeds start, and at most one current row exists per business key. | error |
| `WH_ASOF_ATTRIBUTION` | Fact business time falls in its attributed dimension interval, or explicit unknown is used. | error |
| `REC_SHIPMENTS` | Accepted current source shipment identities reconcile to canonical shipment identities, with tombstones/quarantine explained. | error |
| `REC_COST_RAW` | Received signed amount per currency equals declared physical source control. | error |
| `REC_COST_BRIDGE` | Raw money reconciles to accepted/superseded/duplicate/quarantined money under the ingestion rules. | error |
| `REC_COST_CURRENT` | Current accepted cost state equals FactTransportationCost, grouped by currency. | error |
| `REC_EVENTS` | Accepted current logical source event interpretations reconcile to event facts, with rejected/withdrawn cases explained. | error |
| `VOL_CHANGE` | Row/entity counts stay within an explicit configured range versus a comparable prior successful batch. | warning, or error for declared hard bounds |
| `INC_CHECKPOINT` | Candidate cursor is valid; failed publication does not advance it. | error |
| `INC_IDEMPOTENCY` | Identical rerun leaves published canonical rows, facts, history, totals, and checkpoint unchanged. | error |

Thresholds must be explicit in configuration and included in result `expected`; no undocumented “acceptable” failure percentage exists. Fixtures for the healthy path have zero invalid mandatory records. Unknown dimension rows are valid foreign keys but their use is separately reported, preventing referential integrity from concealing missing references. The first successful batch has no prior-volume baseline; its volume-change check is `skipped` with that reason.

## Reconciliation mathematics

Shipment reconciliation compares distinct current business identities, not physical CDC row count. Explain inserts, updates, deletes, duplicates, quarantined identity, and unresolved records separately. Event reconciliation first selects the current revision per source/event ID, so corrections are not double counted.

For cost ingestion, use signed integer amounts and keep currencies separate. The executable successful-run bridge is:

```text
physical raw amount
  = newly accepted source-evidence amount
  + duplicate or previously accepted source-evidence amount
```

Newly accepted evidence can include older revisions superseded in the same batch. Those revisions remain in the evidence store and do not each contribute to the current fact total. Unsupported currencies and nonparseable amounts block publication; an invalid batch has no accepted financial publication. The local adapter admits only EUR after validating per-currency raw controls, so this bridge cannot implicitly combine currencies. Then apply accepted CDC to the prior cost state:

```text
new current cost total
  = prior current cost total
  - prior contributions of replaced/deleted cost IDs
  + accepted current contributions of those IDs
```

Finally compare the candidate current canonical cost state with the candidate cost fact total. Negative adjustments remain signed. Never compare raw batch totals directly with the entire cumulative fact table or sum a cost fact after a one-to-many delivery-event join.

## Partial-feed behavior and recovery

A missing cost shard fails `ING_EXPECTED_FILES` before any product is published. The candidate batch may retain parsed diagnostics but cannot expose a mixed current state. Recovery supplies the missing declared object without mutating previously accepted object content, reruns the batch, validates all controls, and publishes once. A producer correction that changes an immutable manifest requires a new batch identity; it cannot masquerade as a rerun.

## Required acceptance suite

| Category | Required assertion |
|---|---|
| Unit/adapter | Each of the five source formats normalizes correctly; unsupported schema and wrong required type fail with lineage. |
| Entity resolution | Exact source alias resolves; unknown remains unresolved; two overlapping distinct-source targets never auto-merge. Within one alias source ID, a successor ends the prior interval and a tombstone removes the mapping until a later upsert; boundary ends are exclusive. |
| SQL/model | Fact grain uniqueness, orphan checks, as-of joins, reconciliation, and SCD interval queries return zero unexplained violations. |
| SCD | Type 1 text correction updates display text; Type 2 effective change preserves previous range; backdated correction repairs the affected range without overlap. Country/region tombstones must retain current inactive status instead of disappearing during interval compression. |
| Incremental | Full replay and incremental application yield equivalent current business state. |
| Idempotency | Duplicate row/file and completed-batch rerun do not multiply facts, history, or money. |
| Historical correction | Higher revision supersedes the same logical event/cost; previous source evidence remains queryable. |
| Late arrival | An event older than the checkpoint's business date is still integrated; early fact references use key `0` and are restated when evidence arrives. |
| Completeness | Missing source, missing cost shard, wrong hash, wrong row control, and wrong money control block atomic publication. |
| Tombstone | Deleted operational record changes current active status without erasing historical evidence; cost deletions have explicit current-total treatment. |
| Recovery | After a failed attempt, corrected new delivery/rerun publishes exactly once and advances the cursor exactly once. |
| Referential integrity | All dimension keys exist, including unknown rows; unknown usage is separately counted. |
| Monitoring | A foreign-reference spike and unexpected country convention change produce structured failing/warning evidence with configured thresholds. |

CloudWatch and local JSON logging should expose run ID, batch ID, status, duration, row counts, quarantine counts, and quality failures. Monitoring data is operational evidence; no invented production success rate or freshness achievement belongs in documentation.
