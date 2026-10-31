# Source-system specifications

All sources are independently generated fixtures. Formats intentionally differ; identifiers are synthetic and do not reproduce a proprietary schema. Source records preserve their original values alongside normalized fields. This document specifies the executable adapter contract; examples of operational problems are generated against that contract.

## Shared delivery contract

Every publication batch has a manifest with `batch_id`, `ingest_sequence`, `created_at`, `required_sources`, `files`, and `controls`. Each `files` entry contains exactly the generated fields `source`, `path`, `schema_version`, `sha256`, and `rows`. Each source control contains `files` and `rows`; the transportation-cost control additionally contains `amount_minor`, grouped by currency. Source manifests do not contain per-file `dataset` or `bytes` fields. Dataset labels below describe the source content; they are not additional manifest columns. Release-export manifests are a separate contract. The [generator](../src/logistics_platform/generator.py) and [manifest validator](../src/logistics_platform/contracts.py) define executable property names and types.

All five sources are required: `shipment_ops`, `carrier_delivery`, `package_events`, `transport_costs`, and `reference_master`. Expected file counts/shards and row counts are declared before ingestion. An explicit zero-row file is a valid empty extract; an absent file is not. CSV header lines and blank JSONL lines do not count as data rows. A duplicate row counts toward physical row controls, but not canonical counts after deterministic deduplication.

The cost manifest declares expected shards as well as totals. Fewer shards must fail even when the received rows happen to add up to a plausible amount. Monetary values use the source column `amount_minor`; for the supported currency EUR, a minor unit is one cent. Controls are grouped by currency; unsupported currencies block publication rather than being summed into EUR.

Input files are immutable. Reuse of a published batch ID with different manifest or object checksums is rejected. Producer-side fixture creation also refuses to overwrite an existing nonempty batch directory. Identical file content under a different path is a duplicate delivery when source, schema version, and hash match. Keep every receipt record; transform logical content once. A published manifest rerun is a no-op with a recorded run result.

A manifest proves agreement with the producer's declared completeness contract. It cannot prove that the producer omitted records before constructing the manifest; independent volume and business controls are also required. No inferred “latest file” replaces an explicit file list.

The normalized adapter envelope contains `source_system`, `entity_type`, `source_id`, `business_key`, `revision`, `operation`, `effective_at`, `updated_at`, and `attributes`. Local `evidence` adds `payload_hash`, `raw_hash`, `row_number`, `batch_id`, `run_id`, and `raw_record`; ingestion/recording time is obtained through the linked run report. The per-file `schema_version` remains in the original source manifest, not a dedicated evidence-table column. Spark evidence retains normalized fields, hashes, and raw JSON, with batch/run context in the immutable output path and report; it does not have the local table's identical physical lineage columns. See [execution-contract.md](execution-contract.md) and [the actual local catalogue](catalog.json). All normalized instants use UTC and retain raw timestamp strings. Timestamps must contain offsets; naive timestamps have no implicitly guessed timezone. `updated_at` tracks source modification; `occurred_at`, `effective_at`, and `incurred_at` express business time. The publication checkpoint is an ingestion sequence, never an event-time filter.

## 1. Shipment operations — PostgreSQL-style CDC export

**System ID:** `shipment_ops` · **Format:** JSONL · **Dataset:** operational entities.

This synthetic export models a relational source; a running PostgreSQL service is not required for the local demonstration.

Exact envelope fields:

```json
{"entity_type":"shipment","source_id":"OPS-SHP-000017","business_key":"SHP-000017","revision":1,"operation":"upsert","effective_at":"2026-01-01T10:00:00Z","updated_at":"2026-01-01T10:01:00Z","attributes":{"order_key":"ORD-000017","carrier_alias":"ASTER","origin_facility_alias":"FAC-LIS","destination_facility_alias":"FAC-MAD","route_key":"RTE-LIS-MAD","service_alias":"STD","country_code":"ES","state":"in_transit","created_at":"2026-01-01T09:00:00Z","promised_at":"2026-01-04T09:00:00Z"}}
```

`entity_type` is `order`, `shipment`, or `package`; `operation` is `upsert` or `delete`. Revision increases per `(entity_type, source_id)`. Exact repeated versions are duplicates; different payloads at the same revision are contract conflicts. Deletes retain source and business identity and never erase prior evidence.

| Entity type | Business key | Attributes used by integration |
|---|---|---|
| `order` | Organization-scoped order number | `created_at`, `country_code`; source extras remain evidence and are not automatically projected. |
| `shipment` | Organization-scoped shipment number | `order_key`, `carrier_alias`, `origin_facility_alias`, `destination_facility_alias`, `route_key`, `service_alias`, `country_code`, `state`, `created_at`, and `promised_at`. |
| `package` | Organization-scoped package number | `shipment_key`, optional `weight_grams`; a separate package creation timestamp is not projected. |

Attribute requirements are versioned by the adapter. Each upsert is a replacement source snapshot, not a field-level patch: missing optional attributes can become null or the documented default in the new projection; values are not automatically copied from the preceding revision. Missing required attributes fail validation. The earlier complete payload remains in evidence. Source IDs identify operational rows, not canonical entities. A changed source ID may resolve to an existing entity only through the same accepted business key. Initial extraction emits current rows as upserts; subsequent batches contain changes and tombstones. The business effective time may precede `updated_at` by several days. Generated problems include duplicate CDC records, source-ID changes, missing attributes, stale assertions, impossible timestamps, and historical corrections.

## 2. Carrier delivery files

**System ID:** `carrier_delivery` · **Format:** CSV · **Dataset:** delivery events.

| Version | Required column contract |
|---|---|
| `1` | `event_id,shipment_key,package_key,carrier_alias,state,occurred_at,updated_at,revision,verified` |
| `2` | `event_id,shipment_key,package_key,carrier_alias,delivery_state,event_time,updated_at,revision,verified` |

The per-file manifest selects the adapter. Version 2 renames `state` to `delivery_state` and `occurred_at` to `event_time`; the adapter maps these to the same typed canonical fields. An unsupported version, missing mandatory column, or version/header mismatch fails validation. Mandatory renames are never guessed. Both supported CSV versions require their exact declared column list and order. Additional or renamed columns require a supported adapter contract; the current versions do not silently accept them.

The observation key is `(source_system, event_id)`; the highest valid `revision` is its current interpretation. Higher revisions preserve earlier payloads as superseded evidence. A corrected `delivered` event can become another state; the previous delivery confirmation must then be removed from the current projection while remaining in audit history.

`verified` must parse as an explicit boolean. Only true, supported carrier delivery assertions qualify for the highest state-precedence tier. A delivered string alone is not verification. In this synthetic contract, `verified=true` models upstream verification; it does not claim a real-world proof system.

Generated aliases `CAR-AP`, `ASTER`, `Aster Parcel`, and `AP-001` resolve only through a source-scoped, dated rule. Records can arrive days after occurrence. Generated problems include identical files under new names, duplicate events, schema v1/v2 changes, conflicting aliases, unverified delivery, and disagreement with operational state.

## 3. Package-event feed

**System ID:** `package_events` · **Format:** JSONL · **Dataset:** package observations.

Exact envelope fields:

```json
{"event":{"id":"EVT-17","state":"in_transit","occurred_at":"2026-01-01T12:00:00Z"},"shipment_key":"SHP-000017","package_key":"PKG-000017","carrier_alias":"ASTER","updated_at":"2026-01-01T12:01:00Z","revision":1}
```

The logical observation ID is `event.id`. The highest revision supersedes prior interpretations. The manifest supplies schema version. The canonical state vocabulary is `created`, `collected`, `in_transit`, `out_for_delivery`, `delivered`, `delivery_failed`, `returned`, and `cancelled`; source normalization uses the declared versioned mappings. The adapter retains the nested original JSON. Optional additions may be preserved; required-field type changes, invalid offsets, or unsupported versions fail validation.

Events can arrive out of order or extremely late. Processing never discards an event because its occurrence precedes an ingestion checkpoint. Package-to-shipment relationships must match accepted membership when available; contradictory links are quarantined. Missing reference descriptions may produce explicit unknown dimension keys without merging identities. Package evidence does not become verified carrier evidence merely by using the state `delivered`.

## 4. Transportation-cost system

**System ID:** `transport_costs` · **Format:** sharded CSV · **Dataset:** cost entries.

Exact columns:

```text
cost_id,shipment_key,carrier_alias,amount_minor,currency,cost_type,incurred_at,updated_at,revision,operation
```

`cost_id` is the source business identifier for one cost entry. `amount_minor` is a signed integer; `currency` is explicitly `EUR`. `operation` is `upsert` or `delete`. A correction is a higher revision of the same cost ID; only the current accepted revision contributes to the current fact total. All previous amounts remain in source history. Negative entries represent adjustments or reversals and retain their own cost IDs. A delete is an auditable source tombstone; current and historical financial views must state whether tombstoned entries are excluded, never silently lose their evidence.

The grain is independent of package/event count. Finance totals use unique accepted current cost entries; joining costs to every delivery event must not multiply amounts. Raw controls include physical duplicates. The executable ingestion bridge partitions parseable raw amount into newly accepted evidence revisions and duplicate/replayed evidence. New evidence can include subsequently superseded versions; these remain auditable and are excluded only when current state is projected. Invalid monetary rows block the batch. Published totals separately reconcile to the accepted current cost state after applying CDC.

Every batch declares the entire expected shard set and signed controls. Missing shards, wrong totals, noninteger amounts, or unsupported currency block publication. A parse failure is never coerced to zero. An entry whose canonical shipment has not arrived blocks the shipment-relationship gate. A known shipment with missing descriptive dimensions may use explicit unknown surrogate keys, subject to the documented quality threshold; those are different conditions. Generated incidents include missing shards, repeated entries, wrong totals, orphan keys, and late historical corrections.

## 5. Reference/master-data feed

**System ID:** `reference_master` · **Format:** JSONL · **Dataset:** reference records.

Exact envelope fields:

```json
{"entity_type":"carrier","source_id":"REF-CAR-AP","business_key":"CAR-AP","revision":1,"operation":"upsert","effective_at":"2026-01-01T00:00:00Z","updated_at":"2026-01-01T00:01:00Z","attributes":{"name":"Aster Parcel"}}
```

`entity_type` is `carrier`, `facility`, `route`, `country`, `region`, `service_level`, or `alias`. `operation` is `upsert` or `delete`. Reference versions retain provenance, lineage, business/effective dates, source modification timestamp, and active/inactive status. Half-open intervals are `[effective_from, effective_to)`; `9999-12-31T00:00:00Z` is the open-ended sentinel. A higher revision corrects the same source identity/effective boundary; a new boundary preserves the earlier business history. Country and region label changes remain Type 1, while active/inactive status changes retain effective history.

Alias records use these exact attributes:

```json
{"target_type":"carrier","source":"carrier_delivery","alias":"ASTER","target_key":"CAR-AP","effective_to":"9999-12-31T00:00:00Z","rule_id":"ER-SYN-0006"}
```

An alias row represents an approved deterministic mapping with an auditable `rule_id`. Within one alias `source_id`, the next effective boundary ends the preceding assertion: the usable end is the earlier of its explicit `effective_to` and the next boundary. A tombstone closes the preceding interval and supplies no mapping until a subsequent upsert. Corrected revisions at the same boundary are selected before intervals are constructed. Different source IDs with overlapping effective mappings to distinct targets remain ambiguous. Approval belongs to the authoritative reference feed, not an inferred similarity score. Match source, target entity type, conservatively normalized alias, and business-effective interval. Normalization trims surrounding whitespace and changes case; punctuation is not discarded. Multiple active distinct targets produce an explicit ambiguous resolution and block publication of the conflicting reference set. No fuzzy match or arbitrary first-row selection is allowed.

Countries use a controlled, versioned uppercase two-letter reference set. Existing controlled codes are global country keys: a contradictory alias such as `PT` → `ES` is rejected by the shared adapter with `CAN_RELATION_CONFLICT`, rather than being ignored behind a direct-key lookup. An unrecognized convention such as `UK` is not silently converted to `GB`; a source-scoped explicit alias rule is required. Reference rows may arrive after facts. Known business identity can precede descriptive dimensions, producing an unknown historical attribution that is repaired when dated reference evidence arrives.

Generated problems include conflicting aliases, invalid countries, missing facilities, service-description changes, carrier Type 1 spelling corrections, and backdated facility/region Type 2 changes.

## Cross-source identity and time rules

- Shipment, order, and package keys belong to the fictional organization's namespace. External records explicitly carry them. Missing or contradictory identity is quarantined, never inferred from nearby dates, names, or amounts.
- Source keys are namespaced by source and entity type. Canonical IDs derive from normalized entity type and accepted business key. Surrogate dimension keys identify warehouse versions.
- Shipment carrier/facility/service identity is resolved at `attributes.created_at`, matching shipment dimensional attribution; the shipment state observation separately uses envelope `effective_at`. Event and cost identity resolution use their occurrence/incurred time. Alias lifecycle boundaries apply before matching. Correction selection uses source revision; ingestion order does not establish business precedence.
- Distinct logical observation IDs remain distinct observations even with identical payloads. Delivery duplication is handled separately from legitimate repeated business observations.
- An old business fact in a new ingestion batch remains eligible. The implemented engines rebuild complete projections from accepted evidence, producing the same business result as full replay over that history; they do not claim to update only impacted partitions.
