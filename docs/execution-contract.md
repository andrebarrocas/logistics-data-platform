# Executable contract

This contract fixes implementation names after the conceptual models. All SQL instants are UTC `TIMESTAMP`; parsed aware timestamps are converted before persistence. Open intervals end at `9999-12-31 00:00:00`. Names below are exact.

## Adapter envelope

The normalized evidence envelope is `source_system, entity_type, source_id, business_key, revision, operation, effective_at, updated_at, attributes`. Its immutable identity is `(source_system, entity_type, source_id, revision)`. Additional ingestion fields: `payload_hash, raw_hash, row_number, batch_id, run_id`. Raw payloads are retained. `operation` is `upsert` or `delete`. Country keys must occur in the controlled synthetic reference subset, never merely match a two-letter regex.

## Typed projection inputs

Python validates and resolves evidence, then populates the following transactional staging tables. Absent canonical reference IDs are SQL NULL. The SQL model resolves those to surrogate 0. Monetary values are BIGINT minor units. Every staging row includes `raw_hash VARCHAR, run_id VARCHAR`.

| Table | Fields beyond raw_hash/run_id |
|---|---|
| stg_reference | entity_type, business_key, canonical_id, canonical_value, attributes JSON, effective_from TIMESTAMP, revision BIGINT, source_updated_at TIMESTAMP, status, source_id |
| stg_order | canonical_id, business_key, created_at TIMESTAMP, country_id, is_deleted BOOLEAN, source_id, revision BIGINT |
| stg_package | canonical_id, business_key, shipment_id, weight_grams BIGINT, is_deleted BOOLEAN, source_id, revision BIGINT |
| stg_shipment | canonical_id, business_key, order_id, carrier_id, origin_facility_id, destination_facility_id, route_id, country_id, service_level_id, created_at TIMESTAMP, promised_at TIMESTAMP, state, is_deleted BOOLEAN, source_id, revision BIGINT, updated_at TIMESTAMP, state_occurred_at TIMESTAMP |
| stg_event | source_system, event_id, event_canonical_id, shipment_id, package_id, carrier_id, state, occurred_at TIMESTAMP, source_updated_at TIMESTAMP, verified BOOLEAN, revision BIGINT |
| stg_cost | cost_id, cost_canonical_id, shipment_id, carrier_id, amount_minor BIGINT, currency, cost_type, incurred_at TIMESTAMP, revision BIGINT, source_updated_at TIMESTAMP, is_deleted BOOLEAN |

All columns not otherwise typed are VARCHAR. Reference attributes use business keys: `country_key, region_key, origin_facility_key, destination_facility_key, default_service_key`; optional tracked attributes are `operating_status, target_business_days, distance_km`. `canonical_value` comes from `attributes.name`. Alias reference records are processed into a separate resolver, not a dimension.

Reference staging retains all effective boundaries with their highest corrected source revision. Non-reference staging contains the current highest revision per logical source identity. An authority conflict at the same effective boundary must fail before projection. SQL ranks shipment state candidates separately from shipment identity and structural fields. The adapter always sets `stg_shipment.state_occurred_at` from the operational record's normalized `effective_at`; this is distinct from shipment creation and source update time. Operational delivery milestones use that state occurrence. SQL retains a `created_at` fallback only for older direct staging fixtures; normal ingestion supplies the explicit occurrence. `canonical_shipment.operational_state_occurred_at` preserves the operational input, while `canonical_shipment.state_occurred_at` describes the selected winning assertion.

## SQL functions and entrypoints

`canonical_id(entity_type, business_key)` returns UUID5 text. `surrogate_key(entity_type, canonical_id, effective_from)` returns deterministic positive BIGINT from the first 8 SHA256 bytes, masked to 63 bits (zero maps to one). Input timestamps serialize as `YYYY-MM-DDTHH:MM:SS.ffffffZ`. Hash collisions are checked as a publication invariant.

The SQL module exposes `create_staging(connection)` and `project(connection, run_id)`; the caller owns the transaction. `quality_checks(connection)` returns structured `{check_id,severity,observed,expected,passed,details}` records. SQL files are repository resources and package data.

Publication audit stores previous and new product row JSON keyed by run and table/entity identity. This plus immutable source revisions is the recorded-time audit. The local implementation intentionally rebuilds projection tables from accepted evidence each publication; incremental file/revision ingestion is distinct from full projection recomputation. Measured scale limits must be disclosed.
