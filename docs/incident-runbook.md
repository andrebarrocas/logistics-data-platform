# Incident engineering runbook

These are reproducible **synthetic** failures, not accounts of real production incidents. Each exercise first publishes a healthy baseline, applies the incident delivery, then applies a recovery delivery and repeats that recovery. The runner writes the actual pipeline results to per-scenario JSON reports and fails if expected publication, rejection, recovery, or idempotency outcomes do not occur.

Run from the installed project environment:

```bash
python - <<'PY'
from pathlib import Path
from logistics_platform.incidents import run_incidents
reports = run_incidents(Path("work/incident-exercises"), shipments=12, seed=42)
for report in reports:
    print(report["scenario"], report["passed"])
PY
pytest tests/test_incidents.py -q
```

Use a new output directory for a repeat experiment; immutable fixture batches cannot be overwritten. Each scenario creates an isolated warehouse, so an intentional fault cannot contaminate another investigation. Tests additionally compare all published facts, reference history, accepted evidence, and the committed-batch ledger before/after failure and after replay.

## Common investigation sequence

Start with the run's JSON quality report and committed watermark. Identify the earliest failing invariant, then trace source → manifest → raw file/hash/row → normalized evidence → resolution → selected canonical value → fact. Avoid repairing an analytical row directly; that loses the cause and will be overwritten by replay.

Useful SQL against the local incident warehouse:

```sql
SELECT run_id,batch_id,status FROM pipeline_runs;
SELECT run_id,rule_id,severity,status,result
FROM quality_results WHERE status='fail';
SELECT batch_id,ingest_sequence,manifest_hash FROM published_batches
ORDER BY ingest_sequence;
SELECT source_system,source_id,revision,operation,effective_at,updated_at,raw_hash
FROM evidence WHERE source_id='COST-000001' ORDER BY revision;
SELECT source_system,object_path,raw_hash,row_count,duplicate
FROM file_receipts ORDER BY source_system,object_path;
SELECT currency,sum(amount_minor) AS amount_minor
FROM fact_transportation_cost GROUP BY currency;
```

A failed candidate retains its structured run report even though its candidate evidence and facts are rolled back. The original fixture files remain the source of truth for investigating failed input. A new corrected payload receives a new batch ID at the next **unpublished** ingestion sequence; failed attempts do not consume that sequence. Restoring a missing immutable object can replay the exact original manifest.

## 1. Unannounced carrier schema change — `schema_drift`

1. **Symptom:** Delivery updates stop publishing while previously published products remain available.
2. **Detection:** `ING_SCHEMA` rejects a v1 file whose mandatory `state` column became `unannounced_status`. The fixture recalculates its correct checksum, deliberately proving schema validation detects the change rather than relying on accidental corruption.
3. **Investigation:** Compare manifest version, CSV header, adapter's required columns, physical counts, and checksum. Check whether the producer changed field meaning or only names. Confirm the watermark remains at baseline and inspect the failed report's expected/received headers.
4. **Root cause:** The producer changed a mandatory field without an agreed contract version; the ingest adapter cannot safely infer semantics.
5. **Remediation:** Accept only a documented version. The recovery uses the implemented v2 contract (`delivery_state`, `event_time`) and explicitly declares v2 in the manifest.
6. **Recovery:** Publish the corrected delivery under a new batch identity at the unconsumed sequence, reconcile event counts and chosen state, then rerun to prove a no-op. No manual fact patch is needed.
7. **Prevention:** Versioned source contracts, producer/consumer fixture tests, manifest schema identifiers, and mandatory-header checks before staging. Additive fields and mandatory renames have different compatibility policies.

## 2. Identical carrier file delivered twice — `duplicate_file`

1. **Symptom:** Physical received row/file counts increase without an increase in legitimate shipment events.
2. **Detection:** `ING_DUPLICATE_FILE` and duplicate-version results record repeat receipt. Publication succeeds with visible warnings because logical identity and payload agree.
3. **Investigation:** Compare source, schema, checksum, event ID, revision, and payload. Identical bytes under a new filename are delivery duplication; distinct event IDs that happen to have similar payloads are not automatically duplicates.
4. **Root cause:** A synthetic transport retry emitted the same file under another filename.
5. **Remediation:** Preserve both receipts, apply each logical event version once, and retain exact duplicates as auditable counts. No source event is guessed away.
6. **Recovery:** Publish without multiplied facts or money; the tests compare shipment/event counts and transportation totals to baseline. Repeat the recovery delivery and confirm `noop` with unchanged published state.
7. **Prevention:** Immutable manifests, source-aware content hashes, stable event IDs and revisions, and uniqueness tests. File paths alone are insufficient deduplication keys.

## 3. Partial transportation-cost feed — `partial_cost`

1. **Symptom:** One expected cost shard is missing; the received subset could otherwise look like a reasonable smaller daily amount.
2. **Detection:** `ING_EXPECTED_FILES` fails against the predeclared two-shard inventory. Cost row and money controls provide independent subsequent safeguards.
3. **Investigation:** Enumerate manifest files and actual objects, verify which shard is absent, inspect producer delivery state, and compare received versus expected row/amount controls. Confirm every SSOT product and the watermark remains at the baseline.
4. **Root cause:** The producer delivery was interrupted after sending one of two shards. Successful parsing of the first shard is not completion.
5. **Remediation:** Restore the exact missing object under the unchanged manifest, including its original content hash. Do not reduce the manifest's expected totals to make the incomplete input pass.
6. **Recovery:** Reprocess the same declared batch once all files exist. The recovered batch publishes all dependent products atomically. Replay must produce `noop`, and prior facts/evidence must have remained unchanged during failure.
7. **Prevention:** Manifest-last producer handoff, explicit required shard inventories, raw signed totals by currency, atomic publication, and alarms on missing expected deliveries. A producer manifest alone cannot detect omitted upstream records before manifest creation, so volume and business checks also remain necessary.

## 4. Conflicting reference alias — `ambiguous_alias`

1. **Symptom:** `ASTER` from `carrier_delivery` points to two different canonical carrier business keys.
2. **Detection:** `REF_AMBIGUOUS_ALIAS` returns the overlapping candidate rules and blocks the entire candidate publication.
3. **Investigation:** Compare source scope, entity type, normalized alias, effective intervals, target business keys, and rule IDs. A mapping in another source or nonoverlapping time window is not a conflict; two current distinct targets are.
4. **Root cause:** An erroneous authoritative feed row assigned a known Aster alias to Meridian Freight without ending or correcting the original rule.
5. **Remediation:** Obtain explicit reference-steward evidence and issue an audited higher revision mapping the alias to its supported target. Never choose the lexicographically first target or a similarity score.
6. **Recovery:** Publish the corrected new delivery at the unconsumed sequence. Reevaluate affected identities and facts, check no carrier multiplication, and verify idempotent replay.
7. **Prevention:** Source-scoped unique effective mapping checks, overlap validation in reference publishing, explicit steward provenance, and ambiguity tests that reverse input order.

## 5. Several-day-old historical correction — `historical_correction`

1. **Symptom:** A previously reported cost is corrected by 125 cents, a delivery timestamp is revised, and a facility region change is effective before its receipt date.
2. **Detection:** New higher source revisions are recognized even though their business timestamps precede the current delivery date. Current-cost reconciliation and SCD interval checks run before publication.
3. **Investigation:** Compare logical source IDs and all revisions; distinguish update/receipt time from business effective time. Inspect which facility version covers each fact's business instant and which event revision currently wins state precedence.
4. **Root cause:** The synthetic upstream systems corrected earlier mistaken assertions and supplied the authoritative revision several days later. This is a supported business event, not input corruption.
5. **Remediation:** Preserve previous evidence, select the higher cost/event revisions, split the facility Type 2 interval at the backdated boundary, and recompute affected projections. A date-only ingestion filter would miss the change.
6. **Recovery:** The incident delivery itself publishes. Tests prove current cost increases by exactly 125 cents, both cost revisions remain in evidence, and the facility has two valid historical versions. Follow with an empty complete batch and repeat it to verify stable state.
7. **Prevention:** Revision-aware CDC, separate recorded/business time, half-open nonoverlapping intervals, audit snapshots, and full-replay equivalence tests. Current local projection rebuilding is a correctness-first implementation; its measured scale limits are documented separately.

## 6. Extremely late package event — `late_event`

1. **Symptom:** An event occurring on January 2 is received with a January 7 source update after later milestones are already known.
2. **Detection:** Event occurrence precedes source modification, and the event appears in current event facts after the new ingestion batch. Freshness and event lateness are separate measurements.
3. **Investigation:** Compare event occurrence, source update, batch sequence, and previously accepted logical event IDs. Determine whether this is a new observation or a correction of an existing event. Order chronology by business time rather than receipt.
4. **Root cause:** A simulated mobile/feed outage delayed transmission of a real earlier package observation.
5. **Remediation:** Ingest the new logical event and recompute affected chronology without reducing the shipment's stronger verified delivery evidence. Do not discard it with an event-time watermark cutoff.
6. **Recovery:** Publication adds exactly one event and leaves cost totals unchanged. The succeeding empty batch and repeat demonstrate no duplication. The late source observation remains traceable to its own raw line.
7. **Prevention:** Checkpoint ingestion sequences, retain source event IDs, monitor lateness distributions, and allow explicitly bounded operational backfills with old business dates. A backfill must obey the same gates as ordinary delivery.

## 7. Unresolved foreign-reference spike — `foreign_key_spike`

1. **Symptom:** A new operations update carries unmapped carrier, origin, and destination aliases across shipments.
2. **Detection:** `CAN_UNRESOLVED_IDENTITY` exceeds the configured unknown-reference threshold (the local default is 0.25). The fixture corrupts multiple reference fields so the increase is material across all attempted resolutions.
3. **Investigation:** Group unresolved decisions by source, entity type, raw alias, and rule. Compare the last good producer payload, current reference coverage, and effective intervals. Key `0` satisfying a warehouse foreign key must not hide a sudden loss of reference quality.
4. **Root cause:** A simulated producer configuration replaced agreed identifiers with an unregistered namespace.
5. **Remediation:** Restore the agreed producer identifiers with a higher source revision. Alternatively, a real steward could supply explicit supported crosswalk evidence; this exercise does not manufacture such evidence.
6. **Recovery:** The failed batch leaves prior facts and watermark unchanged. A corrected new batch at the unconsumed sequence resolves aliases, publishes, and passes repeat no-op checks.
7. **Prevention:** Monitor unknown usage separately from orphan keys, enforce producer identifier contracts, validate reference coverage before a release, and retain configurable explicit thresholds in quality outputs. Raising a threshold is not a root-cause fix.

## 8. Country convention changes — `country_drift`

1. **Symptom:** Operations unexpectedly uses `UK`, outside the controlled country business keys, in a feed whose previous shipment country was `ES`.
2. **Detection:** `REF_INVALID_COUNTRY` fails despite `UK` resembling a two-letter code. Syntax alone does not establish a valid reference value or geographic truth.
3. **Investigation:** Inspect original country fields, source schema version, authoritative country set, and any source-scoped aliases. Determine whether the producer changed coding convention, population, or assignment logic; an unrequested rewrite to `GB` could conceal either error.
4. **Root cause:** A synthetic producer release replaced the configured country assignment with a different convention/value without a contract or reference change.
5. **Remediation:** The fixture restores the prior correct `ES` assertion in a higher source revision. If the intended new business value were actually GB, it would require independent explicit source evidence and an approved policy, not a string heuristic.
6. **Recovery:** Publish the corrected batch only after country and fact attribution checks pass. Confirm the failed run changed neither country dimensions nor published facts; repeat recovery is a no-op.
7. **Prevention:** Controlled country membership, source-scoped aliases where justified, producer contract tests, monitoring of value distributions, and review of semantic as well as syntactic schema changes.

## Evidence and limitations

The runner and tests exercise actual local transformation, SQL, reference resolution, transaction rollback, reconciliation, and replay. CloudWatch alarm creation, remote producer outages, S3 object delivery, Glue jobs, and Redshift workload execution require separate deployed-cloud validation; these local incident results do not claim those operations occurred. Keep generated result JSON and commands with any published claim of an exercise outcome.
