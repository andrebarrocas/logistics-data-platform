# Deterministic entity resolution

Resolution is a rules engine over explicit identity evidence. It never infers identity from edit distance, geographic proximity, payload similarity, or a carrier's display name alone. Aster Parcel, Meridian Freight, and Vale Courier and their aliases are independently synthetic.

## Evidence model

The conceptual curated crosswalk contains `entity_type`, `source_system`, `source_key`, `business_key`, `canonical_id`, `valid_from`, `valid_to`, `status`, `evidence_type`, `evidence_reference`, `recorded_at`, and `run_id`. Evidence references point to a synthetic master assertion, an explicit globally scoped key in a validated source contract, or a reviewed synthetic correction. In executable storage, alias records are immutable `evidence` rows whose attributes contain `target_type`, `source`, `alias`, `target_key`, `effective_to`, and `rule_id`; the envelope supplies effective start, revision, and provenance. Decisions in `resolution_decisions` retain candidate keys and rules. An alias mapping is scoped to source and entity type; a string that works for one source does not automatically work in another.

The generated fixture maps `ASTER`, `Aster Parcel`, `AP-001`, and `CAR-AP` to business key `CAR-AP`, through separate source-scoped rules for `carrier_delivery`, `transport_costs`, and the other operational feeds. These mappings are accepted because the synthetic reference feed supplies exact assertions, not because labels look similar. An overlapping `carrier_delivery` mapping from `ASTER` to both `CAR-AP` and `CAR-MF` is ambiguous.

## Ordered rules

The `ER001`–`ER006` labels below describe logical policy requirements; they are not a claim that those strings are emitted by every lookup. Executable alias decisions retain the actual curated `rule_id` values (for example `ER-SYN-0006`) and lowercase outcomes. Global-key validation and relationship failures are enforced by schema/quality gates.

| Rule | Condition | Outcome |
|---|---|---|
| `ER001` | Valid source schema supplies an explicitly globally scoped business key and no contradictory active curated mapping | Resolve UUID5 from the validated business key; retain source-contract evidence. |
| `ER002` | One active, lookup-time-applicable curated source-key mapping exists | Resolve to its target; retain candidate business key and actual rule IDs, linked to immutable alias evidence. |
| `ER003` | Multiple applicable curated mappings point to different business keys | `ambiguous`; no chosen canonical ID; the conflicting reference set blocks publication. |
| `ER004` | A source identity is reused for another business key, package/shipment membership contradicts accepted identity, or a country alias redefines a controlled global country key (for example `PT` → `ES`) | Reject the candidate publication. Contradictory controlled-country aliases emit `CAN_RELATION_CONFLICT`; source evidence and diagnostics remain available. |
| `ER005` | No applicable curated alias mapping exists | `unresolved`; retain lookup evidence. Missing descriptive references follow the explicit unknown-member threshold; missing required business identity fails validation. |
| `ER006` | Exact duplicate evidence rows point to one business key and do not disagree on validity/status | Collapse identical evidence for selection, retain ingestion provenance; resolve once. |

Evaluate contradictions and ambiguity before accepting a superficially valid key. Rule numbering identifies rules, not a license to return early before checking contradictory evidence. A source-supplied “business key” without a globally scoped contract remains a source identifier. Empty strings, null keys, malformed values, and unsupported country code conventions are unresolved or rejected according to the schema contract; they are never hashed into canonical IDs.

## Time and identity corrections

Shipment structural aliases use shipment creation time; event and cost aliases use occurrence/incurred time. The lookup instant is matched against half-open alias lifecycle intervals, so a present-day mapping does not automatically identify an older record. There is no physical `identity_assertion` table. The local `resolution_decisions` JSON contains `entity_type`, `source_system`, `source_id`, `source_value`, `effective_at`, `outcome`, `canonical_id`, `candidate_business_keys`, `rules`, and `decision_id`. It records candidates and the result at one lookup instant, not a copied full mapping interval or separate rule-version field. The table wrapper supplies `run_id`; `pipeline_runs` provides recording time. Alias source revisions and effective intervals are recoverable from immutable reference `evidence` using the retained scope/value, rule IDs, candidates, and accepted run history. See [the canonical storage description](canonical-model.md) and [execution-contract.md](execution-contract.md).

A corrected alias does not rewrite history silently. The steward publishes a new version or explicit correction, the pipeline identifies affected observations, and a bounded replay emits new resolution decisions. Previous decisions remain in the audit ledger. Any change to previously published canonical attribution produces a new resolution decision and a changed publication snapshot, then triggers dimensional/fact reconciliation.

## Source precedence is separate

Resolution answers **which entity** a record concerns. Attribute precedence answers **which assertion** currently represents that entity. A verified delivery event does not gain authority to change a carrier's identity, and an authoritative carrier reference does not gain authority to confirm shipment delivery. Policies are versioned and recorded separately.

## Quarantine and replay

Failed-run reports retain quarantine records containing raw record, source/raw object/row identity, and reason code. Resolution decision records retain candidate keys and applied rule IDs. Remediation is demonstrated by a later successful run from corrected evidence; a dedicated case-management table with first/last-seen lifecycle fields is outside this bounded implementation. Resolution can succeed only after explicit evidence changes or a permitted schema normalization is configured. Replaying unchanged ambiguous evidence produces the same candidate outcome; each attempted run may retain its own diagnostic report. No deduplicated quarantine-case management table is claimed.

Resolution outputs describe individual alias lookups, not a one-to-one partition of source records. One source record can require several reference lookups, while an already validated global business key can require none. Unknown-reference ratios therefore use attempted lookups as their explicit denominator. Adapter/relationship rejections are separate quality results. Earlier failed reports remain immutable when a later corrected load succeeds, preserving the evidence and run sequence for incident review.

## Required examples and assertions

- Two aliases with curated evidence produce one canonical carrier and two traceable source mappings.
- The same source key in different systems does not collide without crosswalk evidence.
- Similar names without curated evidence remain unresolved.
- Two applicable mappings with different targets remain ambiguous regardless of sort order.
- Reusing a source identity for a different business key or contradicting package ownership rejects the candidate publication.
- A controlled global country key cannot be redirected by an alias: `PT` → `ES` is rejected with `CAN_RELATION_CONFLICT`. An approved alias for a noncanonical source convention, such as `UK` → `GB`, is a different case.
- At `valid_to`, the expired mapping is not applicable; a successor at that instant is applicable.
- Identical evidence replay does not change canonical IDs or create additional current mappings.
- An unknown reference produces a pending fact reference only if the shipment identity itself is sound.
- A package claiming a different known shipment does not silently move between shipments.
- A backdated mapping correction retains old decisions; comparing retained publication snapshots identifies the facts whose canonical attribution changed.

## Alias lifecycle implementation

Each alias source record retains its effective boundaries. A successor boundary closes the preceding interval at the earlier of its declared end and the successor start. A tombstone closes the preceding assertion and contributes no active mapping until a later upsert. Distinct source assertions with overlapping different targets remain ambiguous. Shipment structural aliases resolve at shipment creation; event and cost aliases resolve at occurrence/incurrence. Both the local resolver and Spark carrier-resolution output use these clocks.
