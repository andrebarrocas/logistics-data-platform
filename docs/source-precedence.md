# Source precedence and conflict decisions

A single source of truth is an auditable projection of preserved evidence. It is not a destructive overwrite of conflicting records. All source observations, invalid records, previous revisions, identity-resolution outcomes, selected winners, and rule IDs remain available.

## Resolve identity before selecting values

1. Parse a record under its declared source schema.
2. Deduplicate exact source versions and reject conflicting payloads at the same logical key/revision.
3. Apply CDC so each source logical entity/event has a current interpretation; keep superseded revisions as history.
4. Resolve entity identity using an accepted business key or a dated, source-scoped crosswalk.
5. Classify the resolution as `resolved`, `unresolved`, or `ambiguous` and preserve the evidence.
6. Select attributes from eligible observations under the rules below. Unresolved or ambiguous records cannot assert a value about a guessed entity.

An exact business key is deterministic only inside its documented namespace. A source row ID is not a shipment number. Alias match evidence records the normalized source value, source, entity type, lookup time, rule ID, target business key, and outcome. Shipment carrier/facility/service identity uses shipment creation time; event/cost identity uses occurrence/incurred time. State occurrence remains a separate timestamp. Within one alias source ID, the next effective boundary closes the previous interval (or its earlier explicit end); a delete supplies no candidate. Only the highest corrected revision at a boundary participates. Distinct source IDs that overlap with different targets remain ambiguous. No name similarity threshold can auto-merge records.

## Attribute authority

| Attribute group | Authority | Conflict behavior |
|---|---|---|
| Canonical reference names and controlled country/service codes | Effective `reference_master` record | Operational values remain source aliases; conflicts or overlaps in reference authority fail the reference quality gate. |
| Order, shipment, and package business identity and membership | Accepted organization business keys and `shipment_ops` membership | Contradictory package/shipment links are quarantined and reported; identity is never changed to make a join succeed. |
| Shipment route, service, and facility assignment | Latest current `shipment_ops` assertion, resolved through dated reference keys | Preserve original alias/key and the reference mapping used. Missing descriptive dimensions may use key `0`. |
| Transportation amounts and currency | Current accepted `transport_costs` version per `cost_id` | Never substitute an estimate from another source. Preserve corrections and tombstones, reconcile the current contribution explicitly. |
| Reference display spelling | Authoritative Type 1 correction | Audit the source correction even if dimension display text is overwritten. |
| Historical facility/route/region classification | Authoritative effective-dated Type 2 reference version | Rebuild affected validity intervals and restate impacted fact attribution. |

## Shipment-state authority

State selection uses the following policy:

| Priority | Evidence tier | Eligibility |
|---|---|---|
| 300, highest | Verified carrier delivery assertion | Carrier source, `verified=true`, resolved shipment, accepted state and timestamp. |
| 200 | Confirmed operational observation | Accepted `shipment_ops` observation with a concrete observed state and valid business time. |
| 100, lowest | Package event, unverified carrier observation, or estimate | Accepted observation with valid business time; never promoted to verified delivery by age or ingestion order. |

A verified delivery assertion wins over a contradictory lower-tier transit assertion even when the latter is received later. First select the current revision of each logical observation; then choose the highest eligible tier and the latest occurrence within that tier. Candidates with equal authority and occurrence but different states are an explicit unresolved conflict and block publication. Equal-state ties select deterministic lineage using source update time, source system, and source logical observation ID; this does not change the selected business value.

A correction to the winning event must supersede the previous revision **before** precedence is reevaluated. Otherwise a retracted delivered revision could remain a permanent false winner. A late observation with old occurrence time remains in event history even if it does not replace the current state.

Concrete state transitions are checked within the source event history as well as the consolidated timeline. Example normal progression is `created → collected → in_transit → out_for_delivery → delivered`. A `delivery_failed` attempt may be followed by another `out_for_delivery`; `delivered → returned` is allowed when explicitly observed. Missing intermediate observations permit a forward jump; other terminal reversals require an explicit correction. Contradictory terminal transitions or cancellation after delivery are surfaced as quality exceptions and preserved. Arrival order is not transition order. Revisions of one event are corrections, not additional transitions.

Package-level observations can inform the relevant shipment's event evidence but do not by themselves prove all packages in a multi-package shipment were delivered. Shipment delivery must be supported by an explicit shipment-level confirmation or the configured all-packages aggregation with complete known membership; partial membership cannot produce a verified whole-shipment delivery.

## Timestamps and milestones

The latest supported state and a milestone timestamp are separate projections. An estimated delivery timestamp never populates actual delivery time. Actual delivery time comes from the selected eligible delivery assertion's business occurrence, not its file receipt time. `first_observed_at`, `source_updated_at`, and `ingested_at` remain separate. Retractions can remove an actual delivery milestone from the current projection while retaining the historical decision.

## Decision evidence

Each selected canonical value or state is attributable to a decision containing canonical entity ID, attribute, selected value, chosen source record/event ID and revision, business occurrence/effective time, source update time, batch/object/row lineage, policy/rule identifier, evidence tier, and alternatives with rejection/supersession reasons. A decision must be reproducible from immutable inputs and the versioned policy.

## Acceptance cases

| Case | Required outcome |
|---|---|
| `ASTER` has exactly one effective approved carrier mapping | Resolve to its existing canonical carrier and record the rule ID. |
| The same alias has two effective targets | Return ambiguous; preserve both mappings; fail reference publication. |
| Carrier reports delivered with `verified=false`; operations report in transit | Apply the documented operational/unverified tier policy; do not invent verified delivery evidence. |
| Verified carrier delivery conflicts with later-received operations transit | Choose verified delivery and preserve the conflict. |
| Higher revision changes the winning delivery event to in transit | Supersede the earlier interpretation and recompute state and actual delivery timestamp. |
| Old package event arrives in a new batch | Ingest it and recompute affected history; do not discard by watermark. |
| Late reference version changes historical facility region | Rebuild affected Type 2 intervals and reattribute historical facts at their business time. |
| One of two packages is delivered | Do not infer complete shipment delivery from that package event alone. |
| Identical candidates arrive in a different physical order | Produce the same canonical result, preserving source lineage. |
