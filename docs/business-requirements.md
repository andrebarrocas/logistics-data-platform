# Business requirements

## Purpose and scope

**Multi-Carrier Logistics Canonical Data Platform** turns independently generated, inconsistent operational records into auditable canonical entities, historical warehouse dimensions, and self-service logistics data products.

**Subtitle:** Building a trustworthy Single Source of Truth from inconsistent operational systems.

The fictional organization operates within Europe and contracts with the fictional carriers **Aster Parcel**, **Meridian Freight**, and **Vale Courier**. All companies, operational records, schemas, identifiers, volumes, and incidents are invented. This project does not describe or claim association with Amazon or any real logistics company. AWS names identify proposed infrastructure products only.

The primary consumers are operations analysts, finance analysts, and downstream data engineers. The authoritative products must explain both their selected values and the observations they superseded or rejected. “Authoritative” means a published decision under documented evidence and precedence rules; it does not mean every source observation is true or every ambiguous entity has been identified.

## Business questions

1. Which shipments, packages, orders, carriers, facilities, routes, and service levels refer to the same known business entities?
2. What is the latest supported shipment state, and which source evidence justifies it?
3. What happened at an event's business time, and when did the platform learn about or correct it?
4. What transportation costs were posted, reversed, and attributed to each shipment, without multiplying costs through package or event joins?
5. Is a daily delivery complete and reconciled enough to publish?
6. Can an engineer reproduce an incident, identify the root cause, recover safely, and explain the prevention control?

## Non-negotiable requirements

| ID | Requirement | Acceptance evidence |
|---|---|---|
| BR-01 | Independently generate all operational source data with a recorded seed and scale. | Reproducible input files and generator configuration; no imported company dataset. |
| BR-02 | Ingest all five source systems defined in [source-systems.md](source-systems.md). | Raw objects and a validated batch manifest covering every required source, including explicit empty extracts. |
| BR-03 | Preserve immutable raw evidence and identify every parsed record by source, object checksum, and physical row or line. | Selected and rejected records trace to original input. |
| BR-04 | Separate source identifiers, business keys, canonical identifiers, and warehouse surrogate keys. | Canonical and dimensional model documents and executable uniqueness checks. |
| BR-05 | Resolve entities only using approved deterministic evidence. | Exact crosswalk test, ambiguous crosswalk test, unresolved case, and persisted decision evidence. |
| BR-06 | Preserve source disagreement and apply explicit field-level precedence. | Conflicting delivery observations remain queryable beside the selected winner and reason. |
| BR-07 | Support SCD Type 1 and Type 2 with non-overlapping half-open validity ranges. | Rename/correction and historical change tests, including an effective-date backfill. |
| BR-08 | Process full and incremental CDC, tombstones, late observations, revisions, and bounded backfills. | Initial-load and incremental state agree with replay under the same source history. |
| BR-09 | Make reruns and duplicate object deliveries safe. | Replaying a completed batch does not change facts, dimension versions, published totals, or watermarks. |
| BR-10 | Gate publication on completeness and reconciliation. | Missing cost shard, wrong cost control total, or required-source omission preserves the previous published state. |
| BR-11 | Publish structured quality results for every attempt, including failures before transformation. | Machine-readable results contain run, batch, rule, observed and expected values, status, and evidence. |
| BR-12 | Deliver analyst-facing shipment, delivery-event, and transportation-cost products with documented grain. | Product contracts define owner role, schema, grain, freshness, quality, lineage, and example usage. |
| BR-13 | Include deployable AWS infrastructure and an independently runnable local correctness path. | Terraform validation, CI tests, documented deployment prerequisites, and local demonstration. |
| BR-14 | Demonstrate serious SQL and explain warehouse design tradeoffs. | Temporal joins, deduplication, reconciliation, SCD, upsert, ranking, and gap-and-island queries. |
| BR-15 | Record only actually measured benchmark results. | Benchmark command, environment, dataset size, query, repetitions, EXPLAIN evidence, and raw timings; unrun Redshift work is explicitly unmeasured. |
| BR-16 | Reproduce the documented incident cases end to end. | Each incident has symptom, detection, investigation, root cause, remediation, recovery, and prevention. |

## Operational semantics

- A **batch** is a declared publication unit containing objects from every required source. An explicit zero-row object is a valid empty extract; an absent object is not an empty extract.
- A **run** is an execution attempt against a batch. A failed run retains diagnostics but cannot move the ingestion checkpoint or replace published products.
- A **source update time** says when the upstream system changed a record. An **event/effective time** says when the business event or reference version applies. An **ingestion time** says when the platform received it. These are separate fields.
- The checkpoint tracks committed ingestion batches/sequence, not event occurrence time. A five-day-old event received today is eligible for processing today.
- An operational deletion is a tombstone. It deactivates the relevant source membership and can set the canonical deleted flag; it does not erase historical observations or financial postings.
- Monetary values are signed integer cents and explicitly denominated in EUR. There is no implicit currency conversion, floating-point money, or unlabelled cross-currency aggregation.
- Unknown and unresolved references are explicit. Noncritical missing dimensions may use warehouse key `0`; they remain visible in quality results and can be reassigned after reference evidence arrives. Invalid identity-bearing keys and ambiguous identity mappings are quarantined.
- Publication is atomic across canonical state and dependent products for a batch. A partially delivered cost feed blocks all products from that batch, so shipment and finance views cannot silently describe different completeness states.

## Freshness, ownership, and performance

The demonstration is batch oriented. Product freshness is the timestamp of the latest successfully published batch plus its source coverage, not an invented service-level achievement. Operations and finance ownership are fictional role names, never invented employee contacts.

Generator scale is configurable. Correctness tests use small fixtures; a separate scale command supports a materially larger dataset within the available machine's resources. Any benchmark distinguishes local engine measurements from actual Redshift measurements. No production throughput, latency, savings, reliability, or optimization benefit is claimed without evidence.

## Phase acceptance gates

| Phase | Exit condition |
|---|---|
| 0 — design | Business requirements, five source contracts, domain/canonical/dimensional models, resolution and precedence rules, quality specification, roadmap, and ADRs exist and are consistent. No pipeline implementation precedes this gate. |
| 1 — sources and ingestion | Seeded sources, manifests, raw lineage, contract validation, duplicate delivery handling, and failure diagnostics run locally and pass tests. |
| 2 — canonical integration | Deterministic resolution, immutable observations, conflict decisions, canonical projections, and reference data run with unresolved and ambiguous fixtures. |
| 3 — history and marts | SCD behavior, three facts, seven dimensions, unknown members, historical attribution, tombstones, and correction reprocessing pass tests. |
| 4 — incremental reliability | Atomic publication, checkpoints, CDC, reruns, bounded backfills, quality controls, reconciliation, and incident recovery are demonstrated. |
| 5 — cloud and engineering | Glue/PySpark path, AWS infrastructure, CI, SQL workloads, self-service contracts, lineage, and reproducible benchmark procedures are validated to the extent available. Unexecuted cloud steps are clearly labelled. |

Every completed phase must contain working behavior, tests, and documentation. A deployment blueprint or benchmark procedure alone does not constitute a successful cloud execution or measured improvement.
