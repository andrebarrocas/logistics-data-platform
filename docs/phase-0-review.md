# Phase 0 design gate

Status: **passed before application implementation**, 2026-10-04.

The design review compared business-requirements.md, source-systems.md, domain-model.md, canonical-model.md, dimensional-model.md, entity-resolution.md, source-precedence.md, data-quality.md, architecture.md, implementation-roadmap.md, execution-contract.md and ADRs 0001–0004. These artifacts existed before code development.

Resolved cross-document differences:

- Source name is `transport_costs` everywhere. The five exact source IDs and required-file completeness rule are fixed.
- Monetary contract is signed integer `amount_minor` EUR cents. Higher revisions replace the current contribution with audited history; independent credits have separate IDs. Fact dimension time is `incurred_at`.
- Reference validity uses half-open intervals with `9999-12-31` as open end. Type 1 labels are excluded from Type 2 tracked hashes.
- Canonical IDs use the UUID5 namespace derived from `https://example.invalid/mcl-platform`. Dimension surrogate keys are stable positive 63-bit SHA256-derived values, checked for collisions; 0 is unknown.
- Shipment structural attributes and typed staging names are fixed by execution-contract.md. Status authority is 300/200/100; equal-time equal-authority disagreement blocks publication. Package observations alone cannot establish whole-shipment delivery.
- Ingestion sequence is the watermark. Business occurrence dates never filter new source revisions. Failed publication cannot advance it.
- The local reference implementation incrementally ingests files/revisions and rebuilds projections transactionally. This is an explicit correctness-first tradeoff, not a claim of constant-time incremental warehouse transformation.
- Local SQL/Spark tests, static infrastructure validation, and real cloud execution are distinct evidence levels. AWS deployment and Redshift benchmark results must be reported as unexecuted unless actually run.

Acceptance mapping is recorded in the quality specification and implementation roadmap. Implementation may refine physical naming, but must preserve these semantic contracts and update documentation/tests together.
