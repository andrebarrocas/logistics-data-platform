# Implementation roadmap and design gate

Phase 0 is a mandatory documentation gate. No application implementation starts until business requirements, source specifications, domain/canonical/dimensional models, resolution/precedence rules, quality specification, this roadmap and ADRs exist and have been checked together.

| Phase | Deliverable | Runnable acceptance evidence |
|---|---|---|
| 0 | Consistent design contracts and ADRs | Recorded cross-document review in phase-0-review.md |
| 1 | Seeded five-source generator, manifests, adapters, canonical evidence, deterministic identity | Unit tests for keys, schema evolution, ambiguity, timestamp and hash validation |
| 2 | SQL integration, reference history, dimensional marts, transaction publication, incremental processing | Full/incremental/rerun/backfill/correction/deletion tests, SQL reconciliations |
| 3 | Incident engineering and analyst products | Reproducible scenario runner with failure/recovery assertions, structured run results, product catalog |
| 4 | PySpark path, AWS infrastructure, Redshift loading, CI | Local Spark tests, Terraform validate, Redshift dialect parsing, mocked API tests; deployed acceptance separately recorded |
| 5 | Workload measurement and release verification | Actual local benchmark JSON/plans, optional live Redshift benchmark runner, complete test/lint/type results |

Every phase must provide working functionality and evidence. Infrastructure validation is not a deployment; mocked APIs and SQL parsing are not warehouse execution. If cloud credentials or a funded warehouse are unavailable, clearly record that limit and provide runnable deployment/acceptance commands. Do not invent benchmark results.

Priorities: business correctness and SQL first, reproducibility second, physical warehouse tuning supported by measurement third. No frontend, chatbot, streaming broker, orchestration server, or unrelated platform is needed.

## Recorded implementation status

| Phase | Current evidence and boundary |
|---|---|
| 0 — design | Complete: [the recorded design gate](phase-0-review.md), model contracts, quality specification, and ADRs precede implementation. |
| 1 — source integration | Implemented and exercised locally: seeded five-source generation, manifests, schema versions, deterministic resolution, and malformed-input/alias lifecycle regression tests. See [generator tests](../tests/test_generator.py), [pipeline contracts](../tests/test_pipeline_contracts.py), and [review regressions](../tests/test_review_regressions.py). |
| 2 — warehouse correctness | Implemented and exercised locally: canonical SQL, Type 1/2 history, current facts, temporal attribution, transactional publication, incremental corrections, replay, unknown members, and tombstones. See [warehouse tests](../tests/test_warehouse.py) and [execution contracts](execution-contract.md). |
| 3 — incidents and products | Complete local exercises: eight synthetic incident/recovery scenarios, structured quality reports, and [the generated 24-product schema catalogue](catalog.json). See [incident assertions](../tests/test_incidents.py), [runbook](incident-runbook.md), and [actual Spark development incident](spark-plan-incident.md). |
| 4 — Spark/cloud engineering | Implemented and locally validated: five raw sources through Spark canonical/history/marts to immutable 24-product release; full/incremental integration passed, and all-product model parity passed. Terraform validation, Redshift SQL parsing, and mocked publication tests provide separate offline evidence. Live AWS deployment and Glue/Redshift acceptance have not run. |
| 5 — measurement and release | Actual [local SQL benchmark samples/plans](benchmarks.md) are recorded; the live Redshift harness is implemented but its measurements remain unexecuted. Final repository-wide test/lint/type totals are recorded only after the final verification run, not inferred from these selected tests. |

The Spark recovery evidence records a 41.34-second passing integration test and an 18.37-second passing parity test. These timings establish completion of those test attempts; they do not establish a performance improvement or deployed-cloud throughput.
