# Engineering decisions and verification

## Implementation decisions

The platform resolves inconsistent operational data through explicit identity,
history, and reconciliation rules. The following implementations and tests
demonstrate those rules.

| Decision | Implementation to inspect | Observable result |
|---|---|---|
| Preserve disagreement and require deterministic identity evidence | [Resolver](../src/logistics_platform/resolution.py), [resolution rules](entity-resolution.md) | Similar carrier names cannot silently merge; overlapping contradictory aliases block publication. |
| Make correction semantics explicit in SQL | [SCD SQL](../src/logistics_platform/sql/001_reference.sql), [state selection](../src/logistics_platform/sql/002_canonical.sql) | Type 1 display corrections coexist with Type 2 operational history; superseded observations stop contributing to current state. |
| Resolve dimensional versions at the fact's business time | [Temporal mart SQL](../src/logistics_platform/sql/003_marts.sql), [fact grains](dimensional-model.md) | Late facts and backdated reference corrections produce historically appropriate foreign keys. |
| Gate a complete publication rather than accept partial feeds | [Pipeline transaction](../src/logistics_platform/pipeline.py), [quality rules](data-quality.md) | Missing cost shards, invalid countries, and ambiguous mappings leave previously published products and checkpoints available. |
| Keep distributed results consistent with a small correctness reference | [Spark SQL](../src/logistics_platform/spark_sql/), [24-product parity test](../tests/test_spark_warehouse.py) | The local engine and real Spark agree on normalized product schemas and business rows in the shared fixture. |
| Separate object publication from warehouse visibility | [Release publisher](../src/logistics_platform/cloud.py), [cloud ADR](adr/0004-cloud-security-and-publication.md) | Hash-verified immutable releases feed a serialized warehouse transaction with a durable replay ledger. |

Full projection rebuilding from accepted incremental evidence makes corrections
and replay tractable; transformation cost grows with retained history. The benchmark artifacts
measure a specified local SQL workload; cloud throughput requires its own
experiment.

## Verification on 29 October 2026

The application was checked out into a fresh Git worktree and exercised again.
The revalidation report records a fingerprint of the tested Python and SQL
source files; the canonical transformation code remains unchanged.

| Check | Observed outcome |
|---|---|
| Python, SQL, and mocked AWS tests | 95 passed; zero failures or skips. |
| Real Spark 3.5.4 / Java 17 tests | 9 passed; zero failures or skips, including full/incremental release and all-product SQL parity. |
| Public CLI demo, 1,000 synthetic shipments, seed 42 | `published`, `published`, `noop`; checkpoints `1`, `2`, `2`. |
| Published data after the incremental batch | 1,000 shipment facts, 999 active SSOT shipments, 2,001 event observations, 1,000 current cost entries. |
| Signed current financial control | 4,991,568 EUR minor units; all 24 exported product files passed schema, count, byte-size, and checksum verification. |
| Incident runner | All eight scenarios passed their detection, recovery, and replay assertions. |
| Built wheel in a separate virtual environment | 100-shipment CLI demo and complete 24-product export passed; packaged SQL resources were available. |
| Static checks | Ruff, formatting, mypy, and 18 Redshift SQL statements passed. |
| Infrastructure | Fresh readonly initialization and Terraform validation passed after correcting platform checksums. |

Machine-readable evidence: [revalidation report](../reports/revalidation-2026-10-29.json),
[core JUnit results](../reports/tests/2026-10-29-core.xml),
[Spark JUnit results](../reports/tests/2026-10-29-spark.xml), and
[CLI demo outcomes](../reports/tests/2026-10-29-demo.json).
Test durations are correctness evidence, not performance benchmarks.

### CI issue found during the review

An initial CI run passed its Python and Spark jobs but failed Linux Terraform validation: the
installed AWS provider did not match a platform checksum in the committed
lockfile. Local macOS validation had not caught this runner-specific failure.

The lockfile was regenerated from signed HashiCorp packages for both supported
runner platforms using:

```bash
terraform -chdir=infra providers lock -platform=darwin_arm64 -platform=linux_amd64
terraform -chdir=infra init -backend=false -input=false -lockfile=readonly
terraform -chdir=infra validate -no-color
```

CI retains readonly locking and now also builds the distributable, installs its
wheel into a separate environment, runs the public CLI, and verifies every
exported product. It uploads the JUnit, demo, and release-manifest artifacts.
The [live workflow](https://github.com/andrebarrocas/logistics-data-platform/actions/workflows/ci.yml)
and README badge expose the actual status of each pushed revision.

## What the evidence covers

This is an independently synthetic project with working local and Spark
execution. AWS API tests use mocks; Terraform validation checks configuration.
No AWS resources were deployed during this review, and no live Glue/Redshift
acceptance or Redshift performance results are claimed. The
[deployment guide](aws-deployment.md) defines that separate acceptance procedure.
