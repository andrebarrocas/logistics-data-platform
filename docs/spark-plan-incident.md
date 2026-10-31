# Local Spark incident: query-plan expansion exhausted the JVM heap

This incident occurred while implementing and testing this repository locally. It is an actual engineering failure involving independently generated synthetic data; it is not a story about a production customer, a real logistics company, or an AWS outage.

| Evidence | Recorded observation |
|---|---|
| Runtime | Local PySpark 3.5.4 with Java 17 and a JVM heap configured at 1 GB. |
| Workload | Full source-to-canonical-to-warehouse-to-release integration using three synthetic shipments, reference history, and the five generated source feeds. |
| Failure | `java.lang.OutOfMemoryError` surfaced while Spark expanded/rendered query execution information in `QueryExecution.explainString`. |
| Failed attempt | The pre-remediation pytest attempt failed after **137.03 seconds**. This is elapsed time for a failed test, not a throughput benchmark. |
| Prior mitigation | Distributed caching and separate actions alone did not sufficiently bound retained logical-plan complexity. |
| Recovery state | **Verified locally:** the full/incremental integration test passed in 41.34 seconds under the same configured 1 GB JVM heap. A separate all-product model-parity test passed in 18.37 seconds. These are pytest durations, not comparative performance benchmarks. |

## 1. Symptom

The small integration fixture exhausted the local Spark JVM heap before completing the full product-release flow. Three shipments should not require substantial storage for business rows, yet the process failed during execution-plan handling. The failure prevented a complete validated release; partially written internal attempt files are not evidence of a publishable product.

The affected flow connects [source orchestration](../src/logistics_platform/spark_job.py), [typed canonical staging](../src/logistics_platform/spark_staging.py), and [Spark warehouse projection](../src/logistics_platform/spark_warehouse.py). The integration test is [test_spark_integration.py](../tests/test_spark_integration.py).

## 2. Detection

The end-to-end test failed with JVM heap exhaustion and an execution-plan stack containing `QueryExecution.explainString`. The failure happened despite a tiny business fixture and existing caches. The observed failure location made query-plan construction, retained lineage, and serialization the immediate investigation targets.

The release acceptance condition remains unchanged: every blocking gate must pass and all declared products must be written and reconciled before a successful release marker is accepted. An exception or an incomplete attempt cannot count as successful publication.

## 3. Investigation

The investigation followed the query dependencies rather than scaling the input down further:

1. Confirm the exact input was the three-shipment synthetic full/incremental fixture and identify the failing integration path.
2. Trace each typed staging DataFrame back through normalization, revision selection, effective alias resolution, and identity joins.
3. Follow those plans into SCD windows, canonical shipment state selection, temporal fact attribution, dimensions, quality checks, and product writes.
4. Check whether caching shortened the logical dependency graph or only made execution results reusable.
5. Inspect how repeated quality queries expanded their temporal joins, including the earlier approach of combining many gates into one `UNION` plan.
6. Preserve all correctness gates while testing explicit intermediate storage boundaries.

The key observation was that a cached intermediate could still carry the full source and temporal logical lineage. Reusing it in many dependent queries repeatedly exposed a large query representation to planning and diagnostic rendering. The input row count alone therefore did not explain the failure. No heap dump or detailed allocation profile is claimed by this record.

## 4. Root cause

Deeply nested source, identity, window, and temporal-join plans were expanded across multiple downstream products and checks. Combining many complex gates amplified the plan representation; running separate cached queries reduced one source of expansion but still retained excessive upstream lineage metadata.

The practical failure mechanism was excessive query-plan/lineage complexity under the available JVM heap. The small synthetic dataset isolated this from a demonstrated large-row-volume limit. This diagnosis does not establish a general Spark scale threshold or prove that all heap pressure in this application will have the same cause.

## 5. Remediation

[`project_spark`](../src/logistics_platform/spark_warehouse.py) now accepts `materialization_uri`, supplied by the [job orchestration](../src/logistics_platform/spark_job.py) as an immutable attempt prefix.

The projection writes and re-reads Parquet at these boundaries:

- All six typed inputs: `stg_reference`, `stg_order`, `stg_package`, `stg_shipment`, `stg_event`, and `stg_cost`.
- The complex derived relations `dim_reference`, `canonical_shipment`, and `fact_attribution`.

Re-reading these files creates bounded input plans for dependent work. Distributed caches can then reuse those bounded scans. Writes use `errorifexists`; a fresh attempt uses a new prefix instead of overwriting an incomplete prior attempt. The intermediate path is an implementation artifact, not a committed consumer release.

[`quality_checks_spark`](../src/logistics_platform/spark_warehouse.py) also executes each gate as a separate scalar aggregate. Only its result crosses to the driver; the application does not collect all fact records to Python to avoid the planning problem. Integrity, temporal, and financial checks retain their original meaning.

This remediation adds intermediate I/O and storage. Its intent is to bound logical lineage and make correctness validation executable. No performance improvement is inferred merely from changing the implementation, and increasing heap size alone is not recorded as the resolution.

## 6. Recovery

**Recovery was verified locally.** After adding durable Parquet boundaries and separate scalar gates, [the full/incremental integration test](../tests/test_spark_integration.py) passed in **41.34 seconds** with the same configured 1 GB JVM heap. [The separate warehouse parity test](../tests/test_spark_warehouse.py) passed in **18.37 seconds** and compared all 24 product schemas and normalized business rows with the local SQL implementation.

The integrated test generated an initial and incremental batch for three shipments. For both batches it asserted:

- Spark returned `ready`, with every blocking quality result passing.
- The version-2 validated Spark snapshot contained all 24 declared release products.
- Every product row count matched the local pipeline, and every output shard's physical count and SHA-256 matched its manifest.
- The immutable release marker matched the manifest hash.
- Transportation-cost controls matched the local current total; the historical correction changed it by exactly 125 EUR cents.
- The incremental run accepted the prior evidence history, and generated Redshift publication SQL included merge and cost-reconciliation logic.

The separate parity test used a shared typed fixture with a backdated facility-region change, a Type 1 carrier-label correction, a delivery milestone, and a signed cost credit. It compared normalized rows for every product while excluding publication timestamps/run IDs, then required the warehouse quality checks to pass. This complements the integrated test's raw-input, completeness, history-handoff, and release-artifact assertions.

Reproduce in the repository's pinned Python/Java 17 environment, with consistent UTC worker/driver configuration:

```bash
python -m pytest tests/test_spark_integration.py -q
python -m pytest tests/test_spark_warehouse.py -q
```

The 137.03-second failed attempt, 41.34-second integration pass, and 18.37-second parity pass describe different test outcomes or workloads. They are **not** an optimization comparison, a throughput measurement, or an AWS Glue/Redshift benchmark. Recovery proves the tested local flow can complete with its correctness gates under the stated heap configuration. No deployed AWS execution is inferred.

## 7. Prevention

Keep durable boundaries at the documented high-complexity projections, and retain end-to-end tests that include both an initial publication and an incremental correction. Small fixtures must cover the full dependency graph because row volume is not the only driver of planning memory.

Review new multiway temporal joins and combined quality plans for repeated expansion. Preserve the distinction between cached execution results and truncated logical lineage. Monitor driver heap failures and planning symptoms separately from executor row-processing pressure. Intermediate attempt paths need an explicit retention policy; their cleanup must never imply that failed publication artifacts were valid.

Maintain exact typed schemas when re-reading Parquet, and keep the same identity, historical, financial, and publication invariants on both sides of a storage boundary. Larger synthetic workloads and AWS Glue execution require their own measured validation after this local recovery passes.

## Separate timestamp finding

Local parity work also exposed host-timezone interpretation of Python-deserialized Spark timestamps, especially the `1900-01-01` unknown-member sentinel on a Europe/Lisbon host. [The parity test](../tests/test_spark_warehouse.py) compares SQL-rendered UTC values; warehouse sessions use UTC, and local worker/driver UTC configuration must be consistent when Python timestamp conversion is involved. This is a separate correctness concern. It does not explain or substantiate the query-plan heap failure.
