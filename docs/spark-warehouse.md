# Spark canonical and warehouse projections

`logistics_platform.spark_warehouse` executes the canonical, reference, historical dimension, fact, and SSOT product models in Spark SQL. Its runtime does not import DuckDB. The four checked-in resources under `src/logistics_platform/spark_sql/` are explicit Spark 3.5 ports of the corresponding local SQL models, with the same names, grain, identity functions, state authority, and half-open temporal joins.

The integrated entry point is `spark_job.run_job(..., build_products=True, release_uri=..., release_id=...)`. It validates the five source manifests, accepts prior evidence history for incremental processing, prepares typed staging, invokes these warehouse models, runs the blocking gates, and writes an immutable version-2 release containing all 24 products. The lower-level projection interfaces are:

```python
products = project_spark(spark, typed_staging, run_id, materialization_uri=run_prefix + "/_warehouse")
quality = quality_checks_spark(spark)
# The caller must block publication when any ERROR check fails.
```

`typed_staging` contains the six typed DataFrame inputs documented in `execution-contract.md`: reference, order, package, shipment, event, and cost tables. The module checks required columns and normalizes declared types. Identity UDFs use the shared UUID5/SHA256 functions and preserve SQL nulls. All SQL timestamps use a UTC session. Operational state occurrence comes from the explicit effective-time field; shipment creation is a separate analytical attribution timestamp.

Production callers provide a run-scoped S3 materialization prefix. The six typed staging inputs and three complex boundaries (`dim_reference`, `canonical_shipment`, `fact_attribution`) are written as Parquet and re-read before downstream work. These writes use `errorifexists`: retry attempts must use a new immutable run prefix. Durable boundaries truncate logical query lineage and permit executor recovery; distributed caches then avoid repeating scans. Dimension, attribution, and fact intermediates are cached by Spark. Fact datasets are never collected to the Python driver. The public result is a dictionary of DataFrames including all 24 release products and supporting evidence/quality relations; the release exporter writes their partitions using Spark.

Quality gates execute distributed SQL for uniqueness, interval validity, surrogate collisions, every fact's role-playing foreign keys, as-of identity/time agreement, reference attributes and parents, route endpoints, state authority, state transitions, impossible timestamps, source-to-product counts, and signed amount reconciliation. Only one scalar aggregate per check reaches the driver. Pending descriptive references produce a warning and visible degraded product status; ambiguous identity or blocking inconsistencies prevent publication.

Each gate runs as a separate bounded plan. Combining many temporal join gates into one UNION duplicated their query-plan representations and exhausted a 1 GB local JVM heap during task serialization on a tiny fixture. Cache-only sequential plans also retained excessive lineage metadata. The implementation therefore combines separate gate plans with durable Parquet boundaries while retaining the correctness gates. Recovery was verified by the full/incremental integration test under the same configured 1 GB heap; [the incident record](spark-plan-incident.md) documents the failure, investigation, remediation, and evidence. Its pytest durations are not throughput or warehouse performance measurements.

The parity test `tests/test_spark_warehouse.py` supplies the same independently generated small staging fixture to DuckDB and Spark. It compares every release product while excluding publication timestamps, and includes a backdated facility-region change, an authoritative carrier label correction, a delivery milestone, and a signed cost credit. Timestamp comparison uses SQL UTC rendering because Python Spark Row deserialization otherwise applies the host timezone; the historical 1900 unknown-member timestamp exposes this difference on Europe/Lisbon hosts.

The integrated raw-to-release test passed locally in 41.34 seconds, and the all-24-product model-parity test passed in 18.37 seconds. These are separate correctness-test durations, not a comparison of execution speed. The integration assertions validate full/incremental history handoff, product counts against the local implementation, every release file checksum/count, current financial controls, and the 125-cent correction.

This is full projection recomputation from accepted current evidence and retained reference boundaries. It does not claim that every mart transformation updates only changed partitions. Spark execution and Redshift loading are separate steps: successful local Spark checks do not establish measured performance of a deployed AWS Glue job or Redshift cluster.
