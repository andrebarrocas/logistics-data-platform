# Measurements and Redshift optimization

No production metrics are claimed. This repository has actual local benchmark artifacts and a live Redshift harness; Redshift timings are **not run** in the checked-in evidence.

## Actual local result

Command: `logistics benchmark --output reports/benchmarks/local --rows 1000000 --repetitions 7`.

Environment: DuckDB 1.4.1, Python 3.11.14, macOS ARM64, four threads, 1 GB memory limit. Independently generated deterministic data: 1,000,000 rows across 365 service dates and three fictional carriers. Each query filters one week, joins the carrier dimension and groups signed amounts and counts. One warm-up query and EXPLAIN ANALYZE precede seven alternating-order measured repetitions.

| Variant | Physical rows | Median query time | Measured range |
|---|---:|---:|---:|
| Unordered fact | 1,000,000 | 0.835 ms | 0.812–0.946 ms |
| Date-ordered fact | 1,000,000 | 0.572 ms | 0.545–0.614 ms |
| Daily preaggregation | 1,095 | 0.288 ms | 0.284–0.325 ms |

These are observed local warm-memory query times, not promised improvements. Every variant returned the same result SHA256. Build/refresh time, object storage, distributed shuffles, concurrent users and cold storage are excluded. At sub-millisecond durations, scheduling and fixed client overhead matter; repeating on another machine can change ordering and magnitudes.

Artifacts: [all timing samples and exact result](../reports/benchmarks/local/results.json), [unordered plan](../reports/benchmarks/local/unordered.explain.txt), [sorted plan](../reports/benchmarks/local/date_sorted.explain.txt), [aggregate plan](../reports/benchmarks/local/daily_preaggregation.explain.txt). The [generator and measurement implementation](../src/logistics_platform/benchmark.py) is reproducible. Full source-to-product pipeline throughput is not inferred from this isolated SQL workload.

## Redshift experiment design

The [live harness](../scripts/benchmark_redshift.py) requires an already committed synthetic release. It creates only the disposable `benchmark_logistics` schema when `--prepare` is explicitly supplied. It compares equivalent results from:

1. `DISTSTYLE EVEN`, sorted by shipment ID.
2. `DISTKEY(carrier_sk)`, sorted by creation date and carrier, joining an `ALL` carrier dimension.
3. A daily carrier materialized view with additive shipment/package/delivery measures.

The carrier key has only three distinct business entities in the default fixture. **This is an intentionally questionable distribution candidate.** Low cardinality can concentrate data on a few slices; the benchmark is designed to expose that tradeoff, not to recommend a key automatically. Shipment-ID distribution is a candidate for future colocated shipment/event/cost joins. AUTO remains the initial product-table policy until measured evidence justifies another choice.

Date sort order can support range pruning when the leading key matches the filter. A primary-key sort may suit point lookup but not that date workload. The materialized view reduces scan input only for queries compatible with its daily grain and additive measures. Its refresh/storage costs and staleness contract must be considered separately from query latency. Encoding and statistics are inspected through `SVV_TABLE_INFO` and `PG_TABLE_DEF`; no compression savings are guessed.

The runner disables the result cache in each session batch, verifies cache-hit metadata, compares exact results, records query IDs, server execution/queue/elapsed microseconds, API wall time, EXPLAIN plans, runtime version, physical layout and Redshift capacity. It interleaves trials with a fixed shuffle seed, requires at least three repetitions and rejects release changes during measurement. `EXCEPT` checks ensure same-row-count benchmark clones are not stale. Failure writes a failed report rather than fabricated samples.

```bash
python scripts/benchmark_redshift.py --workgroup YOUR_EXISTING_WORKGROUP \
  --database logistics --region eu-west-1 --prepare --repetitions 7 \
  --date-from 2026-01-01 --date-to 2026-02-01 \
  --output work/redshift-benchmark.json
```

Default source fixtures are designed for edge-case correctness and span only a short business interval. A meaningful physical-layout experiment must generate/load a suitably broad time range and disclose distinct key counts, selectivity, skew, unsorted percentage, capacity, and query concurrency. Increasing row count alone does not prove realistic workload diversity. Inspect [setup and investigation SQL](../sql/redshift/).

Primary references: [Redshift distribution design](https://docs.aws.amazon.com/redshift/latest/dg/c_best-practices-best-dist-key.html), [sort-key design](https://docs.aws.amazon.com/redshift/latest/dg/c_best-practices-sort-key.html), [MERGE semantics](https://docs.aws.amazon.com/redshift/latest/dg/r_MERGE.html), [SYS_QUERY_HISTORY](https://docs.aws.amazon.com/redshift/latest/dg/SYS_QUERY_HISTORY.html). These explain mechanisms; the repository's measured artifacts establish observations only for the environment that ran them.
