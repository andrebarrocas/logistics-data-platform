"""Actual reproducible local SQL measurements; never a substitute for Redshift."""

from __future__ import annotations

import hashlib
import json
import platform
import statistics
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import duckdb


def benchmark(output: Path, rows: int = 1_000_000, repetitions: int = 5) -> dict[str, Any]:
    if rows < 1000 or repetitions < 3:
        raise ValueError("Benchmarks require at least 1000 rows and three measured repetitions")
    output.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect(":memory:")
    con.execute("SET threads=4")
    con.execute("SET memory_limit='1GB'")
    # Deterministic modular arithmetic, independently synthetic. No imported records.
    con.execute(f"""CREATE TABLE events_unordered AS
        SELECT i::BIGINT shipment_key, (i%3)::INTEGER carrier_key,
        DATE '2025-01-01'+((i*7919)%365)::INTEGER service_date,
        (500+(i*104729)%20000)::BIGINT amount_minor
        FROM range({rows}) t(i)""")
    con.execute(
        "CREATE TABLE carriers AS SELECT i::INTEGER carrier_key, 'Fictional carrier '||i carrier_name FROM range(3) t(i)"
    )
    con.execute(
        "CREATE TABLE events_sorted AS SELECT * FROM events_unordered ORDER BY service_date,carrier_key"
    )
    con.execute(
        "CREATE TABLE daily_cost AS SELECT service_date,carrier_key,sum(amount_minor)::BIGINT amount_minor,count(*)::BIGINT shipments FROM events_unordered GROUP BY service_date,carrier_key"
    )
    con.execute("ANALYZE")
    workload = """SELECT c.carrier_name, sum(f.amount_minor)::BIGINT amount_minor,
        {count}::BIGINT shipments FROM {table} f JOIN carriers c USING(carrier_key)
        WHERE service_date >= DATE '2025-06-01' AND service_date < DATE '2025-06-08'
        GROUP BY c.carrier_name ORDER BY c.carrier_name"""
    variants = {
        "unordered": ("events_unordered", "count(*)"),
        "date_sorted": ("events_sorted", "count(*)"),
        "daily_preaggregation": ("daily_cost", "sum(f.shipments)"),
    }
    expected = None
    results: dict[str, Any] = {}
    for name, (table, count) in variants.items():
        query = workload.format(table=table, count=count)
        result = con.execute(query).fetchall()
        if expected is None:
            expected = result
        elif result != expected:
            raise AssertionError(f"Benchmark variant {name} changes the result")
        (output / f"{name}.sql").write_text(query + ";\n")
        explain = con.execute("EXPLAIN ANALYZE " + query).fetchall()
        (output / f"{name}.explain.txt").write_text(
            "\n".join(str(row[-1]) for row in explain) + "\n"
        )
        results[name] = {
            "seconds": [],
            "result_sha256": hashlib.sha256(json.dumps(result).encode()).hexdigest(),
            "table_rows": con.execute(f"SELECT count(*) FROM {table}").fetchone()[0],
        }
    # Alternate orders to reduce a systematic cache/order advantage. Data already warm.
    for trial in range(repetitions):
        names = list(variants) if trial % 2 == 0 else list(reversed(variants))
        for name in names:
            table, count = variants[name]
            start = time.perf_counter()
            result = con.execute(workload.format(table=table, count=count)).fetchall()
            duration = time.perf_counter() - start
            if result != expected:
                raise AssertionError("Measured query changed its result")
            results[name]["seconds"].append(duration)
    for result in results.values():
        result["median_seconds"] = statistics.median(result["seconds"])
        result["min_seconds"] = min(result["seconds"])
        result["max_seconds"] = max(result["seconds"])
    report = {
        "engine": "DuckDB",
        "engine_version": duckdb.__version__,
        "execution_profile": "local-in-memory",
        "measured_at": datetime.now(timezone.utc).isoformat(),
        "machine": {
            "os": platform.system(),
            "architecture": platform.machine(),
            "python": platform.python_version(),
        },
        "rows": rows,
        "threads": 4,
        "memory_limit": "1GB",
        "repetitions": repetitions,
        "warmup": "one query and EXPLAIN ANALYZE per variant",
        "trial_order": "alternating forward/reverse",
        "generation": "i=0..rows-1; day=(i*7919)%365; cents=500+(i*104729)%20000",
        "result": expected,
        "variants": results,
        "limitations": [
            "Local CPU/columnar timings only; no Redshift distribution, WLM, network or compression conclusions.",
            "Warm in-memory workload; materialization build cost and refresh cost excluded from query timings.",
            "Preaggregation is valid only for the specified additive measures and daily grain.",
        ],
    }
    (output / "results.json").write_text(json.dumps(report, indent=2) + "\n")
    con.close()
    return report
