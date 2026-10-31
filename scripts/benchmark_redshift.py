#!/usr/bin/env python3
"""Execute real Redshift comparisons; never fills in unavailable measurements.

Requires an existing deployed warehouse and explicit operator invocation. The
Data API result cache is disabled inside every measured batch. Reported server
microseconds come from SYS_QUERY_HISTORY, separate from API round-trip timings.
"""

from __future__ import annotations

import argparse
import datetime as dt
import hashlib
import json
import random
import statistics
import time
from pathlib import Path
from typing import Any

import boto3
import sqlglot

from logistics_platform.cloud import PublicationError, wait_statement

ROOT = Path(__file__).resolve().parents[1]


def records(client: Any, statement_id: str) -> list[list[Any]]:
    rows: list[list[Any]] = []
    arguments = {"Id": statement_id}
    while True:
        result = client.get_statement_result(**arguments)
        for row in result["Records"]:
            rows.append(
                [None if value.get("isNull") else next(iter(value.values())) for value in row]
            )
        token = result.get("NextToken")
        if not token:
            return rows
        arguments["NextToken"] = token


def execute(
    client: Any, workgroup: str, database: str, sql: str
) -> tuple[list[list[Any]], dict[str, Any], float]:
    started = time.perf_counter()
    response = client.batch_execute_statement(
        WorkgroupName=workgroup,
        Database=database,
        Sqls=[
            "SET enable_result_cache_for_session TO OFF",
            "SET mv_enable_aqmv_for_session TO FALSE",
            sql,
        ],
    )
    description = wait_statement(client, response["Id"])
    elapsed = time.perf_counter() - started
    query = description["SubStatements"][-1]
    result = records(client, query["Id"]) if query.get("HasResultSet") else []
    return result, query, elapsed


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--workgroup", required=True)
    parser.add_argument("--database", default="logistics")
    parser.add_argument("--region", default="eu-west-1")
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument(
        "--prepare",
        action="store_true",
        help="Recreate only the disposable benchmark_logistics tables and materialized view",
    )
    parser.add_argument("--repetitions", type=int, default=5)
    parser.add_argument("--warmups", type=int, default=1)
    parser.add_argument("--date-from", type=dt.date.fromisoformat, default=dt.date(2026, 1, 1))
    parser.add_argument("--date-to", type=dt.date.fromisoformat, default=dt.date(2027, 1, 1))
    args = parser.parse_args()
    if args.repetitions < 3 or args.warmups < 0 or args.date_from >= args.date_to:
        parser.error("Use >=3 repetitions, nonnegative warmups, and increasing date bounds")
    if args.output.exists():
        parser.error("Output already exists; preserve benchmark evidence and choose a new path")
    client = boto3.client("redshift-data", region_name=args.region)
    serverless = boto3.client("redshift-serverless", region_name=args.region)
    workgroup = serverless.get_workgroup(workgroupName=args.workgroup)["workgroup"]
    identity_sql = "SELECT release_id,manifest_sha256,sequence FROM ssot.publication_runs ORDER BY sequence DESC LIMIT 1"
    release_before, _, _ = execute(client, args.workgroup, args.database, identity_sql)
    if not release_before:
        raise PublicationError("Benchmark requires a committed product release")
    if args.prepare:
        setup = (ROOT / "sql/redshift/benchmark_setup.sql").read_text()
        statements = [
            expression.sql(dialect="redshift")
            for expression in sqlglot.parse(setup, read="redshift")
            if expression is not None
        ]
        response = client.batch_execute_statement(
            WorkgroupName=args.workgroup, Database=args.database, Sqls=statements
        )
        wait_statement(client, response["Id"])
    counts, _, _ = execute(
        client,
        args.workgroup,
        args.database,
        "SELECT (SELECT COUNT(*) FROM ssot.fact_shipment),(SELECT COUNT(*) FROM benchmark_logistics.shipment_even),(SELECT COUNT(*) FROM benchmark_logistics.shipment_key)",
    )
    if not counts or counts[0][0] < 1 or len(set(counts[0])) != 1:
        raise PublicationError("Missing/stale benchmark data; run with --prepare")
    # Exact relational equivalence prevents stale same-row-count clones.
    mismatch, _, _ = execute(
        client,
        args.workgroup,
        args.database,
        "SELECT COUNT(*) FROM ((SELECT * FROM ssot.fact_shipment EXCEPT SELECT * FROM benchmark_logistics.shipment_even) UNION ALL (SELECT * FROM benchmark_logistics.shipment_even EXCEPT SELECT * FROM ssot.fact_shipment) UNION ALL (SELECT * FROM ssot.fact_shipment EXCEPT SELECT * FROM benchmark_logistics.shipment_key) UNION ALL (SELECT * FROM benchmark_logistics.shipment_key EXCEPT SELECT * FROM ssot.fact_shipment)) mismatches",
    )
    if mismatch != [[0]]:
        raise PublicationError("Benchmark clone data does not equal committed fact data")
    base = (ROOT / "sql/redshift/carrier_performance.sql").read_text()
    mv = (ROOT / "sql/redshift/carrier_performance_mv.sql").read_text()
    queries = {
        "even_primary_key_sort": base.replace("{{table}}", "shipment_even"),
        "carrier_distribution_date_sort": base.replace("{{table}}", "shipment_key"),
        "daily_materialized_view": mv,
    }
    for name in queries:
        queries[name] = (
            queries[name]
            .replace("{{date_from}}", args.date_from.isoformat())
            .replace("{{date_to}}", args.date_to.isoformat())
        )
    report: dict[str, Any] = {
        "status": "running",
        "engine": "Amazon Redshift Serverless",
        "captured_at": dt.datetime.now(dt.UTC).isoformat(),
        "region": args.region,
        "base_capacity_rpu": workgroup["baseCapacity"],
        "max_capacity_rpu": workgroup.get("maxCapacity"),
        "release": release_before[0],
        "shipment_count": counts[0][0],
        "warmups_per_variant": args.warmups,
        "repetitions": args.repetitions,
        "result_cache": "disabled and verified in SYS_QUERY_HISTORY",
        "materialized_query_rewrite": "disabled to isolate the explicit materialized-view variant",
        "ordering": "interleaved deterministic shuffle seed 42",
        "queries": {},
        "measurements": [],
    }
    args.output.parent.mkdir(parents=True, exist_ok=True)
    try:
        expected_result = None
        for name, sql in queries.items():
            plan, _, _ = execute(client, args.workgroup, args.database, "EXPLAIN " + sql)
            report["queries"][name] = {"sql": sql, "explain": plan}
            for _ in range(args.warmups):
                execute(client, args.workgroup, args.database, sql)
        order = list(queries) * args.repetitions
        random.Random(42).shuffle(order)
        for name in order:
            result, query, elapsed = execute(client, args.workgroup, args.database, queries[name])
            if not result:
                raise PublicationError(
                    "Benchmark filter returned no rows; choose representative dates"
                )
            if expected_result is None:
                expected_result = result
            elif result != expected_result:
                raise PublicationError("Variants returned different analytical results")
            query_id = int(query["RedshiftQueryId"])
            metrics, _, _ = execute(
                client,
                args.workgroup,
                args.database,
                "SELECT execution_time,queue_time,elapsed_time,result_cache_hit,redshift_version "
                f"FROM sys_query_history WHERE query_id={query_id}",
            )
            if len(metrics) != 1 or metrics[0][3] is not False:
                raise PublicationError("Missing server timing or result cache unexpectedly used")
            report["measurements"].append(
                {
                    "variant": name,
                    "query_id": query_id,
                    "statement_id": query["Id"],
                    "execution_microseconds": metrics[0][0],
                    "queue_microseconds": metrics[0][1],
                    "elapsed_microseconds": metrics[0][2],
                    "api_wall_seconds": elapsed,
                    "redshift_version": metrics[0][4],
                    "result_sha256": hashlib.sha256(
                        json.dumps(result, sort_keys=True).encode()
                    ).hexdigest(),
                }
            )
        release_after, _, _ = execute(client, args.workgroup, args.database, identity_sql)
        if release_after != release_before:
            raise PublicationError("Product release changed during benchmark")
        physical, _, _ = execute(
            client,
            args.workgroup,
            args.database,
            'SELECT "table",diststyle,sortkey1,skew_rows,unsorted,size,encoded,stats_off FROM svv_table_info WHERE "schema"=\'benchmark_logistics\' ORDER BY "table"',
        )
        report["physical_tables"] = physical
        encodings, _, _ = execute(
            client,
            args.workgroup,
            args.database,
            'SELECT tablename,"column",type,encoding,distkey,sortkey FROM pg_table_def '
            "WHERE schemaname='benchmark_logistics' ORDER BY tablename,\"column\"",
        )
        report["column_encodings"] = encodings
        report["median_execution_microseconds"] = {
            name: statistics.median(
                item["execution_microseconds"]
                for item in report["measurements"]
                if item["variant"] == name
            )
            for name in queries
        }
        report["status"] = "complete"
    except Exception as exc:
        report["status"] = "failed"
        report["error"] = str(exc)
        raise
    finally:
        args.output.write_text(json.dumps(report, indent=2, default=str) + "\n")
    print(args.output)


if __name__ == "__main__":
    main()
