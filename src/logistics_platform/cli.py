"""Reproducible local workflow and explicit opt-in AWS delivery commands."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any

from logistics_platform.generator import generate
from logistics_platform.pipeline import Pipeline, connect


def summary(report: dict[str, Any]) -> dict[str, Any]:
    return {
        key: report[key]
        for key in ("run_id", "batch_id", "status", "watermark", "duration_seconds", "error")
        if key in report
    } | {
        "failed_checks": [
            q["rule_id"]
            for q in report.get("quality", [])
            if q["status"] == "fail" and q["severity"] == "error"
        ]
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="command", required=True)
    for name in ("generate", "demo", "incidents"):
        p = sub.add_parser(name)
        p.add_argument("--output", type=Path, default=Path("work") / name)
        p.add_argument("--shipments", type=int, default=100)
        p.add_argument("--seed", type=int, default=42)
    run = sub.add_parser("run")
    run.add_argument("manifest", type=Path)
    run.add_argument("--database", type=Path, default=Path("work/warehouse.duckdb"))
    backfill = sub.add_parser("backfill")
    backfill.add_argument("--input", type=Path, required=True)
    backfill.add_argument("--database", type=Path, required=True)
    backfill.add_argument("--start", type=int, required=True)
    backfill.add_argument("--end", type=int, required=True)
    bench = sub.add_parser("benchmark")
    bench.add_argument("--output", type=Path, default=Path("work/benchmark"))
    bench.add_argument("--rows", type=int, default=1_000_000)
    bench.add_argument("--repetitions", type=int, default=5)
    query = sub.add_parser("query")
    query.add_argument("--database", type=Path, required=True)
    query.add_argument("--sql", required=True)
    export = sub.add_parser("export")
    export.add_argument("--database", type=Path, required=True)
    export.add_argument("--output", type=Path, required=True)
    export.add_argument("--release-id", required=True)
    upload = sub.add_parser("upload")
    upload.add_argument("--input", type=Path, required=True)
    upload.add_argument("--bucket", required=True)
    upload.add_argument("--prefix", default="releases")
    raw = sub.add_parser("upload-inputs")
    raw.add_argument("--manifest", type=Path, required=True)
    raw.add_argument("--bucket", required=True)
    raw.add_argument("--prefix", required=True)
    load = sub.add_parser("load")
    load.add_argument("--manifest-uri", required=True)
    load.add_argument("--workgroup", required=True)
    load.add_argument("--database", default="logistics")
    load.add_argument("--copy-role-arn", required=True)
    args = parser.parse_args()
    failed = False
    result: Any
    if args.command in {"generate", "demo"}:
        paths = generate(args.output / "input", args.shipments, args.seed)
        if args.command == "generate":
            result = [str(p) for p in paths]
        else:
            pipeline = Pipeline(args.output / "warehouse.duckdb")
            reports = [pipeline.run(p) for p in paths]
            if reports[-1]["status"] != "failed":
                reports.append(pipeline.run(paths[-1]))
            result = [summary(r) for r in reports]
            failed = any(r["status"] == "failed" for r in reports)
    elif args.command == "run":
        report = Pipeline(args.database).run(args.manifest)
        result = summary(report)
        failed = report["status"] == "failed"
    elif args.command == "backfill":
        reports = Pipeline(args.database).backfill(
            list(args.input.rglob("manifest.json")), args.start, args.end
        )
        result = [summary(r) for r in reports]
        failed = any(r["status"] == "failed" for r in reports)
    elif args.command == "incidents":
        from logistics_platform.incidents import run_incidents

        result = run_incidents(args.output, args.shipments, args.seed)
    elif args.command == "benchmark":
        from logistics_platform.benchmark import benchmark

        result = benchmark(args.output, args.rows, args.repetitions)
    elif args.command == "query":
        con = connect(args.database)
        try:
            cursor = con.execute(args.sql)
            columns = [c[0] for c in cursor.description]
            result = [dict(zip(columns, row, strict=True)) for row in cursor.fetchall()]
        finally:
            con.close()
    elif args.command == "export":
        from logistics_platform.cloud import PRODUCT_KEYS, export_release, product_foreign_keys

        con = connect(args.database)
        try:
            con.execute("BEGIN TRANSACTION")
            latest = con.execute(
                "SELECT b.ingest_sequence,r.report FROM published_batches b JOIN pipeline_runs r USING(run_id) ORDER BY ingest_sequence DESC LIMIT 1"
            ).fetchone()
            if not latest:
                raise ValueError("No published warehouse snapshot exists")
            report = json.loads(latest[1])
            result = str(
                export_release(
                    con,
                    args.output,
                    args.release_id,
                    latest[0],
                    PRODUCT_KEYS,
                    report["quality"],
                    foreign_keys=product_foreign_keys(con),
                )
            )
            con.execute("COMMIT")
        finally:
            con.close()
    elif args.command == "upload":
        import boto3

        from logistics_platform.cloud import upload_release

        result = upload_release(args.input, args.bucket, args.prefix, boto3.client("s3"))
    elif args.command == "upload-inputs":
        import boto3

        from logistics_platform.cloud import upload_inputs

        result = upload_inputs(args.manifest, args.bucket, args.prefix, boto3.client("s3"))
    else:
        import boto3

        from logistics_platform.cloud import load_release

        result = load_release(
            args.manifest_uri,
            args.workgroup,
            args.database,
            args.copy_role_arn,
            s3_client=boto3.client("s3"),
            data_client=boto3.client("redshift-data"),
        )
    print(json.dumps(result, indent=2, default=str))
    if failed:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
