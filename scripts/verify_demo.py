"""Validate CLI publication outcomes and every exported product's file controls."""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
from pathlib import Path

from logistics_platform.cloud import PRODUCT_KEYS, release_files, validate_manifest


def verify(demo_report: Path, release_manifest: Path) -> dict[str, object]:
    reports = json.loads(demo_report.read_text())
    if [run["status"] for run in reports] != ["published", "published", "noop"]:
        raise ValueError("Demo must publish full/incremental batches and replay without changes")
    if [run["watermark"] for run in reports] != [1, 2, 2]:
        raise ValueError("Replay must preserve the second batch's checkpoint")
    if any(run["failed_checks"] for run in reports):
        raise ValueError("Demo has blocking quality failures")
    manifest = json.loads(release_manifest.read_text())
    validate_manifest(manifest)
    if manifest["sequence"] != 2:
        raise ValueError("Export must represent the committed incremental snapshot")
    if {table["name"] for table in manifest["tables"]} != set(PRODUCT_KEYS):
        raise ValueError("Release must contain exactly all 24 documented products")
    for table in manifest["tables"]:
        rows = 0
        for item in release_files(table):
            path = release_manifest.parent / item["file"]
            with path.open("rb") as stream:
                checksum = hashlib.file_digest(stream, "sha256").hexdigest()
            if checksum != item["sha256"] or path.stat().st_size != item["bytes"]:
                raise ValueError(f"Export checksum/size mismatch: {table['name']}")
            with path.open(newline="") as stream:
                reader = csv.reader(stream)
                if next(reader) != [column["name"] for column in table["columns"]]:
                    raise ValueError(f"Export column mismatch: {table['name']}")
                count = sum(1 for _ in reader)
            if count != item["row_count"]:
                raise ValueError(f"Export shard count mismatch: {table['name']}")
            rows += count
        if rows != table["row_count"]:
            raise ValueError(f"Export table count mismatch: {table['name']}")
    return {"status": "passed", "products": len(manifest["tables"]), "watermarks": [1, 2, 2]}


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--demo-report", type=Path, required=True)
    parser.add_argument("--release-manifest", type=Path, required=True)
    args = parser.parse_args()
    print(json.dumps(verify(args.demo_report, args.release_manifest)))


if __name__ == "__main__":
    main()
