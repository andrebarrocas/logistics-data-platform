#!/usr/bin/env python3
"""Parse supplied Redshift SQL; this is not a warehouse execution test."""

from pathlib import Path

import sqlglot
from sqlglot import exp

root = Path(__file__).resolve().parents[1]
count = 0
for path in sorted((root / "sql/redshift").glob("*.sql")):
    sql = (
        path.read_text()
        .replace("{{table}}", "shipment_even")
        .replace("{{date_from}}", "2026-01-01")
        .replace("{{date_to}}", "2027-01-01")
    )
    for statement in sqlglot.parse(sql, read="redshift"):
        if isinstance(statement, exp.Command):
            raise ValueError(f"Parser fallback does not validate this statement in {path}")
        if statement is not None:
            count += 1
print(f"Parsed {count} Redshift statements; live AWS execution remains separate.")
