"""Catalogue metadata is extracted from the actual published SQL objects."""

from __future__ import annotations

import hashlib
import json
import runpy
from pathlib import Path

import duckdb

from logistics_platform.cloud import PRODUCT_KEYS
from logistics_platform.generator import generate
from logistics_platform.pipeline import Pipeline

EXPORT = runpy.run_path(str(Path(__file__).parents[1] / "scripts" / "export_catalog.py"))[
    "export_catalog"
]


def test_catalog_has_all_actual_schemas_valid_examples_and_recorded_freshness(
    tmp_path: Path,
) -> None:
    database = tmp_path / "warehouse.duckdb"
    pipeline = Pipeline(database)
    manifests = generate(tmp_path / "inputs", shipments=3)
    for manifest in manifests:
        assert pipeline.run(manifest)["status"] == "published"
    # A later audit attempt must not replace the committed run's quality/freshness.
    assert pipeline.run(manifests[-1])["status"] == "noop"
    before = hashlib.sha256(database.read_bytes()).hexdigest()
    destination = tmp_path / "catalog.json"
    catalog = EXPORT(database, destination)
    assert hashlib.sha256(database.read_bytes()).hexdigest() == before
    assert catalog == json.loads(destination.read_text())
    assert catalog["product_count"] == len(PRODUCT_KEYS) == 24
    assert {product["name"] for product in catalog["products"]} == set(PRODUCT_KEYS)
    assert catalog["latest_attempt"]["status"] == "noop"
    with duckdb.connect(str(database), read_only=True) as connection:
        for product in catalog["products"]:
            name = product["name"]
            actual = connection.execute(f'DESCRIBE SELECT * FROM "{name}"').fetchall()
            assert [
                (column["name"], column["data_type"], column["nullable"])
                for column in product["schema"]
            ] == [(column[0], column[1], column[2] == "YES") for column in actual]
            assert product["declared_unique_key"] == [PRODUCT_KEYS[name]]
            assert product["freshness"]["published_ingest_sequence"] == 2
            assert product["freshness"]["published_batch_id"] == "batch002"
            assert product["quality"]["publication_status"] in {"PASSED", "PASSED_WITH_WARNINGS"}
            assert product["owner"] and product["grain"] and product["lineage"]["source_systems"]
            connection.execute("EXPLAIN " + product["example_sql"]).fetchall()
