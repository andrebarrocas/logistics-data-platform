"""Serialized, transactional publication with retained raw data and structured diagnostics."""

import fcntl
import json
import time
from collections import defaultdict
from datetime import datetime, timezone
from hashlib import sha256
from pathlib import Path
from typing import Any
from uuid import uuid4

import duckdb

from logistics_platform.contracts import (
    COUNTRIES,
    REFERENCE_TYPES,
    SOURCES,
    ContractError,
    Record,
    compact,
    digest,
    integer,
    normalize,
    read_rows,
    validate_manifest,
)
from logistics_platform.keys import canonical_id, surrogate_key
from logistics_platform.resolution import Resolver, latest


def connect(path: Path | str) -> duckdb.DuckDBPyConnection:
    con = duckdb.connect(str(path))
    con.execute("SET TimeZone='UTC'")
    con.create_function("canonical_id", canonical_id, ["VARCHAR", "VARCHAR"], "VARCHAR")
    con.create_function("surrogate_key", surrogate_key, ["VARCHAR", "VARCHAR", "VARCHAR"], "BIGINT")
    return con


def check(
    rule: str,
    observed: Any,
    expected: Any,
    passed: bool,
    *,
    severity: str = "error",
    details: Any = None,
) -> dict[str, Any]:
    severity = {"warn": "warning", "critical": "error"}.get(severity.lower(), severity.lower())
    return {
        "rule_id": rule,
        "dataset": "platform",
        "severity": severity,
        "status": "pass" if passed else "fail",
        "observed": observed,
        "expected": expected,
        "failed_count": 0 if passed else 1,
        "checked_count": 1,
        "details": details or {},
    }


class Pipeline:
    def __init__(self, database: Path | str, *, max_unknown_ratio: float = 0.25):
        self.database = Path(database)
        self.database.parent.mkdir(parents=True, exist_ok=True)
        self.audit_dir = self.database.parent / "runs"
        self.raw_dir = self.database.parent / "raw"
        self.audit_dir.mkdir(exist_ok=True)
        self.raw_dir.mkdir(exist_ok=True)
        if not 0 <= max_unknown_ratio <= 1:
            raise ValueError("max_unknown_ratio must be between 0 and 1")
        self.max_unknown_ratio = max_unknown_ratio

    @staticmethod
    def _bootstrap(con: duckdb.DuckDBPyConnection) -> None:
        con.execute("""
          CREATE TABLE IF NOT EXISTS published_batches(batch_id VARCHAR PRIMARY KEY, ingest_sequence BIGINT UNIQUE, manifest_hash VARCHAR, run_id VARCHAR, published_at TIMESTAMP);
          CREATE TABLE IF NOT EXISTS evidence(source_system VARCHAR, entity_type VARCHAR, source_id VARCHAR, business_key VARCHAR, revision BIGINT, operation VARCHAR, effective_at VARCHAR, updated_at VARCHAR, attributes JSON, payload_hash VARCHAR, raw_hash VARCHAR, row_number BIGINT, batch_id VARCHAR, run_id VARCHAR, raw_record JSON, PRIMARY KEY(source_system,entity_type,source_id,revision));
          CREATE TABLE IF NOT EXISTS file_receipts(run_id VARCHAR, source_system VARCHAR, object_path VARCHAR, raw_hash VARCHAR, row_count BIGINT, duplicate BOOLEAN);
          CREATE TABLE IF NOT EXISTS pipeline_runs(run_id VARCHAR PRIMARY KEY,batch_id VARCHAR,status VARCHAR,report JSON);
          CREATE TABLE IF NOT EXISTS quality_results(run_id VARCHAR,rule_id VARCHAR,severity VARCHAR,status VARCHAR,result JSON);
          CREATE TABLE IF NOT EXISTS resolution_decisions(run_id VARCHAR,decision_id VARCHAR,decision JSON);
          CREATE TABLE IF NOT EXISTS publication_history(run_id VARCHAR,table_name VARCHAR,row_hash VARCHAR,row_data JSON);
        """)

    def run(self, manifest_path: Path | str) -> dict[str, Any]:
        from logistics_platform.warehouse import create_staging, project, quality_checks

        started = time.perf_counter()
        run_id = str(uuid4())
        report: dict[str, Any] = {
            "run_id": run_id,
            "batch_id": "unknown",
            "status": "failed",
            "quality": [],
            "resolutions": [],
            "started_at": datetime.now(timezone.utc).isoformat(),
        }
        path = Path(manifest_path).resolve()
        lock_path = self.database.with_suffix(self.database.suffix + ".lock")
        with lock_path.open("a") as lock:
            fcntl.flock(lock, fcntl.LOCK_EX)
            con = connect(self.database)
            self._bootstrap(con)
            in_transaction = False
            try:
                manifest = json.loads(path.read_text())
                validate_manifest(manifest)
                report["batch_id"] = manifest["batch_id"]
                report["ingest_sequence"] = integer(manifest["ingest_sequence"], "ingest_sequence")
                mh = digest(manifest)
                prior = con.execute(
                    "SELECT manifest_hash FROM published_batches WHERE batch_id=?",
                    [report["batch_id"]],
                ).fetchone()
                if prior:
                    if prior[0] != mh:
                        raise ContractError(
                            "ING_BATCH_IDENTITY", "Committed batch identity has different content"
                        )
                    # Verify bytes even on a no-op: raw delivery mutation cannot masquerade as a safe rerun.
                    self._load_files(path, manifest, report)
                    report["status"] = "noop"
                    report["quality"].append(
                        check("INC_IDEMPOTENCY", "unchanged", "unchanged", True)
                    )
                else:
                    cursor = con.execute(
                        "SELECT coalesce(max(ingest_sequence),0) FROM published_batches"
                    ).fetchone()[0]
                    if report["ingest_sequence"] != cursor + 1:
                        raise ContractError(
                            "INC_CHECKPOINT",
                            f"Expected sequence {cursor + 1}, received {report['ingest_sequence']}",
                        )
                    rows = self._load_files(path, manifest, report)
                    con.execute("BEGIN TRANSACTION")
                    in_transaction = True
                    self._ingest(con, rows, report)
                    evidence = self._evidence(con)
                    create_staging(con)
                    self._stage(con, evidence, report)
                    project(con, run_id)
                    for result in quality_checks(con):
                        report["quality"].append(
                            check(
                                result.get("check_id", result.get("rule_id", "SQL_CHECK")),
                                result["observed"],
                                result["expected"],
                                bool(result["passed"]),
                                severity=result.get("severity", "error"),
                                details=result.get("details"),
                            )
                        )
                    self._additional_checks(con, report)
                    if any(
                        q["severity"] == "error" and q["status"] == "fail"
                        for q in report["quality"]
                    ):
                        raise ContractError(
                            "PUBLICATION_GATE",
                            "Critical quality checks failed; candidate transaction rolled back",
                        )
                    con.execute(
                        "INSERT INTO published_batches VALUES (?,?,?,?,current_timestamp)",
                        [report["batch_id"], report["ingest_sequence"], mh, run_id],
                    )
                    for decision in report["resolutions"]:
                        con.execute(
                            "INSERT INTO resolution_decisions VALUES (?,?,?)",
                            [run_id, decision["decision_id"], compact(decision)],
                        )
                    self._snapshot(con, run_id)
                    con.execute("COMMIT")
                    in_transaction = False
                    report["status"] = "published"
            except (ContractError, ValueError, KeyError, TypeError, OSError, duckdb.Error) as exc:
                if in_transaction:
                    con.execute("ROLLBACK")
                rule = exc.rule if isinstance(exc, ContractError) else "PIPELINE_FAILURE"
                report["quality"].append(check(rule, str(exc), "valid complete publication", False))
                report["error"] = str(exc)
            report["watermark"] = con.execute(
                "SELECT coalesce(max(ingest_sequence),0) FROM published_batches"
            ).fetchone()[0]
            report["duration_seconds"] = round(time.perf_counter() - started, 6)
            report["ended_at"] = datetime.now(timezone.utc).isoformat()
            for result in report["quality"]:
                result.update(
                    run_id=run_id, batch_id=report["batch_id"], evaluated_at=report["ended_at"]
                )
                con.execute(
                    "INSERT INTO quality_results VALUES (?,?,?,?,?)",
                    [
                        run_id,
                        result["rule_id"],
                        result["severity"],
                        result["status"],
                        compact(result),
                    ],
                )
            con.execute(
                "INSERT INTO pipeline_runs VALUES (?,?,?,?)",
                [run_id, report["batch_id"], report["status"], compact(report)],
            )
            con.close()
            destination = self.audit_dir / f"{run_id}.json"
            destination.write_text(json.dumps(report, indent=2, default=str) + "\n")
            return report

    def _load_files(
        self, path: Path, manifest: dict[str, Any], report: dict[str, Any]
    ) -> list[tuple[str, int, dict[str, Any], dict[str, Any]]]:
        if set(manifest.get("required_sources", [])) != SOURCES:
            raise ContractError(
                "ING_REQUIRED_SOURCES", "Manifest must declare all five required sources"
            )
        files = manifest["files"]
        if not isinstance(files, list) or {f["source"] for f in files} != SOURCES:
            raise ContractError(
                "ING_REQUIRED_SOURCES",
                "Every required source needs an explicit file, including empty feeds",
            )
        if len({f["path"] for f in files}) != len(files):
            raise ContractError("ING_EXPECTED_FILES", "Manifest paths must be unique")
        rows_out = []
        counts: dict[str, dict[str, int]] = defaultdict(lambda: {"files": 0, "rows": 0})
        money: dict[str, int] = defaultdict(int)
        seen: set[tuple[str, int, str]] = set()
        receipts = []
        for entry in files:
            source = entry["source"]
            source_path = (path.parent / entry["path"]).resolve()
            if not source_path.is_relative_to(path.parent) or not source_path.is_file():
                raise ContractError(
                    "ING_EXPECTED_FILES", f"Missing or unsafe declared file: {entry['path']}"
                )
            content = source_path.read_bytes()
            raw_hash = sha256(content).hexdigest()
            archive = self.raw_dir / raw_hash
            if not archive.exists():
                archive.write_bytes(content)
            if raw_hash != entry["sha256"]:
                raise ContractError("ING_CHECKSUM", f"Checksum mismatch: {entry['path']}")
            version = integer(entry["schema_version"], "schema_version")
            rows = read_rows(content, source, version)
            if len(rows) != entry["rows"]:
                raise ContractError("ING_ROW_COUNT", f"Row control mismatch: {entry['path']}")
            counts[source]["files"] += 1
            counts[source]["rows"] += len(rows)
            duplicate = (source, version, raw_hash) in seen
            seen.add((source, version, raw_hash))
            receipts.append(
                {
                    "source": source,
                    "path": entry["path"],
                    "sha256": raw_hash,
                    "rows": len(rows),
                    "duplicate": duplicate,
                }
            )
            for number, row in enumerate(rows, 1):
                if source == "transport_costs":
                    money[str(row["currency"])] += integer(row["amount_minor"], "amount_minor")
                # Repeated physical files are still represented in raw controls and duplicate accounting.
                rows_out.append(
                    (
                        source,
                        version,
                        row,
                        {
                            "raw_hash": raw_hash,
                            "row_number": number,
                            "path": entry["path"],
                            "duplicate_file": duplicate,
                        },
                    )
                )
        for source in SOURCES:
            control = manifest["controls"][source]
            for field in ("files", "rows"):
                if counts[source][field] != control[field]:
                    raise ContractError(
                        "ING_EXPECTED_FILES" if field == "files" else "ING_ROW_COUNT",
                        f"{source} {field}: received {counts[source][field]}, expected {control[field]}",
                    )
        expected_money = manifest["controls"]["transport_costs"]["amount_minor"]
        if any(
            money.get(k, 0) != expected_money.get(k, 0)
            for k in money.keys() | expected_money.keys()
        ):
            raise ContractError(
                "REC_COST_RAW",
                f"Raw cost control: received {dict(money)}, expected {expected_money}",
            )
        report["receipts"] = receipts
        report["raw_cost_amount_minor"] = dict(money)
        report["physical_rows"] = sum(c["rows"] for c in counts.values())
        report["quality"].extend(
            [
                check("ING_MANIFEST_CONTROLS", counts, manifest["controls"], True),
                check("REC_COST_RAW", dict(money), expected_money, True),
                check(
                    "ING_DUPLICATE_FILE",
                    sum(r["duplicate"] for r in receipts),
                    0,
                    not any(r["duplicate"] for r in receipts),
                    severity="warning",
                ),
            ]
        )
        return rows_out

    def _ingest(
        self, con: duckdb.DuckDBPyConnection, rows: list[Any], report: dict[str, Any]
    ) -> None:
        existing = {
            (r[0], r[1], r[2], r[3]): r[4]
            for r in con.execute(
                "SELECT source_system,entity_type,source_id,revision,payload_hash FROM evidence"
            ).fetchall()
        }
        inserts = []
        duplicate_rows = 0
        new_money = 0
        duplicate_money = 0
        for source, version, row, lineage in rows:
            try:
                rec = normalize(source, row, version)
            except ContractError as exc:
                report.setdefault("quarantine", []).append(
                    {"reason": exc.rule, "raw_record": row, **lineage}
                )
                raise
            key = (rec.source_system, rec.entity_type, rec.source_id, rec.revision)
            if key in existing:
                if existing[key] != rec.payload_hash:
                    raise ContractError(
                        "SRC_VERSION_CONFLICT",
                        f"Different payloads claim the same source revision: {key}",
                    )
                duplicate_rows += 1
                duplicate_money += rec.attributes.get("amount_minor", 0)
                continue
            existing[key] = rec.payload_hash
            new_money += rec.attributes.get("amount_minor", 0)
            inserts.append(
                [
                    rec.source_system,
                    rec.entity_type,
                    rec.source_id,
                    rec.business_key,
                    rec.revision,
                    rec.operation,
                    rec.effective_at,
                    rec.updated_at,
                    compact(rec.attributes),
                    rec.payload_hash,
                    lineage["raw_hash"],
                    lineage["row_number"],
                    report["batch_id"],
                    report["run_id"],
                    compact(row),
                ]
            )
        if inserts:
            con.executemany("INSERT INTO evidence VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)", inserts)
        for r in report["receipts"]:
            con.execute(
                "INSERT INTO file_receipts VALUES (?,?,?,?,?,?)",
                [report["run_id"], r["source"], r["path"], r["sha256"], r["rows"], r["duplicate"]],
            )
        report.update(accepted_revisions=len(inserts), duplicate_rows=duplicate_rows)
        report["quality"].append(
            check(
                "SRC_DUPLICATE_VERSION", duplicate_rows, 0, duplicate_rows == 0, severity="warning"
            )
        )
        raw_money = sum(report["raw_cost_amount_minor"].values())
        report["quality"].append(
            check(
                "REC_COST_BRIDGE",
                new_money + duplicate_money,
                raw_money,
                new_money + duplicate_money == raw_money,
                details={
                    "new_evidence_amount_minor": new_money,
                    "duplicate_or_replayed_amount_minor": duplicate_money,
                    "note": "New evidence includes superseded revisions; current-state totals are independently reconciled in SQL.",
                },
            )
        )

    @staticmethod
    def _evidence(con: duckdb.DuckDBPyConnection) -> list[tuple[Record, dict[str, Any]]]:
        return [
            (
                Record(
                    source_system=r[0],
                    entity_type=r[1],
                    source_id=r[2],
                    business_key=r[3],
                    revision=r[4],
                    operation=r[5],
                    effective_at=r[6],
                    updated_at=r[7],
                    attributes=json.loads(r[8]),
                ),
                {"raw_hash": r[9], "run_id": r[10]},
            )
            for r in con.execute(
                "SELECT source_system,entity_type,source_id,business_key,revision,operation,effective_at,updated_at,attributes,raw_hash,run_id FROM evidence ORDER BY source_system,entity_type,source_id,revision"
            ).fetchall()
        ]

    def _stage(
        self,
        con: duckdb.DuckDBPyConnection,
        records: list[tuple[Record, dict[str, Any]]],
        report: dict[str, Any],
    ) -> None:
        resolver = Resolver(records)
        conflicts = resolver.conflicts()
        if conflicts:
            report["resolutions"] = conflicts
            raise ContractError("REF_AMBIGUOUS_ALIAS", compact(conflicts[:5]))
        current = latest(records)
        staged: dict[str, list[dict[str, Any]]] = defaultdict(list)
        # Reconcile changed source IDs through explicit business identity, not row ID.
        business: dict[tuple[str, str], tuple[Record, dict[str, Any]]] = {}
        for rec, lineage in current:
            if rec.entity_type in {"order", "shipment", "package"}:
                key = (rec.entity_type, rec.business_key)
                prior = business.get(key)
                if (
                    prior
                    and prior[0].updated_at == rec.updated_at
                    and (
                        prior[0].attributes != rec.attributes or prior[0].operation != rec.operation
                    )
                ):
                    raise ContractError(
                        "CAN_DUPLICATE_BUSINESS_KEY",
                        f"Equal-authority conflicting records for {key}",
                    )
                if prior is None or (rec.updated_at, rec.source_id) > (
                    prior[0].updated_at,
                    prior[0].source_id,
                ):
                    business[key] = (rec, lineage)
            elif rec.entity_type != "alias":
                self._stage_record(staged, rec, lineage, resolver, report)
        for rec, lineage in business.values():
            self._stage_record(staged, rec, lineage, resolver, report)
        report["resolutions"] = resolver.decisions
        unresolved = sum(d["outcome"] != "resolved" for d in resolver.decisions)
        ratio = unresolved / max(1, len(resolver.decisions))
        report["quality"].append(
            check(
                "CAN_UNRESOLVED_IDENTITY",
                ratio,
                self.max_unknown_ratio,
                ratio <= self.max_unknown_ratio,
                details={"unresolved": unresolved, "attempted": len(resolver.decisions)},
            )
        )
        if unresolved:
            report["quality"].append(
                check("REF_MISSING_VALUE", unresolved, 0, False, severity="warning")
            )
        for table, rows in staged.items():
            columns = [r[1] for r in con.execute(f"PRAGMA table_info('{table}')").fetchall()]
            con.executemany(
                f"INSERT INTO {table} VALUES ({','.join('?' for _ in columns)})",
                [[row.get(c) for c in columns] for row in rows],
            )

    def _stage_record(
        self,
        staged: dict[str, list[dict[str, Any]]],
        rec: Record,
        lineage: dict[str, Any],
        resolver: Resolver,
        report: dict[str, Any],
    ) -> None:
        a = rec.attributes
        base = {
            "canonical_id": canonical_id(rec.entity_type, rec.business_key),
            "business_key": rec.business_key,
            "source_id": rec.source_id,
            "revision": rec.revision,
            "source_updated_at": rec.updated_at,
            "updated_at": rec.updated_at,
            "is_deleted": rec.operation == "delete",
            "raw_hash": lineage["raw_hash"],
            "run_id": report["run_id"],
        }

        def resolve(entity: str, alias: str | None, at: str = rec.effective_at) -> str | None:
            return resolver.resolve(entity, rec.source_system, alias, at, rec.source_id)

        def country(value: str | None, at: str) -> str | None:
            if value in COUNTRIES:
                return canonical_id("country", value)
            resolved = resolve("country", value, at)
            if resolved is None:
                raise ContractError(
                    "REF_INVALID_COUNTRY", f"Invalid country value {value!r} on {rec.source_id}"
                )
            return resolved

        if rec.entity_type in REFERENCE_TYPES:
            if rec.entity_type == "country" and rec.business_key not in COUNTRIES:
                raise ContractError(
                    "REF_INVALID_COUNTRY", f"Unsupported reference country {rec.business_key}"
                )
            base.update(
                entity_type=rec.entity_type,
                canonical_value=a["name"],
                attributes=compact(a),
                effective_from=rec.effective_at,
                status="INACTIVE" if rec.operation == "delete" else "ACTIVE",
            )
            staged["stg_reference"].append(base)
        elif rec.entity_type == "order":
            base.update(
                created_at=a["created_at"],
                country_id=country(a.get("country_code"), a["created_at"]),
            )
            staged["stg_order"].append(base)
        elif rec.entity_type == "package":
            base.update(
                shipment_id=canonical_id("shipment", a["shipment_key"]),
                weight_grams=a.get("weight_grams"),
            )
            staged["stg_package"].append(base)
        elif rec.entity_type == "shipment":
            base.update(
                order_id=canonical_id("order", a["order_key"]),
                carrier_id=resolve("carrier", a.get("carrier_alias"), a["created_at"]),
                origin_facility_id=resolve(
                    "facility", a.get("origin_facility_alias"), a["created_at"]
                ),
                destination_facility_id=resolve(
                    "facility", a.get("destination_facility_alias"), a["created_at"]
                ),
                route_id=canonical_id("route", a["route_key"]),
                service_level_id=resolve("service_level", a.get("service_alias"), a["created_at"]),
                country_id=country(a.get("country_code"), a["created_at"]),
                created_at=a["created_at"],
                promised_at=a["promised_at"],
                state=a.get("state", "created"),
                state_occurred_at=rec.effective_at,
            )
            staged["stg_shipment"].append(base)
        elif rec.entity_type == "event":
            if rec.operation == "delete":
                return
            base.update(
                source_system=rec.source_system,
                event_id=rec.source_id,
                event_canonical_id=base["canonical_id"],
                shipment_id=canonical_id("shipment", a["shipment_key"]),
                package_id=canonical_id("package", a["package_key"]) if a["package_key"] else None,
                carrier_id=resolve("carrier", a.get("carrier_alias")),
                state=a["state"],
                occurred_at=rec.effective_at,
                verified=a["verified"],
            )
            staged["stg_event"].append(base)
        elif rec.entity_type == "cost":
            base.update(
                cost_id=rec.source_id,
                cost_canonical_id=base["canonical_id"],
                shipment_id=canonical_id("shipment", a["shipment_key"]),
                carrier_id=resolve("carrier", a.get("carrier_alias")),
                amount_minor=a["amount_minor"],
                currency=a["currency"],
                cost_type=a["cost_type"],
                incurred_at=rec.effective_at,
            )
            staged["stg_cost"].append(base)

    def _additional_checks(self, con: duckdb.DuckDBPyConnection, report: dict[str, Any]) -> None:
        checks = {
            "CAN_ORDER_RELATION": "SELECT count(*) FROM stg_shipment s LEFT JOIN stg_order o ON s.order_id=o.canonical_id WHERE o.canonical_id IS NULL",
            "CAN_PACKAGE_RELATION": "SELECT count(*) FROM stg_package p LEFT JOIN stg_shipment s ON p.shipment_id=s.canonical_id WHERE s.canonical_id IS NULL",
            "CAN_EVENT_MEMBERSHIP": "SELECT count(*) FROM stg_event e LEFT JOIN stg_package p ON e.package_id=p.canonical_id WHERE e.package_id IS NOT NULL AND (p.canonical_id IS NULL OR p.shipment_id<>e.shipment_id)",
            "CAN_EVENT_SHIPMENT": "SELECT count(*) FROM stg_event e LEFT JOIN stg_shipment s ON e.shipment_id=s.canonical_id WHERE s.canonical_id IS NULL",
            "CAN_COST_SHIPMENT": "SELECT count(*) FROM stg_cost c LEFT JOIN stg_shipment s ON c.shipment_id=s.canonical_id WHERE s.canonical_id IS NULL",
            "CAN_EVENT_TIMESTAMP": "SELECT count(*) FROM stg_event e JOIN stg_shipment s ON e.shipment_id=s.canonical_id WHERE e.occurred_at<s.created_at",
            "REF_BOUNDARY_CONFLICT": "SELECT count(*) FROM (SELECT entity_type,canonical_id,effective_from FROM stg_reference GROUP BY ALL HAVING count(DISTINCT attributes)>1)",
        }
        for rule, sql in checks.items():
            observed = con.execute(sql).fetchone()[0]
            report["quality"].append(check(rule, observed, 0, observed == 0))
        pending = con.execute("SELECT count(*) FROM pending_fact_reference").fetchone()[0]
        slots = con.execute("SELECT count(*)*8 FROM fact_attribution").fetchone()[0]
        pending_ratio = pending / max(1, slots)
        report["quality"].append(
            check(
                "REF_UNKNOWN_RATE",
                pending_ratio,
                self.max_unknown_ratio,
                pending_ratio <= self.max_unknown_ratio,
            )
        )
        current_count = con.execute("SELECT count(*) FROM stg_shipment").fetchone()[0]
        prior = con.execute(
            "SELECT count(*) FROM publication_history WHERE table_name='fact_shipment' AND run_id=(SELECT run_id FROM published_batches ORDER BY ingest_sequence DESC LIMIT 1)"
        ).fetchone()[0]
        if prior:
            ratio = current_count / prior
            report["quality"].append(
                check(
                    "VOL_CHANGE",
                    ratio,
                    {"min": 0.5, "max": 2.0},
                    0.5 <= ratio <= 2.0,
                    severity="warning",
                )
            )
        else:
            result = check(
                "VOL_CHANGE",
                current_count,
                "prior published baseline",
                True,
                severity="info",
                details="First publication has no comparable baseline",
            )
            result["status"] = "skipped"
            report["quality"].append(result)

    @staticmethod
    def _snapshot(con: duckdb.DuckDBPyConnection, run_id: str) -> None:
        tables = [
            r[0]
            for r in con.execute(
                "SELECT table_name FROM information_schema.tables WHERE table_schema='main' AND (table_name LIKE 'fact_%' OR table_name LIKE 'canonical_%' OR table_name='dim_reference')"
            ).fetchall()
        ]
        for table in tables:
            con.execute(
                f"INSERT INTO publication_history SELECT ?,?,sha256(to_json(t)::VARCHAR),to_json(t) FROM {table} t",
                [run_id, table],
            )

    def backfill(self, manifests: list[Path], start: int, end: int) -> list[dict[str, Any]]:
        if start < 1 or end < start or end - start >= 100:
            raise ValueError("Backfills require a bounded range of at most 100 ingestion sequences")
        selected = sorted((int(json.loads(p.read_text())["ingest_sequence"]), p) for p in manifests)
        selected = [(n, p) for n, p in selected if start <= n <= end]
        if [n for n, _ in selected] != list(range(start, end + 1)):
            raise ValueError("Backfill range must contain exactly one manifest for every sequence")
        results = []
        for _, p in selected:
            result = self.run(p)
            results.append(result)
            if result["status"] == "failed":
                break
        return results
