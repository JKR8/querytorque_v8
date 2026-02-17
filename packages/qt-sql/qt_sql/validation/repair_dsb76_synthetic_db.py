"""Repair an existing DSB76 synthetic DuckDB in place.

This avoids full regeneration by only patching queries that still return
zero rows (or fail) via targeted top-up and deterministic seeding.

Usage:
  PYTHONPATH=packages/qt-shared:packages/qt-sql \
  python3 -m qt_sql.validation.repair_dsb76_synthetic_db \
    --db /mnt/d/qt_synth/postgres_dsb_76_synthetic.duckdb
"""

from __future__ import annotations

import argparse
import hashlib
import json
import logging
from pathlib import Path
from typing import Any, Dict, List, Tuple

import duckdb

from .build_dsb76_synthetic_db import (
    _build_query_context,
    _count_query_rows,
    _force_seed_for_query,
    _merge_filters,
    _merge_fk,
    _merge_table_schemas,
    _tables_in_not_exists,
    _top_up_for_query,
)
from .synthetic_validator import SchemaFromDB, SyntheticValidator


logger = logging.getLogger(__name__)


def _existing_tables(conn: duckdb.DuckDBPyConnection) -> set[str]:
    rows = conn.execute(
        """
        SELECT LOWER(table_name)
        FROM information_schema.tables
        WHERE table_schema NOT IN ('information_schema', 'pg_catalog')
        """
    ).fetchall()
    return {str(r[0]).lower() for r in rows if r and r[0]}


def _stable_small_int(seed: str, modulo: int) -> int:
    if modulo <= 0:
        return 0
    digest = hashlib.md5(seed.encode("utf-8")).hexdigest()
    return int(digest[:8], 16) % modulo


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Repair an existing postgres_dsb_76 synthetic DuckDB in place."
    )
    parser.add_argument(
        "--db",
        required=True,
        help="Existing DuckDB file to repair in place.",
    )
    parser.add_argument(
        "--queries-dir",
        default="packages/qt-sql/qt_sql/benchmarks/postgres_dsb_76/queries",
        help="Directory containing DSB76 query SQL files.",
    )
    parser.add_argument(
        "--report",
        default="",
        help="Optional JSON report path. Defaults to <db>.repair.report.json",
    )
    parser.add_argument(
        "--dialect",
        default="postgres",
        help="Source query dialect (default: postgres).",
    )
    parser.add_argument(
        "--reference-db",
        default="",
        help="Optional reference DB DSN for schema extraction.",
    )
    parser.add_argument(
        "--query-timeout-s",
        type=int,
        default=20,
        help="Slow confirmation timeout (seconds).",
    )
    parser.add_argument(
        "--probe-timeout-s",
        type=int,
        default=2,
        help="Fast timeout used during iterative repair loops.",
    )
    parser.add_argument(
        "--topup-dim-rows",
        type=int,
        default=1800,
        help="Top-up rows per dimension table for failing queries.",
    )
    parser.add_argument(
        "--topup-fact-rows",
        type=int,
        default=7200,
        help="Top-up rows per fact table for failing queries.",
    )
    parser.add_argument(
        "--topup-retries",
        type=int,
        default=1,
        help="Top-up attempts per failing query before force-seed.",
    )
    parser.add_argument(
        "--force-seed-attempts",
        type=int,
        default=12,
        help="Deterministic seed attempts per pass for unresolved queries.",
    )
    parser.add_argument(
        "--force-seed-rows",
        type=int,
        default=10,
        help="Rows inserted per force-seed attempt per table.",
    )
    parser.add_argument(
        "--repair-passes",
        type=int,
        default=4,
        help="Number of repair passes over unresolved queries.",
    )
    parser.add_argument(
        "--stubborn-trials",
        type=int,
        default=10,
        help="Extra bounded local-search trials for unresolved queries.",
    )
    parser.add_argument(
        "--min-query-rows",
        type=int,
        default=1,
        help="Hard minimum rows required for success.",
    )
    parser.add_argument(
        "--preferred-query-rows",
        type=int,
        default=10,
        help="Preferred minimum rows for stronger validator signal.",
    )
    parser.add_argument(
        "--verbose",
        action="store_true",
        help="Enable debug logs.",
    )

    args = parser.parse_args()
    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
    )

    db_path = Path(args.db)
    if not db_path.exists():
        raise FileNotFoundError(f"DB not found: {db_path}")

    queries_dir = Path(args.queries_dir)
    sql_files = sorted(queries_dir.glob("*.sql"))
    if not sql_files:
        raise FileNotFoundError(f"No SQL files found in {queries_dir}")

    report_path = (
        Path(args.report)
        if args.report
        else db_path.with_suffix(db_path.suffix + ".repair.report.json")
    )

    reference_db = (args.reference_db or "").strip()
    validator = SyntheticValidator(reference_db=None, dialect=args.dialect)
    validator.reference_db = reference_db
    if reference_db and SchemaFromDB.supports_dsn(reference_db):
        validator.schema_extractor = SchemaFromDB(reference_db)
        logger.info("Using reference DB schema extraction: %s", reference_db)
    elif reference_db:
        logger.warning(
            "Reference DB DSN unsupported for schema extraction, falling back to AST-only mode: %s",
            reference_db,
        )

    # Build global model from benchmark SQL (no data regeneration).
    query_contexts: List[Dict[str, Any]] = []
    global_tables: Dict[str, Dict[str, Any]] = {}
    global_fk: Dict[str, Dict[str, Tuple[str, str]]] = {}
    global_filters: Dict[str, Dict[str, List[Any]]] = {}
    for sql_file in sql_files:
        qctx = _build_query_context(validator, sql_file, args.dialect)
        query_contexts.append(qctx)
        _merge_table_schemas(global_tables, qctx["tables"])
        _merge_fk(global_fk, qctx["fk_relationships"])
        _merge_filters(global_filters, qctx["filter_values"])

    conn = duckdb.connect(str(db_path))
    validator.conn = conn
    for stmt in ("SET enable_progress_bar=false", "PRAGMA disable_progress_bar"):
        try:
            conn.execute(stmt)
        except Exception:
            pass

    # If the DB is partial, create missing tables only; never drop/recreate existing ones.
    missing = {
        t: schema
        for t, schema in global_tables.items()
        if t.lower() not in _existing_tables(conn)
    }
    if missing:
        logger.info("Creating %d missing tables in existing DB", len(missing))
        validator._create_schema(missing)

    # Ensure indexes exist for benchmark expressions/filters.
    for qctx in query_contexts:
        try:
            validator._create_indexes(global_tables, qctx["sql_duckdb"])
        except Exception:
            continue

    min_rows_required = max(1, int(args.min_query_rows))
    preferred_rows = max(min_rows_required, int(args.preferred_query_rows))
    probe_limit = preferred_rows if preferred_rows > 0 else 11
    fast_timeout_s = max(1, int(args.probe_timeout_s))
    slow_timeout_s = max(fast_timeout_s, int(args.query_timeout_s))

    def _probe_rows(sql_duckdb: str, *, allow_slow_fallback: bool) -> tuple[int, str | None]:
        try:
            rows = _count_query_rows(conn, sql_duckdb, fast_timeout_s, probe_limit=probe_limit)
            return int(rows), None
        except Exception as exc:
            if allow_slow_fallback:
                try:
                    rows = _count_query_rows(conn, sql_duckdb, slow_timeout_s, probe_limit=probe_limit)
                    return int(rows), None
                except Exception as slow_exc:
                    return 0, str(slow_exc)
            return 0, str(exc)

    unresolved_names = {qctx["name"] for qctx in query_contexts}
    failure_streak = {qctx["name"]: 0 for qctx in query_contexts}
    final_rows: Dict[str, int] = {}
    final_errors: Dict[str, str | None] = {}
    pass_results: List[Dict[str, Any]] = []

    max_passes = max(1, int(args.repair_passes))
    for pass_idx in range(1, max_passes + 1):
        logger.info("Repair pass %d/%d over %d unresolved queries", pass_idx, max_passes, len(unresolved_names))
        pass_entries: List[Dict[str, Any]] = []
        next_unresolved: set[str] = set()

        for qctx in query_contexts:
            name = qctx["name"]
            if name not in unresolved_names:
                continue

            sql_duckdb = qctx["sql_duckdb"]
            entry: Dict[str, Any] = {
                "query": name,
                "pass": pass_idx,
                "rows": 0,
                "success": False,
                "topup_attempts": 0,
                "forced_seed_attempts": 0,
                "error": None,
            }

            rows, err = _probe_rows(sql_duckdb, allow_slow_fallback=False)
            entry["rows"] = rows
            entry["success"] = rows >= min_rows_required
            entry["error"] = err

            if not entry["success"]:
                # Enforce diversity after repeated failures: larger and varied top-up sizes.
                streak = max(1, int(failure_streak.get(name, 0)) + 1)
                streak_boost = min(8, 2 ** max(0, streak - 1))
                pass_dim_rows = max(10, int(args.topup_dim_rows) * pass_idx * streak_boost)
                pass_fact_rows = max(10, int(args.topup_fact_rows) * pass_idx * streak_boost)
                for attempt in range(1, max(0, int(args.topup_retries)) + 1):
                    try:
                        attempt_jitter = 1 + attempt + _stable_small_int(
                            f"{name}:topup:{pass_idx}:{attempt}", 3
                        )
                        _top_up_for_query(
                            conn=conn,
                            validator=validator,
                            qctx=qctx,
                            global_tables=global_tables,
                            fact_rows=pass_fact_rows * attempt_jitter,
                            dim_rows=pass_dim_rows * attempt_jitter,
                        )
                        rows, err = _probe_rows(sql_duckdb, allow_slow_fallback=False)
                        entry["rows"] = rows
                        entry["topup_attempts"] = attempt
                        entry["success"] = rows >= min_rows_required
                        entry["error"] = None if entry["success"] else err
                        if entry["success"]:
                            break
                    except Exception as exc:
                        entry["topup_attempts"] = attempt
                        entry["error"] = str(exc)
            else:
                pass_dim_rows = max(10, int(args.topup_dim_rows) * pass_idx)
                pass_fact_rows = max(10, int(args.topup_fact_rows) * pass_idx)

            if not entry["success"] and qctx.get("tables"):
                anti_tables = _tables_in_not_exists(sql_duckdb)
                seed_rows = max(
                    1,
                    int(args.force_seed_rows)
                    + (pass_idx - 1)
                    + max(0, int(failure_streak.get(name, 0))),
                )
                base_variant = (
                    (pass_idx - 1)
                    * max(1, int(args.force_seed_attempts))
                    * seed_rows
                    + _stable_small_int(f"{name}:seed_base", 10000)
                )
                for seed_attempt in range(1, max(1, int(args.force_seed_attempts)) + 1):
                    try:
                        skip_tables = anti_tables
                        # If repeated passes fail, periodically explore non-skipped seeds.
                        if failure_streak.get(name, 0) >= 2 and seed_attempt % 3 == 0:
                            skip_tables = set()
                        _force_seed_for_query(
                            conn=conn,
                            qctx=qctx,
                            global_tables=global_tables,
                            fk_relationships=global_fk,
                            seed_variant=base_variant + (seed_attempt * seed_rows),
                            seed_rows=seed_rows,
                            skip_tables=skip_tables,
                        )
                        rows, err = _probe_rows(sql_duckdb, allow_slow_fallback=False)
                        entry["rows"] = rows
                        entry["forced_seed_attempts"] = seed_attempt
                        entry["success"] = rows >= min_rows_required
                        entry["error"] = None if entry["success"] else err
                        if entry["success"]:
                            break
                    except Exception as exc:
                        entry["forced_seed_attempts"] = seed_attempt
                        entry["error"] = str(exc)

            # Final bounded local-search fallback for stubborn zero-row queries.
            if not entry["success"] and qctx.get("tables") and int(args.stubborn_trials) > 0:
                anti_tables = _tables_in_not_exists(sql_duckdb)
                base_seed_rows = max(
                    1,
                    int(args.force_seed_rows)
                    + (pass_idx - 1)
                    + max(0, int(failure_streak.get(name, 0))),
                )
                for trial in range(1, max(1, int(args.stubborn_trials)) + 1):
                    try:
                        trial_dim = pass_dim_rows * (1 + trial // 2) * (
                            1 + _stable_small_int(f"{name}:trial_dim:{pass_idx}:{trial}", 2)
                        )
                        trial_fact = pass_fact_rows * (1 + trial // 2) * (
                            1 + _stable_small_int(f"{name}:trial_fact:{pass_idx}:{trial}", 3)
                        )
                        _top_up_for_query(
                            conn=conn,
                            validator=validator,
                            qctx=qctx,
                            global_tables=global_tables,
                            fact_rows=max(10, trial_fact),
                            dim_rows=max(10, trial_dim),
                        )

                        trial_seed_rows = base_seed_rows + (trial % 5)
                        trial_variant = (
                            _stable_small_int(f"{name}:trial_var:{pass_idx}:{trial}", 100000)
                            + trial * trial_seed_rows
                        )
                        skip_tables = anti_tables
                        if trial % 2 == 0:
                            skip_tables = set()
                        _force_seed_for_query(
                            conn=conn,
                            qctx=qctx,
                            global_tables=global_tables,
                            fk_relationships=global_fk,
                            seed_variant=trial_variant,
                            seed_rows=trial_seed_rows,
                            skip_tables=skip_tables,
                        )

                        rows, err = _probe_rows(sql_duckdb, allow_slow_fallback=False)
                        if rows > entry["rows"]:
                            entry["rows"] = rows
                        entry["success"] = rows >= min_rows_required
                        entry["error"] = None if entry["success"] else err
                        if entry["success"]:
                            break
                    except Exception as exc:
                        entry["error"] = str(exc)

            final_rows[name] = int(entry["rows"])
            final_errors[name] = entry["error"]
            if not entry["success"]:
                failure_streak[name] = int(failure_streak.get(name, 0)) + 1
                next_unresolved.add(name)
            else:
                failure_streak[name] = 0

            logger.info(
                "[pass %d] %s rows=%s success=%s topup=%d seed=%d",
                pass_idx,
                name,
                entry["rows"],
                entry["success"],
                entry["topup_attempts"],
                entry["forced_seed_attempts"],
            )
            pass_entries.append(entry)

        pass_results.append(
            {
                "pass": pass_idx,
                "queries_checked": len(pass_entries),
                "queries_resolved_this_pass": len(unresolved_names) - len(next_unresolved),
                "queries_still_unresolved": len(next_unresolved),
                "results": pass_entries,
            }
        )

        unresolved_names = next_unresolved
        if not unresolved_names:
            break

    # Final slow confirmation only for unresolved queries to avoid repeated 20s waits.
    if unresolved_names:
        logger.info(
            "Final slow confirmation over %d unresolved queries (timeout=%ss)",
            len(unresolved_names),
            slow_timeout_s,
        )
        still_unresolved: set[str] = set()
        unresolved_lookup = {qctx["name"]: qctx for qctx in query_contexts}
        for name in sorted(unresolved_names):
            qctx = unresolved_lookup[name]
            rows, err = _probe_rows(qctx["sql_duckdb"], allow_slow_fallback=True)
            final_rows[name] = rows
            final_errors[name] = err
            if rows < min_rows_required:
                still_unresolved.add(name)
        unresolved_names = still_unresolved

    total = len(query_contexts)
    ok = total - len(unresolved_names)
    preferred_ok = sum(1 for qctx in query_contexts if final_rows.get(qctx["name"], 0) >= preferred_rows)

    payload = {
        "summary": {
            "total_queries": total,
            "queries_with_rows": ok,
            "queries_without_rows_or_failed": total - ok,
            "queries_with_preferred_rows": preferred_ok,
            "preferred_rows_threshold": preferred_rows,
            "repair_passes_requested": max_passes,
            "repair_passes_run": len(pass_results),
            "db": str(db_path),
            "reference_db": reference_db,
        },
        "unresolved_queries": sorted(unresolved_names),
        "final_rows": {k: final_rows.get(k, 0) for k in sorted(final_rows)},
        "final_errors": {k: final_errors.get(k) for k in sorted(final_errors)},
        "passes": pass_results,
    }

    report_path.parent.mkdir(parents=True, exist_ok=True)
    report_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    conn.close()

    logger.info(
        "Repair complete. queries_with_rows=%d/%d report=%s unresolved=%d",
        ok,
        total,
        report_path,
        len(unresolved_names),
    )
    return 0 if not unresolved_names else 2


if __name__ == "__main__":
    raise SystemExit(main())
