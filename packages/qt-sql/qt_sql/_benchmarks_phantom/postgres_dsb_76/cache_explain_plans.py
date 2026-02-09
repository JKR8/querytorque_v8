#!/usr/bin/env python3
"""
Cache EXPLAIN ANALYZE plans for all 76 DSB queries (postgres_dsb_76).

Connects to PostgreSQL, runs EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) for each
query in queries/, and saves results to explains/sf10/{query_id}.json.

Usage:
    python cache_explain_plans.py                     # cache all 76 queries
    python cache_explain_plans.py --dry-run            # list what would be cached
    python cache_explain_plans.py --queries query001_multi_i1 query013_spj_i2
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
from pathlib import Path

import psycopg2

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
DSN = "postgres://jakc9:jakc9@127.0.0.1:5434/dsb_sf10"
STATEMENT_TIMEOUT_S = 300
SCRIPT_DIR = Path(__file__).resolve().parent
QUERIES_DIR = SCRIPT_DIR / "queries"
EXPLAINS_DIR = SCRIPT_DIR / "explains" / "sf10"


def query_id_from_filename(filename: str) -> str:
    """Strip .sql extension to get the query id."""
    return filename.removesuffix(".sql")


def discover_queries(query_filter: list[str] | None = None) -> list[tuple[str, Path]]:
    """Return sorted list of (query_id, path) for all .sql files in queries/."""
    sql_files = sorted(QUERIES_DIR.glob("*.sql"))
    if not sql_files:
        print(f"ERROR: No .sql files found in {QUERIES_DIR}", file=sys.stderr)
        sys.exit(1)

    results = []
    for p in sql_files:
        qid = query_id_from_filename(p.name)
        if query_filter and qid not in query_filter:
            continue
        results.append((qid, p))

    if query_filter:
        found_ids = {qid for qid, _ in results}
        missing = set(query_filter) - found_ids
        if missing:
            print(f"WARNING: Queries not found: {sorted(missing)}", file=sys.stderr)

    return results


def extract_execution_time_ms(plan_json: list) -> float | None:
    """Extract total execution time in ms from the top-level plan node."""
    try:
        top = plan_json[0]
        # The top-level object has "Execution Time" key
        return top.get("Execution Time")
    except (IndexError, TypeError, KeyError):
        return None


def run_explain(conn, sql: str) -> dict:
    """
    Run EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) for the given SQL.

    Returns dict with keys: plan_json, execution_time_ms, error.
    """
    explain_sql = f"EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON)\n{sql}"
    try:
        with conn.cursor() as cur:
            cur.execute(explain_sql)
            rows = cur.fetchall()
            # psycopg2 returns JSON plan as a list in the first column
            plan_json = rows[0][0]
            exec_time = extract_execution_time_ms(plan_json)
            return {
                "plan_json": plan_json,
                "execution_time_ms": exec_time,
                "error": None,
            }
    except psycopg2.Error as e:
        # Rollback so the connection is usable for the next query
        conn.rollback()
        return {
            "plan_json": None,
            "execution_time_ms": None,
            "error": str(e).strip(),
        }


def main():
    parser = argparse.ArgumentParser(
        description="Cache EXPLAIN ANALYZE plans for DSB-76 queries."
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="List queries that would be cached without running them.",
    )
    parser.add_argument(
        "--queries",
        nargs="+",
        metavar="QUERY_ID",
        help="Only cache specific queries (e.g. query001_multi_i1 query013_spj_i2).",
    )
    args = parser.parse_args()

    queries = discover_queries(args.queries)
    total = len(queries)
    print(f"Discovered {total} queries in {QUERIES_DIR}")

    if args.dry_run:
        print("\n--- DRY RUN: would cache these queries ---")
        for qid, path in queries:
            out_path = EXPLAINS_DIR / f"{qid}.json"
            exists = "EXISTS" if out_path.exists() else "NEW"
            print(f"  [{exists}] {qid}  ->  explains/sf10/{qid}.json")
        print(f"\nTotal: {total} queries")
        return

    # Ensure output directory exists
    EXPLAINS_DIR.mkdir(parents=True, exist_ok=True)

    # Connect to PostgreSQL
    print(f"Connecting to {DSN.split('@')[1]} ...")
    try:
        conn = psycopg2.connect(DSN)
        conn.autocommit = True
    except psycopg2.Error as e:
        print(f"ERROR: Cannot connect to PostgreSQL: {e}", file=sys.stderr)
        sys.exit(1)

    # Set statement timeout
    with conn.cursor() as cur:
        cur.execute(f"SET statement_timeout = '{STATEMENT_TIMEOUT_S}s';")
    print(f"Statement timeout: {STATEMENT_TIMEOUT_S}s")

    # Run EXPLAIN for each query
    successes = 0
    failures = 0
    skipped = 0
    results_summary: list[dict] = []

    print(f"\n{'='*70}")
    for i, (qid, path) in enumerate(queries, 1):
        sql = path.read_text(encoding="utf-8").strip()
        if not sql:
            print(f"[{i:3d}/{total}] {qid:40s}  SKIP (empty file)")
            skipped += 1
            continue

        print(f"[{i:3d}/{total}] {qid:40s}  ", end="", flush=True)
        t0 = time.perf_counter()
        result = run_explain(conn, sql)
        elapsed = time.perf_counter() - t0

        if result["error"]:
            failures += 1
            err_short = result["error"][:80].replace("\n", " ")
            print(f"ERROR ({elapsed:.1f}s) -- {err_short}")
            # Still save the error record so we know it was attempted
            output = {
                "query_id": qid,
                "plan_json": None,
                "execution_time_ms": None,
                "error": result["error"],
            }
        else:
            successes += 1
            exec_ms = result["execution_time_ms"]
            exec_str = f"{exec_ms:.1f}ms" if exec_ms is not None else "?"
            print(f"OK  {exec_str:>12s}  ({elapsed:.1f}s wall)")
            output = {
                "query_id": qid,
                "plan_json": result["plan_json"],
                "execution_time_ms": result["execution_time_ms"],
            }

        # Save to file
        out_path = EXPLAINS_DIR / f"{qid}.json"
        out_path.write_text(
            json.dumps(output, indent=2, default=str) + "\n",
            encoding="utf-8",
        )

        results_summary.append(
            {
                "query_id": qid,
                "status": "OK" if result["error"] is None else "ERROR",
                "execution_time_ms": result.get("execution_time_ms"),
                "wall_time_s": round(elapsed, 2),
            }
        )

    conn.close()

    # Print summary
    print(f"\n{'='*70}")
    print("SUMMARY")
    print(f"  Total:     {total}")
    print(f"  Success:   {successes}")
    print(f"  Errors:    {failures}")
    print(f"  Skipped:   {skipped}")
    print(f"  Output:    {EXPLAINS_DIR}/")

    if failures > 0:
        print(f"\nFailed queries:")
        for r in results_summary:
            if r["status"] == "ERROR":
                print(f"  - {r['query_id']}")

    # Save summary
    summary_path = EXPLAINS_DIR / "_summary.json"
    summary_path.write_text(
        json.dumps(
            {
                "total": total,
                "success": successes,
                "errors": failures,
                "skipped": skipped,
                "queries": results_summary,
            },
            indent=2,
        )
        + "\n",
        encoding="utf-8",
    )
    print(f"\nSummary saved to {summary_path}")


if __name__ == "__main__":
    main()
