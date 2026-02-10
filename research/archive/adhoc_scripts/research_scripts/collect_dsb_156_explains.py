#!/usr/bin/env python3
"""Collect EXPLAIN plans (no ANALYZE) for all 156 DSB queries on PostgreSQL SF10.

Uses EXPLAIN without ANALYZE to avoid executing each query (which can take
minutes on SF10). The estimated plan is sufficient for the optimizer to
understand join strategies, scan types, and cost estimates.

Outputs JSON files in the format expected by swarm_prep.py:
  packages/qt-sql/ado/benchmarks/postgres_dsb_156/explains/sf10/{qid}.json

Usage:
    python3 research/scripts/collect_dsb_156_explains.py
"""

from __future__ import annotations

import json
import re
import time
from pathlib import Path

import psycopg2

REPO_ROOT = Path(__file__).resolve().parent.parent.parent
BENCHMARK_DIR = REPO_ROOT / "packages" / "qt-sql" / "ado" / "benchmarks" / "postgres_dsb_156"
QUERIES_DIR = BENCHMARK_DIR / "queries"
EXPLAINS_DIR = BENCHMARK_DIR / "explains" / "sf10"

DSN = "host=127.0.0.1 port=5434 dbname=dsb_sf10 user=jakc9 password=jakc9"
TIMEOUT_MS = 60_000  # 1 minute per query (EXPLAIN only, no execution)


def clean_sql(sql_text: str) -> str:
    """Remove comment-only lines and trailing semicolons."""
    lines = []
    for line in sql_text.splitlines():
        if line.strip().startswith("--"):
            continue
        lines.append(line)
    clean = "\n".join(lines).strip()
    while clean.endswith(";"):
        clean = clean[:-1].rstrip()
    return clean


def query_sort_key(path: Path) -> tuple[int, str]:
    """Sort by numeric query id, then suffix."""
    stem = path.stem.lower()
    match = re.search(r"query[_]?(\d+)", stem)
    if not match:
        return (10**9, stem)
    return (int(match.group(1)), stem)


def main():
    EXPLAINS_DIR.mkdir(parents=True, exist_ok=True)

    query_files = sorted(QUERIES_DIR.glob("query*.sql"), key=query_sort_key)
    if not query_files:
        raise FileNotFoundError(f"No query files in {QUERIES_DIR}")

    # Skip already-collected
    already = {f.stem for f in EXPLAINS_DIR.glob("*.json")}
    todo = [f for f in query_files if f.stem not in already]

    print(f"Found {len(query_files)} queries, {len(already)} already collected, {len(todo)} to do")

    if not todo:
        print("All explains already collected.")
        return

    conn = psycopg2.connect(DSN)
    conn.autocommit = True
    cur = conn.cursor()

    ok = 0
    errors = 0
    t_start = time.time()

    for idx, qpath in enumerate(todo, 1):
        qid = qpath.stem
        sql = clean_sql(qpath.read_text())

        try:
            cur.execute(f"SET statement_timeout = {TIMEOUT_MS}")

            t0 = time.perf_counter()
            cur.execute(f"EXPLAIN (COSTS, VERBOSE, FORMAT JSON) {sql}")
            json_rows = cur.fetchall()
            plan_ms = (time.perf_counter() - t0) * 1000.0

            plan_json = json_rows[0][0]
            if isinstance(plan_json, str):
                plan_json = json.loads(plan_json)

            # EXPLAIN without ANALYZE has no execution/planning time
            # but still has estimated costs which the optimizer uses
            payload = {
                "query_id": qid,
                "engine": "postgresql",
                "benchmark": "dsb_156",
                "scale_factor": 10,
                "execution_time_ms": None,
                "planning_time_ms": plan_ms,
                "plan_json": plan_json,
                "original_sql": sql,
            }

            (EXPLAINS_DIR / f"{qid}.json").write_text(
                json.dumps(payload, indent=2, default=str)
            )

            ok += 1
            print(f"  [{idx}/{len(todo)}] {qid}: OK (plan={plan_ms:.0f}ms)")

        except Exception as exc:
            errors += 1
            print(f"  [{idx}/{len(todo)}] {qid}: ERROR {exc}")
            # Reconnect after errors to reset connection state
            try:
                conn.rollback()
            except Exception:
                pass
            try:
                cur.close()
                conn.close()
            except Exception:
                pass
            conn = psycopg2.connect(DSN)
            conn.autocommit = True
            cur = conn.cursor()

    cur.close()
    conn.close()

    elapsed = time.time() - t_start
    total = len(list(EXPLAINS_DIR.glob("*.json")))
    print(f"\nDone in {elapsed:.0f}s: {ok} new, {errors} errors, {total} total explains")


if __name__ == "__main__":
    main()
