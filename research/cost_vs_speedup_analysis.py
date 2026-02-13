#!/usr/bin/env python3
"""
Cost vs Speedup Analysis
========================
Compares PostgreSQL EXPLAIN cost estimates against actual runtime speedups
from the V2 DSB-76 swarm archive.

Outputs: research/cost_vs_speedup.csv
"""

import csv
import json
import math
import os
import sys
from pathlib import Path

import psycopg2

# -- Paths -----------------------------------------------------------------
ARCHIVE = Path(
    "/mnt/c/Users/jakc9/Documents/QueryTorque_V8/packages/qt-sql/qt_sql/"
    "benchmarks/postgres_dsb_76/swarm_sessions_v2_20260213_archive"
)
QUERIES = Path(
    "/mnt/c/Users/jakc9/Documents/QueryTorque_V8/packages/qt-sql/qt_sql/"
    "benchmarks/postgres_dsb_76/queries"
)
OUTPUT = Path(
    "/mnt/c/Users/jakc9/Documents/QueryTorque_V8/research/cost_vs_speedup.csv"
)

DSN = "postgres://jakc9:jakc9@127.0.0.1:5434/dsb_sf10"
STATEMENT_TIMEOUT_MS = 10_000


def get_total_cost(conn, sql: str) -> float | None:
    """Run EXPLAIN (FORMAT JSON) and return the top-level Total Cost."""
    try:
        with conn.cursor() as cur:
            cur.execute(f"SET LOCAL statement_timeout = '{STATEMENT_TIMEOUT_MS}'")
            cur.execute(f"EXPLAIN (FORMAT JSON) {sql}")
            plan_json = cur.fetchone()[0]
            # psycopg2 returns the JSON already parsed as a Python list
            return plan_json[0]["Plan"]["Total Cost"]
    except Exception as e:
        conn.rollback()
        print(f"    EXPLAIN error: {e}")
        return None


def main():
    conn = psycopg2.connect(DSN)
    conn.autocommit = False  # we need transactions for SET LOCAL

    rows = []
    skipped = []

    # Cache original costs so we don't re-EXPLAIN the same query per worker
    original_cost_cache: dict[str, float | None] = {}

    query_dirs = sorted(d for d in ARCHIVE.iterdir() if d.is_dir())
    print(f"Found {len(query_dirs)} query directories in archive")

    for qdir in query_dirs:
        query_id = qdir.name  # e.g. "query001_multi_i1"
        query_file = QUERIES / f"{query_id}.sql"
        if not query_file.exists():
            print(f"  SKIP {query_id}: original SQL not found at {query_file}")
            skipped.append((query_id, "no_original_sql"))
            continue

        original_sql = query_file.read_text().strip()

        # Get original cost (cached)
        if query_id not in original_cost_cache:
            original_cost_cache[query_id] = get_total_cost(conn, original_sql)
            if original_cost_cache[query_id] is None:
                print(f"  WARN {query_id}: EXPLAIN failed on original SQL")

        orig_cost = original_cost_cache[query_id]

        fan_out = qdir / "iteration_00_fan_out"
        if not fan_out.exists():
            print(f"  SKIP {query_id}: no iteration_00_fan_out")
            skipped.append((query_id, "no_fan_out"))
            continue

        for wid in range(1, 5):
            result_file = fan_out / f"worker_{wid:02d}" / "result.json"
            if not result_file.exists():
                continue

            try:
                result = json.loads(result_file.read_text())
            except json.JSONDecodeError:
                print(f"  WARN {query_id}/W{wid}: bad JSON in result.json")
                continue

            status = result.get("status", "")
            if status not in ("WIN", "IMPROVED"):
                continue

            optimized_sql = result.get("optimized_sql", "").strip()
            if not optimized_sql:
                print(f"  WARN {query_id}/W{wid}: empty optimized_sql")
                continue

            actual_speedup = result.get("speedup")
            if actual_speedup is None:
                continue

            # Strip any SET LOCAL lines that might be prepended to the SQL
            sql_lines = optimized_sql.split("\n")
            clean_lines = []
            for line in sql_lines:
                stripped = line.strip().upper()
                if stripped.startswith("SET LOCAL") or stripped.startswith("SET "):
                    continue
                clean_lines.append(line)
            clean_sql = "\n".join(clean_lines).strip()

            opt_cost = get_total_cost(conn, clean_sql)
            if opt_cost is None:
                print(f"  WARN {query_id}/W{wid}: EXPLAIN failed on optimized SQL")
                skipped.append((query_id, f"worker_{wid}_explain_fail"))
                continue

            if orig_cost is None:
                print(f"  SKIP {query_id}/W{wid}: no original cost")
                continue

            cost_ratio = orig_cost / opt_cost if opt_cost > 0 else float("inf")
            cost_would_predict_win = opt_cost < orig_cost

            rows.append({
                "query_id": query_id,
                "worker_id": wid,
                "original_cost": round(orig_cost, 2),
                "optimized_cost": round(opt_cost, 2),
                "cost_ratio": round(cost_ratio, 4),
                "actual_speedup": round(actual_speedup, 4),
                "status": status,
                "cost_would_predict_win": cost_would_predict_win,
            })

            print(
                f"  {query_id}/W{wid}: cost_ratio={cost_ratio:.2f}x  "
                f"actual={actual_speedup:.2f}x  predict={'YES' if cost_would_predict_win else 'NO'}  "
                f"[{status}]"
            )

    conn.close()

    # -- Write CSV -------------------------------------------------------------
    OUTPUT.parent.mkdir(parents=True, exist_ok=True)
    fieldnames = [
        "query_id", "worker_id", "original_cost", "optimized_cost",
        "cost_ratio", "actual_speedup", "status", "cost_would_predict_win",
    ]
    with open(OUTPUT, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)

    print(f"\n{'='*60}")
    print(f"Wrote {len(rows)} rows to {OUTPUT}")
    print(f"Skipped {len(skipped)} items")

    # -- Summary stats ---------------------------------------------------------
    if rows:
        correct = sum(1 for r in rows if r["cost_would_predict_win"])
        incorrect = len(rows) - correct
        print(f"\nCost-model accuracy on WIN/IMPROVED candidates:")
        print(f"  Correctly predicted improvement: {correct}/{len(rows)} ({100*correct/len(rows):.1f}%)")
        print(f"  Cost model said NO improvement:  {incorrect}/{len(rows)} ({100*incorrect/len(rows):.1f}%)")

        # Correlation
        xs = [r["cost_ratio"] for r in rows]
        ys = [r["actual_speedup"] for r in rows]
        n = len(xs)
        mx = sum(xs) / n
        my = sum(ys) / n
        cov = sum((x - mx) * (y - my) for x, y in zip(xs, ys)) / n
        sx = math.sqrt(sum((x - mx) ** 2 for x in xs) / n)
        sy = math.sqrt(sum((y - my) ** 2 for y in ys) / n)
        if sx > 0 and sy > 0:
            r_val = cov / (sx * sy)
            print(f"\n  Pearson r(cost_ratio, actual_speedup) = {r_val:.4f}")

        # Log-space correlation (more appropriate for ratios)
        lxs = [math.log(max(x, 0.001)) for x in xs]
        lys = [math.log(max(y, 0.001)) for y in ys]
        lmx = sum(lxs) / n
        lmy = sum(lys) / n
        lcov = sum((x - lmx) * (y - lmy) for x, y in zip(lxs, lys)) / n
        lsx = math.sqrt(sum((x - lmx) ** 2 for x in lxs) / n)
        lsy = math.sqrt(sum((y - lmy) ** 2 for y in lys) / n)
        if lsx > 0 and lsy > 0:
            lr = lcov / (lsx * lsy)
            print(f"  Pearson r(log cost_ratio, log actual_speedup) = {lr:.4f}")

        # Breakdown by status
        for st in ("WIN", "IMPROVED"):
            subset = [r for r in rows if r["status"] == st]
            if subset:
                c = sum(1 for r in subset if r["cost_would_predict_win"])
                print(f"\n  {st} ({len(subset)} rows): cost predicted {c}/{len(subset)} ({100*c/len(subset):.1f}%)")
                avg_cost_ratio = sum(r["cost_ratio"] for r in subset) / len(subset)
                avg_speedup = sum(r["actual_speedup"] for r in subset) / len(subset)
                print(f"    Avg cost_ratio: {avg_cost_ratio:.2f}x  Avg actual_speedup: {avg_speedup:.2f}x")


if __name__ == "__main__":
    main()
