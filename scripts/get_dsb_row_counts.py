#!/usr/bin/env python3
"""Query PG14 SF10 DSB benchmark and get row counts for all 156 queries.

Output format: JSON with query_id and row_count (rbot-compatible format).

Usage (from project root):
    PYTHONPATH=packages/qt-shared:packages/qt-sql:. python3 get_dsb_row_counts.py
"""

import json
import sys
import time
from datetime import datetime
from pathlib import Path

import psycopg2
from psycopg2.extras import RealDictCursor

# ── Configuration ─────────────────────────────────────────────────────────
BENCHMARK_DIR = "packages/qt-sql/qt_sql/benchmarks/postgres_dsb"
QUERIES_DIR = Path(BENCHMARK_DIR) / "queries"

# PG14 SF10 connection
PG_DSN = "dbname=dsb_sf10 user=jakc9 password=jakc9 host=127.0.0.1 port=5434"
TIMEOUT_S = 300


def get_pg_connection():
    """Create PostgreSQL connection."""
    conn = psycopg2.connect(PG_DSN)
    conn.set_session(autocommit=True)
    return conn


def discover_queries() -> list[str]:
    """Auto-discover all 156 DSB queries, sorted by name."""
    sql_files = sorted(QUERIES_DIR.glob("*.sql"))
    return [f.stem for f in sql_files]


def get_row_count(conn, sql: str, query_id: str, timeout_s: int = TIMEOUT_S) -> dict:
    """Execute query and return row count.

    Returns dict with:
      - query_id: query identifier
      - row_count: number of rows (or -1 on error/timeout)
      - status: SUCCESS, TIMEOUT, ERROR
      - error_msg: error message if failed
      - elapsed_s: execution time in seconds
    """
    result = {
        "query_id": query_id,
        "row_count": -1,
        "status": "UNKNOWN",
        "error_msg": None,
        "elapsed_s": 0.0,
    }

    t_start = time.time()

    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            # Set statement timeout
            cur.execute(f"SET statement_timeout TO {int(timeout_s * 1000)};")

            # Execute query
            cur.execute(sql)

            # Fetch all rows and count
            rows = cur.fetchall()
            row_count = len(rows)

            result.update({
                "row_count": row_count,
                "status": "SUCCESS",
                "elapsed_s": round(time.time() - t_start, 2),
            })

    except psycopg2.errors.QueryCanceled:
        result.update({
            "status": "TIMEOUT",
            "error_msg": f"Query cancelled after {timeout_s}s",
            "elapsed_s": round(time.time() - t_start, 2),
        })
    except psycopg2.Error as e:
        result.update({
            "status": "ERROR",
            "error_msg": str(e),
            "elapsed_s": round(time.time() - t_start, 2),
        })
    except Exception as e:
        result.update({
            "status": "ERROR",
            "error_msg": f"Unexpected: {str(e)}",
            "elapsed_s": round(time.time() - t_start, 2),
        })

    return result


def main():
    """Main execution."""
    # Discover queries
    query_ids = discover_queries()
    n_total = len(query_ids)

    print(f"\n{'#'*70}")
    print(f"  DSB Row Count Collection — PG14 SF10")
    print(f"  Queries: {n_total}")
    print(f"  DSN: {PG_DSN}")
    print(f"  Timeout: {TIMEOUT_S}s")
    print(f"{'#'*70}\n")

    # Connect
    try:
        conn = get_pg_connection()
        print(f"  Connected to PostgreSQL\n")
    except Exception as e:
        print(f"  FATAL: Failed to connect — {e}")
        sys.exit(1)

    results = []
    t_batch = time.time()

    for i, query_id in enumerate(query_ids, 1):
        sql_path = QUERIES_DIR / f"{query_id}.sql"

        if not sql_path.exists():
            print(f"  [{i:3d}/{n_total}] {query_id:<25} SKIP (file not found)")
            results.append({
                "query_id": query_id,
                "row_count": -1,
                "status": "SKIP",
                "error_msg": "Query file not found",
            })
            continue

        sql = sql_path.read_text()
        result = get_row_count(conn, sql, query_id)
        results.append(result)

        # Status indicator
        status_fmt = "✓" if result["status"] == "SUCCESS" else "✗"
        row_str = f"{result['row_count']:>8}" if result["row_count"] >= 0 else "ERROR"

        print(f"  [{i:3d}/{n_total}] {query_id:<25} {result['status']:<10} {row_str:>8} "
              f"({result['elapsed_s']:>6.2f}s)")

    conn.close()

    # Summary
    total_elapsed = time.time() - t_batch
    successes = [r for r in results if r["status"] == "SUCCESS"]
    timeouts = [r for r in results if r["status"] == "TIMEOUT"]
    errors = [r for r in results if r["status"] == "ERROR"]
    skips = [r for r in results if r["status"] == "SKIP"]

    print(f"\n{'='*70}")
    print(f"  Summary")
    print(f"{'='*70}")
    print(f"  Total:    {len(results)}")
    print(f"  Success:  {len(successes)} ({100*len(successes)//len(results)}%)")
    print(f"  Timeout:  {len(timeouts)}")
    print(f"  Error:    {len(errors)}")
    print(f"  Skip:     {len(skips)}")
    print(f"  Elapsed:  {total_elapsed:.1f}s\n")

    # Save results
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    out_dir = Path(BENCHMARK_DIR) / "row_counts"
    out_dir.mkdir(parents=True, exist_ok=True)

    output = {
        "benchmark": "dsb",
        "engine": "postgresql",
        "scale_factor": 10,
        "pg_version": "14.3",
        "timestamp": datetime.now().isoformat(),
        "total_elapsed_s": round(total_elapsed, 1),
        "summary": {
            "total": len(results),
            "success": len(successes),
            "timeout": len(timeouts),
            "error": len(errors),
            "skip": len(skips),
        },
        "queries": results,
    }

    # Save full results
    results_path = out_dir / f"row_counts_{timestamp}.json"
    results_path.write_text(json.dumps(output, indent=2))
    print(f"  Full results: {results_path}")

    # Save compact format (query_id, row_count only)
    compact_path = out_dir / f"row_counts_{timestamp}_compact.csv"
    with open(compact_path, "w") as f:
        f.write("query_id,row_count,status\n")
        for r in results:
            rc = r.get("row_count", -1)
            st = r.get("status", "UNKNOWN")
            f.write(f"{r['query_id']},{rc},{st}\n")
    print(f"  Compact CSV: {compact_path}")

    # Latest symlink
    latest_path = out_dir / "row_counts_latest.json"
    latest_path.unlink(missing_ok=True)
    latest_path.symlink_to(results_path.name)
    print(f"  Latest link: {latest_path}\n")


if __name__ == "__main__":
    main()
