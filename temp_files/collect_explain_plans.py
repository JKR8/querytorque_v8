#!/usr/bin/env python3
"""Collect EXPLAIN plans for all 99 TPC-DS queries."""

import duckdb
import os
import json
from pathlib import Path

# Paths
DB_PATH = "/mnt/d/TPC-DS/tpcds_sf100.duckdb"
QUERY_DIR = "/mnt/d/TPC-DS/queries_sf100_duckdb"
OUTPUT_DIR = "/mnt/c/Users/jakc9/Documents/QueryTorque_V8/temp_files/explain_plans"

def main():
    # Create output directory
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    # Connect to DuckDB (read-only to be safe)
    print(f"Connecting to DuckDB: {DB_PATH}")
    con = duckdb.connect(DB_PATH, read_only=True)

    results = {}
    errors = []

    # Process each query
    for i in range(1, 100):
        query_file = Path(QUERY_DIR) / f"query_{i}.sql"

        if not query_file.exists():
            print(f"  [SKIP] query_{i}.sql not found")
            continue

        print(f"Processing query_{i}.sql...", end=" ")

        try:
            # Read the query
            sql = query_file.read_text()

            # Get EXPLAIN plan
            explain_sql = f"EXPLAIN {sql}"
            result = con.execute(explain_sql).fetchall()
            plan = "\n".join(row[0] for row in result)

            # Get EXPLAIN ANALYZE (estimated costs)
            explain_analyze_sql = f"EXPLAIN ANALYZE {sql}"
            try:
                analyze_result = con.execute(explain_analyze_sql).fetchall()
                analyze_plan = "\n".join(row[0] for row in analyze_result)
            except Exception as e:
                analyze_plan = f"ERROR: {str(e)}"

            # Save individual plan
            plan_file = Path(OUTPUT_DIR) / f"query_{i}_explain.txt"
            plan_file.write_text(f"=== EXPLAIN ===\n{plan}\n\n=== EXPLAIN ANALYZE ===\n{analyze_plan}")

            results[f"query_{i}"] = {
                "explain": plan,
                "explain_analyze": analyze_plan if not analyze_plan.startswith("ERROR") else None,
                "status": "success"
            }
            print("[OK]")

        except Exception as e:
            error_msg = str(e)
            print(f"[ERROR] {error_msg[:50]}...")
            errors.append({"query": i, "error": error_msg})
            results[f"query_{i}"] = {
                "explain": None,
                "explain_analyze": None,
                "status": "error",
                "error": error_msg
            }

    con.close()

    # Save summary JSON
    summary_file = Path(OUTPUT_DIR) / "all_plans_summary.json"
    with open(summary_file, "w") as f:
        json.dump(results, f, indent=2)

    # Print summary
    success_count = sum(1 for r in results.values() if r["status"] == "success")
    print(f"\n{'='*50}")
    print(f"Completed: {success_count}/99 queries successful")
    print(f"Output directory: {OUTPUT_DIR}")

    if errors:
        print(f"\nErrors ({len(errors)}):")
        for err in errors:
            print(f"  - Query {err['query']}: {err['error'][:80]}...")

if __name__ == "__main__":
    main()
