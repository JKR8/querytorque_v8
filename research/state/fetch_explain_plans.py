#!/usr/bin/env python3
"""
Fetch EXPLAIN ANALYZE for all 99 TPC-DS queries on SF10.

For each query:
- If State 1 (WIN/IMPROVED): EXPLAIN ANALYZE on the best optimized SQL
- If State 0 (NEUTRAL/baseline): EXPLAIN ANALYZE on the original SQL

Output structure:
  research/state/explain_plans/qN_explain.txt    - EXPLAIN ANALYZE output
  research/state/queries/qN_current.sql          - Current state SQL (original or optimized)
"""

import duckdb
import yaml
import sys
import os
from pathlib import Path

PROJECT = Path("/mnt/c/Users/jakc9/Documents/QueryTorque_V8")
DB_PATH = "/mnt/d/TPC-DS/tpcds_sf10.duckdb"
BASELINE_DIR = Path("/mnt/d/TPC-DS/queries_duckdb_converted")
STATE_DIR = PROJECT / "research" / "state_histories_all_99"
OUTPUT_EXPLAIN = PROJECT / "research" / "state" / "explain_plans"
OUTPUT_QUERIES = PROJECT / "research" / "state" / "queries"

# Where optimized SQL lives, searched in priority order
OPTIMIZED_SEARCH_PATHS = [
    PROJECT / "retry_neutrals",       # retry_neutrals/qN/wN_optimized.sql
    PROJECT / "retry_collect",         # retry_collect/qN/wN_optimized.sql
    PROJECT / "research" / "CONSOLIDATED_BENCHMARKS" / "kimi_q1-q30_optimization",  # qN/output_optimized.sql
    PROJECT / "research" / "CONSOLIDATED_BENCHMARKS" / "kimi_q31-q99_optimization",  # qN/output_optimized.sql
]


def load_state(query_num: int) -> dict:
    """Load state history for a query, return best worker info"""
    yaml_path = STATE_DIR / f"q{query_num}_state_history.yaml"
    if not yaml_path.exists():
        return {"best_speedup": 1.0, "best_worker": None, "states": []}

    with open(yaml_path) as f:
        data = yaml.safe_load(f)

    return data


def find_optimized_sql(query_num: int, best_worker: str) -> Path:
    """Find the optimized SQL file for a query's best worker"""

    # Parse worker info from state_id like "retry3w_2" or "W2"
    worker_num = None
    if best_worker:
        for char in best_worker:
            if char.isdigit():
                worker_num = char
                break

    # Search retry_neutrals and retry_collect first (most recent)
    for base in [PROJECT / "retry_neutrals", PROJECT / "retry_collect"]:
        query_dir = base / f"q{query_num}"
        if query_dir.exists() and worker_num:
            worker_file = query_dir / f"w{worker_num}_optimized.sql"
            if worker_file.exists():
                return worker_file
            # Try all workers, pick any
            for wf in sorted(query_dir.glob("w*_optimized.sql")):
                return wf

    # Search kimi results
    for kimi_base in [
        PROJECT / "research" / "CONSOLIDATED_BENCHMARKS" / "kimi_q1-q30_optimization",
        PROJECT / "research" / "CONSOLIDATED_BENCHMARKS" / "kimi_q31-q99_optimization",
    ]:
        optimized = kimi_base / f"q{query_num}" / "output_optimized.sql"
        if optimized.exists():
            return optimized
        benchmark = kimi_base / "benchmark_ready" / f"q{query_num}_optimized.sql"
        if benchmark.exists():
            return benchmark

    return None


def clean_sql(sql_text: str) -> str:
    """Strip comments and trailing semicolons for EXPLAIN ANALYZE"""
    lines = []
    for line in sql_text.split('\n'):
        stripped = line.strip()
        if stripped.startswith('--'):
            continue
        lines.append(line)
    clean = '\n'.join(lines).strip()
    # Remove trailing semicolons
    while clean.endswith(';'):
        clean = clean[:-1].strip()
    return clean


def run_explain(con, sql: str, query_num: int) -> str:
    """Run EXPLAIN ANALYZE and return the output"""
    try:
        result = con.execute(f"EXPLAIN ANALYZE {sql}").fetchall()
        if not result:
            return "No output"
        # DuckDB EXPLAIN ANALYZE may return multiple rows/columns
        # Collect all text content
        lines = []
        for row in result:
            for col in row:
                if col is not None:
                    lines.append(str(col))
        return '\n'.join(lines) if lines else "No output"
    except Exception as e:
        return f"ERROR: {str(e)}"


def main():
    # Create output directories
    OUTPUT_EXPLAIN.mkdir(parents=True, exist_ok=True)
    OUTPUT_QUERIES.mkdir(parents=True, exist_ok=True)

    # Connect to SF10 database (read-only)
    print(f"Connecting to {DB_PATH}...", file=sys.stderr)
    con = duckdb.connect(DB_PATH, read_only=True)

    total = 99
    wins = 0
    baselines = 0
    errors = 0

    for q in range(1, total + 1):
        state = load_state(q)
        best_speedup = state.get('best_speedup', 1.0)
        best_worker = None

        # Find best worker from states
        for s in state.get('states', []):
            if s.get('speedup', 1.0) == best_speedup and s.get('state_id') != 'baseline':
                best_worker = s.get('state_id')

        # Determine which SQL to use
        use_optimized = best_speedup >= 1.1 and best_worker
        sql_path = None
        state_label = "baseline"

        if use_optimized:
            sql_path = find_optimized_sql(q, best_worker)
            if sql_path:
                state_label = f"optimized ({best_worker}, {best_speedup:.2f}x)"
                wins += 1
            else:
                # Couldn't find optimized file, fall back to baseline
                use_optimized = False

        if not use_optimized:
            sql_path = BASELINE_DIR / f"query_{q}.sql"
            state_label = "baseline"
            baselines += 1

        if not sql_path or not sql_path.exists():
            print(f"Q{q}: SKIP - no SQL file found", file=sys.stderr)
            errors += 1
            continue

        # Read and clean SQL
        sql_text = sql_path.read_text()
        clean = clean_sql(sql_text)

        # Save current state SQL
        query_output = OUTPUT_QUERIES / f"q{q}_current.sql"
        with open(query_output, 'w') as f:
            f.write(f"-- Q{q} current state: {state_label}\n")
            f.write(f"-- Source: {sql_path}\n")
            f.write(f"-- Best speedup: {best_speedup:.2f}x\n\n")
            f.write(sql_text)

        # Run EXPLAIN ANALYZE
        print(f"Q{q}: {state_label}...", file=sys.stderr, end=" ")
        explain_output = run_explain(con, clean, q)

        # Save explain plan
        explain_file = OUTPUT_EXPLAIN / f"q{q}_explain.txt"
        with open(explain_file, 'w') as f:
            f.write(f"-- Q{q} EXPLAIN ANALYZE (SF10)\n")
            f.write(f"-- State: {state_label}\n")
            f.write(f"-- Best speedup: {best_speedup:.2f}x\n")
            f.write(f"-- Source SQL: {sql_path}\n\n")
            f.write(explain_output)

        if explain_output.startswith("ERROR"):
            print(f"ERROR", file=sys.stderr)
            errors += 1
        else:
            print(f"OK", file=sys.stderr)

    con.close()

    print(f"\nDone: {wins} optimized, {baselines} baseline, {errors} errors", file=sys.stderr)
    print(f"Explain plans: {OUTPUT_EXPLAIN}", file=sys.stderr)
    print(f"Current SQL:   {OUTPUT_QUERIES}", file=sys.stderr)


if __name__ == "__main__":
    main()
