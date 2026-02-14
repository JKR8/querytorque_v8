"""Benchmark gold examples with DIFFERENT (fewer scans) plans on Snowflake.

3x3 validation: 3 runs, discard 1st (warmup), average last 2.
Uses MEDIUM warehouse for consistent timing.
"""
import json
import os
import re
import time
import glob
from pathlib import Path
from urllib.parse import urlparse, unquote, parse_qs

import snowflake.connector

BASE = Path("/mnt/c/Users/jakc9/Documents/QueryTorque_V8")
DUCKDB_EXAMPLES = BASE / "packages" / "qt-sql" / "qt_sql" / "examples" / "duckdb"
PG_EXAMPLES = BASE / "packages" / "qt-sql" / "qt_sql" / "examples" / "postgres"

DSN = os.environ.get(
    "QT_SNOWFLAKE_DSN",
    "snowflake://jkdl:QGdg%2A%24WC%25O62xS71@CVRYJTF-AW47074"
    "/SNOWFLAKE_SAMPLE_DATA/TPCDS_SF10TCL?warehouse=COMPUTE_WH&role=ACCOUNTADMIN",
)

# Queries with DIFFERENT plans and FEWER scans/operators
TARGETS = [
    ("duckdb", "composite_decorrelate_union", "9→7 scans, 14→12 aggs"),
    ("duckdb", "deferred_window_aggregation", "4→3 scans"),
    ("duckdb", "multi_intersect_exists_cte", "25→19 scans, 33→31 aggs"),
    ("duckdb", "shared_dimension_multi_channel", "18→15 scans"),
    ("duckdb", "single_pass_aggregation", "1 fewer filter"),
    ("pg", "explicit_join_materialized", "15→11 scans, 73→65 depth"),
    ("pg", "inline_decorrelate_materialized", "5→3 scans"),
    ("pg", "shared_scan_decorrelate", "5→3 scans"),
    ("pg", "single_pass_aggregation", "24→21 scans, 32→8 aggs"),
    ("pg", "intersect_to_exists", "62→45 depth, 25→8 aggs"),
]

N_RUNS = 3
TIMEOUT_S = 300


def parse_dsn(dsn):
    parsed = urlparse(dsn)
    path_parts = parsed.path.strip("/").split("/")
    params = parse_qs(parsed.query)
    return {
        "account": parsed.hostname,
        "user": parsed.username,
        "password": unquote(parsed.password),
        "database": path_parts[0] if len(path_parts) > 0 else "",
        "schema": path_parts[1] if len(path_parts) > 1 else "PUBLIC",
        "warehouse": params.get("warehouse", ["COMPUTE_WH"])[0],
        "role": params.get("role", [""])[0],
    }


def get_connection():
    p = parse_dsn(DSN)
    return snowflake.connector.connect(
        account=p["account"], user=p["user"], password=p["password"],
        database=p["database"], schema=p["schema"],
        warehouse=p["warehouse"], role=p["role"],
    )


def adapt_for_snowflake(sql: str) -> str:
    """Convert DuckDB/PG SQL to Snowflake-compatible syntax."""
    lines = []
    for line in sql.splitlines():
        stripped = line.strip().upper()
        if stripped.startswith("PRAGMA"):
            continue
        if stripped.startswith("SET ") and "=" in stripped:
            continue
        lines.append(line)
    sql = "\n".join(lines).strip()
    sql = sql.rstrip(";").strip()

    # Fix INTERVAL syntax
    sql = re.sub(
        r"INTERVAL\s+(\d+)\s+(DAY|MONTH|YEAR|HOUR|MINUTE|SECOND)S?",
        r"INTERVAL '\1 \2'", sql, flags=re.IGNORECASE)
    sql = re.sub(
        r"INTERVAL\s+'(\d+)'\s+(DAY|MONTH|YEAR|HOUR|MINUTE|SECOND)S?",
        r"INTERVAL '\1 \2'", sql, flags=re.IGNORECASE)

    # Replace FILTER (WHERE ...) with CASE WHEN
    def replace_filter(match):
        agg_func = match.group(1)
        agg_arg = match.group(2)
        condition = match.group(3)
        if agg_arg == "*":
            return f"{agg_func}(CASE WHEN {condition} THEN 1 END)"
        return f"{agg_func}(CASE WHEN {condition} THEN {agg_arg} END)"

    sql = re.sub(
        r'(\w+)\(([^)]*?)\)\s+FILTER\s*\(\s*WHERE\s+(.*?)\)',
        replace_filter, sql, flags=re.IGNORECASE | re.DOTALL)

    # Remove MATERIALIZED keyword from CTEs
    sql = re.sub(r'\bAS\s+MATERIALIZED\s*\(', 'AS (', sql, flags=re.IGNORECASE)
    sql = re.sub(r'\bAS\s+NOT\s+MATERIALIZED\s*\(', 'AS (', sql, flags=re.IGNORECASE)

    # Remove PG hints
    sql = re.sub(r'/\*\+.*?\*/', '', sql, flags=re.DOTALL)

    return sql


def run_query_timed(con, sql, timeout_s=TIMEOUT_S):
    """Run a query and return elapsed ms."""
    adapted = adapt_for_snowflake(sql)
    try:
        cur = con.cursor()
        start = time.perf_counter()
        cur.execute(adapted)
        _ = cur.fetchall()
        elapsed = (time.perf_counter() - start) * 1000
        row_count = cur.rowcount
        cur.close()
        return elapsed, row_count, None
    except Exception as e:
        return None, None, str(e)[:200]


def load_example(engine, name):
    """Load a gold example JSON file."""
    if engine == "duckdb":
        path = DUCKDB_EXAMPLES / f"{name}.json"
    else:
        path = PG_EXAMPLES / f"{name}.json"

    with open(path) as f:
        return json.load(f)


def main():
    print("Connecting to Snowflake...")
    con = get_connection()

    # Disable result cache
    cur = con.cursor()
    cur.execute("ALTER SESSION SET USE_CACHED_RESULT = FALSE")
    cur.close()

    # Scale up to MEDIUM
    print("Scaling warehouse to MEDIUM...")
    cur = con.cursor()
    cur.execute("ALTER WAREHOUSE COMPUTE_WH SET WAREHOUSE_SIZE = 'MEDIUM'")
    cur.close()
    # Wait for resize
    time.sleep(5)
    cur = con.cursor()
    cur.execute("SELECT CURRENT_WAREHOUSE(), 1+1")
    cur.fetchall()
    cur.close()
    print("Warehouse MEDIUM ready.\n")

    results = []

    print(f"{'Name':<40} {'Engine':<6} {'Speedup':>8} {'Orig_ms':>10} {'Opt_ms':>10} {'Orig_rows':>10} {'Opt_rows':>10} {'Reason'}")
    print("-" * 120)

    try:
        for engine, name, reason in TARGETS:
            data = load_example(engine, name)
            orig_sql = data.get("original_sql", "")
            opt_sql = data.get("optimized_sql", "")

            if not orig_sql or not opt_sql:
                print(f"{name:<40} {engine:<6} SKIP (missing SQL)")
                continue

            # Run original N_RUNS times
            orig_times = []
            orig_rows = None
            for i in range(N_RUNS):
                t, rows, err = run_query_timed(con, orig_sql)
                if err:
                    print(f"{name:<40} {engine:<6} ORIG_ERROR: {err[:60]}")
                    break
                orig_times.append(t)
                orig_rows = rows

            if len(orig_times) < N_RUNS:
                results.append({"name": name, "engine": engine, "status": "ORIG_ERROR"})
                continue

            # Run optimized N_RUNS times
            opt_times = []
            opt_rows = None
            for i in range(N_RUNS):
                t, rows, err = run_query_timed(con, opt_sql)
                if err:
                    print(f"{name:<40} {engine:<6} OPT_ERROR: {err[:60]}")
                    break
                opt_times.append(t)
                opt_rows = rows

            if len(opt_times) < N_RUNS:
                results.append({"name": name, "engine": engine, "status": "OPT_ERROR",
                                "orig_ms": sum(orig_times[1:]) / (N_RUNS - 1)})
                continue

            # 3x3: discard warmup, average last 2
            orig_avg = sum(orig_times[1:]) / (N_RUNS - 1)
            opt_avg = sum(opt_times[1:]) / (N_RUNS - 1)
            speedup = orig_avg / opt_avg if opt_avg > 0 else 0

            # Row count check
            row_match = "OK" if orig_rows == opt_rows else f"MISMATCH({orig_rows}vs{opt_rows})"

            result = {
                "name": name, "engine": engine, "status": "OK",
                "speedup": round(speedup, 2),
                "orig_ms": round(orig_avg, 1), "opt_ms": round(opt_avg, 1),
                "orig_times": [round(t, 1) for t in orig_times],
                "opt_times": [round(t, 1) for t in opt_times],
                "orig_rows": orig_rows, "opt_rows": opt_rows,
                "row_match": row_match, "reason": reason,
            }
            results.append(result)

            verdict = "WIN" if speedup >= 1.5 else "IMPROVED" if speedup >= 1.05 else "NEUTRAL" if speedup >= 0.95 else "REGRESSION"
            print(f"{name:<40} {engine:<6} {speedup:>7.2f}x {orig_avg:>10.1f} {opt_avg:>10.1f} {orig_rows:>10} {opt_rows:>10} {reason} [{row_match}] {verdict}")

    finally:
        # Always scale back down
        print("\nScaling warehouse back to XSMALL...")
        cur = con.cursor()
        cur.execute("ALTER WAREHOUSE COMPUTE_WH SET WAREHOUSE_SIZE = 'XSMALL'")
        cur.close()
        print("Warehouse restored to XSMALL.")
        con.close()

    # Summary
    print(f"\n{'=' * 90}")
    print("SUMMARY")
    print("=" * 90)

    valid = [r for r in results if r["status"] == "OK"]
    valid.sort(key=lambda r: r["speedup"], reverse=True)

    wins = [r for r in valid if r["speedup"] >= 1.5]
    improved = [r for r in valid if 1.05 <= r["speedup"] < 1.5]
    neutral = [r for r in valid if 0.95 <= r["speedup"] < 1.05]
    regression = [r for r in valid if r["speedup"] < 0.95]

    print(f"  WIN (>=1.5x): {len(wins)}")
    for r in wins:
        print(f"    {r['name']} ({r['engine']}): {r['speedup']:.2f}x")
    print(f"  IMPROVED (>=1.05x): {len(improved)}")
    for r in improved:
        print(f"    {r['name']} ({r['engine']}): {r['speedup']:.2f}x")
    print(f"  NEUTRAL: {len(neutral)}")
    print(f"  REGRESSION: {len(regression)}")
    for r in regression:
        print(f"    {r['name']} ({r['engine']}): {r['speedup']:.2f}x")

    errors = [r for r in results if r["status"] != "OK"]
    if errors:
        print(f"  ERRORS: {len(errors)}")
        for r in errors:
            print(f"    {r['name']} ({r['engine']}): {r['status']}")

    # Save
    output = str(BASE / "research" / "snowflake" / "gold_benchmark_results.json")
    with open(output, "w") as f:
        json.dump(results, f, indent=2)
    print(f"\nResults saved to {output}")


if __name__ == "__main__":
    main()
