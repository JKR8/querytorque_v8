"""Run EXPLAIN on all gold examples (DuckDB + PG) against Snowflake.

Compares plan structure between original and optimized SQL.
Reports: SAME, DIFFERENT (with details), ERROR.
"""
import json
import os
import re
import sys
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
        r"INTERVAL '\1 \2'",
        sql, flags=re.IGNORECASE,
    )
    sql = re.sub(
        r"INTERVAL\s+'(\d+)'\s+(DAY|MONTH|YEAR|HOUR|MINUTE|SECOND)S?",
        r"INTERVAL '\1 \2'",
        sql, flags=re.IGNORECASE,
    )

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
        replace_filter, sql, flags=re.IGNORECASE | re.DOTALL,
    )

    # Remove MATERIALIZED keyword from CTEs (PG-specific)
    sql = re.sub(r'\bAS\s+MATERIALIZED\s*\(', 'AS (', sql, flags=re.IGNORECASE)
    sql = re.sub(r'\bAS\s+NOT\s+MATERIALIZED\s*\(', 'AS (', sql, flags=re.IGNORECASE)

    # Remove PG hints (/*+ ... */)
    sql = re.sub(r'/\*\+.*?\*/', '', sql, flags=re.DOTALL)

    return sql


def run_explain(con, sql: str):
    """Run EXPLAIN on a query, return parsed plan info."""
    adapted = adapt_for_snowflake(sql)
    if not adapted.strip():
        return None, "EMPTY_SQL"

    try:
        cur = con.cursor()
        cur.execute(f"EXPLAIN {adapted}")
        rows = cur.fetchall()
        cur.close()
    except Exception as e:
        return None, str(e)[:200]

    # Parse plan
    plan_lines = []
    global_stats = None
    operators = {}
    table_scans = 0
    total_partitions = 0
    scanned_partitions = 0
    total_bytes = 0

    for row in rows:
        if len(row) >= 7:
            step, id_, parent, op, *rest = row[:7]
            if op == "GlobalStats":
                global_stats = rest[:3] if len(rest) >= 3 else rest
                if len(rest) >= 3:
                    total_partitions = rest[0] or 0
                    scanned_partitions = rest[1] or 0
                    total_bytes = rest[2] or 0
            else:
                plan_lines.append((step, id_, parent, op))
                operators[op] = operators.get(op, 0) + 1
                if op == "TableScan":
                    table_scans += 1

    return {
        "global_stats": global_stats,
        "total_partitions": total_partitions,
        "scanned_partitions": scanned_partitions,
        "total_bytes": total_bytes,
        "table_scans": table_scans,
        "operators": operators,
        "plan_depth": len(plan_lines),
    }, None


def compare_plans(orig_plan, opt_plan):
    """Compare two EXPLAIN plans, return verdict + details."""
    if orig_plan is None or opt_plan is None:
        return "ERROR", "missing plan"

    diffs = []

    # Compare partitions
    orig_parts = orig_plan["scanned_partitions"]
    opt_parts = opt_plan["scanned_partitions"]
    if orig_parts != opt_parts:
        pct = ((orig_parts - opt_parts) / orig_parts * 100) if orig_parts > 0 else 0
        diffs.append(f"partitions: {orig_parts}→{opt_parts} ({pct:+.1f}%)")

    # Compare bytes
    orig_bytes = orig_plan["total_bytes"]
    opt_bytes = opt_plan["total_bytes"]
    if orig_bytes != opt_bytes:
        pct = ((orig_bytes - opt_bytes) / orig_bytes * 100) if orig_bytes > 0 else 0
        diffs.append(f"bytes: {orig_bytes:,}→{opt_bytes:,} ({pct:+.1f}%)")

    # Compare table scans
    if orig_plan["table_scans"] != opt_plan["table_scans"]:
        diffs.append(f"scans: {orig_plan['table_scans']}→{opt_plan['table_scans']}")

    # Compare plan depth
    if orig_plan["plan_depth"] != opt_plan["plan_depth"]:
        diffs.append(f"depth: {orig_plan['plan_depth']}→{opt_plan['plan_depth']}")

    # Compare operator counts
    all_ops = set(list(orig_plan["operators"].keys()) + list(opt_plan["operators"].keys()))
    op_diffs = []
    for op in sorted(all_ops):
        oc = orig_plan["operators"].get(op, 0)
        nc = opt_plan["operators"].get(op, 0)
        if oc != nc:
            op_diffs.append(f"{op}:{oc}→{nc}")
    if op_diffs:
        diffs.append(f"ops: {', '.join(op_diffs[:5])}")

    if not diffs:
        return "SAME", ""
    return "DIFFERENT", "; ".join(diffs)


def main():
    print("Connecting to Snowflake...")
    con = get_connection()

    # Disable result cache
    cur = con.cursor()
    cur.execute("ALTER SESSION SET USE_CACHED_RESULT = FALSE")
    cur.close()
    print("Connected. Result cache disabled.\n")

    results = []

    # Process DuckDB examples
    print("=" * 90)
    print("DUCKDB GOLD EXAMPLES")
    print("=" * 90)

    duckdb_files = sorted(glob.glob(str(DUCKDB_EXAMPLES / "*.json")))
    for fpath in duckdb_files:
        name = os.path.basename(fpath).replace(".json", "")
        with open(fpath) as f:
            data = json.load(f)

        orig_sql = data.get("original_sql", "")
        opt_sql = data.get("optimized_sql", "")

        if not orig_sql or not opt_sql:
            print(f"  {name:<40} SKIP (missing SQL)")
            continue

        orig_plan, orig_err = run_explain(con, orig_sql)
        opt_plan, opt_err = run_explain(con, opt_sql)

        if orig_err:
            print(f"  {name:<40} ORIG_ERROR: {orig_err[:80]}")
            results.append({"name": name, "engine": "duckdb", "verdict": "ORIG_ERROR", "error": orig_err})
            continue
        if opt_err:
            print(f"  {name:<40} OPT_ERROR: {opt_err[:80]}")
            results.append({"name": name, "engine": "duckdb", "verdict": "OPT_ERROR", "error": opt_err})
            continue

        verdict, details = compare_plans(orig_plan, opt_plan)

        speedup = data.get("speedup", "?")
        parts_info = f"parts:{orig_plan['scanned_partitions']}→{opt_plan['scanned_partitions']}" if orig_plan and opt_plan else ""
        print(f"  {name:<40} {verdict:<10} {parts_info:<25} {details[:60]}")

        results.append({
            "name": name, "engine": "duckdb", "verdict": verdict,
            "details": details, "duckdb_speedup": speedup,
            "orig_partitions": orig_plan["scanned_partitions"],
            "opt_partitions": opt_plan["scanned_partitions"],
            "orig_bytes": orig_plan["total_bytes"],
            "opt_bytes": opt_plan["total_bytes"],
        })

    # Process PG examples
    print(f"\n{'=' * 90}")
    print("POSTGRESQL GOLD EXAMPLES")
    print("=" * 90)

    pg_files = sorted(glob.glob(str(PG_EXAMPLES / "*.json")))
    for fpath in pg_files:
        name = os.path.basename(fpath).replace(".json", "")
        with open(fpath) as f:
            data = json.load(f)

        orig_sql = data.get("original_sql", "")
        opt_sql = data.get("optimized_sql", "")

        if not orig_sql or not opt_sql:
            print(f"  {name:<40} SKIP (missing SQL)")
            continue

        orig_plan, orig_err = run_explain(con, orig_sql)
        opt_plan, opt_err = run_explain(con, opt_sql)

        if orig_err:
            print(f"  {name:<40} ORIG_ERROR: {orig_err[:80]}")
            results.append({"name": name, "engine": "pg", "verdict": "ORIG_ERROR", "error": orig_err})
            continue
        if opt_err:
            print(f"  {name:<40} OPT_ERROR: {opt_err[:80]}")
            results.append({"name": name, "engine": "pg", "verdict": "OPT_ERROR", "error": opt_err})
            continue

        verdict, details = compare_plans(orig_plan, opt_plan)

        speedup = data.get("speedup", "?")
        parts_info = f"parts:{orig_plan['scanned_partitions']}→{opt_plan['scanned_partitions']}" if orig_plan and opt_plan else ""
        print(f"  {name:<40} {verdict:<10} {parts_info:<25} {details[:60]}")

        results.append({
            "name": name, "engine": "pg", "verdict": verdict,
            "details": details, "pg_speedup": speedup,
            "orig_partitions": orig_plan["scanned_partitions"],
            "opt_partitions": opt_plan["scanned_partitions"],
            "orig_bytes": orig_plan["total_bytes"],
            "opt_bytes": opt_plan["total_bytes"],
        })

    con.close()

    # Summary
    print(f"\n{'=' * 90}")
    print("SUMMARY")
    print("=" * 90)

    verdicts = {}
    for r in results:
        v = r["verdict"]
        verdicts[v] = verdicts.get(v, 0) + 1

    for v in ["SAME", "DIFFERENT", "ORIG_ERROR", "OPT_ERROR", "ERROR"]:
        if v in verdicts:
            print(f"  {v}: {verdicts[v]}")

    # Show DIFFERENT details
    diff_results = [r for r in results if r["verdict"] == "DIFFERENT"]
    if diff_results:
        print(f"\nDIFFERENT plans (potential Snowflake opportunities):")
        for r in diff_results:
            engine = r["engine"]
            name = r["name"]
            det = r["details"]
            print(f"  [{engine}] {name}: {det}")

    # Save
    output = str(BASE / "research" / "snowflake" / "gold_example_explains.json")
    with open(output, "w") as f:
        json.dump(results, f, indent=2)
    print(f"\nResults saved to {output}")


if __name__ == "__main__":
    main()
