"""Batch EXPLAIN comparison: all DuckDB winners on Snowflake.

Runs EXPLAIN (not execution) on original + optimized SQL for every
DuckDB TPC-DS winning query. Compares plan structure to find queries
where Snowflake produces a DIFFERENT plan for the rewrite.

Key signals:
  - Different partition counts (pruning opportunity)
  - Different number of TableScans (scan consolidation)
  - Different plan depth (simpler = potentially faster)
  - Structural differences (condAggr, WithClause, etc.)
"""

import csv
import json
import os
import re
import sys
import time
from pathlib import Path
from urllib.parse import urlparse, unquote

import snowflake.connector

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

DSN = os.environ.get(
    "QT_SNOWFLAKE_DSN",
    "snowflake://jkdl:QGdg%2A%24WC%25O62xS71@CVRYJTF-AW47074"
    "/SNOWFLAKE_SAMPLE_DATA/TPCDS_SF10TCL?warehouse=COMPUTE_WH&role=ACCOUNTADMIN",
)

BASE = Path("/mnt/c/Users/jakc9/Documents/QueryTorque_V8")
OPT_DIR = BASE / "research" / "ALL_OPTIMIZATIONS" / "duckdb_tpcds"
LEADERBOARD = BASE / "research" / "archive" / "benchmark_results" / "CONSOLIDATED_BENCHMARKS" / "DuckDB_TPC-DS_Leaderboard_v3_20260206.csv"
RESULTS_DIR = BASE / "research" / "snowflake" / "results"
RESULTS_DIR.mkdir(exist_ok=True)


def parse_dsn(dsn):
    parsed = urlparse(dsn)
    path_parts = parsed.path.strip("/").split("/")
    from urllib.parse import parse_qs
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


def strip_duckdb_syntax(sql: str) -> str:
    """Remove DuckDB-specific syntax to make SQL Snowflake-compatible."""
    lines = []
    for line in sql.splitlines():
        stripped = line.strip().upper()
        # Skip PRAGMA lines
        if stripped.startswith("PRAGMA"):
            continue
        # Skip SET statements (DuckDB-specific)
        if stripped.startswith("SET ") and "=" in stripped:
            continue
        lines.append(line)
    sql = "\n".join(lines).strip()

    # Remove trailing semicolons (Snowflake connector handles this)
    sql = sql.rstrip(";").strip()

    # Fix DuckDB interval syntax: INTERVAL 30 DAY → INTERVAL '30 DAY'
    # DuckDB:    INTERVAL 30 DAY  /  INTERVAL 14 DAY  /  INTERVAL '30' DAY
    # Snowflake: INTERVAL '30 DAY'
    # Pattern 1: INTERVAL N UNIT (no quotes)
    sql = re.sub(
        r"INTERVAL\s+(\d+)\s+(DAY|MONTH|YEAR|HOUR|MINUTE|SECOND)S?",
        r"INTERVAL '\1 \2'",
        sql,
        flags=re.IGNORECASE,
    )
    # Pattern 2: INTERVAL 'N' UNIT (DuckDB quoted number + separate unit)
    sql = re.sub(
        r"INTERVAL\s+'(\d+)'\s+(DAY|MONTH|YEAR|HOUR|MINUTE|SECOND)S?",
        r"INTERVAL '\1 \2'",
        sql,
        flags=re.IGNORECASE,
    )

    # Replace FILTER (WHERE ...) with CASE WHEN equivalent
    # Pattern: COUNT(*) FILTER (WHERE condition)
    # → COUNT(CASE WHEN condition THEN 1 END)
    def replace_filter(match):
        agg_func = match.group(1)  # e.g., COUNT, SUM, AVG
        agg_arg = match.group(2)   # e.g., *, column_name
        condition = match.group(3)  # e.g., ss_quantity BETWEEN 1 AND 20
        if agg_arg == "*":
            return f"{agg_func}(CASE WHEN {condition} THEN 1 END)"
        else:
            return f"{agg_func}(CASE WHEN {condition} THEN {agg_arg} END)"

    sql = re.sub(
        r'(\w+)\(([^)]*?)\)\s+FILTER\s*\(\s*WHERE\s+(.*?)\)',
        replace_filter,
        sql,
        flags=re.IGNORECASE | re.DOTALL,
    )

    return sql


def split_statements(sql: str) -> list[str]:
    """Split multi-statement SQL into individual statements.

    Returns list of non-empty SQL statements (without comments/blanks).
    """
    # Split on semicolons, filter empty
    parts = sql.split(";")
    stmts = []
    for p in parts:
        cleaned = strip_comments(p).strip()
        if cleaned and not cleaned.upper().startswith("--"):
            stmts.append(cleaned)
    return stmts


def strip_comments(sql: str) -> str:
    lines = [l for l in sql.splitlines() if not l.strip().startswith("--")]
    return "\n".join(lines).strip()


def find_optimized_sql(query_num: str, source: str, worker: str) -> str | None:
    """Find the optimized SQL file for a given query/source/worker combo."""
    qdir = OPT_DIR / query_num.lower()
    if not qdir.exists():
        return None

    # Map source to directory names
    candidates = []
    if source == "Kimi":
        candidates = ["kimi"]
    elif source == "Evo":
        candidates = ["evo"]
    elif source == "Retry3W":
        w = worker
        candidates = [f"retry_collect_w{w}", f"retry_sf10_winners_w{w}"]
    elif source == "Retry4W":
        w = worker
        candidates = [
            f"retry_sf10_winners_w{w}",
            f"retry_neutrals_w{w}",
        ]

    for cand in candidates:
        path = qdir / cand / "optimized.sql"
        if path.exists():
            return path.read_text()

    # Fallback: try all subdirectories
    for subdir in sorted(qdir.iterdir()):
        path = subdir / "optimized.sql"
        if path.exists():
            return path.read_text()

    return None


def extract_plan_metrics(explain_rows: list) -> dict:
    """Extract key metrics from EXPLAIN output rows."""
    metrics = {
        "total_operators": len(explain_rows),
        "table_scans": [],
        "total_partitions_scanned": 0,
        "total_partitions_total": 0,
        "fact_table_scans": 0,
        "has_condaggr": False,
        "has_union_all": False,
        "has_with_clause": False,
        "has_window_function": False,
        "join_count": 0,
        "aggregate_count": 0,
        "filter_count": 0,
    }

    fact_tables = {"STORE_SALES", "CATALOG_SALES", "WEB_SALES", "INVENTORY",
                   "STORE_RETURNS", "CATALOG_RETURNS", "WEB_RETURNS"}

    for row in explain_rows:
        row_str = " | ".join(str(v) for v in row if v is not None)

        if "TableScan" in row_str:
            # Extract table name and partition info
            parts = row_str.split("|")
            table_info = [p.strip() for p in parts if "TableScan" in p or "TPCDS_SF10TCL" in p]
            table_name = ""
            parts_scanned = 0
            parts_total = 0

            for p in parts:
                p = p.strip()
                if "TPCDS_SF10TCL." in p:
                    table_name = p.split("TPCDS_SF10TCL.")[-1].split()[0].strip()
                    # Remove alias if present (e.g., "STORE_SALES | SS |")
                    table_name = table_name.split("|")[0].strip()

            # Try to find partition counts (numeric columns)
            nums = re.findall(r'\b(\d{2,})\b', row_str)
            if len(nums) >= 2:
                # Usually: partitions_scanned | partitions_total | bytes
                parts_scanned = int(nums[0])
                parts_total = int(nums[1])

            scan_info = {
                "table": table_name,
                "partitions_scanned": parts_scanned,
                "partitions_total": parts_total,
            }
            metrics["table_scans"].append(scan_info)
            metrics["total_partitions_scanned"] += parts_scanned
            metrics["total_partitions_total"] += parts_total

            # Check if it's a fact table
            clean_name = table_name.upper().split("|")[0].strip()
            if any(ft in clean_name for ft in fact_tables):
                metrics["fact_table_scans"] += 1

        if "condAggr" in row_str:
            metrics["has_condaggr"] = True
        if "UnionAll" in row_str:
            metrics["has_union_all"] = True
        if "WithClause" in row_str or "WithReference" in row_str:
            metrics["has_with_clause"] = True
        if "WindowFunction" in row_str:
            metrics["has_window_function"] = True
        if "Join" in row_str and "JoinFilter" not in row_str:
            metrics["join_count"] += 1
        if "Aggregate" in row_str:
            metrics["aggregate_count"] += 1
        if "Filter" in row_str and "JoinFilter" not in row_str:
            metrics["filter_count"] += 1

    return metrics


def compare_plans(orig_metrics, opt_metrics) -> dict:
    """Compare two plans and identify differences."""
    diffs = {}

    # Fact table scan difference
    orig_fact = orig_metrics["fact_table_scans"]
    opt_fact = opt_metrics["fact_table_scans"]
    if orig_fact != opt_fact:
        diffs["fact_scans"] = f"{orig_fact} → {opt_fact}"

    # Total partitions scanned
    orig_parts = orig_metrics["total_partitions_scanned"]
    opt_parts = opt_metrics["total_partitions_scanned"]
    if orig_parts > 0 and opt_parts > 0 and orig_parts != opt_parts:
        ratio = opt_parts / orig_parts if orig_parts > 0 else 0
        diffs["partitions"] = f"{orig_parts} → {opt_parts} ({ratio:.2f}x)"

    # Operator count
    orig_ops = orig_metrics["total_operators"]
    opt_ops = opt_metrics["total_operators"]
    if abs(orig_ops - opt_ops) > 2:
        diffs["operators"] = f"{orig_ops} → {opt_ops}"

    # Join count
    if orig_metrics["join_count"] != opt_metrics["join_count"]:
        diffs["joins"] = f"{orig_metrics['join_count']} → {opt_metrics['join_count']}"

    # Structural changes
    if orig_metrics["has_condaggr"] != opt_metrics["has_condaggr"]:
        diffs["condaggr"] = f"{'Y' if orig_metrics['has_condaggr'] else 'N'} → {'Y' if opt_metrics['has_condaggr'] else 'N'}"
    if not orig_metrics["has_union_all"] and opt_metrics["has_union_all"]:
        diffs["union_added"] = True
    if not orig_metrics["has_with_clause"] and opt_metrics["has_with_clause"]:
        diffs["cte_added"] = True

    return diffs


def main():
    print("=" * 80)
    print("BATCH EXPLAIN COMPARISON: All DuckDB Winners on Snowflake")
    print("=" * 80)

    # Load leaderboard
    with open(LEADERBOARD) as f:
        reader = csv.DictReader(f)
        winners = []
        for row in reader:
            status = row.get("Status", "")
            if status in ("WIN", "IMPROVED"):
                winners.append(row)

    print(f"\nLoaded {len(winners)} winning/improved queries from leaderboard")

    # Connect to Snowflake
    conn = get_connection()
    cur = conn.cursor()
    cur.execute("ALTER SESSION SET USE_CACHED_RESULT = FALSE")
    print("Connected to Snowflake\n")

    results = []
    errors = []

    for i, w in enumerate(winners):
        query = w["Query"]
        speedup = w["Best_Speedup"]
        source = w["Best_Source"]
        status = w["Status"]
        transform = w.get("Transform", "")

        # Determine worker
        worker = ""
        if source == "Retry3W":
            worker = w.get("Retry3W_Worker", "")
        elif source == "Retry4W":
            worker = w.get("Retry4W_Worker", "")

        print(f"[{i+1}/{len(winners)}] {query} ({speedup}, {source}"
              f"{'/W'+worker if worker else ''}, {transform or '?'})")

        # Find original SQL
        qdir = OPT_DIR / query.lower()
        orig_path = qdir / "original.sql"
        if not orig_path.exists():
            print(f"  SKIP: no original.sql found")
            errors.append({"query": query, "error": "no original.sql"})
            continue

        orig_sql = strip_comments(strip_duckdb_syntax(orig_path.read_text()))

        # Find optimized SQL
        opt_raw = find_optimized_sql(query, source, worker)
        if not opt_raw:
            print(f"  SKIP: no optimized.sql found for {source}/W{worker}")
            errors.append({"query": query, "error": f"no optimized.sql for {source}"})
            continue

        opt_sql = strip_comments(strip_duckdb_syntax(opt_raw))

        # Run EXPLAIN on original
        orig_explain = None
        opt_explain = None
        orig_metrics = None
        opt_metrics = None

        try:
            cur.execute(f"EXPLAIN {orig_sql}")
            orig_explain = cur.fetchall()
            orig_metrics = extract_plan_metrics(orig_explain)
        except Exception as e:
            err = str(e)[:120]
            print(f"  EXPLAIN original FAILED: {err}")
            errors.append({"query": query, "variant": "original", "error": err})
            continue

        try:
            cur.execute(f"EXPLAIN {opt_sql}")
            opt_explain = cur.fetchall()
            opt_metrics = extract_plan_metrics(opt_explain)
        except Exception as e:
            err = str(e)[:120]
            print(f"  EXPLAIN optimized FAILED: {err}")
            errors.append({"query": query, "variant": "optimized", "error": err})
            # Still record original metrics
            results.append({
                "query": query, "speedup": speedup, "source": source,
                "transform": transform, "status": status,
                "orig_metrics": orig_metrics,
                "opt_metrics": None,
                "opt_error": err,
                "diffs": {},
            })
            continue

        # Compare plans
        diffs = compare_plans(orig_metrics, opt_metrics)

        # Determine signal strength
        signal = "SAME"
        if diffs.get("fact_scans") or diffs.get("partitions"):
            signal = "DIFFERENT_SCAN"
        elif len(diffs) > 0:
            signal = "MINOR_DIFF"

        results.append({
            "query": query, "speedup": speedup, "source": source,
            "transform": transform, "status": status,
            "orig_metrics": orig_metrics,
            "opt_metrics": opt_metrics,
            "diffs": diffs,
            "signal": signal,
        })

        # Print summary
        if diffs:
            diff_str = ", ".join(f"{k}={v}" for k, v in diffs.items())
            print(f"  {signal}: {diff_str}")
        else:
            o_scans = orig_metrics["fact_table_scans"]
            p_scans = opt_metrics["fact_table_scans"]
            o_parts = orig_metrics["total_partitions_scanned"]
            print(f"  SAME: {o_scans} fact scans, {o_parts} parts scanned")

    # Summary
    print("\n" + "=" * 80)
    print("SUMMARY")
    print("=" * 80)

    same = [r for r in results if r.get("signal") == "SAME"]
    diff_scan = [r for r in results if r.get("signal") == "DIFFERENT_SCAN"]
    minor = [r for r in results if r.get("signal") == "MINOR_DIFF"]
    failed = [r for r in results if r.get("opt_metrics") is None]

    print(f"\n  SAME plan:          {len(same)} queries (Snowflake already optimizes)")
    print(f"  DIFFERENT scan:     {len(diff_scan)} queries (POTENTIAL OPPORTUNITY)")
    print(f"  Minor differences:  {len(minor)} queries")
    print(f"  Optimized failed:   {len(failed)} queries (syntax error)")
    print(f"  Skipped (no files): {len(errors)} queries")

    if diff_scan:
        print(f"\n--- QUERIES WITH DIFFERENT SCAN PATTERNS (INVESTIGATE) ---")
        for r in diff_scan:
            print(f"  {r['query']} ({r['speedup']}, {r['transform']}): {r['diffs']}")

    if minor:
        print(f"\n--- QUERIES WITH MINOR DIFFERENCES ---")
        for r in minor:
            print(f"  {r['query']} ({r['speedup']}, {r['transform']}): {r['diffs']}")

    if failed:
        print(f"\n--- QUERIES WHERE OPTIMIZED SQL FAILED ON SNOWFLAKE ---")
        for r in failed:
            print(f"  {r['query']} ({r['speedup']}): {r.get('opt_error', '?')[:80]}")

    # Save full results
    output = {
        "total_winners": len(winners),
        "analyzed": len(results),
        "same_plan": len(same),
        "different_scan": len(diff_scan),
        "minor_diff": len(minor),
        "failed": len(failed),
        "skipped": len(errors),
        "results": results,
        "errors": errors,
    }

    out_path = RESULTS_DIR / "batch_explain_comparison.json"
    with open(out_path, "w") as f:
        json.dump(output, f, indent=2, default=str)
    print(f"\nFull results: {out_path}")

    conn.close()
    print("Done.")


if __name__ == "__main__":
    main()
