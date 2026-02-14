"""Snowflake Warehouse Downsizing Benchmark Runner.

Theory: Query rewrites + clustering + QAS can let smaller warehouses
handle workloads that normally require 2-4x larger compute.

Usage:
    # Single query proof-of-concept (Q21)
    python research/snowflake/runner.py --query q21

    # Full batch (all queries in queries/ directory)
    python research/snowflake/runner.py --all

    # Diagnostics only (no benchmark)
    python research/snowflake/runner.py --diagnostics

    # Specific warehouse sizes to test
    python research/snowflake/runner.py --query q21 --sizes XSMALL,SMALL,MEDIUM
"""

import argparse
import json
import os
import sys
import time
from datetime import datetime
from pathlib import Path
from urllib.parse import urlparse, parse_qs, unquote

import snowflake.connector

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

# DSN from environment or default (same as batch_rewrite_test.py)
DSN = os.environ.get(
    "QT_SNOWFLAKE_DSN",
    "snowflake://jkdl:QGdg%2A%24WC%25O62xS71@CVRYJTF-AW47074"
    "/SNOWFLAKE_SAMPLE_DATA/TPCDS_SF10TCL?warehouse=COMPUTE_WH&role=ACCOUNTADMIN",
)

SCRIPT_DIR = Path(__file__).parent
QUERIES_DIR = SCRIPT_DIR / "queries"
RESULTS_DIR = SCRIPT_DIR / "results"

# Validation: 3 runs, discard 1st (warmup), average last 2
NUM_RUNS = 3
WARMUP_RUNS = 1
TIMEOUT_S = 300  # 5 minutes max per query


def parse_dsn(dsn: str) -> dict:
    """Parse Snowflake DSN into connection params."""
    parsed = urlparse(dsn)
    params = parse_qs(parsed.query)
    path_parts = parsed.path.strip("/").split("/")
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
    """Create a Snowflake connection from DSN."""
    p = parse_dsn(DSN)
    conn = snowflake.connector.connect(
        account=p["account"],
        user=p["user"],
        password=p["password"],
        database=p["database"],
        schema=p["schema"],
        warehouse=p["warehouse"],
        role=p["role"],
    )
    return conn


# ---------------------------------------------------------------------------
# Phase 1: Diagnostics
# ---------------------------------------------------------------------------

def run_diagnostics(conn) -> dict:
    """Run forensic diagnostic queries to identify bottlenecks.

    Returns dict with spill info, clustering info, and warehouse status.
    """
    cur = conn.cursor()
    results = {}

    # 1. Identify disk spillers (last 24h)
    print("\n[DIAG] Checking for disk spilling queries (last 24h)...")
    try:
        cur.execute("""
            SELECT
                QUERY_ID,
                SUBSTR(QUERY_TEXT, 1, 80) AS query_preview,
                WAREHOUSE_SIZE,
                EXECUTION_TIME / 1000 AS seconds,
                BYTES_SPILLED_TO_LOCAL_STORAGE / 1024 / 1024 / 1024 AS gb_spilled_local,
                BYTES_SPILLED_TO_REMOTE_STORAGE / 1024 / 1024 / 1024 AS gb_spilled_remote,
                PARTITIONS_SCANNED,
                PARTITIONS_TOTAL
            FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
            WHERE START_TIME > DATEADD(hour, -24, CURRENT_TIMESTAMP())
              AND (BYTES_SPILLED_TO_LOCAL_STORAGE > 0
                   OR BYTES_SPILLED_TO_REMOTE_STORAGE > 0)
            ORDER BY gb_spilled_remote DESC
            LIMIT 20
        """)
        cols = [d[0].lower() for d in cur.description]
        spill_rows = [dict(zip(cols, row)) for row in cur.fetchall()]
        results["spill_queries"] = spill_rows
        if spill_rows:
            print(f"  Found {len(spill_rows)} spilling queries")
            for r in spill_rows[:5]:
                print(f"    {r['seconds']:.1f}s | local={r['gb_spilled_local']:.2f}GB "
                      f"remote={r['gb_spilled_remote']:.2f}GB | {r['query_preview'][:60]}")
        else:
            print("  No spilling queries found (good!)")
    except Exception as e:
        print(f"  Spill check failed (may need ACCOUNT_USAGE access): {e}")
        results["spill_queries"] = []

    # 2. Clustering info for key tables
    print("\n[DIAG] Checking clustering on fact tables...")
    for table in ["INVENTORY", "STORE_SALES", "WEB_SALES", "CATALOG_SALES"]:
        try:
            date_col = {
                "INVENTORY": "INV_DATE_SK",
                "STORE_SALES": "SS_SOLD_DATE_SK",
                "WEB_SALES": "WS_SOLD_DATE_SK",
                "CATALOG_SALES": "CS_SOLD_DATE_SK",
            }[table]
            cur.execute(f"SELECT SYSTEM$CLUSTERING_INFORMATION('{table}', '({date_col})')")
            info = json.loads(cur.fetchone()[0])
            results[f"clustering_{table.lower()}"] = info
            depth = info.get("average_overlap_depth", "N/A")
            total_parts = info.get("total_partition_count", "N/A")
            const_parts = info.get("total_constant_partition_count", "N/A")
            print(f"  {table}: overlap_depth={depth}, "
                  f"partitions={total_parts}, constant={const_parts}")
        except Exception as e:
            print(f"  {table}: clustering check failed: {e}")
            results[f"clustering_{table.lower()}"] = {"error": str(e)}

    # 3. Current warehouse info
    print("\n[DIAG] Current warehouse status...")
    try:
        cur.execute("SHOW WAREHOUSES")
        cols = [d[0].lower() for d in cur.description]
        wh_rows = [dict(zip(cols, row)) for row in cur.fetchall()]
        results["warehouses"] = wh_rows
        for w in wh_rows:
            name = w.get("name", "?")
            size = w.get("size", "?")
            state = w.get("state", "?")
            qas = w.get("enable_query_acceleration", "?")
            print(f"  {name}: size={size}, state={state}, QAS={qas}")
    except Exception as e:
        print(f"  Warehouse check failed: {e}")
        results["warehouses"] = []

    cur.close()
    return results


# ---------------------------------------------------------------------------
# Phase 2: Benchmark Runner
# ---------------------------------------------------------------------------

def get_query_history_metrics(conn, query_id: str) -> dict:
    """Fetch detailed metrics for a completed query from QUERY_HISTORY.

    Uses INFORMATION_SCHEMA (real-time, no 45-min lag like ACCOUNT_USAGE).
    """
    cur = conn.cursor()
    metrics = {}
    try:
        cur.execute(f"""
            SELECT
                QUERY_ID,
                EXECUTION_TIME / 1000 AS seconds,
                BYTES_SCANNED / 1024 / 1024 AS mb_scanned,
                BYTES_SPILLED_TO_LOCAL_STORAGE / 1024 / 1024 AS mb_spilled_local,
                BYTES_SPILLED_TO_REMOTE_STORAGE / 1024 / 1024 AS mb_spilled_remote,
                PARTITIONS_SCANNED,
                PARTITIONS_TOTAL,
                ROWS_PRODUCED,
                COMPILATION_TIME / 1000 AS compile_seconds,
                QUEUED_OVERLOAD_TIME / 1000 AS queued_seconds
            FROM TABLE(INFORMATION_SCHEMA.QUERY_HISTORY_BY_SESSION())
            WHERE QUERY_ID = '{query_id}'
        """)
        row = cur.fetchone()
        if row:
            cols = [d[0].lower() for d in cur.description]
            metrics = dict(zip(cols, row))
    except Exception as e:
        metrics["error"] = str(e)
    cur.close()
    return metrics


def run_single_query(conn, sql: str, timeout_s: int = TIMEOUT_S) -> dict:
    """Execute a query and capture timing + query_id for metric lookup.

    Returns: {elapsed_s, query_id, row_count, status, error}
    """
    cur = conn.cursor()
    try:
        cur.execute(f"ALTER SESSION SET STATEMENT_TIMEOUT_IN_SECONDS = {timeout_s}")
        start = time.perf_counter()
        cur.execute(sql)
        rows = cur.fetchall()
        elapsed = time.perf_counter() - start
        query_id = cur.sfqid  # Snowflake query ID
        cur.close()
        return {
            "elapsed_s": round(elapsed, 4),
            "query_id": query_id,
            "row_count": len(rows),
            "status": "OK",
            "error": None,
        }
    except Exception as e:
        elapsed = time.perf_counter() - start
        query_id = getattr(cur, "sfqid", None)
        cur.close()
        return {
            "elapsed_s": round(elapsed, 4),
            "query_id": query_id,
            "row_count": 0,
            "status": "ERROR" if "timeout" not in str(e).lower() else "TIMEOUT",
            "error": str(e),
        }


def benchmark_query(conn, sql: str, label: str, num_runs: int = NUM_RUNS) -> dict:
    """Run a query N times with validation protocol.

    Protocol: 3 runs, discard 1st (warmup), average last 2.
    """
    print(f"\n  [{label}] Running {num_runs}x benchmark...")
    runs = []
    for i in range(num_runs):
        tag = "warmup" if i < WARMUP_RUNS else f"measure-{i - WARMUP_RUNS + 1}"
        result = run_single_query(conn, sql)
        runs.append(result)

        status_str = result["status"]
        if result["status"] == "OK":
            print(f"    Run {i+1}/{num_runs} ({tag}): {result['elapsed_s']:.3f}s "
                  f"({result['row_count']} rows)")
        else:
            print(f"    Run {i+1}/{num_runs} ({tag}): {status_str} - {result['error'][:80]}")
            # If error/timeout on warmup, still try measure runs
            # If error on measure run, abort
            if i >= WARMUP_RUNS:
                break

    # Collect metrics for last run (most recent in QUERY_HISTORY)
    last_qid = runs[-1].get("query_id")
    detailed_metrics = {}
    if last_qid:
        detailed_metrics = get_query_history_metrics(conn, last_qid)

    # Compute average of measure runs (skip warmup)
    measure_runs = [r for r in runs[WARMUP_RUNS:] if r["status"] == "OK"]
    if measure_runs:
        avg_time = sum(r["elapsed_s"] for r in measure_runs) / len(measure_runs)
    else:
        avg_time = None

    return {
        "label": label,
        "runs": runs,
        "measure_runs_count": len(measure_runs),
        "avg_time_s": round(avg_time, 4) if avg_time else None,
        "detailed_metrics": detailed_metrics,
        "status": "OK" if measure_runs else runs[-1]["status"],
    }


def set_warehouse(conn, warehouse_name: str, size: str,
                  qas_enabled: bool = False, qas_scale: int = 8):
    """Configure warehouse size and QAS settings."""
    cur = conn.cursor()
    print(f"\n[CONFIG] Setting warehouse {warehouse_name} to {size}, QAS={qas_enabled}")

    cur.execute(f"ALTER WAREHOUSE {warehouse_name} SET WAREHOUSE_SIZE = '{size}'")
    cur.execute(f"ALTER WAREHOUSE {warehouse_name} RESUME IF SUSPENDED")

    if qas_enabled:
        cur.execute(f"""
            ALTER WAREHOUSE {warehouse_name} SET
                ENABLE_QUERY_ACCELERATION = TRUE
                QUERY_ACCELERATION_MAX_SCALE_FACTOR = {qas_scale}
        """)
    else:
        cur.execute(f"""
            ALTER WAREHOUSE {warehouse_name} SET
                ENABLE_QUERY_ACCELERATION = FALSE
        """)

    # Verify
    cur.execute(f"SHOW WAREHOUSES LIKE '{warehouse_name}'")
    cols = [d[0].lower() for d in cur.description]
    info = dict(zip(cols, cur.fetchone()))
    print(f"  Verified: size={info.get('size')}, "
          f"QAS={info.get('enable_query_acceleration')}")
    cur.close()


# ---------------------------------------------------------------------------
# Phase 3: Full Test Matrix
# ---------------------------------------------------------------------------

def run_test_matrix(conn, query_id: str, original_sql: str, optimized_sql: str,
                    warehouse: str, sizes: list[str],
                    test_qas: bool = True) -> dict:
    """Run the full test matrix for a single query.

    Matrix dimensions:
    - Warehouse size: XSMALL, SMALL, MEDIUM (configurable)
    - QAS: on/off
    - Query variant: original vs optimized

    Returns structured results dict ready for JSON serialization.
    """
    results = {
        "query_id": query_id,
        "timestamp": datetime.now().isoformat(),
        "dsn_account": parse_dsn(DSN)["account"],
        "database": parse_dsn(DSN)["database"],
        "schema": parse_dsn(DSN)["schema"],
        "warehouse": warehouse,
        "validation_protocol": f"{NUM_RUNS} runs, discard first {WARMUP_RUNS} (warmup), avg remaining",
        "tests": [],
    }

    for size in sizes:
        # QAS configurations to test
        qas_configs = [False]
        if test_qas:
            qas_configs.append(True)

        for qas in qas_configs:
            set_warehouse(conn, warehouse, size, qas_enabled=qas)

            # Small delay to let warehouse resize settle
            time.sleep(2)

            for variant, sql in [("original", original_sql), ("optimized", optimized_sql)]:
                label = f"{size}|QAS={'ON' if qas else 'OFF'}|{variant}"
                bench = benchmark_query(conn, sql, label)

                test_result = {
                    "warehouse_size": size,
                    "qas_enabled": qas,
                    "variant": variant,
                    "avg_time_s": bench["avg_time_s"],
                    "status": bench["status"],
                    "runs": bench["runs"],
                    "detailed_metrics": bench["detailed_metrics"],
                }
                results["tests"].append(test_result)

    return results


def print_summary(results: dict):
    """Print a formatted summary table of results."""
    print("\n" + "=" * 80)
    print(f"RESULTS SUMMARY: {results['query_id']}")
    print("=" * 80)
    print(f"{'Size':<10} {'QAS':<6} {'Variant':<12} {'Avg (s)':<10} {'Status':<10} "
          f"{'Spill Local':<14} {'Spill Remote':<14} {'Pruning'}")
    print("-" * 100)

    for t in results["tests"]:
        m = t.get("detailed_metrics", {})
        spill_l = m.get("mb_spilled_local", "")
        spill_r = m.get("mb_spilled_remote", "")
        parts_scanned = m.get("partitions_scanned", "")
        parts_total = m.get("partitions_total", "")

        spill_l_str = f"{spill_l:.0f}MB" if isinstance(spill_l, (int, float)) else "-"
        spill_r_str = f"{spill_r:.0f}MB" if isinstance(spill_r, (int, float)) else "-"
        pruning = f"{parts_scanned}/{parts_total}" if parts_scanned != "" else "-"

        avg = t["avg_time_s"]
        avg_str = f"{avg:.3f}" if avg is not None else "FAIL"

        print(f"{t['warehouse_size']:<10} "
              f"{'ON' if t['qas_enabled'] else 'OFF':<6} "
              f"{t['variant']:<12} "
              f"{avg_str:<10} "
              f"{t['status']:<10} "
              f"{spill_l_str:<14} "
              f"{spill_r_str:<14} "
              f"{pruning}")

    # Compute speedup ratios
    print("\n--- Speedup Analysis ---")
    by_config = {}
    for t in results["tests"]:
        key = (t["warehouse_size"], t["qas_enabled"])
        by_config.setdefault(key, {})[t["variant"]] = t["avg_time_s"]

    for (size, qas), variants in sorted(by_config.items()):
        orig = variants.get("original")
        opt = variants.get("optimized")
        qas_str = "QAS=ON" if qas else "QAS=OFF"
        if orig and opt and opt > 0:
            speedup = orig / opt
            print(f"  {size} {qas_str}: {orig:.3f}s -> {opt:.3f}s = {speedup:.2f}x speedup")
        elif orig is None and opt:
            print(f"  {size} {qas_str}: original FAILED, optimized={opt:.3f}s")
        elif orig and opt is None:
            print(f"  {size} {qas_str}: original={orig:.3f}s, optimized FAILED")
        else:
            print(f"  {size} {qas_str}: both FAILED")


def load_query(query_id: str, variant: str) -> str:
    """Load SQL from queries/ directory."""
    path = QUERIES_DIR / f"{query_id}_{variant}.sql"
    if not path.exists():
        raise FileNotFoundError(f"Query file not found: {path}")
    return path.read_text().strip()


def discover_queries() -> list[str]:
    """Find all query IDs that have both original and optimized variants."""
    originals = set()
    optimized = set()
    for f in QUERIES_DIR.glob("*_original.sql"):
        originals.add(f.stem.replace("_original", ""))
    for f in QUERIES_DIR.glob("*_optimized.sql"):
        optimized.add(f.stem.replace("_optimized", ""))
    return sorted(originals & optimized)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(
        description="Snowflake Warehouse Downsizing Benchmark",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python research/snowflake/runner.py --query q21
  python research/snowflake/runner.py --query q21 --sizes XSMALL,MEDIUM
  python research/snowflake/runner.py --all
  python research/snowflake/runner.py --diagnostics
        """,
    )
    parser.add_argument("--query", "-q", help="Query ID to test (e.g., q21)")
    parser.add_argument("--all", action="store_true", help="Run all queries in queries/")
    parser.add_argument("--diagnostics", "-d", action="store_true",
                        help="Run diagnostics only")
    parser.add_argument("--sizes", default="XSMALL,SMALL,MEDIUM",
                        help="Comma-separated warehouse sizes (default: XSMALL,SMALL,MEDIUM)")
    parser.add_argument("--no-qas", action="store_true",
                        help="Skip QAS tests (only test without QAS)")
    parser.add_argument("--warehouse", default="COMPUTE_WH",
                        help="Warehouse name (default: COMPUTE_WH)")
    parser.add_argument("--runs", type=int, default=NUM_RUNS,
                        help=f"Number of benchmark runs (default: {NUM_RUNS})")

    args = parser.parse_args()

    if not (args.query or args.all or args.diagnostics):
        parser.print_help()
        sys.exit(1)

    global NUM_RUNS
    NUM_RUNS = args.runs

    sizes = [s.strip().upper() for s in args.sizes.split(",")]
    RESULTS_DIR.mkdir(exist_ok=True)

    print("=" * 60)
    print("SNOWFLAKE WAREHOUSE DOWNSIZING BENCHMARK")
    print("=" * 60)
    print(f"Sizes to test: {sizes}")
    print(f"QAS testing: {'OFF' if args.no_qas else 'ON'}")
    print(f"Runs per test: {NUM_RUNS} ({WARMUP_RUNS} warmup + {NUM_RUNS - WARMUP_RUNS} measure)")

    conn = get_connection()
    print("Connected to Snowflake.")

    try:
        # Phase 1: Diagnostics
        if args.diagnostics or args.query or args.all:
            diag_results = run_diagnostics(conn)
            diag_path = RESULTS_DIR / "diagnostics.json"
            with open(diag_path, "w") as f:
                json.dump(diag_results, f, indent=2, default=str)
            print(f"\nDiagnostics saved to {diag_path}")

        if args.diagnostics:
            return

        # Phase 2-3: Benchmark
        query_ids = []
        if args.query:
            query_ids = [args.query]
        elif args.all:
            query_ids = discover_queries()
            print(f"\nDiscovered {len(query_ids)} queries: {query_ids}")

        for qid in query_ids:
            print(f"\n{'=' * 60}")
            print(f"BENCHMARKING: {qid.upper()}")
            print(f"{'=' * 60}")

            original_sql = load_query(qid, "original")
            optimized_sql = load_query(qid, "optimized")

            results = run_test_matrix(
                conn, qid, original_sql, optimized_sql,
                warehouse=args.warehouse,
                sizes=sizes,
                test_qas=not args.no_qas,
            )

            # Save results
            ts = datetime.now().strftime("%Y%m%d_%H%M%S")
            result_path = RESULTS_DIR / f"{qid}_{ts}.json"
            with open(result_path, "w") as f:
                json.dump(results, f, indent=2, default=str)
            print(f"\nResults saved to {result_path}")

            # Print summary
            print_summary(results)

    finally:
        # Restore warehouse to X-Small to minimize cost
        try:
            cur = conn.cursor()
            cur.execute(f"ALTER WAREHOUSE {args.warehouse} SET WAREHOUSE_SIZE = 'XSMALL'")
            cur.execute(f"ALTER WAREHOUSE {args.warehouse} SET "
                        f"ENABLE_QUERY_ACCELERATION = FALSE")
            cur.close()
            print(f"\n[CLEANUP] Warehouse restored to XSMALL, QAS=OFF")
        except Exception:
            pass
        conn.close()
        print("Connection closed.")


if __name__ == "__main__":
    main()
