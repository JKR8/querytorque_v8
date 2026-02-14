"""Q21 Warehouse Downsizing Proof of Concept.

Single-query, forensic-level proof that query rewrite + smaller warehouse
produces IDENTICAL results faster and cheaper.

Steps:
  1. Diagnostics: clustering status, warehouse state
  2. Equivalence: Run both variants, compare row-by-row with MD5 checksum
  3. Benchmark: Time both variants across warehouse sizes
  4. Forensics: Spill bytes, partition pruning, compilation time

Usage:
    python research/snowflake/prove_q21.py
"""

import hashlib
import json
import os
import sys
import time
from datetime import datetime
from pathlib import Path
from urllib.parse import urlparse, parse_qs, unquote

import snowflake.connector

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

DSN = os.environ.get(
    "QT_SNOWFLAKE_DSN",
    "snowflake://jkdl:QGdg%2A%24WC%25O62xS71@CVRYJTF-AW47074"
    "/SNOWFLAKE_SAMPLE_DATA/TPCDS_SF10TCL?warehouse=COMPUTE_WH&role=ACCOUNTADMIN",
)

SCRIPT_DIR = Path(__file__).parent
RESULTS_DIR = SCRIPT_DIR / "results"
RESULTS_DIR.mkdir(exist_ok=True)

QUERY_ID = os.environ.get("QT_QUERY", "q36")  # Default to Q36 (STORE_SALES stress test)
ORIGINAL_SQL = (SCRIPT_DIR / "queries" / f"{QUERY_ID}_original.sql").read_text().strip()
OPTIMIZED_SQL = (SCRIPT_DIR / "queries" / f"{QUERY_ID}_optimized.sql").read_text().strip()

# Strip comments from SQL for cleaner execution
def strip_comments(sql: str) -> str:
    lines = [l for l in sql.splitlines() if not l.strip().startswith("--")]
    return "\n".join(lines).strip()

ORIGINAL_SQL = strip_comments(ORIGINAL_SQL)
OPTIMIZED_SQL = strip_comments(OPTIMIZED_SQL)

TIMEOUT_S = int(os.environ.get("QT_TIMEOUT", "600"))  # configurable, default 10 min
NUM_BENCH_RUNS = 3  # 3 runs: discard 1st (warmup), average last 2


def parse_dsn(dsn: str) -> dict:
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
    p = parse_dsn(DSN)
    return snowflake.connector.connect(
        account=p["account"], user=p["user"], password=p["password"],
        database=p["database"], schema=p["schema"],
        warehouse=p["warehouse"], role=p["role"],
    )


QAS_AVAILABLE = None  # detected at runtime


def set_warehouse(conn, size: str, qas: bool = False, qas_scale: int = 8):
    """Resize warehouse and configure QAS (if available on account)."""
    global QAS_AVAILABLE
    cur = conn.cursor()
    wh = parse_dsn(DSN)["warehouse"]
    print(f"\n[CONFIG] Warehouse {wh} -> {size}", end="")
    cur.execute(f"ALTER WAREHOUSE {wh} SET WAREHOUSE_SIZE = '{size}'")
    cur.execute(f"ALTER WAREHOUSE {wh} RESUME IF SUSPENDED")

    # Try QAS — may not be available on all account tiers
    actual_qas = "N/A"
    if qas and QAS_AVAILABLE is not False:
        try:
            cur.execute(f"ALTER WAREHOUSE {wh} SET ENABLE_QUERY_ACCELERATION = TRUE "
                        f"QUERY_ACCELERATION_MAX_SCALE_FACTOR = {qas_scale}")
            QAS_AVAILABLE = True
            actual_qas = "ON"
        except Exception:
            QAS_AVAILABLE = False
            print(" (QAS not available on this account)", end="")
    elif not qas and QAS_AVAILABLE is True:
        try:
            cur.execute(f"ALTER WAREHOUSE {wh} SET ENABLE_QUERY_ACCELERATION = FALSE")
            actual_qas = "OFF"
        except Exception:
            pass

    print()

    # Wait for resize
    time.sleep(3)
    cur.execute(f"SHOW WAREHOUSES LIKE '{wh}'")
    cols = [d[0].lower() for d in cur.description]
    info = dict(zip(cols, cur.fetchone()))
    actual_size = info.get("size", "?")
    print(f"  Verified: size={actual_size}, QAS={actual_qas}")
    cur.close()
    return {"size": actual_size, "qas": actual_qas}


# ---------------------------------------------------------------------------
# Phase 1: Diagnostics
# ---------------------------------------------------------------------------

def run_diagnostics(conn) -> dict:
    """Forensic diagnostics: clustering, spill history, warehouse state."""
    cur = conn.cursor()
    diag = {}

    print("\n" + "=" * 70)
    print("PHASE 1: DIAGNOSTICS")
    print("=" * 70)

    # 1. Clustering on key fact tables
    cluster_checks = [
        ("STORE_SALES", "SS_SOLD_DATE_SK"),
        ("STORE_SALES", "SS_SOLD_DATE_SK, SS_ITEM_SK"),
        ("INVENTORY", "INV_DATE_SK"),
        ("CATALOG_SALES", "CS_SOLD_DATE_SK"),
        ("WEB_SALES", "WS_SOLD_DATE_SK"),
    ]
    for table, cols in cluster_checks:
        print(f"\n--- Clustering: {table} ({cols}) ---")
        try:
            cur.execute(f"SELECT SYSTEM$CLUSTERING_INFORMATION('{table}', '({cols})')")
            info = json.loads(cur.fetchone()[0])
            key = f"clustering_{table.lower()}_{cols.lower().replace(', ', '_')}"
            diag[key] = info
            print(f"  overlap_depth: {info.get('average_overlap_depth', 'N/A')}")
            print(f"  total_partitions: {info.get('total_partition_count', 'N/A')}")
            print(f"  constant_partitions: {info.get('total_constant_partition_count', 'N/A')}")
        except Exception as e:
            print(f"  ERROR: {e}")

    # 2. Table sizes
    print("\n--- Table Sizes ---")
    for table in ["STORE_SALES", "INVENTORY", "CATALOG_SALES", "WEB_SALES",
                   "WAREHOUSE", "ITEM", "STORE", "DATE_DIM"]:
        try:
            cur.execute(f"SELECT COUNT(*) FROM {table}")
            count = cur.fetchone()[0]
            diag[f"row_count_{table.lower()}"] = count
            print(f"  {table}: {count:,} rows")
        except Exception as e:
            print(f"  {table}: ERROR {e}")

    # 3. Recent spill history (if available)
    print("\n--- Recent Spill History (last 24h) ---")
    try:
        cur.execute("""
            SELECT
                QUERY_ID,
                SUBSTR(QUERY_TEXT, 1, 60) AS preview,
                WAREHOUSE_SIZE,
                EXECUTION_TIME / 1000 AS seconds,
                BYTES_SPILLED_TO_LOCAL_STORAGE / 1024 / 1024 AS mb_local,
                BYTES_SPILLED_TO_REMOTE_STORAGE / 1024 / 1024 AS mb_remote
            FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
            WHERE START_TIME > DATEADD(hour, -24, CURRENT_TIMESTAMP())
              AND (BYTES_SPILLED_TO_LOCAL_STORAGE > 0
                   OR BYTES_SPILLED_TO_REMOTE_STORAGE > 0)
            ORDER BY mb_remote DESC
            LIMIT 10
        """)
        cols = [d[0].lower() for d in cur.description]
        rows = [dict(zip(cols, r)) for r in cur.fetchall()]
        diag["recent_spills"] = rows
        if rows:
            for r in rows[:5]:
                print(f"  {r['seconds']:.1f}s | local={r['mb_local']:.0f}MB "
                      f"remote={r['mb_remote']:.0f}MB | {r['preview'][:50]}")
        else:
            print("  No spilling queries (good)")
    except Exception as e:
        print(f"  Spill history unavailable: {e}")
        diag["recent_spills"] = {"error": str(e)}

    # 4. Current warehouse state
    print("\n--- Warehouse State ---")
    wh = parse_dsn(DSN)["warehouse"]
    try:
        cur.execute(f"SHOW WAREHOUSES LIKE '{wh}'")
        cols = [d[0].lower() for d in cur.description]
        info = dict(zip(cols, cur.fetchone()))
        diag["warehouse"] = {k: str(v) for k, v in info.items()
                             if k in ("name", "size", "state", "type",
                                      "enable_query_acceleration",
                                      "query_acceleration_max_scale_factor",
                                      "auto_suspend")}
        for k, v in diag["warehouse"].items():
            print(f"  {k}: {v}")
    except Exception as e:
        print(f"  ERROR: {e}")

    cur.close()
    return diag


# ---------------------------------------------------------------------------
# Phase 1b: EXPLAIN Plans
# ---------------------------------------------------------------------------

def capture_explain_plans(conn) -> dict:
    """Capture EXPLAIN plans for both variants on MEDIUM warehouse.

    Shows the optimizer's intended plan — critically, partition pruning
    estimates and join strategies differ between original and optimized.
    """
    print("\n" + "=" * 70)
    print("PHASE 1b: EXPLAIN PLANS")
    print("=" * 70)

    set_warehouse(conn, "MEDIUM", qas=False)
    cur = conn.cursor()
    plans = {}

    for label, sql in [("original", ORIGINAL_SQL), ("optimized", OPTIMIZED_SQL)]:
        print(f"\n  EXPLAIN {label}...")
        try:
            cur.execute(f"EXPLAIN {sql}")
            rows = cur.fetchall()
            cols = [d[0] for d in cur.description]
            lines = []
            for row in rows:
                parts = [str(v) for v in row if v is not None]
                if parts:
                    lines.append(" | ".join(parts))
            plan_text = "\n".join(lines)
            plans[label] = plan_text
            # Show first 30 lines
            for line in lines[:30]:
                print(f"    {line}")
            if len(lines) > 30:
                print(f"    ... ({len(lines)} total lines)")
        except Exception as e:
            print(f"    ERROR: {e}")
            plans[label] = f"ERROR: {e}"

    cur.close()
    return plans


# ---------------------------------------------------------------------------
# Phase 2: Equivalence Verification
# ---------------------------------------------------------------------------

def row_to_canonical(row: tuple) -> str:
    """Convert a result row to a canonical string for checksumming.

    Handles None, float precision, and type normalization.
    """
    parts = []
    for val in row:
        if val is None:
            parts.append("NULL")
        elif isinstance(val, float):
            # 6 decimal places to avoid floating-point noise
            parts.append(f"{val:.6f}")
        elif isinstance(val, (int,)):
            parts.append(str(val))
        else:
            parts.append(str(val).strip())
    return "|".join(parts)


def compute_result_checksum(rows: list[tuple], columns: list[str]) -> dict:
    """Compute MD5 checksum over sorted result set.

    Returns: {checksum, row_count, column_count, columns, sample_rows}
    """
    # Canonicalize each row
    canonical = [row_to_canonical(r) for r in rows]

    # Sort for order-independent comparison (Q21 has ORDER BY so should match,
    # but sort anyway for safety)
    canonical_sorted = sorted(canonical)

    # Compute checksum over all rows
    hasher = hashlib.md5()
    for line in canonical_sorted:
        hasher.update(line.encode("utf-8"))
        hasher.update(b"\n")

    # Also compute per-row checksums for diff identification
    row_checksums = [hashlib.md5(line.encode("utf-8")).hexdigest()[:12]
                     for line in canonical_sorted]

    return {
        "checksum": hasher.hexdigest(),
        "row_count": len(rows),
        "column_count": len(columns),
        "columns": columns,
        "sample_rows_canonical": canonical_sorted[:5],  # first 5 for inspection
        "row_checksums": row_checksums,  # for per-row diff
    }


def verify_equivalence(conn) -> dict:
    """Run both queries, compare results row-by-row.

    Uses a warehouse large enough to finish the original query.
    """
    print("\n" + "=" * 70)
    print("PHASE 2: EQUIVALENCE VERIFICATION")
    print("=" * 70)
    print("Running both original and optimized queries to compare results.")
    print("Will size up warehouse if needed to ensure original finishes.\n")

    result = {"verified": False, "details": {}}

    # Size up to MEDIUM to ensure original finishes
    set_warehouse(conn, "MEDIUM", qas=True)

    cur = conn.cursor()
    cur.execute(f"ALTER SESSION SET STATEMENT_TIMEOUT_IN_SECONDS = {TIMEOUT_S}")

    # Run original
    print("\n  Running ORIGINAL query...")
    t0 = time.perf_counter()
    try:
        cur.execute(ORIGINAL_SQL)
        orig_columns = [d[0].lower() for d in cur.description]
        orig_rows = cur.fetchall()
        orig_time = time.perf_counter() - t0
        orig_qid = cur.sfqid
        print(f"    OK: {len(orig_rows)} rows in {orig_time:.3f}s (qid={orig_qid})")
    except Exception as e:
        orig_time = time.perf_counter() - t0
        print(f"    FAILED after {orig_time:.1f}s: {e}")
        # Try on LARGE
        print("    Retrying on LARGE...")
        set_warehouse(conn, "LARGE", qas=True)
        cur.execute(f"ALTER SESSION SET STATEMENT_TIMEOUT_IN_SECONDS = {TIMEOUT_S}")
        t0 = time.perf_counter()
        try:
            cur.execute(ORIGINAL_SQL)
            orig_columns = [d[0].lower() for d in cur.description]
            orig_rows = cur.fetchall()
            orig_time = time.perf_counter() - t0
            orig_qid = cur.sfqid
            print(f"    OK on LARGE: {len(orig_rows)} rows in {orig_time:.3f}s")
        except Exception as e2:
            print(f"    FAILED on LARGE too: {e2}")
            result["details"]["error"] = f"Original query failed even on LARGE: {e2}"
            cur.close()
            return result

    # Run optimized (can use X-Small since it's fast)
    print("\n  Running OPTIMIZED query...")
    # Keep same warehouse for fairness in equivalence check
    t0 = time.perf_counter()
    cur.execute(OPTIMIZED_SQL)
    opt_columns = [d[0].lower() for d in cur.description]
    opt_rows = cur.fetchall()
    opt_time = time.perf_counter() - t0
    opt_qid = cur.sfqid
    print(f"    OK: {len(opt_rows)} rows in {opt_time:.3f}s (qid={opt_qid})")

    cur.close()

    # Compare
    print("\n  --- COMPARISON ---")

    # Column check
    print(f"  Columns original:  {orig_columns}")
    print(f"  Columns optimized: {opt_columns}")
    cols_match = orig_columns == opt_columns
    print(f"  Columns match: {cols_match}")

    # Row count
    print(f"  Rows original:  {len(orig_rows)}")
    print(f"  Rows optimized: {len(opt_rows)}")
    rows_match = len(orig_rows) == len(opt_rows)
    print(f"  Row count match: {rows_match}")

    # Checksums
    orig_ck = compute_result_checksum(orig_rows, orig_columns)
    opt_ck = compute_result_checksum(opt_rows, opt_columns)

    print(f"  Checksum original:  {orig_ck['checksum']}")
    print(f"  Checksum optimized: {opt_ck['checksum']}")
    checksums_match = orig_ck["checksum"] == opt_ck["checksum"]
    print(f"  Checksums match: {checksums_match}")

    # If checksums don't match, find the diff
    diffs = []
    if not checksums_match:
        print("\n  !!! CHECKSUMS DO NOT MATCH — finding differences !!!")
        orig_set = set(orig_ck["row_checksums"])
        opt_set = set(opt_ck["row_checksums"])
        only_orig = orig_set - opt_set
        only_opt = opt_set - orig_set
        print(f"  Rows only in original: {len(only_orig)}")
        print(f"  Rows only in optimized: {len(only_opt)}")

        # Show actual differing rows
        orig_canonical = sorted([row_to_canonical(r) for r in orig_rows])
        opt_canonical = sorted([row_to_canonical(r) for r in opt_rows])

        # Find first N diffs
        max_diffs = 10
        i, j = 0, 0
        while i < len(orig_canonical) and j < len(opt_canonical) and len(diffs) < max_diffs:
            if orig_canonical[i] == opt_canonical[j]:
                i += 1
                j += 1
            elif orig_canonical[i] < opt_canonical[j]:
                diffs.append({"type": "only_original", "row": orig_canonical[i]})
                i += 1
            else:
                diffs.append({"type": "only_optimized", "row": opt_canonical[j]})
                j += 1

        for d in diffs[:5]:
            print(f"    {d['type']}: {d['row'][:100]}")

    verified = cols_match and rows_match and checksums_match

    print(f"\n  {'PASS' if verified else 'FAIL'}: Equivalence {'verified' if verified else 'NOT verified'}")
    if verified:
        print(f"  MD5 checksum: {orig_ck['checksum']}")
        print(f"  {len(orig_rows)} rows, {len(orig_columns)} columns — IDENTICAL")

    result["verified"] = verified
    result["details"] = {
        "columns_match": cols_match,
        "rows_match": rows_match,
        "checksums_match": checksums_match,
        "original_checksum": orig_ck["checksum"],
        "optimized_checksum": opt_ck["checksum"],
        "original_row_count": len(orig_rows),
        "optimized_row_count": len(opt_rows),
        "original_columns": orig_columns,
        "optimized_columns": opt_columns,
        "original_time_s": round(orig_time, 4),
        "optimized_time_s": round(opt_time, 4),
        "original_query_id": orig_qid,
        "optimized_query_id": opt_qid,
        "original_sample": orig_ck["sample_rows_canonical"],
        "optimized_sample": opt_ck["sample_rows_canonical"],
        "diffs": diffs if diffs else None,
    }

    return result


# ---------------------------------------------------------------------------
# Phase 3: Benchmark
# ---------------------------------------------------------------------------

def get_query_metrics(conn, query_id: str) -> dict:
    """Fetch detailed execution metrics for a query ID.

    Uses GET_QUERY_OPERATOR_STATS for partition-level detail,
    and QUERY_HISTORY for spill/scan totals.
    """
    cur = conn.cursor()
    metrics = {}
    try:
        # Method 1: QUERY_HISTORY via RESULT_SCAN on LAST_QUERY_ID
        # This is more reliable than QUERY_HISTORY_BY_SESSION
        cur.execute(f"""
            SELECT
                EXECUTION_TIME,
                COMPILATION_TIME,
                BYTES_SCANNED,
                BYTES_SPILLED_TO_LOCAL_STORAGE,
                BYTES_SPILLED_TO_REMOTE_STORAGE,
                PARTITIONS_SCANNED,
                PARTITIONS_TOTAL,
                ROWS_PRODUCED,
                QUEUED_OVERLOAD_TIME,
                QUEUED_PROVISIONING_TIME
            FROM TABLE(INFORMATION_SCHEMA.QUERY_HISTORY())
            WHERE QUERY_ID = '{query_id}'
            LIMIT 1
        """)
        row = cur.fetchone()
        if row:
            cols = [d[0].lower() for d in cur.description]
            raw = dict(zip(cols, row))
            # Convert to human-readable
            bytes_scanned = raw.get("bytes_scanned", 0) or 0
            bytes_spilled_local = raw.get("bytes_spilled_to_local_storage", 0) or 0
            bytes_spilled_remote = raw.get("bytes_spilled_to_remote_storage", 0) or 0
            total_spill = bytes_spilled_local + bytes_spilled_remote

            metrics = {
                "execution_ms": raw.get("execution_time"),
                "compilation_ms": raw.get("compilation_time"),
                "mb_scanned": round(bytes_scanned / 1024 / 1024, 2),
                "mb_spilled_local": round(bytes_spilled_local / 1024 / 1024, 2),
                "mb_spilled_remote": round(bytes_spilled_remote / 1024 / 1024, 2),
                # Spill-to-scan ratio: if > 0, warehouse is undersized for this query shape
                "spill_to_scan_ratio": round(total_spill / bytes_scanned, 4) if bytes_scanned > 0 else 0,
                "partitions_scanned": raw.get("partitions_scanned"),
                "partitions_total": raw.get("partitions_total"),
                # Pruning efficiency: % of partitions skipped
                "pruning_pct": round(
                    (1 - (raw.get("partitions_scanned", 0) or 0) / (raw.get("partitions_total", 1) or 1)) * 100, 1
                ) if raw.get("partitions_total") else None,
                "rows_produced": raw.get("rows_produced"),
                "queued_overload_ms": raw.get("queued_overload_time"),
                "queued_provisioning_ms": raw.get("queued_provisioning_time"),
            }
        else:
            # Fallback: try ACCOUNT_USAGE (has 45-min lag but more reliable)
            metrics["note"] = "INFORMATION_SCHEMA.QUERY_HISTORY() returned no rows"
    except Exception as e:
        metrics["error"] = str(e)
    cur.close()
    return metrics


def benchmark_variant(conn, sql: str, label: str) -> dict:
    """Run a query 3x, capture all metrics.

    Protocol: 3 runs, discard 1st (warmup), average last 2.
    """
    print(f"\n  [{label}] Benchmarking ({NUM_BENCH_RUNS} runs)...")
    cur = conn.cursor()
    cur.execute(f"ALTER SESSION SET STATEMENT_TIMEOUT_IN_SECONDS = {TIMEOUT_S}")

    runs = []
    for i in range(NUM_BENCH_RUNS):
        tag = "warmup" if i == 0 else f"measure-{i}"
        t0 = time.perf_counter()
        try:
            cur.execute(sql)
            rows = cur.fetchall()
            elapsed = time.perf_counter() - t0
            qid = cur.sfqid
            status = "OK"
            error = None
            row_count = len(rows)
        except Exception as e:
            elapsed = time.perf_counter() - t0
            qid = getattr(cur, "sfqid", None)
            status = "TIMEOUT" if "timeout" in str(e).lower() else "ERROR"
            error = str(e)[:200]
            row_count = 0

        # Small delay for query history to populate, then fetch metrics
        time.sleep(0.5)
        metrics = get_query_metrics(conn, qid) if qid else {}

        run_data = {
            "run": i + 1,
            "tag": tag,
            "elapsed_s": round(elapsed, 4),
            "query_id": qid,
            "row_count": row_count,
            "status": status,
            "error": error,
            "metrics": metrics,
        }
        runs.append(run_data)

        spill_str = ""
        if metrics.get("mb_spilled_local", 0) > 0 or metrics.get("mb_spilled_remote", 0) > 0:
            spill_str = (f" | SPILL local={metrics.get('mb_spilled_local', 0):.0f}MB "
                         f"remote={metrics.get('mb_spilled_remote', 0):.0f}MB")

        pruning_str = ""
        ps = metrics.get("partitions_scanned")
        pt = metrics.get("partitions_total")
        if ps is not None and pt is not None and pt > 0:
            pct = (1 - ps / pt) * 100
            pruning_str = f" | pruned {pct:.1f}% ({ps}/{pt})"

        if status == "OK":
            print(f"    Run {i+1} ({tag}): {elapsed:.3f}s, {row_count} rows"
                  f"{spill_str}{pruning_str}")
        else:
            print(f"    Run {i+1} ({tag}): {status} after {elapsed:.1f}s — {error[:60]}")
            if i > 0:  # measure run failed, abort
                break

    cur.close()

    # Average of measure runs
    measure = [r for r in runs[1:] if r["status"] == "OK"]
    avg_time = sum(r["elapsed_s"] for r in measure) / len(measure) if measure else None

    return {
        "label": label,
        "runs": runs,
        "avg_time_s": round(avg_time, 4) if avg_time else None,
        "status": "OK" if measure else runs[-1]["status"],
    }


def run_benchmarks(conn) -> list[dict]:
    """Run benchmark matrix: sizes x QAS x variants."""
    print("\n" + "=" * 70)
    print("PHASE 3: BENCHMARK")
    print("=" * 70)

    # Test matrix — each config we want to prove
    # Skip QAS configs if QAS not available on this account
    configs = [
        # (size, qas, description)
        ("XSMALL", False, "Baseline: cheapest warehouse, no help"),
        ("SMALL",  False, "Small warehouse"),
        ("MEDIUM", False, "Medium warehouse (standard rec for SF10)"),
    ]
    if QAS_AVAILABLE:
        configs.extend([
            ("XSMALL", True,  "X-Small + QAS offload"),
            ("SMALL",  True,  "Small + QAS"),
            ("MEDIUM", True,  "Medium + QAS"),
        ])

    all_results = []

    for size, qas, desc in configs:
        print(f"\n{'─' * 60}")
        print(f"CONFIG: {desc}")
        print(f"{'─' * 60}")

        wh_info = set_warehouse(conn, size, qas=qas)

        # Original
        orig_result = benchmark_variant(conn, ORIGINAL_SQL, f"{size}/QAS={'ON' if qas else 'OFF'}/original")
        orig_result["warehouse_size"] = size
        orig_result["qas_enabled"] = qas
        orig_result["variant"] = "original"
        orig_result["config_description"] = desc
        all_results.append(orig_result)

        # Optimized
        opt_result = benchmark_variant(conn, OPTIMIZED_SQL, f"{size}/QAS={'ON' if qas else 'OFF'}/optimized")
        opt_result["warehouse_size"] = size
        opt_result["qas_enabled"] = qas
        opt_result["variant"] = "optimized"
        opt_result["config_description"] = desc
        all_results.append(opt_result)

        # Quick speedup report for this config
        if orig_result["avg_time_s"] and opt_result["avg_time_s"] and opt_result["avg_time_s"] > 0:
            speedup = orig_result["avg_time_s"] / opt_result["avg_time_s"]
            print(f"\n  => SPEEDUP: {orig_result['avg_time_s']:.3f}s -> "
                  f"{opt_result['avg_time_s']:.3f}s = {speedup:.2f}x")
        elif orig_result["status"] != "OK" and opt_result["status"] == "OK":
            print(f"\n  => Original FAILED, optimized ran in {opt_result['avg_time_s']:.3f}s")

    return all_results


# ---------------------------------------------------------------------------
# Summary
# ---------------------------------------------------------------------------

def print_final_summary(equiv: dict, benchmarks: list[dict]):
    """Print the full forensic summary."""
    print("\n" + "=" * 70)
    print(f"FINAL SUMMARY: {QUERY_ID.upper()} WAREHOUSE DOWNSIZING PROOF")
    print("=" * 70)

    # Equivalence
    ed = equiv["details"]
    print(f"\nEQUIVALENCE: {'VERIFIED' if equiv['verified'] else 'FAILED'}")
    print(f"  Rows: {ed['original_row_count']} original, {ed['optimized_row_count']} optimized")
    print(f"  MD5:  {ed['original_checksum']}")
    print(f"  Cols: {ed['original_columns']}")

    # Benchmark table
    print(f"\n{'Size':<8} {'QAS':<5} {'Variant':<11} {'Avg(s)':<9} {'Status':<8} "
          f"{'Scan MB':<10} {'Spill-L':<10} {'Spill-R':<10} {'Spill/Scan':<11} "
          f"{'Prune%':<8} {'Parts':<15} {'Compile':<10}")
    print("-" * 120)

    for b in benchmarks:
        # Get metrics from last measure run
        measure_runs = [r for r in b["runs"][1:] if r["status"] == "OK"]
        m = measure_runs[-1]["metrics"] if measure_runs else {}

        scanned = m.get("mb_scanned", 0)
        sl = m.get("mb_spilled_local", 0)
        sr = m.get("mb_spilled_remote", 0)
        ratio = m.get("spill_to_scan_ratio", 0)
        ps = m.get("partitions_scanned", "")
        pt = m.get("partitions_total", "")
        prune_pct = m.get("pruning_pct", "")
        comp = m.get("compilation_ms", "")

        scan_str = f"{scanned:.0f}" if scanned else "-"
        sl_str = f"{sl:.0f}MB" if sl else "-"
        sr_str = f"{sr:.0f}MB" if sr else "-"
        ratio_str = f"{ratio:.3f}" if ratio else "-"
        prune_str = f"{prune_pct}%" if prune_pct != "" and prune_pct is not None else "-"
        parts_str = f"{ps}/{pt}" if ps != "" else "-"
        comp_str = f"{comp}ms" if comp != "" else "-"
        avg_str = f"{b['avg_time_s']:.3f}" if b["avg_time_s"] else "FAIL"

        print(f"{b['warehouse_size']:<8} "
              f"{'ON' if b['qas_enabled'] else 'OFF':<5} "
              f"{b['variant']:<11} "
              f"{avg_str:<9} "
              f"{b['status']:<8} "
              f"{scan_str:<10} "
              f"{sl_str:<10} "
              f"{sr_str:<10} "
              f"{ratio_str:<11} "
              f"{prune_str:<8} "
              f"{parts_str:<15} "
              f"{comp_str:<10}")

    # Speedup analysis
    print("\n--- SPEEDUP BY CONFIG ---")
    by_config = {}
    for b in benchmarks:
        key = (b["warehouse_size"], b["qas_enabled"])
        by_config.setdefault(key, {})[b["variant"]] = b

    for (size, qas), variants in sorted(by_config.items()):
        orig = variants.get("original", {})
        opt = variants.get("optimized", {})
        qas_str = "QAS=ON " if qas else "QAS=OFF"
        o_time = orig.get("avg_time_s")
        p_time = opt.get("avg_time_s")

        if o_time and p_time and p_time > 0:
            speedup = o_time / p_time
            credit_map = {"XSMALL": 1, "SMALL": 2, "MEDIUM": 4, "LARGE": 8, "XLARGE": 16}
            credits = credit_map.get(size, "?")
            print(f"  {size:<8} {qas_str}: {o_time:.3f}s -> {p_time:.3f}s "
                  f"= {speedup:.2f}x speedup ({credits} credits/hr)")
        elif o_time is None and p_time:
            print(f"  {size:<8} {qas_str}: original TIMEOUT, optimized {p_time:.3f}s")
        elif o_time and p_time is None:
            print(f"  {size:<8} {qas_str}: original {o_time:.3f}s, optimized FAILED")
        else:
            print(f"  {size:<8} {qas_str}: both FAILED on this size")

    # Data scan reduction analysis
    print("\n--- DATA SCAN REDUCTION ---")
    for (size, qas), variants in sorted(by_config.items()):
        orig = variants.get("original", {})
        opt = variants.get("optimized", {})
        qas_str = "QAS=ON " if qas else "QAS=OFF"

        o_runs = [r for r in orig.get("runs", [])[1:] if r.get("status") == "OK"]
        p_runs = [r for r in opt.get("runs", [])[1:] if r.get("status") == "OK"]
        if o_runs and p_runs:
            o_scan = o_runs[-1].get("metrics", {}).get("mb_scanned", 0)
            p_scan = p_runs[-1].get("metrics", {}).get("mb_scanned", 0)
            o_prune = o_runs[-1].get("metrics", {}).get("pruning_pct", 0) or 0
            p_prune = p_runs[-1].get("metrics", {}).get("pruning_pct", 0) or 0
            if o_scan > 0:
                reduction = (1 - p_scan / o_scan) * 100
                print(f"  {size:<8} {qas_str}: {o_scan:.0f}MB -> {p_scan:.0f}MB "
                      f"({reduction:.1f}% less data scanned)")
                print(f"    Original pruning: {o_prune:.1f}% | Optimized pruning: {p_prune:.1f}%")

    # Business case
    print("\n--- BUSINESS CASE ---")
    credit_map = {"XSMALL": 1, "SMALL": 2, "MEDIUM": 4, "LARGE": 8, "XLARGE": 16}

    # Find smallest warehouse where optimized works
    opt_smallest = None
    for b in benchmarks:
        if b["variant"] == "optimized" and b["status"] == "OK":
            opt_smallest = b
            break

    # Find smallest warehouse where original works
    orig_smallest = None
    for b in benchmarks:
        if b["variant"] == "original" and b["status"] == "OK":
            orig_smallest = b
            break

    if opt_smallest:
        size = opt_smallest["warehouse_size"]
        credits = credit_map.get(size, "?")
        print(f"  Smallest viable warehouse for optimized Q21: {size} ({credits} credits/hr)")
        print(f"  Optimized time: {opt_smallest['avg_time_s']:.3f}s")

    if orig_smallest:
        size = orig_smallest["warehouse_size"]
        credits = credit_map.get(size, "?")
        print(f"  Smallest viable warehouse for original Q21:  {size} ({credits} credits/hr)")
        print(f"  Original time: {orig_smallest['avg_time_s']:.3f}s")

    if opt_smallest and orig_smallest:
        opt_credits = credit_map.get(opt_smallest["warehouse_size"], 0)
        orig_credits = credit_map.get(orig_smallest["warehouse_size"], 0)
        if orig_credits > 0:
            savings_pct = (1 - opt_credits / orig_credits) * 100
            print(f"\n  CREDIT SAVINGS: {orig_credits} -> {opt_credits} credits/hr "
                  f"({savings_pct:.0f}% reduction)")
            # Per-query cost (credits * seconds / 3600)
            if orig_smallest["avg_time_s"] and opt_smallest["avg_time_s"]:
                orig_cost = orig_credits * orig_smallest["avg_time_s"] / 3600
                opt_cost = opt_credits * opt_smallest["avg_time_s"] / 3600
                print(f"  Per-query cost: {orig_cost:.6f} -> {opt_cost:.6f} credits "
                      f"({(1 - opt_cost / orig_cost) * 100:.1f}% cheaper)")
    elif opt_smallest and not orig_smallest:
        print(f"\n  Original query CANNOT RUN on any tested size!")
        print(f"  Optimized runs on {opt_smallest['warehouse_size']} — "
              f"infinite ROI (enables workload that was impossible)")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    print("=" * 70)
    print(f"{QUERY_ID.upper()} WAREHOUSE DOWNSIZING PROOF OF CONCEPT")
    print(f"Started: {datetime.now().isoformat()}")
    print("=" * 70)

    conn = get_connection()
    print("Connected to Snowflake.\n")

    # CRITICAL: Disable result cache — otherwise every run after the first
    # returns from cache in ~80ms and we measure nothing
    cur = conn.cursor()
    cur.execute("ALTER SESSION SET USE_CACHED_RESULT = FALSE")
    print("[CONFIG] Result cache DISABLED (USE_CACHED_RESULT = FALSE)")
    cur.close()

    all_data = {
        "query_id": QUERY_ID.upper(),
        "started": datetime.now().isoformat(),
        "theory": "Query rewrite + clustering + QAS enables 2-4x warehouse downsizing",
        "result_cache": "DISABLED",
    }

    try:
        # Phase 1: Diagnostics
        diag = run_diagnostics(conn)
        all_data["diagnostics"] = diag

        # Phase 1b: EXPLAIN plans (capture query plans for both variants)
        explains = capture_explain_plans(conn)
        all_data["explain_plans"] = explains

        # Phase 2: Equivalence (MUST pass before benchmarking)
        equiv = verify_equivalence(conn)
        all_data["equivalence"] = equiv

        if not equiv["verified"]:
            print("\n!!! EQUIVALENCE FAILED — aborting benchmark !!!")
            print("Fix the rewrite before proceeding.")
            # Still save what we have
            ts = datetime.now().strftime("%Y%m%d_%H%M%S")
            with open(RESULTS_DIR / f"{QUERY_ID}_FAILED_{ts}.json", "w") as f:
                json.dump(all_data, f, indent=2, default=str)
            return

        # Phase 3: Benchmark (only if equivalence verified)
        benchmarks = run_benchmarks(conn)
        all_data["benchmarks"] = benchmarks
        all_data["finished"] = datetime.now().isoformat()

        # Print summary
        print_final_summary(equiv, benchmarks)

        # Save full results
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        result_path = RESULTS_DIR / f"{QUERY_ID}_proof_{ts}.json"
        with open(result_path, "w") as f:
            json.dump(all_data, f, indent=2, default=str)
        print(f"\nFull results saved to {result_path}")

    finally:
        # Restore to X-Small
        try:
            wh = parse_dsn(DSN)["warehouse"]
            cur = conn.cursor()
            cur.execute(f"ALTER WAREHOUSE {wh} SET WAREHOUSE_SIZE = 'XSMALL'")
            cur.execute(f"ALTER WAREHOUSE {wh} SET ENABLE_QUERY_ACCELERATION = FALSE")
            cur.close()
            print(f"\n[CLEANUP] Warehouse restored to XSMALL, QAS=OFF")
        except Exception:
            pass
        conn.close()
        print("Done.")


if __name__ == "__main__":
    main()
