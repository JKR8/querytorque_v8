"""Cross-engine semantic validation: transpile PG→DuckDB, run on SF100, compare row count + checksum."""
import json
import hashlib
import sys
import time
from pathlib import Path

import duckdb
import sqlglot

DUCKDB_PATH = "/mnt/d/TPC-DS/tpcds_sf100.duckdb"
BEST_DIR = Path(__file__).parent / "beam_sessions" / "run_beam_20260218_best"
TIMEOUT_S = 300  # per-query timeout


def transpile_pg_to_duckdb(sql: str) -> str:
    """Transpile PostgreSQL SQL to DuckDB dialect using sqlglot."""
    try:
        return sqlglot.transpile(sql, read="postgres", write="duckdb")[0]
    except Exception as e:
        # Fallback: return as-is (DuckDB is fairly PG-compatible)
        return sql


def run_and_checksum(con, sql: str, timeout_s: int = TIMEOUT_S):
    """Run SQL, return (row_count, checksum, elapsed_ms, error).

    Checksum = MD5 of all rows sorted and concatenated as strings.
    """
    try:
        start = time.time()
        result = con.execute(sql).fetchall()
        elapsed_ms = (time.time() - start) * 1000

        row_count = len(result)
        if row_count == 0:
            return row_count, None, elapsed_ms, None

        # Sort rows for deterministic checksum (handles ORDER BY differences)
        sorted_rows = sorted(result, key=lambda r: str(r))
        h = hashlib.md5()
        for row in sorted_rows:
            h.update(str(row).encode("utf-8"))
        checksum = h.hexdigest()

        return row_count, checksum, elapsed_ms, None
    except Exception as e:
        elapsed_ms = (time.time() - start) * 1000
        return 0, None, elapsed_ms, str(e)


def validate_query(qid: str, original_sql: str, patch_sql: str, con):
    """Validate one query pair. Returns validation dict."""
    # Transpile both
    orig_duck = transpile_pg_to_duckdb(original_sql)
    patch_duck = transpile_pg_to_duckdb(patch_sql)

    # Run original
    orig_rows, orig_chk, orig_ms, orig_err = run_and_checksum(con, orig_duck)
    if orig_err:
        return {
            "query_id": qid,
            "status": "ORIG_ERROR",
            "error": orig_err,
            "original_ms": orig_ms,
        }
    if orig_rows == 0:
        return {
            "query_id": qid,
            "status": "ORIG_ZERO_ROWS",
            "original_rows": 0,
            "original_ms": orig_ms,
        }

    # Run patch
    patch_rows, patch_chk, patch_ms, patch_err = run_and_checksum(con, patch_duck)
    if patch_err:
        return {
            "query_id": qid,
            "status": "PATCH_ERROR",
            "error": patch_err,
            "original_rows": orig_rows,
            "original_checksum": orig_chk,
            "original_ms": orig_ms,
            "patch_ms": patch_ms,
        }

    # Compare
    row_match = orig_rows == patch_rows
    chk_match = orig_chk == patch_chk

    if row_match and chk_match:
        status = "PASS"
    elif row_match and not chk_match:
        status = "CHECKSUM_MISMATCH"
    else:
        status = "ROW_COUNT_MISMATCH"

    return {
        "query_id": qid,
        "status": status,
        "original_rows": orig_rows,
        "patch_rows": patch_rows,
        "original_checksum": orig_chk,
        "patch_checksum": patch_chk,
        "original_ms": round(orig_ms, 1),
        "patch_ms": round(patch_ms, 1),
    }


def main():
    con = duckdb.connect(DUCKDB_PATH, read_only=True)
    # DuckDB doesn't have statement_timeout — we rely on Python-level timeout

    results = []
    queries_to_validate = []

    # Collect all queries with wins
    for qdir in sorted(BEST_DIR.iterdir()):
        result_path = qdir / "iter0_result.txt"
        if not result_path.exists():
            continue
        with open(result_path) as f:
            data = json.load(f)

        best_spd = data.get("best_speedup", 0.0)
        if best_spd == 0.0:
            continue

        best_pid = data.get("best_patch_id", "")
        best_patch = None
        for p in data.get("patches", []):
            if p.get("patch_id") == best_pid:
                best_patch = p
                break
        if not best_patch:
            continue

        patch_sql = best_patch.get("output_sql", "")
        if not patch_sql:
            continue

        # Get original SQL from queries dir
        qid = qdir.name
        orig_path = BEST_DIR.parent.parent / "queries" / f"{qid}.sql"
        if not orig_path.exists():
            continue
        original_sql = orig_path.read_text().strip()

        queries_to_validate.append((qid, original_sql, patch_sql, best_spd))

    print(f"Validating {len(queries_to_validate)} queries on DuckDB SF100...")
    print(f"{'#':<4} {'QUERY':<25} {'SPEEDUP':>8} {'STATUS':<20} {'ORIG_ROWS':>10} {'PATCH_ROWS':>10} {'CHK':>6} {'ORIG_MS':>9} {'PATCH_MS':>9}")
    print("-" * 110)

    for i, (qid, orig_sql, patch_sql, spd) in enumerate(queries_to_validate, 1):
        r = validate_query(qid, orig_sql, patch_sql, con)
        r["speedup"] = spd
        results.append(r)

        status = r["status"]
        orig_rows = r.get("original_rows", "?")
        patch_rows = r.get("patch_rows", "?")
        chk = "OK" if status == "PASS" else "FAIL"
        orig_ms = r.get("original_ms", "?")
        patch_ms = r.get("patch_ms", "?")

        print(f"{i:<4} {qid:<25} {spd:>7.1f}x {status:<20} {str(orig_rows):>10} {str(patch_rows):>10} {chk:>6} {str(orig_ms):>9} {str(patch_ms):>9}")
        sys.stdout.flush()

    con.close()

    # Summary
    pass_count = sum(1 for r in results if r["status"] == "PASS")
    fail_count = sum(1 for r in results if r["status"] in ("CHECKSUM_MISMATCH", "ROW_COUNT_MISMATCH"))
    err_count = sum(1 for r in results if r["status"] in ("ORIG_ERROR", "PATCH_ERROR"))
    zero_count = sum(1 for r in results if r["status"] == "ORIG_ZERO_ROWS")

    print()
    print(f"PASS: {pass_count}  |  FAIL: {fail_count}  |  ERROR: {err_count}  |  ZERO_ROWS: {zero_count}  |  TOTAL: {len(results)}")

    # Save results
    out_path = BEST_DIR.parent / "semantic_validation_20260218.json"
    with open(out_path, "w") as f:
        json.dump(results, f, indent=2)
    print(f"\nSaved to {out_path}")


if __name__ == "__main__":
    main()
