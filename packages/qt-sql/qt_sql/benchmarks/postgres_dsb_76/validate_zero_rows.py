"""Validate ZERO_ROWS queries by stripping the final SELECT's WHERE clause via AST.

Strategy:
  1. Remove WHERE from the outermost SELECT in both original and patch SQL (CTE WHEREs preserved)
  2. Transpile PG->DuckDB
  3. Use EXCEPT set operator in DuckDB to check equivalence (no rows fetched into Python)
     - (orig EXCEPT patch) should be 0 rows
     - (patch EXCEPT orig) should be 0 rows
  4. One connection per query to avoid OOM cascading
"""
import json
import sys
import time
from pathlib import Path

import duckdb
import sqlglot
from sqlglot import exp

DUCKDB_PATH = "/mnt/d/TPC-DS/tpcds_sf100.duckdb"
BEST_DIR = Path(__file__).parent / "beam_sessions" / "run_beam_20260218_best"
QUERIES_DIR = Path(__file__).parent / "queries"

# The 14 queries that returned 0 rows on TPC-DS SF100 data
ZERO_ROWS_QUERIES = [
    "query001_multi_i1",
    "query001_multi_i2",
    "query010_multi_i1",
    "query010_multi_i2",
    "query030_multi_i1",
    "query030_multi_i2",
    "query031_multi_i2",
    "query054_multi_i1",
    "query059_multi_i1",
    "query064_multi_i2",
    "query065_multi_i1",
    "query083_multi_i1",
    "query083_multi_i2",
    "query102_agg_i1",
]


def strip_final_where(sql: str) -> str:
    """Remove WHERE, LIMIT, ORDER BY from the outermost SELECT only.

    Preserves CTE WHERE clauses.
    """
    try:
        parsed = sqlglot.parse_one(sql, read="postgres")
    except Exception:
        return sql

    outer_select = None
    if isinstance(parsed, exp.Select):
        outer_select = parsed
    elif hasattr(parsed, "this") and isinstance(parsed.this, exp.Select):
        outer_select = parsed.this

    if outer_select is None:
        return sql

    for node_type in (exp.Where, exp.Limit, exp.Order):
        node = outer_select.find(node_type, bfs=False)
        if node and node.parent is outer_select:
            node.pop()

    return parsed.sql(dialect="postgres")


def transpile_pg_to_duckdb(sql: str) -> str:
    try:
        return sqlglot.transpile(sql, read="postgres", write="duckdb")[0]
    except Exception:
        return sql


def validate_query(qid: str, orig_sql: str, patch_sql: str):
    """Validate one query using EXCEPT-based set comparison in DuckDB.

    Returns result dict. Opens/closes its own connection to avoid OOM buildup.
    """
    # Strip final WHERE from both
    orig_stripped = strip_final_where(orig_sql)
    patch_stripped = strip_final_where(patch_sql)

    # Transpile
    orig_duck = transpile_pg_to_duckdb(orig_stripped)
    patch_duck = transpile_pg_to_duckdb(patch_stripped)

    con = duckdb.connect(DUCKDB_PATH, read_only=True)
    try:
        # Step 1: Get row counts to confirm non-zero
        start = time.time()
        try:
            orig_count = con.execute(f"SELECT COUNT(*) FROM ({orig_duck})").fetchone()[0]
        except Exception as e:
            return {"query_id": qid, "status": "ORIG_ERROR", "error": str(e)[:200]}
        orig_ms = (time.time() - start) * 1000

        if orig_count == 0:
            return {"query_id": qid, "status": "STILL_ZERO", "original_rows": 0,
                    "original_ms": round(orig_ms, 1)}

        start = time.time()
        try:
            patch_count = con.execute(f"SELECT COUNT(*) FROM ({patch_duck})").fetchone()[0]
        except Exception as e:
            return {"query_id": qid, "status": "PATCH_ERROR", "error": str(e)[:200],
                    "original_rows": orig_count}
        patch_ms = (time.time() - start) * 1000

        if orig_count != patch_count:
            return {
                "query_id": qid, "status": "ROW_MISMATCH",
                "original_rows": orig_count, "patch_rows": patch_count,
                "original_ms": round(orig_ms, 1), "patch_ms": round(patch_ms, 1),
            }

        # Step 2: EXCEPT both directions to check exact equivalence
        # orig EXCEPT patch — rows in orig not in patch
        start = time.time()
        try:
            orig_minus_patch = con.execute(
                f"SELECT COUNT(*) FROM (({orig_duck}) EXCEPT ({patch_duck}))"
            ).fetchone()[0]
        except Exception as e:
            return {"query_id": qid, "status": "EXCEPT_ERROR", "error": str(e)[:200],
                    "original_rows": orig_count, "patch_rows": patch_count}
        except_ms_1 = (time.time() - start) * 1000

        # patch EXCEPT orig — rows in patch not in orig
        start = time.time()
        try:
            patch_minus_orig = con.execute(
                f"SELECT COUNT(*) FROM (({patch_duck}) EXCEPT ({orig_duck}))"
            ).fetchone()[0]
        except Exception as e:
            return {"query_id": qid, "status": "EXCEPT_ERROR", "error": str(e)[:200],
                    "original_rows": orig_count, "patch_rows": patch_count}
        except_ms_2 = (time.time() - start) * 1000

        if orig_minus_patch == 0 and patch_minus_orig == 0:
            status = "PASS"
        else:
            status = "SET_MISMATCH"

        return {
            "query_id": qid, "status": status,
            "original_rows": orig_count, "patch_rows": patch_count,
            "orig_minus_patch": orig_minus_patch,
            "patch_minus_orig": patch_minus_orig,
            "original_ms": round(orig_ms, 1), "patch_ms": round(patch_ms, 1),
            "except_ms": round(except_ms_1 + except_ms_2, 1),
        }
    finally:
        con.close()


def main():
    results = []

    print(f"Validating {len(ZERO_ROWS_QUERIES)} ZERO_ROWS queries (WHERE stripped, EXCEPT comparison)...")
    print(f"{'#':<3} {'QUERY':<25} {'SPD':>7} {'STATUS':<14} {'ORIG_ROWS':>10} {'PATCH_ROWS':>10} {'DIFF':>8} {'TIME':>8}")
    print("-" * 95)

    for i, qid in enumerate(ZERO_ROWS_QUERIES, 1):
        # Load original SQL
        orig_path = QUERIES_DIR / f"{qid}.sql"
        if not orig_path.exists():
            print(f"{i:<3} {qid:<25} {'':>7} SKIP_NO_SQL")
            continue
        original_sql = orig_path.read_text().strip()

        # Load patch SQL from result
        result_path = BEST_DIR / qid / "iter0_result.txt"
        if not result_path.exists():
            print(f"{i:<3} {qid:<25} {'':>7} SKIP_NO_RES")
            continue
        with open(result_path) as f:
            data = json.load(f)

        speedup = data.get("best_speedup", 0.0)
        best_pid = data.get("best_patch_id", "")
        patch_sql = None
        for p in data.get("patches", []):
            if p.get("patch_id") == best_pid and p.get("output_sql"):
                patch_sql = p["output_sql"]
                break
        if not patch_sql:
            print(f"{i:<3} {qid:<25} {speedup:>6.1f}x SKIP_NO_PATCH")
            continue

        r = validate_query(qid, original_sql, patch_sql)
        r["speedup"] = speedup
        results.append(r)

        status = r["status"]
        orig_rows = r.get("original_rows", "?")
        patch_rows = r.get("patch_rows", "?")
        diff = r.get("orig_minus_patch", 0) + r.get("patch_minus_orig", 0)
        total_ms = r.get("original_ms", 0) + r.get("patch_ms", 0) + r.get("except_ms", 0)
        err = r.get("error", "")

        if status in ("ORIG_ERROR", "PATCH_ERROR", "EXCEPT_ERROR"):
            print(f"{i:<3} {qid:<25} {speedup:>6.1f}x {status:<14} {err[:40]}")
        elif status == "STILL_ZERO":
            print(f"{i:<3} {qid:<25} {speedup:>6.1f}x {status:<14} (no rows even without final WHERE)")
        else:
            print(f"{i:<3} {qid:<25} {speedup:>6.1f}x {status:<14} {str(orig_rows):>10} {str(patch_rows):>10} {diff:>8} {total_ms:>7.0f}ms")
        sys.stdout.flush()

    # Summary
    pass_n = sum(1 for r in results if r["status"] == "PASS")
    fail_n = sum(1 for r in results if r["status"] in ("SET_MISMATCH", "ROW_MISMATCH"))
    err_n = sum(1 for r in results if r["status"] in ("ORIG_ERROR", "PATCH_ERROR", "EXCEPT_ERROR"))
    zero_n = sum(1 for r in results if r["status"] == "STILL_ZERO")

    print(f"\nPASS: {pass_n}  |  FAIL: {fail_n}  |  ERROR: {err_n}  |  STILL_ZERO: {zero_n}  |  TOTAL: {len(results)}")

    out_path = Path(__file__).parent / "beam_sessions" / "zero_rows_validation_20260219.json"
    with open(out_path, "w") as f:
        json.dump(results, f, indent=2)
    print(f"Saved to {out_path}")


if __name__ == "__main__":
    main()
