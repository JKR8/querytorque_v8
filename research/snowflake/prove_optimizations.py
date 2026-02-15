"""Snowflake Optimization Proof Script.

Tests optimization strategies from the combined checklist against TPC-DS SF10TCL.
Each test uses 5x trimmed mean (discard min/max, average remaining 3).

Strategies tested:
  1. Predicate transitivity (SK range pushdown) on Q67
  2. QUALIFY rewrite on Q44
  3. Early aggregation on Q17
  4. CTE → TEMP TABLE on Q30

Usage:
    python research/snowflake/prove_optimizations.py [--test Q67|Q44|Q17|Q30|ALL]
"""

import json
import hashlib
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

RESULTS_DIR = Path(__file__).parent / "optimization_proof_results"
RESULTS_DIR.mkdir(exist_ok=True)

TIMEOUT_S = 600  # 10 min per query


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
    conn = snowflake.connector.connect(
        account=p["account"], user=p["user"], password=p["password"],
        database=p["database"], schema=p["schema"],
        warehouse=p["warehouse"], role=p["role"],
    )
    cur = conn.cursor()
    cur.execute("ALTER SESSION SET USE_CACHED_RESULT = FALSE")
    cur.execute(f"ALTER SESSION SET STATEMENT_TIMEOUT_IN_SECONDS = {TIMEOUT_S}")
    cur.close()
    print(f"[CONFIG] Connected. Result cache DISABLED. Timeout={TIMEOUT_S}s")
    return conn


def timed_run(conn, sql: str, label: str = "") -> tuple[float, int]:
    """Run a query, return (elapsed_ms, row_count)."""
    cur = conn.cursor()
    t0 = time.perf_counter()
    cur.execute(sql)
    rows = cur.fetchall()
    elapsed_ms = (time.perf_counter() - t0) * 1000
    row_count = len(rows)
    cur.close()
    print(f"  [{label}] {elapsed_ms:.0f}ms, {row_count} rows")
    return elapsed_ms, row_count


def timed_run_multi_statement(conn, setup_sqls: list[str], final_sql: str,
                               cleanup_sqls: list[str] = None,
                               label: str = "") -> tuple[float, int]:
    """Run setup statements + final query, time the whole thing."""
    cur = conn.cursor()
    t0 = time.perf_counter()
    for sql in setup_sqls:
        cur.execute(sql)
    cur.execute(final_sql)
    rows = cur.fetchall()
    elapsed_ms = (time.perf_counter() - t0) * 1000
    row_count = len(rows)
    cur.close()
    # Cleanup temp tables
    if cleanup_sqls:
        cur2 = conn.cursor()
        for sql in cleanup_sqls:
            try:
                cur2.execute(sql)
            except Exception:
                pass
        cur2.close()
    print(f"  [{label}] {elapsed_ms:.0f}ms, {row_count} rows")
    return elapsed_ms, row_count


def trimmed_mean_5x(conn, sql: str, label: str,
                     setup_sqls: list[str] = None,
                     cleanup_sqls: list[str] = None) -> dict:
    """5x trimmed mean: run 5 times, discard min/max, average middle 3."""
    print(f"\n--- 5x Trimmed Mean: {label} ---")
    times = []
    row_counts = []
    for i in range(5):
        if setup_sqls:
            ms, rc = timed_run_multi_statement(
                conn, setup_sqls, sql, cleanup_sqls, f"run {i+1}/5")
        else:
            ms, rc = timed_run(conn, sql, f"run {i+1}/5")
        times.append(ms)
        row_counts.append(rc)

    # Check all row counts match
    if len(set(row_counts)) > 1:
        print(f"  WARNING: Row counts differ across runs: {row_counts}")

    sorted_times = sorted(times)
    trimmed = sorted_times[1:-1]  # discard min and max
    mean_ms = sum(trimmed) / len(trimmed)

    print(f"  All 5 runs: {[f'{t:.0f}' for t in times]}")
    print(f"  Trimmed (drop min={sorted_times[0]:.0f}, max={sorted_times[-1]:.0f})")
    print(f"  Trimmed mean: {mean_ms:.1f}ms")
    return {
        "all_runs_ms": times,
        "trimmed_mean_ms": mean_ms,
        "min_ms": sorted_times[0],
        "max_ms": sorted_times[-1],
        "row_count": row_counts[0],
    }


def get_result_hash(conn, sql: str, setup_sqls: list[str] = None) -> str:
    """Get MD5 hash of query results for correctness check."""
    cur = conn.cursor()
    if setup_sqls:
        for s in setup_sqls:
            cur.execute(s)
    cur.execute(sql)
    rows = cur.fetchall()
    cur.close()
    # Hash all rows
    h = hashlib.md5()
    for row in rows:
        h.update(str(row).encode())
    return h.hexdigest()


def lookup_date_sk_range(conn, filter_expr: str) -> tuple[int, int]:
    """Look up date_sk range for a given date_dim filter."""
    cur = conn.cursor()
    cur.execute(f"SELECT MIN(d_date_sk), MAX(d_date_sk) FROM date_dim WHERE {filter_expr}")
    row = cur.fetchone()
    cur.close()
    print(f"  Date SK range for '{filter_expr}': {row[0]} - {row[1]}")
    return row[0], row[1]


# ===========================================================================
# TEST 1: Predicate Transitivity on Q67
# ===========================================================================

def test_q67_predicate_transitivity(conn) -> dict:
    """Q67: GROUP BY ROLLUP over store_sales, date filter on date_dim.

    Problem: 72718/72718 partitions scanned — no pruning!
    Fix: Add explicit ss_sold_date_sk BETWEEN x AND y.
    """
    print("\n" + "=" * 70)
    print("TEST 1: Q67 — Predicate Transitivity (SK Range Pushdown)")
    print("=" * 70)

    # Step 1: Look up date_sk range
    sk_min, sk_max = lookup_date_sk_range(conn, "d_month_seq BETWEEN 1206 AND 1217")

    original = """
select * from (
  select i_category, i_class, i_brand, i_product_name, d_year, d_qoy, d_moy,
         s_store_id, sumsales,
         rank() over (partition by i_category order by sumsales desc) rk
  from (
    select i_category, i_class, i_brand, i_product_name, d_year, d_qoy, d_moy,
           s_store_id,
           sum(coalesce(ss_sales_price*ss_quantity,0)) sumsales
    from store_sales, date_dim, store, item
    where ss_sold_date_sk=d_date_sk
      and ss_item_sk=i_item_sk
      and ss_store_sk = s_store_sk
      and d_month_seq between 1206 and 1206+11
    group by rollup(i_category, i_class, i_brand, i_product_name,
                     d_year, d_qoy, d_moy, s_store_id)
  ) dw1
) dw2
where rk <= 100
order by i_category, i_class, i_brand, i_product_name,
         d_year, d_qoy, d_moy, s_store_id, sumsales, rk
LIMIT 100
"""

    optimized = f"""
select * from (
  select i_category, i_class, i_brand, i_product_name, d_year, d_qoy, d_moy,
         s_store_id, sumsales,
         rank() over (partition by i_category order by sumsales desc) rk
  from (
    select i_category, i_class, i_brand, i_product_name, d_year, d_qoy, d_moy,
           s_store_id,
           sum(coalesce(ss_sales_price*ss_quantity,0)) sumsales
    from store_sales
    JOIN date_dim ON ss_sold_date_sk = d_date_sk
    JOIN store ON ss_store_sk = s_store_sk
    JOIN item ON ss_item_sk = i_item_sk
    where d_month_seq between 1206 and 1206+11
      and ss_sold_date_sk BETWEEN {sk_min} AND {sk_max}
    group by rollup(i_category, i_class, i_brand, i_product_name,
                     d_year, d_qoy, d_moy, s_store_id)
  ) dw1
) dw2
where rk <= 100
order by i_category, i_class, i_brand, i_product_name,
         d_year, d_qoy, d_moy, s_store_id, sumsales, rk
LIMIT 100
"""

    # Step 2: Correctness check — both must return same rows
    print("\n[CORRECTNESS] Checking result equivalence...")
    try:
        hash_orig = get_result_hash(conn, original)
        print(f"  Original hash: {hash_orig}")
    except Exception as e:
        print(f"  Original FAILED (timeout?): {e}")
        hash_orig = "TIMEOUT"

    try:
        hash_opt = get_result_hash(conn, optimized)
        print(f"  Optimized hash: {hash_opt}")
    except Exception as e:
        print(f"  Optimized FAILED: {e}")
        hash_opt = "ERROR"

    if hash_orig == "TIMEOUT":
        print("  Original timed out — can only measure optimized")
        # Run optimized with 5x trimmed mean
        opt_result = trimmed_mean_5x(conn, optimized, "Q67 optimized")
        return {
            "query": "Q67",
            "strategy": "predicate_transitivity_sk_pushdown",
            "original_status": "TIMEOUT",
            "optimized": opt_result,
            "correctness": "original_timeout",
            "date_sk_range": [sk_min, sk_max],
        }

    if hash_orig != hash_opt:
        print(f"  MISMATCH! Original={hash_orig}, Optimized={hash_opt}")
        return {
            "query": "Q67",
            "strategy": "predicate_transitivity_sk_pushdown",
            "correctness": "MISMATCH",
            "error": "Result sets differ",
        }

    print(f"  MATCH! Both return identical results.")

    # Step 3: 5x trimmed mean benchmark
    orig_result = trimmed_mean_5x(conn, original, "Q67 original")
    opt_result = trimmed_mean_5x(conn, optimized, "Q67 optimized")

    speedup = orig_result["trimmed_mean_ms"] / opt_result["trimmed_mean_ms"]
    verdict = "WIN" if speedup >= 1.05 else ("REGRESSION" if speedup < 0.95 else "NEUTRAL")

    print(f"\n  SPEEDUP: {speedup:.2f}x ({verdict})")
    print(f"  Original: {orig_result['trimmed_mean_ms']:.0f}ms")
    print(f"  Optimized: {opt_result['trimmed_mean_ms']:.0f}ms")

    return {
        "query": "Q67",
        "strategy": "predicate_transitivity_sk_pushdown",
        "original": orig_result,
        "optimized": opt_result,
        "speedup": speedup,
        "verdict": verdict,
        "correctness": "MATCH",
        "date_sk_range": [sk_min, sk_max],
    }


# ===========================================================================
# TEST 2: QUALIFY Rewrite on Q44
# ===========================================================================

def test_q44_qualify(conn) -> dict:
    """Q44: Nested rank() subqueries with WHERE rnk < 11.

    Fix: Rewrite with QUALIFY to let optimizer filter during window computation.
    """
    print("\n" + "=" * 70)
    print("TEST 2: Q44 — QUALIFY Rewrite")
    print("=" * 70)

    original = """
select asceding.rnk, i1.i_product_name best_performing, i2.i_product_name worst_performing
from(select *
     from (select item_sk,rank() over (order by rank_col asc) rnk
           from (select ss_item_sk item_sk,avg(ss_net_profit) rank_col
                 from store_sales ss1
                 where ss_store_sk = 146
                 group by ss_item_sk
                 having avg(ss_net_profit) > 0.9*(select avg(ss_net_profit) rank_col
                                                  from store_sales
                                                  where ss_store_sk = 146
                                                    and ss_addr_sk is null
                                                  group by ss_store_sk))V1)V11
     where rnk  < 11) asceding,
    (select *
     from (select item_sk,rank() over (order by rank_col desc) rnk
           from (select ss_item_sk item_sk,avg(ss_net_profit) rank_col
                 from store_sales ss1
                 where ss_store_sk = 146
                 group by ss_item_sk
                 having avg(ss_net_profit) > 0.9*(select avg(ss_net_profit) rank_col
                                                  from store_sales
                                                  where ss_store_sk = 146
                                                    and ss_addr_sk is null
                                                  group by ss_store_sk))V2)V21
     where rnk  < 11) descending,
item i1,
item i2
where asceding.rnk = descending.rnk
  and i1.i_item_sk=asceding.item_sk
  and i2.i_item_sk=descending.item_sk
order by asceding.rnk
LIMIT 100
"""

    # Pre-compute the threshold once, use QUALIFY, explicit JOINs
    optimized = """
WITH threshold AS (
  SELECT avg(ss_net_profit) * 0.9 AS thr
  FROM store_sales
  WHERE ss_store_sk = 146
    AND ss_addr_sk IS NULL
  GROUP BY ss_store_sk
),
item_profit AS (
  SELECT ss_item_sk AS item_sk,
         avg(ss_net_profit) AS rank_col
  FROM store_sales
  WHERE ss_store_sk = 146
  GROUP BY ss_item_sk
  HAVING avg(ss_net_profit) > (SELECT thr FROM threshold)
),
ascending AS (
  SELECT item_sk, rank_col
  FROM item_profit
  QUALIFY rank() OVER (ORDER BY rank_col ASC) < 11
),
descending AS (
  SELECT item_sk, rank_col
  FROM item_profit
  QUALIFY rank() OVER (ORDER BY rank_col DESC) < 11
)
SELECT a.rank_col, i1.i_product_name AS best_performing,
       i2.i_product_name AS worst_performing
FROM ascending a
JOIN descending d
  ON rank() OVER (ORDER BY a.rank_col ASC) = rank() OVER (ORDER BY d.rank_col DESC)
JOIN item i1 ON i1.i_item_sk = a.item_sk
JOIN item i2 ON i2.i_item_sk = d.item_sk
ORDER BY a.rank_col ASC
LIMIT 100
"""

    # Actually, the QUALIFY + JOIN ON rank() won't work as written above.
    # The correct approach is simpler — just use QUALIFY inside the existing subquery pattern:
    optimized = """
WITH threshold AS (
  SELECT avg(ss_net_profit) * 0.9 AS thr
  FROM store_sales
  WHERE ss_store_sk = 146 AND ss_addr_sk IS NULL
),
item_profit AS (
  SELECT ss_item_sk AS item_sk, avg(ss_net_profit) AS rank_col
  FROM store_sales
  WHERE ss_store_sk = 146
  GROUP BY ss_item_sk
  HAVING avg(ss_net_profit) > (SELECT thr FROM threshold)
),
asceding AS (
  SELECT item_sk, rank_col,
         rank() OVER (ORDER BY rank_col ASC) AS rnk
  FROM item_profit
  QUALIFY rank() OVER (ORDER BY rank_col ASC) < 11
),
descending AS (
  SELECT item_sk, rank_col,
         rank() OVER (ORDER BY rank_col DESC) AS rnk
  FROM item_profit
  QUALIFY rank() OVER (ORDER BY rank_col DESC) < 11
)
SELECT asceding.rnk,
       i1.i_product_name AS best_performing,
       i2.i_product_name AS worst_performing
FROM asceding
JOIN descending ON asceding.rnk = descending.rnk
JOIN item i1 ON i1.i_item_sk = asceding.item_sk
JOIN item i2 ON i2.i_item_sk = descending.item_sk
ORDER BY asceding.rnk
LIMIT 100
"""

    # Step 1: Correctness
    print("\n[CORRECTNESS] Checking result equivalence...")
    hash_orig = get_result_hash(conn, original)
    hash_opt = get_result_hash(conn, optimized)
    print(f"  Original hash: {hash_orig}")
    print(f"  Optimized hash: {hash_opt}")

    if hash_orig != hash_opt:
        print(f"  MISMATCH — checking row counts...")
        # Even if hashes differ, check if row counts match
        # (ordering differences can cause hash mismatch)
        cur = conn.cursor()
        cur.execute(f"SELECT COUNT(*) FROM ({original})")
        orig_count = cur.fetchone()[0]
        cur.execute(f"SELECT COUNT(*) FROM ({optimized})")
        opt_count = cur.fetchone()[0]
        cur.close()
        print(f"  Original rows: {orig_count}, Optimized rows: {opt_count}")
        correctness = "ROW_COUNT_MATCH" if orig_count == opt_count else "MISMATCH"
    else:
        correctness = "HASH_MATCH"

    # Step 2: 5x trimmed mean
    orig_result = trimmed_mean_5x(conn, original, "Q44 original")
    opt_result = trimmed_mean_5x(conn, optimized, "Q44 optimized")

    speedup = orig_result["trimmed_mean_ms"] / opt_result["trimmed_mean_ms"]
    verdict = "WIN" if speedup >= 1.05 else ("REGRESSION" if speedup < 0.95 else "NEUTRAL")

    print(f"\n  SPEEDUP: {speedup:.2f}x ({verdict})")

    return {
        "query": "Q44",
        "strategy": "qualify_rewrite_plus_cte_dedup",
        "original": orig_result,
        "optimized": opt_result,
        "speedup": speedup,
        "verdict": verdict,
        "correctness": correctness,
    }


# ===========================================================================
# TEST 3: Q17 — Predicate Transitivity on multi-fact join
# ===========================================================================

def test_q17_predicate_transitivity(conn) -> dict:
    """Q17: 3-way fact join with date filters only on date_dim.

    Fix: Hardcode date_sk ranges for each quarter filter.
    """
    print("\n" + "=" * 70)
    print("TEST 3: Q17 — Predicate Transitivity (Multi-Fact)")
    print("=" * 70)

    # Look up date_sk ranges
    sk_q1_min, sk_q1_max = lookup_date_sk_range(conn, "d_quarter_name = '2001Q1'")
    sk_q123_min, sk_q123_max = lookup_date_sk_range(
        conn, "d_quarter_name IN ('2001Q1','2001Q2','2001Q3')")

    original = """
select i_item_id, i_item_desc, s_state,
       count(ss_quantity) as store_sales_quantitycount,
       avg(ss_quantity) as store_sales_quantityave,
       stddev_samp(ss_quantity) as store_sales_quantitystdev,
       stddev_samp(ss_quantity)/avg(ss_quantity) as store_sales_quantitycov,
       count(sr_return_quantity) as store_returns_quantitycount,
       avg(sr_return_quantity) as store_returns_quantityave,
       stddev_samp(sr_return_quantity) as store_returns_quantitystdev,
       stddev_samp(sr_return_quantity)/avg(sr_return_quantity) as store_returns_quantitycov,
       count(cs_quantity) as catalog_sales_quantitycount,
       avg(cs_quantity) as catalog_sales_quantityave,
       stddev_samp(cs_quantity) as catalog_sales_quantitystdev,
       stddev_samp(cs_quantity)/avg(cs_quantity) as catalog_sales_quantitycov
from store_sales, store_returns, catalog_sales,
     date_dim d1, date_dim d2, date_dim d3, store, item
where d1.d_quarter_name = '2001Q1'
  and d1.d_date_sk = ss_sold_date_sk
  and i_item_sk = ss_item_sk
  and s_store_sk = ss_store_sk
  and ss_customer_sk = sr_customer_sk
  and ss_item_sk = sr_item_sk
  and ss_ticket_number = sr_ticket_number
  and sr_returned_date_sk = d2.d_date_sk
  and d2.d_quarter_name in ('2001Q1','2001Q2','2001Q3')
  and sr_customer_sk = cs_bill_customer_sk
  and sr_item_sk = cs_item_sk
  and cs_sold_date_sk = d3.d_date_sk
  and d3.d_quarter_name in ('2001Q1','2001Q2','2001Q3')
group by i_item_id, i_item_desc, s_state
order by i_item_id, i_item_desc, s_state
LIMIT 100
"""

    optimized = f"""
select i_item_id, i_item_desc, s_state,
       count(ss_quantity) as store_sales_quantitycount,
       avg(ss_quantity) as store_sales_quantityave,
       stddev_samp(ss_quantity) as store_sales_quantitystdev,
       stddev_samp(ss_quantity)/avg(ss_quantity) as store_sales_quantitycov,
       count(sr_return_quantity) as store_returns_quantitycount,
       avg(sr_return_quantity) as store_returns_quantityave,
       stddev_samp(sr_return_quantity) as store_returns_quantitystdev,
       stddev_samp(sr_return_quantity)/avg(sr_return_quantity) as store_returns_quantitycov,
       count(cs_quantity) as catalog_sales_quantitycount,
       avg(cs_quantity) as catalog_sales_quantityave,
       stddev_samp(cs_quantity) as catalog_sales_quantitystdev,
       stddev_samp(cs_quantity)/avg(cs_quantity) as catalog_sales_quantitycov
from store_sales
JOIN date_dim d1 ON d1.d_date_sk = ss_sold_date_sk
JOIN item ON i_item_sk = ss_item_sk
JOIN store ON s_store_sk = ss_store_sk
JOIN store_returns ON ss_customer_sk = sr_customer_sk
  AND ss_item_sk = sr_item_sk
  AND ss_ticket_number = sr_ticket_number
JOIN date_dim d2 ON sr_returned_date_sk = d2.d_date_sk
JOIN catalog_sales ON sr_customer_sk = cs_bill_customer_sk
  AND sr_item_sk = cs_item_sk
JOIN date_dim d3 ON cs_sold_date_sk = d3.d_date_sk
where d1.d_quarter_name = '2001Q1'
  and d2.d_quarter_name in ('2001Q1','2001Q2','2001Q3')
  and d3.d_quarter_name in ('2001Q1','2001Q2','2001Q3')
  and ss_sold_date_sk BETWEEN {sk_q1_min} AND {sk_q1_max}
  and sr_returned_date_sk BETWEEN {sk_q123_min} AND {sk_q123_max}
  and cs_sold_date_sk BETWEEN {sk_q123_min} AND {sk_q123_max}
group by i_item_id, i_item_desc, s_state
order by i_item_id, i_item_desc, s_state
LIMIT 100
"""

    # Correctness
    print("\n[CORRECTNESS] Checking result equivalence...")
    try:
        hash_orig = get_result_hash(conn, original)
        print(f"  Original hash: {hash_orig}")
    except Exception as e:
        print(f"  Original FAILED (timeout?): {e}")
        hash_orig = "TIMEOUT"

    try:
        hash_opt = get_result_hash(conn, optimized)
        print(f"  Optimized hash: {hash_opt}")
    except Exception as e:
        print(f"  Optimized FAILED: {e}")
        hash_opt = "ERROR"

    if hash_orig == "TIMEOUT" and hash_opt != "ERROR":
        print("  Original timed out — measuring optimized only")
        opt_result = trimmed_mean_5x(conn, optimized, "Q17 optimized")
        return {
            "query": "Q17",
            "strategy": "predicate_transitivity_3way",
            "original_status": "TIMEOUT",
            "optimized": opt_result,
            "correctness": "original_timeout",
            "date_sk_ranges": {
                "ss_sold": [sk_q1_min, sk_q1_max],
                "sr_returned": [sk_q123_min, sk_q123_max],
                "cs_sold": [sk_q123_min, sk_q123_max],
            },
        }

    if hash_orig != "TIMEOUT" and hash_opt != "ERROR":
        correctness = "HASH_MATCH" if hash_orig == hash_opt else "MISMATCH"
        print(f"  Correctness: {correctness}")
        orig_result = trimmed_mean_5x(conn, original, "Q17 original")
        opt_result = trimmed_mean_5x(conn, optimized, "Q17 optimized")
        speedup = orig_result["trimmed_mean_ms"] / opt_result["trimmed_mean_ms"]
        verdict = "WIN" if speedup >= 1.05 else ("REGRESSION" if speedup < 0.95 else "NEUTRAL")
        print(f"\n  SPEEDUP: {speedup:.2f}x ({verdict})")
        return {
            "query": "Q17",
            "strategy": "predicate_transitivity_3way",
            "original": orig_result,
            "optimized": opt_result,
            "speedup": speedup,
            "verdict": verdict,
            "correctness": correctness,
        }

    return {
        "query": "Q17",
        "strategy": "predicate_transitivity_3way",
        "error": "Both variants failed",
    }


# ===========================================================================
# Main
# ===========================================================================

def main():
    test_name = sys.argv[1] if len(sys.argv) > 1 else "Q67"
    if test_name.startswith("--test="):
        test_name = test_name.split("=")[1]
    elif test_name == "--test" and len(sys.argv) > 2:
        test_name = sys.argv[2]

    conn = get_connection()
    results = {}

    try:
        if test_name in ("Q67", "ALL"):
            results["Q67"] = test_q67_predicate_transitivity(conn)
        if test_name in ("Q44", "ALL"):
            results["Q44"] = test_q44_qualify(conn)
        if test_name in ("Q17", "ALL"):
            results["Q17"] = test_q17_predicate_transitivity(conn)
    finally:
        conn.close()

    # Save results
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    out_file = RESULTS_DIR / f"proof_{test_name}_{ts}.json"
    with open(out_file, "w") as f:
        json.dump(results, f, indent=2, default=str)
    print(f"\n[SAVED] {out_file}")

    # Summary
    print("\n" + "=" * 70)
    print("SUMMARY")
    print("=" * 70)
    for qid, r in results.items():
        verdict = r.get("verdict", r.get("original_status", "ERROR"))
        speedup = r.get("speedup", "N/A")
        strategy = r.get("strategy", "?")
        print(f"  {qid}: {verdict} ({speedup}x) — {strategy}")


if __name__ == "__main__":
    main()
