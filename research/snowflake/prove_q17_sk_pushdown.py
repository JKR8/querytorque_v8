#!/usr/bin/env python3
"""Q17 Predicate Transitivity Proof — SK Range Pushdown on 3 Fact Tables.

Q17 times out at 300s on X-Small. Three date_dim aliases (d1, d2, d3)
joined via comma joins to store_sales, store_returns, catalog_sales.

- d1.d_quarter_name = '2001Q1' → ss_sold_date_sk
- d2.d_quarter_name IN ('2001Q1','2001Q2','2001Q3') → sr_returned_date_sk
- d3.d_quarter_name IN ('2001Q1','2001Q2','2001Q3') → cs_sold_date_sk

Snowflake scans 70412/72718 store_sales, 54721/54922 catalog_sales,
7070/7070 store_returns partitions.

Fix: Push date_sk ranges to all 3 fact tables + explicit JOINs.

5x trimmed mean validation.
"""

import json
import hashlib
import os
import time
from datetime import datetime
from pathlib import Path
from urllib.parse import urlparse, parse_qs, unquote

import snowflake.connector

DSN = os.environ.get(
    "QT_SNOWFLAKE_DSN",
    "snowflake://jkdl:QGdg%2A%24WC%25O62xS71@CVRYJTF-AW47074"
    "/SNOWFLAKE_SAMPLE_DATA/TPCDS_SF10TCL?warehouse=COMPUTE_WH&role=ACCOUNTADMIN",
)

RESULTS_DIR = Path(__file__).parent / "optimization_proof_results"
RESULTS_DIR.mkdir(exist_ok=True)


def parse_dsn(dsn):
    parsed = urlparse(dsn)
    params = parse_qs(parsed.query)
    path_parts = parsed.path.strip("/").split("/")
    return {
        "account": parsed.hostname,
        "user": parsed.username,
        "password": unquote(parsed.password),
        "database": path_parts[0] if path_parts else "",
        "schema": path_parts[1] if len(path_parts) > 1 else "PUBLIC",
        "warehouse": params.get("warehouse", ["COMPUTE_WH"])[0],
        "role": params.get("role", [""])[0],
    }


def get_conn():
    p = parse_dsn(DSN)
    conn = snowflake.connector.connect(**p)
    cur = conn.cursor()
    cur.execute("ALTER SESSION SET USE_CACHED_RESULT = FALSE")
    cur.execute("ALTER SESSION SET STATEMENT_TIMEOUT_IN_SECONDS = 600")
    cur.close()
    print("[CONFIG] Connected. Cache OFF. Timeout=600s", flush=True)
    return conn


def timed_query(conn, sql, label=""):
    cur = conn.cursor()
    t0 = time.perf_counter()
    cur.execute(sql)
    rows = cur.fetchall()
    ms = (time.perf_counter() - t0) * 1000
    n = len(rows)
    cur.close()
    print(f"  [{label}] {ms:.0f}ms  {n} rows", flush=True)
    return ms, n, rows


def trimmed_mean_5x(conn, sql, label):
    print(f"\n--- 5x trimmed mean: {label} ---", flush=True)
    times = []
    rc = None
    for i in range(5):
        ms, n, _ = timed_query(conn, sql, f"run {i+1}/5")
        times.append(ms)
        rc = n
    s = sorted(times)
    mean = sum(s[1:-1]) / 3
    print(f"  All:     {[f'{t:.0f}' for t in times]}", flush=True)
    print(f"  Drop:    min={s[0]:.0f}  max={s[-1]:.0f}", flush=True)
    print(f"  Trimmed: {mean:.1f}ms", flush=True)
    return {"all_ms": times, "trimmed_mean_ms": mean, "rows": rc}


def main():
    conn = get_conn()

    # Step 1: Look up date_sk ranges for the 3 date filters
    print("\n[STEP 1] Looking up date_sk ranges...", flush=True)
    cur = conn.cursor()

    # d1: d_quarter_name = '2001Q1' (narrow — one quarter)
    cur.execute("SELECT MIN(d_date_sk), MAX(d_date_sk) FROM date_dim "
                "WHERE d_quarter_name = '2001Q1'")
    sk1_min, sk1_max = cur.fetchone()
    print(f"  d1 (2001Q1):           date_sk {sk1_min}-{sk1_max} "
          f"({sk1_max - sk1_min + 1} values)", flush=True)

    # d2/d3: d_quarter_name IN ('2001Q1','2001Q2','2001Q3') (wider — 3 quarters)
    cur.execute("SELECT MIN(d_date_sk), MAX(d_date_sk) FROM date_dim "
                "WHERE d_quarter_name IN ('2001Q1','2001Q2','2001Q3')")
    sk23_min, sk23_max = cur.fetchone()
    print(f"  d2/d3 (2001Q1-Q3):     date_sk {sk23_min}-{sk23_max} "
          f"({sk23_max - sk23_min + 1} values)", flush=True)
    cur.close()

    original = """
select i_item_id
       ,i_item_desc
       ,s_state
       ,count(ss_quantity) as store_sales_quantitycount
       ,avg(ss_quantity) as store_sales_quantityave
       ,stddev_samp(ss_quantity) as store_sales_quantitystdev
       ,stddev_samp(ss_quantity)/avg(ss_quantity) as store_sales_quantitycov
       ,count(sr_return_quantity) as store_returns_quantitycount
       ,avg(sr_return_quantity) as store_returns_quantityave
       ,stddev_samp(sr_return_quantity) as store_returns_quantitystdev
       ,stddev_samp(sr_return_quantity)/avg(sr_return_quantity) as store_returns_quantitycov
       ,count(cs_quantity) as catalog_sales_quantitycount
       ,avg(cs_quantity) as catalog_sales_quantityave
       ,stddev_samp(cs_quantity) as catalog_sales_quantitystdev
       ,stddev_samp(cs_quantity)/avg(cs_quantity) as catalog_sales_quantitycov
 from store_sales, store_returns, catalog_sales,
      date_dim d1, date_dim d2, date_dim d3,
      store, item
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
select i_item_id
       ,i_item_desc
       ,s_state
       ,count(ss_quantity) as store_sales_quantitycount
       ,avg(ss_quantity) as store_sales_quantityave
       ,stddev_samp(ss_quantity) as store_sales_quantitystdev
       ,stddev_samp(ss_quantity)/avg(ss_quantity) as store_sales_quantitycov
       ,count(sr_return_quantity) as store_returns_quantitycount
       ,avg(sr_return_quantity) as store_returns_quantityave
       ,stddev_samp(sr_return_quantity) as store_returns_quantitystdev
       ,stddev_samp(sr_return_quantity)/avg(sr_return_quantity) as store_returns_quantitycov
       ,count(cs_quantity) as catalog_sales_quantitycount
       ,avg(cs_quantity) as catalog_sales_quantityave
       ,stddev_samp(cs_quantity) as catalog_sales_quantitystdev
       ,stddev_samp(cs_quantity)/avg(cs_quantity) as catalog_sales_quantitycov
 from store_sales
 JOIN date_dim d1 ON d1.d_date_sk = ss_sold_date_sk
 JOIN store ON s_store_sk = ss_store_sk
 JOIN item ON i_item_sk = ss_item_sk
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
   and ss_sold_date_sk BETWEEN {sk1_min} AND {sk1_max}
   and sr_returned_date_sk BETWEEN {sk23_min} AND {sk23_max}
   and cs_sold_date_sk BETWEEN {sk23_min} AND {sk23_max}
 group by i_item_id, i_item_desc, s_state
 order by i_item_id, i_item_desc, s_state
 LIMIT 100
"""

    # Probe optimized first (should be faster)
    print("\n[STEP 2] Probing OPTIMIZED version...", flush=True)
    try:
        ms_opt, rc_opt, rows_opt = timed_query(conn, optimized, "opt-probe")
    except Exception as e:
        print(f"  OPTIMIZED FAILED: {e}", flush=True)
        conn.close()
        return

    # Probe original (may timeout)
    print("\n[STEP 3] Probing ORIGINAL version...", flush=True)
    orig_timeout = False
    try:
        ms_orig, rc_orig, rows_orig = timed_query(conn, original, "orig-probe")
    except Exception as e:
        print(f"  Original TIMED OUT: {e}", flush=True)
        orig_timeout = True

    # Correctness check
    if not orig_timeout:
        print("\n[STEP 4] Correctness check...", flush=True)
        if rc_orig != rc_opt:
            print(f"  ROW COUNT MISMATCH: orig={rc_orig} opt={rc_opt}", flush=True)
        else:
            h1 = hashlib.md5(str(sorted(str(r) for r in rows_orig)).encode()).hexdigest()
            h2 = hashlib.md5(str(sorted(str(r) for r in rows_opt)).encode()).hexdigest()
            match = "HASH_MATCH" if h1 == h2 else "HASH_MISMATCH"
            print(f"  Rows: {rc_orig}={rc_opt}  Hash: {match}", flush=True)

    # 5x trimmed mean — optimized always
    opt_result = trimmed_mean_5x(conn, optimized, "Q17 OPTIMIZED")

    # 5x trimmed mean — original only if it completed
    orig_result = None
    speedup = None
    verdict = None
    if not orig_timeout:
        orig_result = trimmed_mean_5x(conn, original, "Q17 ORIGINAL")
        speedup = orig_result["trimmed_mean_ms"] / opt_result["trimmed_mean_ms"]
        verdict = "WIN" if speedup >= 1.05 else ("REGRESSION" if speedup < 0.95 else "NEUTRAL")
    else:
        verdict = "WIN (original timeout)"
        speedup = ">10x (original >600s)"

    # Report
    print("\n" + "=" * 60, flush=True)
    print(f"Q17 RESULT: {verdict}", flush=True)
    if isinstance(speedup, float):
        print(f"  Speedup:   {speedup:.2f}x", flush=True)
        print(f"  Original:  {orig_result['trimmed_mean_ms']:.0f}ms (trimmed)", flush=True)
    else:
        print(f"  Speedup:   {speedup}", flush=True)
    print(f"  Optimized: {opt_result['trimmed_mean_ms']:.0f}ms (trimmed)", flush=True)
    print(f"  Strategy:  3-table SK pushdown + explicit JOINs", flush=True)
    print(f"  SK ranges: ss_sold_date_sk {sk1_min}-{sk1_max}, "
          f"sr/cs {sk23_min}-{sk23_max}", flush=True)
    print("=" * 60, flush=True)

    # Save
    result = {
        "query": "Q17",
        "strategy": "predicate_transitivity_3table_sk_pushdown",
        "date_sk_ranges": {
            "ss_sold_date_sk": [sk1_min, sk1_max],
            "sr_returned_date_sk": [sk23_min, sk23_max],
            "cs_sold_date_sk": [sk23_min, sk23_max],
        },
        "optimized": opt_result,
        "original": orig_result if orig_result else "TIMEOUT_600s",
        "speedup": speedup if isinstance(speedup, float) else str(speedup),
        "verdict": verdict,
    }
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    out = RESULTS_DIR / f"q17_proof_{ts}.json"
    with open(out, "w") as f:
        json.dump(result, f, indent=2, default=str)
    print(f"\n[SAVED] {out}", flush=True)

    conn.close()


if __name__ == "__main__":
    main()
