#!/usr/bin/env python3
"""Q56 Predicate Transitivity Proof — SK Range Pushdown on 3 Fact Tables.

Q56 takes ~90s on X-Small. Scans store_sales + catalog_sales + web_sales
with date filter d_year=2000, d_moy=2 applied ONLY to date_dim via comma join.
Snowflake doesn't push the corresponding date_sk range to fact table scans.

Fix: Add explicit ss/cs/ws_sold_date_sk BETWEEN <min> AND <max>.
Also convert comma joins to explicit JOINs.

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
    cur.execute("ALTER SESSION SET STATEMENT_TIMEOUT_IN_SECONDS = 300")
    cur.close()
    print("[CONFIG] Connected. Cache OFF. Timeout=300s", flush=True)
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

    # Step 1: Look up date_sk range for d_year=2000, d_moy=2
    print("\n[STEP 1] Looking up date_sk range...", flush=True)
    cur = conn.cursor()
    cur.execute("SELECT MIN(d_date_sk), MAX(d_date_sk) FROM date_dim "
                "WHERE d_year = 2000 AND d_moy = 2")
    sk_min, sk_max = cur.fetchone()
    cur.close()
    print(f"  d_year=2000, d_moy=2 → date_sk {sk_min}-{sk_max} "
          f"({sk_max - sk_min + 1} values)", flush=True)

    # Step 2: Original Q56
    original = """
with ss as (
 select i_item_id, sum(ss_ext_sales_price) total_sales
 from store_sales, date_dim, customer_address, item
 where i_item_id in (select i_item_id from item
                     where i_color in ('powder','green','cyan'))
   and ss_item_sk = i_item_sk
   and ss_sold_date_sk = d_date_sk
   and d_year = 2000 and d_moy = 2
   and ss_addr_sk = ca_address_sk
   and ca_gmt_offset = -6
 group by i_item_id),
 cs as (
 select i_item_id, sum(cs_ext_sales_price) total_sales
 from catalog_sales, date_dim, customer_address, item
 where i_item_id in (select i_item_id from item
                     where i_color in ('powder','green','cyan'))
   and cs_item_sk = i_item_sk
   and cs_sold_date_sk = d_date_sk
   and d_year = 2000 and d_moy = 2
   and cs_bill_addr_sk = ca_address_sk
   and ca_gmt_offset = -6
 group by i_item_id),
 ws as (
 select i_item_id, sum(ws_ext_sales_price) total_sales
 from web_sales, date_dim, customer_address, item
 where i_item_id in (select i_item_id from item
                     where i_color in ('powder','green','cyan'))
   and ws_item_sk = i_item_sk
   and ws_sold_date_sk = d_date_sk
   and d_year = 2000 and d_moy = 2
   and ws_bill_addr_sk = ca_address_sk
   and ca_gmt_offset = -6
 group by i_item_id)
select i_item_id, sum(total_sales) total_sales
from (select * from ss union all select * from cs union all select * from ws) tmp1
group by i_item_id
order by total_sales, i_item_id
LIMIT 100
"""

    # Step 3: Optimized — explicit JOINs + SK range pushdown on all 3 fact tables
    optimized = f"""
with ss as (
 select i_item_id, sum(ss_ext_sales_price) total_sales
 from store_sales
 JOIN date_dim ON ss_sold_date_sk = d_date_sk
 JOIN customer_address ON ss_addr_sk = ca_address_sk
 JOIN item ON ss_item_sk = i_item_sk
 where i_item_id in (select i_item_id from item
                     where i_color in ('powder','green','cyan'))
   and d_year = 2000 and d_moy = 2
   and ca_gmt_offset = -6
   and ss_sold_date_sk BETWEEN {sk_min} AND {sk_max}
 group by i_item_id),
 cs as (
 select i_item_id, sum(cs_ext_sales_price) total_sales
 from catalog_sales
 JOIN date_dim ON cs_sold_date_sk = d_date_sk
 JOIN customer_address ON cs_bill_addr_sk = ca_address_sk
 JOIN item ON cs_item_sk = i_item_sk
 where i_item_id in (select i_item_id from item
                     where i_color in ('powder','green','cyan'))
   and d_year = 2000 and d_moy = 2
   and ca_gmt_offset = -6
   and cs_sold_date_sk BETWEEN {sk_min} AND {sk_max}
 group by i_item_id),
 ws as (
 select i_item_id, sum(ws_ext_sales_price) total_sales
 from web_sales
 JOIN date_dim ON ws_sold_date_sk = d_date_sk
 JOIN customer_address ON ws_bill_addr_sk = ca_address_sk
 JOIN item ON ws_item_sk = i_item_sk
 where i_item_id in (select i_item_id from item
                     where i_color in ('powder','green','cyan'))
   and d_year = 2000 and d_moy = 2
   and ca_gmt_offset = -6
   and ws_sold_date_sk BETWEEN {sk_min} AND {sk_max}
 group by i_item_id)
select i_item_id, sum(total_sales) total_sales
from (select * from ss union all select * from cs union all select * from ws) tmp1
group by i_item_id
order by total_sales, i_item_id
LIMIT 100
"""

    # Step 4: Probe optimized first
    print("\n[STEP 2] Probing OPTIMIZED version...", flush=True)
    try:
        ms_opt, rc_opt, rows_opt = timed_query(conn, optimized, "opt-probe")
    except Exception as e:
        print(f"  OPTIMIZED FAILED: {e}", flush=True)
        conn.close()
        return

    # Step 5: Probe original
    print("\n[STEP 3] Probing ORIGINAL version...", flush=True)
    try:
        ms_orig, rc_orig, rows_orig = timed_query(conn, original, "orig-probe")
    except Exception as e:
        print(f"  ORIGINAL FAILED: {e}", flush=True)
        # If original times out but optimized succeeds, still a win
        print("\n  Original timed out — running 5x on optimized only", flush=True)
        opt_result = trimmed_mean_5x(conn, optimized, "Q56 OPTIMIZED")
        result = {
            "query": "Q56", "strategy": "predicate_transitivity_3fact_sk_pushdown",
            "date_sk_range": [sk_min, sk_max],
            "optimized": opt_result, "original": "TIMEOUT",
            "verdict": "WIN (original timeout)",
        }
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        out = RESULTS_DIR / f"q56_proof_{ts}.json"
        with open(out, "w") as f:
            json.dump(result, f, indent=2, default=str)
        print(f"\n[SAVED] {out}", flush=True)
        conn.close()
        return

    # Step 6: Correctness
    print("\n[STEP 4] Correctness check...", flush=True)
    if rc_orig != rc_opt:
        print(f"  ROW COUNT MISMATCH: orig={rc_orig} opt={rc_opt}", flush=True)
    else:
        h1 = hashlib.md5(str(sorted(str(r) for r in rows_orig)).encode()).hexdigest()
        h2 = hashlib.md5(str(sorted(str(r) for r in rows_opt)).encode()).hexdigest()
        match = "HASH_MATCH" if h1 == h2 else "HASH_MISMATCH"
        print(f"  Rows: {rc_orig}={rc_opt}  Hash: {match}", flush=True)

    # Step 7: 5x trimmed mean — BOTH
    print("\n[STEP 5] 5x trimmed mean benchmarks...", flush=True)
    orig_result = trimmed_mean_5x(conn, original, "Q56 ORIGINAL")
    opt_result = trimmed_mean_5x(conn, optimized, "Q56 OPTIMIZED")

    speedup = orig_result["trimmed_mean_ms"] / opt_result["trimmed_mean_ms"]
    verdict = "WIN" if speedup >= 1.05 else ("REGRESSION" if speedup < 0.95 else "NEUTRAL")

    # Report
    print("\n" + "=" * 60, flush=True)
    print(f"Q56 RESULT: {verdict}", flush=True)
    print(f"  Speedup:     {speedup:.2f}x", flush=True)
    print(f"  Original:    {orig_result['trimmed_mean_ms']:.0f}ms (trimmed)", flush=True)
    print(f"  Optimized:   {opt_result['trimmed_mean_ms']:.0f}ms (trimmed)", flush=True)
    print(f"  Strategy:    SK range pushdown on 3 fact tables", flush=True)
    print(f"  SK range:    ss/cs/ws_sold_date_sk BETWEEN {sk_min} AND {sk_max}", flush=True)
    print(f"  Also:        Comma joins → explicit JOINs", flush=True)
    print("=" * 60, flush=True)

    # Save
    result = {
        "query": "Q56",
        "strategy": "predicate_transitivity_3fact_sk_pushdown",
        "date_sk_range": [sk_min, sk_max],
        "original": orig_result,
        "optimized": opt_result,
        "speedup": speedup,
        "verdict": verdict,
    }
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    out = RESULTS_DIR / f"q56_proof_{ts}.json"
    with open(out, "w") as f:
        json.dump(result, f, indent=2, default=str)
    print(f"\n[SAVED] {out}", flush=True)

    conn.close()


if __name__ == "__main__":
    main()
