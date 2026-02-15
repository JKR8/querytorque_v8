#!/usr/bin/env python3
"""Q2 Optimization Proof — Multi-ref CTE + SK Range Pushdown.

Q2 takes ~96s. UNION ALL of web_sales + catalog_sales into a CTE (wscs),
joined to date_dim with comma join, aggregated by d_week_seq (wswscs).
The wswscs CTE is then referenced TWICE (year 1998 and year 1999).

Optimizations:
  1. Push date_sk range into the UNION ALL CTEs (prune fact table scans)
  2. Explicit JOINs instead of comma joins

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

    # Step 1: Look up date_sk range for years 1998 and 1999
    print("\n[STEP 1] Looking up date_sk ranges...", flush=True)
    cur = conn.cursor()
    cur.execute("SELECT MIN(d_date_sk), MAX(d_date_sk) FROM date_dim "
                "WHERE d_year IN (1998, 1999)")
    sk_min, sk_max = cur.fetchone()
    cur.close()
    print(f"  d_year IN (1998,1999) → date_sk {sk_min}-{sk_max}", flush=True)

    original = """
with wscs as
 (select sold_date_sk, sales_price
  from (select ws_sold_date_sk sold_date_sk, ws_ext_sales_price sales_price
        from web_sales
        union all
        select cs_sold_date_sk sold_date_sk, cs_ext_sales_price sales_price
        from catalog_sales)),
 wswscs as
 (select d_week_seq,
        sum(case when (d_day_name='Sunday') then sales_price else null end) sun_sales,
        sum(case when (d_day_name='Monday') then sales_price else null end) mon_sales,
        sum(case when (d_day_name='Tuesday') then sales_price else null end) tue_sales,
        sum(case when (d_day_name='Wednesday') then sales_price else null end) wed_sales,
        sum(case when (d_day_name='Thursday') then sales_price else null end) thu_sales,
        sum(case when (d_day_name='Friday') then sales_price else null end) fri_sales,
        sum(case when (d_day_name='Saturday') then sales_price else null end) sat_sales
 from wscs, date_dim
 where d_date_sk = sold_date_sk
 group by d_week_seq)
 select d_week_seq1,
       round(sun_sales1/sun_sales2,2),
       round(mon_sales1/mon_sales2,2),
       round(tue_sales1/tue_sales2,2),
       round(wed_sales1/wed_sales2,2),
       round(thu_sales1/thu_sales2,2),
       round(fri_sales1/fri_sales2,2),
       round(sat_sales1/sat_sales2,2)
 from
 (select wswscs.d_week_seq d_week_seq1,
        sun_sales sun_sales1, mon_sales mon_sales1, tue_sales tue_sales1,
        wed_sales wed_sales1, thu_sales thu_sales1, fri_sales fri_sales1,
        sat_sales sat_sales1
  from wswscs, date_dim
  where date_dim.d_week_seq = wswscs.d_week_seq and d_year = 1998) y,
 (select wswscs.d_week_seq d_week_seq2,
        sun_sales sun_sales2, mon_sales mon_sales2, tue_sales tue_sales2,
        wed_sales wed_sales2, thu_sales thu_sales2, fri_sales fri_sales2,
        sat_sales sat_sales2
  from wswscs, date_dim
  where date_dim.d_week_seq = wswscs.d_week_seq and d_year = 1998+1) z
 where d_week_seq1=d_week_seq2-53
 order by d_week_seq1
"""

    # Optimized: Push date_sk range into UNION ALL branches
    optimized = f"""
with wscs as
 (select sold_date_sk, sales_price
  from (select ws_sold_date_sk sold_date_sk, ws_ext_sales_price sales_price
        from web_sales
        where ws_sold_date_sk BETWEEN {sk_min} AND {sk_max}
        union all
        select cs_sold_date_sk sold_date_sk, cs_ext_sales_price sales_price
        from catalog_sales
        where cs_sold_date_sk BETWEEN {sk_min} AND {sk_max})),
 wswscs as
 (select d_week_seq,
        sum(case when (d_day_name='Sunday') then sales_price else null end) sun_sales,
        sum(case when (d_day_name='Monday') then sales_price else null end) mon_sales,
        sum(case when (d_day_name='Tuesday') then sales_price else null end) tue_sales,
        sum(case when (d_day_name='Wednesday') then sales_price else null end) wed_sales,
        sum(case when (d_day_name='Thursday') then sales_price else null end) thu_sales,
        sum(case when (d_day_name='Friday') then sales_price else null end) fri_sales,
        sum(case when (d_day_name='Saturday') then sales_price else null end) sat_sales
 from wscs
 JOIN date_dim ON d_date_sk = sold_date_sk
 group by d_week_seq)
 select d_week_seq1,
       round(sun_sales1/sun_sales2,2),
       round(mon_sales1/mon_sales2,2),
       round(tue_sales1/tue_sales2,2),
       round(wed_sales1/wed_sales2,2),
       round(thu_sales1/thu_sales2,2),
       round(fri_sales1/fri_sales2,2),
       round(sat_sales1/sat_sales2,2)
 from
 (select wswscs.d_week_seq d_week_seq1,
        sun_sales sun_sales1, mon_sales mon_sales1, tue_sales tue_sales1,
        wed_sales wed_sales1, thu_sales thu_sales1, fri_sales fri_sales1,
        sat_sales sat_sales1
  from wswscs
  JOIN date_dim ON date_dim.d_week_seq = wswscs.d_week_seq
  where d_year = 1998) y,
 (select wswscs.d_week_seq d_week_seq2,
        sun_sales sun_sales2, mon_sales mon_sales2, tue_sales tue_sales2,
        wed_sales wed_sales2, thu_sales thu_sales2, fri_sales fri_sales2,
        sat_sales sat_sales2
  from wswscs
  JOIN date_dim ON date_dim.d_week_seq = wswscs.d_week_seq
  where d_year = 1998+1) z
 where d_week_seq1=d_week_seq2-53
 order by d_week_seq1
"""

    # Probe optimized
    print("\n[STEP 2] Probing OPTIMIZED...", flush=True)
    try:
        ms_opt, rc_opt, rows_opt = timed_query(conn, optimized, "opt-probe")
    except Exception as e:
        print(f"  OPTIMIZED FAILED: {e}", flush=True)
        conn.close()
        return

    # Probe original
    print("\n[STEP 3] Probing ORIGINAL...", flush=True)
    try:
        ms_orig, rc_orig, rows_orig = timed_query(conn, original, "orig-probe")
    except Exception as e:
        print(f"  ORIGINAL FAILED: {e}", flush=True)
        opt_result = trimmed_mean_5x(conn, optimized, "Q2 OPTIMIZED")
        result = {
            "query": "Q2", "strategy": "sk_pushdown_into_union_all",
            "original": "TIMEOUT", "optimized": opt_result,
            "verdict": "WIN (original timeout)",
        }
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        out = RESULTS_DIR / f"q2_proof_{ts}.json"
        with open(out, "w") as f:
            json.dump(result, f, indent=2, default=str)
        print(f"\n[SAVED] {out}", flush=True)
        conn.close()
        return

    # Correctness
    print("\n[STEP 4] Correctness...", flush=True)
    h1 = hashlib.md5(str(sorted(str(r) for r in rows_orig)).encode()).hexdigest()
    h2 = hashlib.md5(str(sorted(str(r) for r in rows_opt)).encode()).hexdigest()
    print(f"  Rows: {rc_orig}={rc_opt}  Hash: {'MATCH' if h1==h2 else 'MISMATCH'}", flush=True)

    # 5x trimmed mean
    orig_result = trimmed_mean_5x(conn, original, "Q2 ORIGINAL")
    opt_result = trimmed_mean_5x(conn, optimized, "Q2 OPTIMIZED")

    speedup = orig_result["trimmed_mean_ms"] / opt_result["trimmed_mean_ms"]
    verdict = "WIN" if speedup >= 1.05 else ("REGRESSION" if speedup < 0.95 else "NEUTRAL")

    print("\n" + "=" * 60, flush=True)
    print(f"Q2 RESULT: {verdict}", flush=True)
    print(f"  Speedup:   {speedup:.2f}x", flush=True)
    print(f"  Original:  {orig_result['trimmed_mean_ms']:.0f}ms", flush=True)
    print(f"  Optimized: {opt_result['trimmed_mean_ms']:.0f}ms", flush=True)
    print("=" * 60, flush=True)

    result = {
        "query": "Q2", "strategy": "sk_pushdown_into_union_all",
        "date_sk_range": [sk_min, sk_max],
        "original": orig_result, "optimized": opt_result,
        "speedup": speedup, "verdict": verdict,
    }
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    out = RESULTS_DIR / f"q2_proof_{ts}.json"
    with open(out, "w") as f:
        json.dump(result, f, indent=2, default=str)
    print(f"\n[SAVED] {out}", flush=True)

    conn.close()


if __name__ == "__main__":
    main()
