"""Test final optimizations on sample database."""

import time
import duckdb
import os

SAMPLE_DB = "/mnt/d/TPC-DS/tpcds_sf100_sampled_1pct.duckdb"
QUERY_DIR = "/mnt/d/TPC-DS/queries_duckdb_converted"


def benchmark(sql: str, runs: int = 3) -> tuple[float, list]:
    conn = duckdb.connect(SAMPLE_DB, read_only=True)
    times = []
    result = None
    for i in range(runs):
        start = time.time()
        try:
            result = conn.execute(sql).fetchall()
        except Exception as e:
            conn.close()
            return -1, str(e)
        times.append(time.time() - start)
    conn.close()
    return (sum(times[1:]) / len(times[1:]) if len(times) > 1 else times[0]), result


def compare(name: str, original: str, optimized: str):
    print(f"\n{'='*60}")
    print(f"{name}")
    print('='*60)

    orig_time, orig_result = benchmark(original)
    if orig_time < 0:
        print(f"Original FAILED: {orig_result}")
        return None, None

    opt_time, opt_result = benchmark(optimized)
    if opt_time < 0:
        print(f"Optimized FAILED: {opt_result}")
        return None, None

    speedup = orig_time / opt_time if opt_time > 0 else 0
    orig_sorted = sorted([str(r) for r in orig_result])
    opt_sorted = sorted([str(r) for r in opt_result])
    correct = (orig_sorted == opt_sorted)

    status = "" if correct else ""
    print(f"Original:  {orig_time:.3f}s | Optimized: {opt_time:.3f}s | {speedup:.2f}x {status}")
    return speedup, correct


def load_query(num):
    with open(os.path.join(QUERY_DIR, f"query_{num}.sql")) as f:
        return f.read()


# Verified optimizations
OPTIMIZATIONS = {
    15: {
        "name": "Q15: Date filter pushdown (catalog_sales)",
        "sql": """
WITH filtered_sales AS (
    SELECT cs_bill_customer_sk, cs_sales_price
    FROM catalog_sales, date_dim
    WHERE cs_sold_date_sk = d_date_sk
      AND d_qoy = 2 AND d_year = 2000
)
SELECT ca_zip, sum(cs_sales_price)
FROM filtered_sales fs, customer c, customer_address ca
WHERE fs.cs_bill_customer_sk = c.c_customer_sk
    AND c.c_current_addr_sk = ca.ca_address_sk
    AND (substring(ca_zip,1,5) in ('85669', '86197','88274','83405','86475',
                                   '85392', '85460', '80348', '81792')
         or ca_state in ('CA','WA','GA')
         or cs_sales_price > 500)
GROUP BY ca_zip
ORDER BY ca_zip
LIMIT 100;
"""
    },

    18: {
        "name": "Q18: Date filter pushdown (catalog_sales)",
        "sql": """
WITH filtered_sales AS (
    SELECT cs_bill_customer_sk, cs_bill_cdemo_sk, cs_item_sk,
           cs_quantity, cs_list_price, cs_sales_price,
           cs_coupon_amt, cs_net_profit
    FROM catalog_sales, date_dim
    WHERE cs_sold_date_sk = d_date_sk
      AND d_year = 2000
)
SELECT i_item_id, ca_country, ca_state, ca_county,
       avg(CAST(cs_quantity AS DECIMAL(12,2))) agg1,
       avg(CAST(cs_list_price AS DECIMAL(12,2))) agg2,
       avg(CAST(cs_coupon_amt AS DECIMAL(12,2))) agg3,
       avg(CAST(cs_sales_price AS DECIMAL(12,2))) agg4,
       avg(CAST(cs_net_profit AS DECIMAL(12,2))) agg5,
       avg(CAST(c_birth_year AS DECIMAL(12,2))) agg6,
       avg(CAST(cd1.cd_dep_count AS DECIMAL(12,2))) agg7
FROM filtered_sales cs, customer_demographics cd1, customer c,
     customer_address ca, item
WHERE cs.cs_bill_cdemo_sk = cd1.cd_demo_sk
  AND cs.cs_bill_customer_sk = c.c_customer_sk
  AND cd1.cd_gender = 'M'
  AND cd1.cd_education_status = 'Unknown'
  AND c.c_current_cdemo_sk = cd1.cd_demo_sk
  AND c.c_current_addr_sk = ca.ca_address_sk
  AND c_birth_month IN (3,8,10,7,2,1)
  AND ca_state IN ('SD','NE','TX','IA','MS','WI','AL')
  AND cs.cs_item_sk = i_item_sk
GROUP BY ROLLUP(i_item_id, ca_country, ca_state, ca_county)
ORDER BY ca_country, ca_state, ca_county, i_item_id
LIMIT 100;
"""
    },

    # Q3 - simple, DuckDB optimizes well already

    7: {
        "name": "Q7: Date filter pushdown (store_sales)",
        "sql": """
WITH filtered_sales AS (
    SELECT ss_item_sk, ss_cdemo_sk, ss_promo_sk,
           ss_quantity, ss_list_price, ss_coupon_amt, ss_sales_price
    FROM store_sales, date_dim
    WHERE ss_sold_date_sk = d_date_sk
      AND d_year = 1998
)
SELECT i_item_id,
       avg(ss_quantity) agg1,
       avg(ss_list_price) agg2,
       avg(ss_coupon_amt) agg3,
       avg(ss_sales_price) agg4
FROM filtered_sales ss, customer_demographics, item, promotion
WHERE ss.ss_item_sk = i_item_sk
  AND ss.ss_cdemo_sk = cd_demo_sk
  AND ss.ss_promo_sk = p_promo_sk
  AND cd_gender = 'F'
  AND cd_marital_status = 'W'
  AND cd_education_status = 'Primary'
  AND (p_channel_email = 'N' OR p_channel_event = 'N')
GROUP BY i_item_id
ORDER BY i_item_id
LIMIT 100;
"""
    },

    25: {
        "name": "Q25: Date filter pushdown for store_sales",
        "sql": """
-- Q25 not in first 23, skip
"""
    },
}


if __name__ == "__main__":
    results = {}

    for qnum in [15, 18, 7]:
        if qnum not in OPTIMIZATIONS:
            continue
        opt = OPTIMIZATIONS[qnum]
        original = load_query(qnum)
        speedup, correct = compare(opt["name"], original, opt["sql"])
        results[qnum] = (speedup, correct)

    print("\n" + "="*60)
    print("FINAL RESULTS")
    print("="*60)
    wins = []
    for qnum, (speedup, correct) in sorted(results.items()):
        if speedup and correct:
            status = "" if speedup >= 2 else ("" if speedup >= 1.2 else "")
            print(f"Q{qnum}: {speedup:.2f}x {status}")
            if speedup >= 1.5:
                wins.append(qnum)
        elif speedup:
            print(f"Q{qnum}: {speedup:.2f}x  (wrong results)")
        else:
            print(f"Q{qnum}: FAILED")

    print(f"\nWins (>=1.5x, correct): {wins}")
