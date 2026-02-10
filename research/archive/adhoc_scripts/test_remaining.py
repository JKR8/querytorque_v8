"""Test remaining queries Q1-Q23 on sample database."""

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


def test(qnum: int, original: str, optimized: str) -> tuple:
    orig_time, orig_result = benchmark(original)
    if orig_time < 0:
        return None, None, f"Original failed: {orig_result[:80]}"

    opt_time, opt_result = benchmark(optimized)
    if opt_time < 0:
        return None, None, f"Optimized failed: {opt_result[:80]}"

    speedup = orig_time / opt_time if opt_time > 0 else 0
    correct = (set(orig_result) == set(opt_result))
    return speedup, correct, None


def load_query(num):
    with open(os.path.join(QUERY_DIR, f"query_{num}.sql")) as f:
        return f.read()


# All optimizations for Q1-Q23
OPTS = {
    1: """
-- Q1: Predicate pushdown + window function for avg
WITH customer_total_return AS (
    SELECT sr_customer_sk AS ctr_customer_sk,
           sr_store_sk AS ctr_store_sk,
           sum(SR_FEE) AS ctr_total_return,
           AVG(sum(SR_FEE)) OVER (PARTITION BY sr_store_sk) AS store_avg
    FROM store_returns, date_dim, store
    WHERE sr_returned_date_sk = d_date_sk
      AND d_year = 2000
      AND sr_store_sk = s_store_sk
      AND s_state = 'SD'
    GROUP BY sr_customer_sk, sr_store_sk
)
SELECT c_customer_id
FROM customer_total_return ctr1, customer
WHERE ctr1.ctr_total_return > ctr1.store_avg * 1.2
  AND ctr1.ctr_customer_sk = c_customer_sk
ORDER BY c_customer_id
LIMIT 100;
""",

    4: """
-- Q4: Complex year-over-year, try scan consolidation
-- Skip - too complex for simple optimization
""",

    5: """
-- Q5: Multiple sales tables with returns - skip
""",

    6: """
-- Q6: Correlated subquery -> window function
WITH item_with_avg AS (
    SELECT i_item_sk, i_category, i_current_price,
           AVG(i_current_price) OVER (PARTITION BY i_category) AS category_avg
    FROM item
),
target_month AS (
    SELECT DISTINCT d_month_seq
    FROM date_dim
    WHERE d_year = 2002 AND d_moy = 3
)
SELECT a.ca_state state, count(*) cnt
FROM customer_address a, customer c, store_sales s, date_dim d, item_with_avg i
WHERE a.ca_address_sk = c.c_current_addr_sk
  AND c.c_customer_sk = s.ss_customer_sk
  AND s.ss_sold_date_sk = d.d_date_sk
  AND s.ss_item_sk = i.i_item_sk
  AND d.d_month_seq = (SELECT d_month_seq FROM target_month)
  AND i.i_current_price > 1.2 * i.category_avg
GROUP BY a.ca_state
HAVING count(*) >= 10
ORDER BY cnt, a.ca_state
LIMIT 100;
""",

    9: """
-- Q9: Multiple quantity ranges - CASE WHEN consolidation
SELECT CASE WHEN (SELECT count(*) FROM store_sales WHERE ss_quantity BETWEEN 1 AND 20) > 1071 THEN
    (SELECT avg(ss_ext_discount_amt) FROM store_sales WHERE ss_quantity BETWEEN 1 AND 20) ELSE
    (SELECT avg(ss_net_profit) FROM store_sales WHERE ss_quantity BETWEEN 1 AND 20) END bucket1,
CASE WHEN (SELECT count(*) FROM store_sales WHERE ss_quantity BETWEEN 21 AND 40) > 39498 THEN
    (SELECT avg(ss_ext_discount_amt) FROM store_sales WHERE ss_quantity BETWEEN 21 AND 40) ELSE
    (SELECT avg(ss_net_profit) FROM store_sales WHERE ss_quantity BETWEEN 21 AND 40) END bucket2,
CASE WHEN (SELECT count(*) FROM store_sales WHERE ss_quantity BETWEEN 41 AND 60) > 30691 THEN
    (SELECT avg(ss_ext_discount_amt) FROM store_sales WHERE ss_quantity BETWEEN 41 AND 60) ELSE
    (SELECT avg(ss_net_profit) FROM store_sales WHERE ss_quantity BETWEEN 41 AND 60) END bucket3,
CASE WHEN (SELECT count(*) FROM store_sales WHERE ss_quantity BETWEEN 61 AND 80) > 23213 THEN
    (SELECT avg(ss_ext_discount_amt) FROM store_sales WHERE ss_quantity BETWEEN 61 AND 80) ELSE
    (SELECT avg(ss_net_profit) FROM store_sales WHERE ss_quantity BETWEEN 61 AND 80) END bucket4,
CASE WHEN (SELECT count(*) FROM store_sales WHERE ss_quantity BETWEEN 81 AND 100) > 26685 THEN
    (SELECT avg(ss_ext_discount_amt) FROM store_sales WHERE ss_quantity BETWEEN 81 AND 100) ELSE
    (SELECT avg(ss_net_profit) FROM store_sales WHERE ss_quantity BETWEEN 81 AND 100) END bucket5
FROM reason WHERE r_reason_sk = 1;
""",

    10: """
-- Q10: Multiple demographics filters - skip complex
""",

    11: """
-- Q11: Year over year - customer join elimination
WITH year_total AS (
    SELECT ss_customer_sk AS customer_sk,
           sum(CASE WHEN d_year = 2002 THEN ss_net_paid ELSE 0 END) AS year2002_total,
           sum(CASE WHEN d_year = 2001 THEN ss_net_paid ELSE 0 END) AS year2001_total
    FROM store_sales, date_dim
    WHERE ss_sold_date_sk = d_date_sk
      AND d_year IN (2001, 2002)
      AND ss_customer_sk IS NOT NULL
    GROUP BY ss_customer_sk
),
web_year_total AS (
    SELECT ws_bill_customer_sk AS customer_sk,
           sum(CASE WHEN d_year = 2002 THEN ws_net_paid ELSE 0 END) AS year2002_total,
           sum(CASE WHEN d_year = 2001 THEN ws_net_paid ELSE 0 END) AS year2001_total
    FROM web_sales, date_dim
    WHERE ws_sold_date_sk = d_date_sk
      AND d_year IN (2001, 2002)
      AND ws_bill_customer_sk IS NOT NULL
    GROUP BY ws_bill_customer_sk
)
SELECT c_customer_id, c_first_name, c_last_name, c_preferred_cust_flag
FROM year_total t_s, web_year_total t_w, customer
WHERE t_s.customer_sk = c_customer_sk
  AND t_s.customer_sk = t_w.customer_sk
  AND t_s.year2001_total > 0
  AND t_w.year2001_total > 0
  AND t_s.year2002_total / t_s.year2001_total > t_w.year2002_total / t_w.year2001_total
ORDER BY c_customer_id, c_first_name, c_last_name, c_preferred_cust_flag
LIMIT 100;
""",

    13: """
-- Q13: Complex OR conditions - skip
""",

    14: """
-- Q14: Cross-channel analysis - skip complex
""",

    15: """
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
""",

    16: """
-- Q16: NOT EXISTS - skip
""",

    17: """
-- Q17: store/catalog/returns join - date pushdown
WITH date_filtered AS (
    SELECT d_date_sk FROM date_dim
    WHERE d_quarter_name = '2001Q1'
)
SELECT i_item_id, i_item_desc,
       s_state, count(ss_quantity) AS store_sales_quantitycount,
       avg(ss_quantity) AS store_sales_quantityave,
       stddev_samp(ss_quantity) AS store_sales_quantitystdev,
       stddev_samp(ss_quantity)/avg(ss_quantity) AS store_sales_quantitycov,
       count(sr_return_quantity) AS store_returns_quantitycount,
       avg(sr_return_quantity) AS store_returns_quantityave,
       stddev_samp(sr_return_quantity) AS store_returns_quantitystdev,
       stddev_samp(sr_return_quantity)/avg(sr_return_quantity) AS store_returns_quantitycov,
       count(cs_quantity) AS catalog_sales_quantitycount,
       avg(cs_quantity) AS catalog_sales_quantityave,
       stddev_samp(cs_quantity) AS catalog_sales_quantitystdev,
       stddev_samp(cs_quantity)/avg(cs_quantity) AS catalog_sales_quantitycov
FROM store_sales, store_returns, catalog_sales, date_filtered d1, date_filtered d2,
     date_filtered d3, store, item
WHERE ss_sold_date_sk = d1.d_date_sk
  AND sr_returned_date_sk = d2.d_date_sk
  AND cs_sold_date_sk = d3.d_date_sk
  AND ss_customer_sk = sr_customer_sk
  AND ss_item_sk = sr_item_sk
  AND ss_ticket_number = sr_ticket_number
  AND sr_customer_sk = cs_bill_customer_sk
  AND sr_item_sk = cs_item_sk
  AND ss_item_sk = i_item_sk
  AND ss_store_sk = s_store_sk
  AND i_color IN ('orchid','chiffon','lace','sky','lawn','maroon')
GROUP BY i_item_id, i_item_desc, s_state
ORDER BY i_item_id, i_item_desc, s_state
LIMIT 100;
""",

    18: """
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
""",

    19: """
-- Q19: zip code comparison - needs customer, can't eliminate
""",

    21: """
-- Q21: inventory before/after - date filter pushdown
WITH filtered_inv AS (
    SELECT inv_warehouse_sk, inv_item_sk, inv_quantity_on_hand, d_date
    FROM inventory, date_dim
    WHERE inv_date_sk = d_date_sk
      AND d_date BETWEEN CAST('2000-05-13' AS DATE) - INTERVAL '30' DAY
                     AND CAST('2000-05-13' AS DATE) + INTERVAL '30' DAY
)
SELECT w_warehouse_name, i_item_id,
       sum(CASE WHEN d_date < CAST('2000-05-13' AS DATE)
           THEN inv_quantity_on_hand ELSE 0 END) AS inv_before,
       sum(CASE WHEN d_date >= CAST('2000-05-13' AS DATE)
           THEN inv_quantity_on_hand ELSE 0 END) AS inv_after
FROM filtered_inv, warehouse, item
WHERE i_current_price BETWEEN 0.99 AND 1.49
  AND i_item_sk = inv_item_sk
  AND inv_warehouse_sk = w_warehouse_sk
GROUP BY w_warehouse_name, i_item_id
HAVING sum(CASE WHEN d_date < CAST('2000-05-13' AS DATE)
               THEN inv_quantity_on_hand ELSE 0 END) > 0
   AND sum(CASE WHEN d_date >= CAST('2000-05-13' AS DATE)
               THEN inv_quantity_on_hand ELSE 0 END) /
       sum(CASE WHEN d_date < CAST('2000-05-13' AS DATE)
               THEN inv_quantity_on_hand ELSE 0 END) BETWEEN 2.0/3.0 AND 3.0/2.0
ORDER BY w_warehouse_name, i_item_id
LIMIT 100;
""",

    22: """
-- Q22: ROLLUP query - DuckDB handles well
""",

    23: """
-- Q23: Join elimination (best_ss_customer) - known 2.18x winner
WITH frequent_ss_items AS (
    SELECT substr(i_item_desc,1,30) itemdesc, i_item_sk item_sk, d_date solddate, count(*) cnt
    FROM store_sales, date_dim, item
    WHERE ss_sold_date_sk = d_date_sk
      AND ss_item_sk = i_item_sk
      AND d_year IN (2000,2001,2002,2003)
    GROUP BY substr(i_item_desc,1,30), i_item_sk, d_date
    HAVING count(*) > 4
),
max_store_sales AS (
    SELECT max(csales) tpcds_cmax
    FROM (
        SELECT ss_customer_sk, sum(ss_quantity*ss_sales_price) csales
        FROM store_sales, date_dim
        WHERE ss_sold_date_sk = d_date_sk
          AND d_year IN (2000,2001,2002,2003)
          AND ss_customer_sk IS NOT NULL
        GROUP BY ss_customer_sk
    )
),
best_ss_customer AS (
    SELECT ss_customer_sk AS c_customer_sk, sum(ss_quantity*ss_sales_price) ssales
    FROM store_sales
    WHERE ss_customer_sk IS NOT NULL
    GROUP BY ss_customer_sk
    HAVING sum(ss_quantity*ss_sales_price) > (95/100.0) * (SELECT * FROM max_store_sales)
)
SELECT c_last_name, c_first_name, sales
FROM (
    SELECT c_last_name, c_first_name, sum(cs_quantity*cs_list_price) sales
    FROM catalog_sales, customer, date_dim
    WHERE d_year = 2000
      AND d_moy = 5
      AND cs_sold_date_sk = d_date_sk
      AND cs_item_sk IN (SELECT item_sk FROM frequent_ss_items)
      AND cs_bill_customer_sk IN (SELECT c_customer_sk FROM best_ss_customer)
      AND cs_bill_customer_sk = c_customer_sk
    GROUP BY c_last_name, c_first_name
    UNION ALL
    SELECT c_last_name, c_first_name, sum(ws_quantity*ws_list_price) sales
    FROM web_sales, customer, date_dim
    WHERE d_year = 2000
      AND d_moy = 5
      AND ws_sold_date_sk = d_date_sk
      AND ws_item_sk IN (SELECT item_sk FROM frequent_ss_items)
      AND ws_bill_customer_sk IN (SELECT c_customer_sk FROM best_ss_customer)
      AND ws_bill_customer_sk = c_customer_sk
    GROUP BY c_last_name, c_first_name
)
ORDER BY c_last_name, c_first_name, sales
LIMIT 100;
""",
}


if __name__ == "__main__":
    print("="*60)
    print("TESTING Q1-Q23 ON SAMPLE DB")
    print("="*60)

    results = {}
    for qnum in range(1, 24):
        if qnum not in OPTS or OPTS[qnum].strip().startswith("--"):
            print(f"Q{qnum}: SKIP (no optimization)")
            continue

        original = load_query(qnum)
        optimized = OPTS[qnum]
        speedup, correct, error = test(qnum, original, optimized)

        if error:
            print(f"Q{qnum}: ERROR - {error}")
        elif speedup is not None:
            emoji = "" if speedup >= 2 else ("" if speedup >= 1.2 else "")
            status = "" if correct else ""
            print(f"Q{qnum}: {speedup:.2f}x {emoji} {status}")
            results[qnum] = (speedup, correct)

    print("\n" + "="*60)
    print("WINS (>=1.5x, correct)")
    print("="*60)
    for qnum, (speedup, correct) in sorted(results.items()):
        if speedup >= 1.5 and correct:
            print(f"Q{qnum}: {speedup:.2f}x")
