"""Batch 7: date-CTE rewrites for Q12, Q54, Q59, Q74, Q81, Q94, Q95.
IMPORTANT: Disables Snowflake result cache to get real execution times.
"""
import snowflake.connector
import time
from urllib.parse import urlparse, parse_qs, unquote

DSN = 'snowflake://jkdl:QGdg%2A%24WC%25O62xS71@CVRYJTF-AW47074/SNOWFLAKE_SAMPLE_DATA/TPCDS_SF10TCL?warehouse=COMPUTE_WH&role=ACCOUNTADMIN'

REWRITES = {

    "query_12": """
WITH date_filter AS (
    SELECT d_date_sk FROM date_dim
    WHERE d_date BETWEEN CAST('1998-04-06' AS DATE)
                     AND CAST('1998-04-06' AS DATE) + INTERVAL '30 DAY'
)
SELECT i_item_id, i_item_desc, i_category, i_class, i_current_price,
       SUM(ws_ext_sales_price) AS itemrevenue,
       SUM(ws_ext_sales_price)*100/SUM(SUM(ws_ext_sales_price)) OVER
           (PARTITION BY i_class) AS revenueratio
FROM web_sales
    JOIN date_filter df ON ws_sold_date_sk = df.d_date_sk
    JOIN item ON ws_item_sk = i_item_sk
WHERE i_category IN ('Books', 'Sports', 'Men')
GROUP BY i_item_id, i_item_desc, i_category, i_class, i_current_price
ORDER BY i_category, i_class, i_item_id, i_item_desc, revenueratio
LIMIT 100;
""",

    "query_54": """
WITH date_filter_1 AS (
    SELECT d_date_sk FROM date_dim WHERE d_moy = 5 AND d_year = 1998
),
date_filter_2 AS (
    SELECT d_date_sk FROM date_dim
    WHERE d_month_seq BETWEEN (SELECT DISTINCT d_month_seq+1 FROM date_dim WHERE d_year = 1998 AND d_moy = 5)
                          AND (SELECT DISTINCT d_month_seq+3 FROM date_dim WHERE d_year = 1998 AND d_moy = 5)
),
my_customers AS (
    SELECT DISTINCT c_customer_sk, c_current_addr_sk
    FROM (
        SELECT cs_sold_date_sk sold_date_sk, cs_bill_customer_sk customer_sk, cs_item_sk item_sk
        FROM catalog_sales
        UNION ALL
        SELECT ws_sold_date_sk sold_date_sk, ws_bill_customer_sk customer_sk, ws_item_sk item_sk
        FROM web_sales
    ) cs_or_ws_sales
    JOIN date_filter_1 df ON sold_date_sk = df.d_date_sk
    JOIN item ON item_sk = i_item_sk
    JOIN customer ON c_customer_sk = cs_or_ws_sales.customer_sk
    WHERE i_category = 'Women' AND i_class = 'maternity'
),
my_revenue AS (
    SELECT c_customer_sk,
           SUM(ss_ext_sales_price) AS revenue
    FROM my_customers
    JOIN store_sales ON c_customer_sk = ss_customer_sk
    JOIN date_filter_2 df ON ss_sold_date_sk = df.d_date_sk
    JOIN customer_address ON c_current_addr_sk = ca_address_sk
    JOIN store ON ca_county = s_county AND ca_state = s_state
    GROUP BY c_customer_sk
),
segments AS (
    SELECT CAST((revenue/50) AS INT) AS segment FROM my_revenue
)
SELECT segment, COUNT(*) AS num_customers, segment*50 AS segment_base
FROM segments
GROUP BY segment
ORDER BY segment, num_customers
LIMIT 100;
""",

    "query_59": """
WITH date_range AS (
    SELECT d_date_sk, d_week_seq, d_day_name
    FROM date_dim
    WHERE d_month_seq BETWEEN 1196 AND 1196 + 23
),
wss AS (
    SELECT dr.d_week_seq,
           ss_store_sk,
           SUM(CASE WHEN dr.d_day_name='Sunday' THEN ss_sales_price ELSE NULL END) sun_sales,
           SUM(CASE WHEN dr.d_day_name='Monday' THEN ss_sales_price ELSE NULL END) mon_sales,
           SUM(CASE WHEN dr.d_day_name='Tuesday' THEN ss_sales_price ELSE NULL END) tue_sales,
           SUM(CASE WHEN dr.d_day_name='Wednesday' THEN ss_sales_price ELSE NULL END) wed_sales,
           SUM(CASE WHEN dr.d_day_name='Thursday' THEN ss_sales_price ELSE NULL END) thu_sales,
           SUM(CASE WHEN dr.d_day_name='Friday' THEN ss_sales_price ELSE NULL END) fri_sales,
           SUM(CASE WHEN dr.d_day_name='Saturday' THEN ss_sales_price ELSE NULL END) sat_sales
    FROM store_sales
    JOIN date_range dr ON ss_sold_date_sk = dr.d_date_sk
    GROUP BY dr.d_week_seq, ss_store_sk
)
SELECT s_store_name1, s_store_id1, d_week_seq1,
       sun_sales1/sun_sales2, mon_sales1/mon_sales2,
       tue_sales1/tue_sales2, wed_sales1/wed_sales2, thu_sales1/thu_sales2,
       fri_sales1/fri_sales2, sat_sales1/sat_sales2
FROM
(SELECT s_store_name s_store_name1, wss.d_week_seq d_week_seq1,
        s_store_id s_store_id1, sun_sales sun_sales1,
        mon_sales mon_sales1, tue_sales tue_sales1,
        wed_sales wed_sales1, thu_sales thu_sales1,
        fri_sales fri_sales1, sat_sales sat_sales1
 FROM wss
 JOIN store ON ss_store_sk = s_store_sk
 JOIN date_dim d ON d.d_week_seq = wss.d_week_seq
 WHERE d_month_seq BETWEEN 1196 AND 1196 + 11) y,
(SELECT s_store_name s_store_name2, wss.d_week_seq d_week_seq2,
        s_store_id s_store_id2, sun_sales sun_sales2,
        mon_sales mon_sales2, tue_sales tue_sales2,
        wed_sales wed_sales2, thu_sales thu_sales2,
        fri_sales fri_sales2, sat_sales sat_sales2
 FROM wss
 JOIN store ON ss_store_sk = s_store_sk
 JOIN date_dim d ON d.d_week_seq = wss.d_week_seq
 WHERE d_month_seq BETWEEN 1196+ 12 AND 1196 + 23) x
WHERE s_store_id1=s_store_id2
  AND d_week_seq1=d_week_seq2-52
ORDER BY s_store_name1, s_store_id1, d_week_seq1
LIMIT 100;
""",

    "query_74": """
WITH date_filter AS (
    SELECT d_date_sk, d_year FROM date_dim WHERE d_year IN (1999, 2000)
),
year_total AS (
    SELECT c_customer_id customer_id,
           c_first_name customer_first_name,
           c_last_name customer_last_name,
           df.d_year AS year,
           STDDEV_SAMP(ss_net_paid) year_total,
           's' sale_type
    FROM customer
    JOIN store_sales ON c_customer_sk = ss_customer_sk
    JOIN date_filter df ON ss_sold_date_sk = df.d_date_sk
    GROUP BY c_customer_id, c_first_name, c_last_name, df.d_year
    UNION ALL
    SELECT c_customer_id customer_id,
           c_first_name customer_first_name,
           c_last_name customer_last_name,
           df.d_year AS year,
           STDDEV_SAMP(ws_net_paid) year_total,
           'w' sale_type
    FROM customer
    JOIN web_sales ON c_customer_sk = ws_bill_customer_sk
    JOIN date_filter df ON ws_sold_date_sk = df.d_date_sk
    GROUP BY c_customer_id, c_first_name, c_last_name, df.d_year
)
SELECT t_s_secyear.customer_id, t_s_secyear.customer_first_name, t_s_secyear.customer_last_name
FROM year_total t_s_firstyear
    ,year_total t_s_secyear
    ,year_total t_w_firstyear
    ,year_total t_w_secyear
WHERE t_s_secyear.customer_id = t_s_firstyear.customer_id
  AND t_s_firstyear.customer_id = t_w_secyear.customer_id
  AND t_s_firstyear.customer_id = t_w_firstyear.customer_id
  AND t_s_firstyear.sale_type = 's'
  AND t_w_firstyear.sale_type = 'w'
  AND t_s_secyear.sale_type = 's'
  AND t_w_secyear.sale_type = 'w'
  AND t_s_firstyear.year = 1999
  AND t_s_secyear.year = 2000
  AND t_w_firstyear.year = 1999
  AND t_w_secyear.year = 2000
  AND t_s_firstyear.year_total > 0
  AND t_w_firstyear.year_total > 0
  AND CASE WHEN t_w_firstyear.year_total > 0 THEN t_w_secyear.year_total / t_w_firstyear.year_total ELSE NULL END
    > CASE WHEN t_s_firstyear.year_total > 0 THEN t_s_secyear.year_total / t_s_firstyear.year_total ELSE NULL END
ORDER BY 2, 1, 3
LIMIT 100;
""",

    "query_81": """
WITH date_filter AS (
    SELECT d_date_sk FROM date_dim WHERE d_year = 2002
),
customer_total_return AS (
    SELECT cr_returning_customer_sk AS ctr_customer_sk,
           ca_state AS ctr_state,
           SUM(cr_return_amt_inc_tax) AS ctr_total_return
    FROM catalog_returns
    JOIN date_filter df ON cr_returned_date_sk = df.d_date_sk
    JOIN customer_address ON cr_returning_addr_sk = ca_address_sk
    GROUP BY cr_returning_customer_sk, ca_state
)
SELECT c_customer_id, c_salutation, c_first_name, c_last_name, ca_street_number, ca_street_name,
       ca_street_type, ca_suite_number, ca_city, ca_county, ca_state, ca_zip, ca_country, ca_gmt_offset,
       ca_location_type, ctr_total_return
FROM customer_total_return ctr1
JOIN customer ON ctr1.ctr_customer_sk = c_customer_sk
JOIN customer_address ON ca_address_sk = c_current_addr_sk
WHERE ctr1.ctr_total_return > (
    SELECT AVG(ctr_total_return)*1.2
    FROM customer_total_return ctr2
    WHERE ctr1.ctr_state = ctr2.ctr_state
)
AND ca_state = 'CA'
ORDER BY c_customer_id, c_salutation, c_first_name, c_last_name, ca_street_number, ca_street_name,
         ca_street_type, ca_suite_number, ca_city, ca_county, ca_state, ca_zip, ca_country, ca_gmt_offset,
         ca_location_type, ctr_total_return
LIMIT 100;
""",

    "query_94": """
WITH date_filter AS (
    SELECT d_date_sk FROM date_dim
    WHERE d_date BETWEEN CAST('2000-2-01' AS DATE)
                     AND CAST('2000-2-01' AS DATE) + INTERVAL '60 DAY'
)
SELECT COUNT(DISTINCT ws_order_number) AS "order count",
       SUM(ws_ext_ship_cost) AS "total shipping cost",
       SUM(ws_net_profit) AS "total net profit"
FROM web_sales ws1
JOIN date_filter df ON ws1.ws_ship_date_sk = df.d_date_sk
JOIN customer_address ON ws1.ws_ship_addr_sk = ca_address_sk
JOIN web_site ON ws1.ws_web_site_sk = web_site_sk
WHERE ca_state = 'OK'
  AND web_company_name = 'pri'
  AND EXISTS (SELECT *
              FROM web_sales ws2
              WHERE ws1.ws_order_number = ws2.ws_order_number
                AND ws1.ws_warehouse_sk <> ws2.ws_warehouse_sk)
  AND NOT EXISTS (SELECT *
                  FROM web_returns wr1
                  WHERE ws1.ws_order_number = wr1.wr_order_number)
ORDER BY COUNT(DISTINCT ws_order_number)
LIMIT 100;
""",

    "query_95": """
WITH date_filter AS (
    SELECT d_date_sk FROM date_dim
    WHERE d_date BETWEEN CAST('1999-2-01' AS DATE)
                     AND CAST('1999-2-01' AS DATE) + INTERVAL '60 DAY'
),
ws_wh AS (
    SELECT ws1.ws_order_number, ws1.ws_warehouse_sk wh1, ws2.ws_warehouse_sk wh2
    FROM web_sales ws1, web_sales ws2
    WHERE ws1.ws_order_number = ws2.ws_order_number
      AND ws1.ws_warehouse_sk <> ws2.ws_warehouse_sk
)
SELECT COUNT(DISTINCT ws_order_number) AS "order count",
       SUM(ws_ext_ship_cost) AS "total shipping cost",
       SUM(ws_net_profit) AS "total net profit"
FROM web_sales ws1
JOIN date_filter df ON ws1.ws_ship_date_sk = df.d_date_sk
JOIN customer_address ON ws1.ws_ship_addr_sk = ca_address_sk
JOIN web_site ON ws1.ws_web_site_sk = web_site_sk
WHERE ca_state = 'NC'
  AND web_company_name = 'pri'
  AND ws1.ws_order_number IN (SELECT ws_order_number FROM ws_wh)
  AND ws1.ws_order_number IN (SELECT wr_order_number
                              FROM web_returns, ws_wh
                              WHERE wr_order_number = ws_wh.ws_order_number)
ORDER BY COUNT(DISTINCT ws_order_number)
LIMIT 100;
""",

}

# ── runner ──────────────────────────────────────────────────────────────
def connect():
    from urllib.parse import urlparse, parse_qs, unquote
    p = urlparse(DSN)
    qs = parse_qs(p.query)
    return snowflake.connector.connect(
        user=unquote(p.username),
        password=unquote(p.password),
        account=p.hostname,
        database=p.path.strip('/').split('/')[0],
        schema=p.path.strip('/').split('/')[1] if '/' in p.path.strip('/') else None,
        warehouse=qs.get('warehouse', [None])[0],
        role=qs.get('role', [None])[0],
    )

def run_one(cur, label, sql, timeout=300):
    cur.execute(f"ALTER SESSION SET STATEMENT_TIMEOUT_IN_SECONDS = {timeout}")
    t0 = time.time()
    try:
        cur.execute(sql)
        rows = cur.fetchall()
        elapsed = int((time.time()-t0)*1000)
        return elapsed, len(rows)
    except Exception as e:
        elapsed = int((time.time()-t0)*1000)
        return elapsed, f"ERROR: {e}"

def main():
    conn = connect()
    cur = conn.cursor()
    # CRITICAL: Disable result cache to get real execution times
    cur.execute("ALTER SESSION SET USE_CACHED_RESULT = FALSE")
    print("Result cache DISABLED")
    cur.close()

    results = {}
    for qname, sql in sorted(REWRITES.items()):
        print(f"\n{'='*60}")
        print(f"  {qname}")
        print(f"{'='*60}")
        cur = conn.cursor()
        # warmup
        ms, rows = run_one(cur, qname, sql, timeout=300)
        print(f"  Warmup... {ms}ms, rows={rows}")
        if isinstance(rows, str):  # error
            results[qname] = (ms, rows)
            cur.close()
            continue
        # 2 measured runs
        ms1, r1 = run_one(cur, qname, sql, timeout=300)
        print(f"  Run 1... {ms1}ms, rows={r1}")
        ms2, r2 = run_one(cur, qname, sql, timeout=300)
        print(f"  Run 2... {ms2}ms, rows={r2}")
        if isinstance(r1, str) or isinstance(r2, str):
            avg = max(ms1, ms2)
            results[qname] = (avg, f"partial error")
        else:
            avg = (ms1+ms2)//2
            results[qname] = (avg, r1)
        print(f"  >> Average: {avg}ms (was TIMEOUT >300s)")
        cur.close()

    print(f"\n\n{'='*60}")
    print(f"  SUMMARY")
    print(f"{'='*60}")
    for qname, (ms, rows) in sorted(results.items()):
        tag = "[WIN]" if isinstance(rows, int) and ms < 300000 else "[ERROR]"
        print(f"  {qname}: {ms}ms  rows={rows}  {tag}")

if __name__ == "__main__":
    main()
