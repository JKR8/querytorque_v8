#!/usr/bin/env python3
"""Batch 8: Push date filters into CTEs for remaining candidates.
Q31 (push 9mo filter), Q74 (convert comma join), Q11 (push 2yr filter),
Q75 (push 2yr filter), Q78 (push 1yr filter)
"""
import time, snowflake.connector
from urllib.parse import unquote

DSN_PARTS = {
    "account": "CVRYJTF-AW47074",
    "user": "jkdl",
    "password": unquote("QGdg%2A%24WC%25O62xS71"),
    "warehouse": "COMPUTE_WH",
    "database": "SNOWFLAKE_SAMPLE_DATA",
    "schema": "TPCDS_SF10TCL",
    "role": "ACCOUNTADMIN",
}

REWRITES = {
    # Q31: Push d_year=2000, d_qoy IN (1,2,3) into both CTEs. 9 months.
    # CTE aggregates to ~counties × quarters = tiny. 6-way join on tiny data.
    "query_31": """
WITH date_filter AS (
    SELECT d_date_sk, d_qoy, d_year
    FROM date_dim
    WHERE d_year = 2000 AND d_qoy IN (1, 2, 3)
),
ss AS (
    SELECT ca_county, d_qoy, d_year, sum(ss_ext_sales_price) AS store_sales
    FROM store_sales
        JOIN date_filter ON ss_sold_date_sk = d_date_sk
        ,customer_address
    WHERE ss_addr_sk = ca_address_sk
    GROUP BY ca_county, d_qoy, d_year
),
ws AS (
    SELECT ca_county, d_qoy, d_year, sum(ws_ext_sales_price) AS web_sales
    FROM web_sales
        JOIN date_filter ON ws_sold_date_sk = d_date_sk
        ,customer_address
    WHERE ws_bill_addr_sk = ca_address_sk
    GROUP BY ca_county, d_qoy, d_year
)
SELECT
    ss1.ca_county,
    ss1.d_year,
    ws2.web_sales/ws1.web_sales web_q1_q2_increase,
    ss2.store_sales/ss1.store_sales store_q1_q2_increase,
    ws3.web_sales/ws2.web_sales web_q2_q3_increase,
    ss3.store_sales/ss2.store_sales store_q2_q3_increase
FROM
    ss ss1, ss ss2, ss ss3,
    ws ws1, ws ws2, ws ws3
WHERE
    ss1.d_qoy = 1 AND ss1.d_year = 2000
    AND ss1.ca_county = ss2.ca_county
    AND ss2.d_qoy = 2 AND ss2.d_year = 2000
    AND ss2.ca_county = ss3.ca_county
    AND ss3.d_qoy = 3 AND ss3.d_year = 2000
    AND ss1.ca_county = ws1.ca_county
    AND ws1.d_qoy = 1 AND ws1.d_year = 2000
    AND ws1.ca_county = ws2.ca_county
    AND ws2.d_qoy = 2 AND ws2.d_year = 2000
    AND ws1.ca_county = ws3.ca_county
    AND ws3.d_qoy = 3 AND ws3.d_year = 2000
    AND CASE WHEN ws1.web_sales > 0 THEN ws2.web_sales/ws1.web_sales ELSE null END
        > CASE WHEN ss1.store_sales > 0 THEN ss2.store_sales/ss1.store_sales ELSE null END
    AND CASE WHEN ws2.web_sales > 0 THEN ws3.web_sales/ws2.web_sales ELSE null END
        > CASE WHEN ss2.store_sales > 0 THEN ss3.store_sales/ss2.store_sales ELSE null END
ORDER BY web_q1_q2_increase
""",

    # Q74: Convert comma join to date CTE. 2 years (1999,2000).
    # CTE groups by customer × year. 4-way self-join on customer-level data.
    "query_74": """
WITH date_filter AS (
    SELECT d_date_sk, d_year FROM date_dim WHERE d_year IN (1999, 2000)
),
year_total AS (
    SELECT c_customer_id customer_id,
           c_first_name customer_first_name,
           c_last_name customer_last_name,
           d_year AS year,
           stddev_samp(ss_net_paid) year_total,
           's' sale_type
    FROM customer
        ,store_sales JOIN date_filter ON ss_sold_date_sk = date_filter.d_date_sk
    WHERE c_customer_sk = ss_customer_sk
    GROUP BY c_customer_id, c_first_name, c_last_name, d_year
    UNION ALL
    SELECT c_customer_id customer_id,
           c_first_name customer_first_name,
           c_last_name customer_last_name,
           d_year AS year,
           stddev_samp(ws_net_paid) year_total,
           'w' sale_type
    FROM customer
        ,web_sales JOIN date_filter ON ws_sold_date_sk = date_filter.d_date_sk
    WHERE c_customer_sk = ws_bill_customer_sk
    GROUP BY c_customer_id, c_first_name, c_last_name, d_year
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
    AND CASE WHEN t_w_firstyear.year_total > 0 THEN t_w_secyear.year_total / t_w_firstyear.year_total ELSE null END
        > CASE WHEN t_s_firstyear.year_total > 0 THEN t_s_secyear.year_total / t_s_firstyear.year_total ELSE null END
ORDER BY 2, 1, 3
LIMIT 100
""",

    # Q11: Push d_year IN (2001, 2002) into CTE. 2 years, 4-way self-join.
    "query_11": """
WITH date_filter AS (
    SELECT d_date_sk, d_year FROM date_dim WHERE d_year IN (2001, 2002)
),
year_total AS (
    SELECT c_customer_id customer_id,
           c_first_name customer_first_name,
           c_last_name customer_last_name,
           c_preferred_cust_flag customer_preferred_cust_flag,
           c_birth_country customer_birth_country,
           c_login customer_login,
           c_email_address customer_email_address,
           d_year dyear,
           sum(ss_ext_list_price-ss_ext_discount_amt) year_total,
           's' sale_type
    FROM customer
        ,store_sales JOIN date_filter ON ss_sold_date_sk = date_filter.d_date_sk
    WHERE c_customer_sk = ss_customer_sk
    GROUP BY c_customer_id, c_first_name, c_last_name,
             c_preferred_cust_flag, c_birth_country, c_login, c_email_address, d_year
    UNION ALL
    SELECT c_customer_id customer_id,
           c_first_name customer_first_name,
           c_last_name customer_last_name,
           c_preferred_cust_flag customer_preferred_cust_flag,
           c_birth_country customer_birth_country,
           c_login customer_login,
           c_email_address customer_email_address,
           d_year dyear,
           sum(ws_ext_list_price-ws_ext_discount_amt) year_total,
           'w' sale_type
    FROM customer
        ,web_sales JOIN date_filter ON ws_sold_date_sk = date_filter.d_date_sk
    WHERE c_customer_sk = ws_bill_customer_sk
    GROUP BY c_customer_id, c_first_name, c_last_name,
             c_preferred_cust_flag, c_birth_country, c_login, c_email_address, d_year
)
SELECT t_s_secyear.customer_id,
       t_s_secyear.customer_first_name,
       t_s_secyear.customer_last_name,
       t_s_secyear.customer_birth_country
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
    AND t_s_firstyear.dyear = 2001
    AND t_s_secyear.dyear = 2002
    AND t_w_firstyear.dyear = 2001
    AND t_w_secyear.dyear = 2002
    AND t_s_firstyear.year_total > 0
    AND t_w_firstyear.year_total > 0
    AND CASE WHEN t_w_firstyear.year_total > 0 THEN t_w_secyear.year_total / t_w_firstyear.year_total ELSE 0.0 END
        > CASE WHEN t_s_firstyear.year_total > 0 THEN t_s_secyear.year_total / t_s_firstyear.year_total ELSE 0.0 END
ORDER BY t_s_secyear.customer_id,
         t_s_secyear.customer_first_name,
         t_s_secyear.customer_last_name,
         t_s_secyear.customer_birth_country
LIMIT 100
""",

    # Q75: Push 2-year filter into 3 UNION branches. Uses explicit JOIN already
    # but no date filter in CTE. Self-join on 2-year aggregated data.
    "query_75": """
WITH date_filter AS (
    SELECT d_date_sk, d_year FROM date_dim WHERE d_year IN (1998, 1999)
),
all_sales AS (
    SELECT d_year, i_brand_id, i_class_id, i_category_id, i_manufact_id,
           SUM(sales_cnt) AS sales_cnt, SUM(sales_amt) AS sales_amt
    FROM (
        SELECT d_year, i_brand_id, i_class_id, i_category_id, i_manufact_id,
               cs_quantity - COALESCE(cr_return_quantity,0) AS sales_cnt,
               cs_ext_sales_price - COALESCE(cr_return_amount,0.0) AS sales_amt
        FROM catalog_sales
            JOIN date_filter ON d_date_sk=cs_sold_date_sk
            JOIN item ON i_item_sk=cs_item_sk
            LEFT JOIN catalog_returns ON (cs_order_number=cr_order_number AND cs_item_sk=cr_item_sk)
        WHERE i_category='Home'
        UNION
        SELECT d_year, i_brand_id, i_class_id, i_category_id, i_manufact_id,
               ss_quantity - COALESCE(sr_return_quantity,0) AS sales_cnt,
               ss_ext_sales_price - COALESCE(sr_return_amt,0.0) AS sales_amt
        FROM store_sales
            JOIN date_filter ON d_date_sk=ss_sold_date_sk
            JOIN item ON i_item_sk=ss_item_sk
            LEFT JOIN store_returns ON (ss_ticket_number=sr_ticket_number AND ss_item_sk=sr_item_sk)
        WHERE i_category='Home'
        UNION
        SELECT d_year, i_brand_id, i_class_id, i_category_id, i_manufact_id,
               ws_quantity - COALESCE(wr_return_quantity,0) AS sales_cnt,
               ws_ext_sales_price - COALESCE(wr_return_amt,0.0) AS sales_amt
        FROM web_sales
            JOIN date_filter ON d_date_sk=ws_sold_date_sk
            JOIN item ON i_item_sk=ws_item_sk
            LEFT JOIN web_returns ON (ws_order_number=wr_order_number AND ws_item_sk=wr_item_sk)
        WHERE i_category='Home'
    ) sales_detail
    GROUP BY d_year, i_brand_id, i_class_id, i_category_id, i_manufact_id
)
SELECT prev_yr.d_year AS prev_year,
       curr_yr.d_year AS year,
       curr_yr.i_brand_id,
       curr_yr.i_class_id,
       curr_yr.i_category_id,
       curr_yr.i_manufact_id,
       prev_yr.sales_cnt AS prev_yr_cnt,
       curr_yr.sales_cnt AS curr_yr_cnt,
       curr_yr.sales_cnt-prev_yr.sales_cnt AS sales_cnt_diff,
       curr_yr.sales_amt-prev_yr.sales_amt AS sales_amt_diff
FROM all_sales curr_yr, all_sales prev_yr
WHERE curr_yr.i_brand_id=prev_yr.i_brand_id
  AND curr_yr.i_class_id=prev_yr.i_class_id
  AND curr_yr.i_category_id=prev_yr.i_category_id
  AND curr_yr.i_manufact_id=prev_yr.i_manufact_id
  AND curr_yr.d_year=1999
  AND prev_yr.d_year=1998
  AND CAST(curr_yr.sales_cnt AS DECIMAL(17,2))/CAST(prev_yr.sales_cnt AS DECIMAL(17,2))<0.9
ORDER BY sales_cnt_diff, sales_amt_diff
LIMIT 100
""",

    # Q78: Push d_year=2000 into 3 CTEs. Currently no date filter.
    # 3-way LEFT JOIN on customer × item × year aggregation.
    "query_78": """
WITH date_filter AS (
    SELECT d_date_sk, d_year FROM date_dim WHERE d_year = 2000
),
ws AS (
    SELECT d_year AS ws_sold_year, ws_item_sk,
           ws_bill_customer_sk ws_customer_sk,
           sum(ws_quantity) ws_qty,
           sum(ws_wholesale_cost) ws_wc,
           sum(ws_sales_price) ws_sp
    FROM web_sales
        LEFT JOIN web_returns ON wr_order_number=ws_order_number AND ws_item_sk=wr_item_sk
        JOIN date_filter ON ws_sold_date_sk = date_filter.d_date_sk
    WHERE wr_order_number IS null
    GROUP BY d_year, ws_item_sk, ws_bill_customer_sk
),
cs AS (
    SELECT d_year AS cs_sold_year, cs_item_sk,
           cs_bill_customer_sk cs_customer_sk,
           sum(cs_quantity) cs_qty,
           sum(cs_wholesale_cost) cs_wc,
           sum(cs_sales_price) cs_sp
    FROM catalog_sales
        LEFT JOIN catalog_returns ON cr_order_number=cs_order_number AND cs_item_sk=cr_item_sk
        JOIN date_filter ON cs_sold_date_sk = date_filter.d_date_sk
    WHERE cr_order_number IS null
    GROUP BY d_year, cs_item_sk, cs_bill_customer_sk
),
ss AS (
    SELECT d_year AS ss_sold_year, ss_item_sk,
           ss_customer_sk,
           sum(ss_quantity) ss_qty,
           sum(ss_wholesale_cost) ss_wc,
           sum(ss_sales_price) ss_sp
    FROM store_sales
        LEFT JOIN store_returns ON sr_ticket_number=ss_ticket_number AND ss_item_sk=sr_item_sk
        JOIN date_filter ON ss_sold_date_sk = date_filter.d_date_sk
    WHERE sr_ticket_number IS null
    GROUP BY d_year, ss_item_sk, ss_customer_sk
)
SELECT ss_item_sk,
       round(ss_qty/(coalesce(ws_qty,0)+coalesce(cs_qty,0)),2) ratio,
       ss_qty store_qty, ss_wc store_wholesale_cost, ss_sp store_sales_price,
       coalesce(ws_qty,0)+coalesce(cs_qty,0) other_chan_qty,
       coalesce(ws_wc,0)+coalesce(cs_wc,0) other_chan_wholesale_cost,
       coalesce(ws_sp,0)+coalesce(cs_sp,0) other_chan_sales_price
FROM ss
LEFT JOIN ws ON (ws_sold_year=ss_sold_year AND ws_item_sk=ss_item_sk AND ws_customer_sk=ss_customer_sk)
LEFT JOIN cs ON (cs_sold_year=ss_sold_year AND cs_item_sk=ss_item_sk AND cs_customer_sk=ss_customer_sk)
WHERE (coalesce(ws_qty,0)>0 OR coalesce(cs_qty, 0)>0) AND ss_sold_year=2000
ORDER BY ss_item_sk, ss_qty desc, ss_wc desc, ss_sp desc,
         other_chan_qty, other_chan_wholesale_cost, other_chan_sales_price, ratio
LIMIT 100
"""
}

TIMEOUT_SEC = 300

def run_query(conn, sql, label):
    cur = conn.cursor()
    try:
        cur.execute(f"ALTER SESSION SET STATEMENT_TIMEOUT_IN_SECONDS = {TIMEOUT_SEC}")
        t0 = time.time()
        cur.execute(sql.strip())
        rows = cur.fetchall()
        elapsed = int((time.time() - t0) * 1000)
        return elapsed, len(rows)
    except Exception as e:
        elapsed = int((time.time() - t0) * 1000)
        return elapsed, f"ERROR: {e}"
    finally:
        cur.close()

def main():
    results = {}
    for qname, sql in REWRITES.items():
        print(f"\n{'='*60}")
        print(f"  {qname}")
        print(f"{'='*60}")

        conn = snowflake.connector.connect(**DSN_PARTS)
        try:
            ms, rows = run_query(conn, sql, f"{qname} warmup")
            print(f"  Warmup... {ms}ms, rows={rows}")
            if isinstance(rows, str) and "ERROR" in rows:
                results[qname] = (ms, rows)
                continue

            times = []
            for i in range(2):
                ms, rows = run_query(conn, sql, f"{qname} run {i+1}")
                print(f"  Run {i+1}... {ms}ms, rows={rows}")
                if isinstance(rows, str) and "ERROR" in rows:
                    results[qname] = (ms, rows)
                    break
                times.append(ms)
            else:
                avg = sum(times) // len(times)
                print(f"  >> Average: {avg}ms (was TIMEOUT >300s)")
                results[qname] = (avg, rows)
        finally:
            conn.close()

    print(f"\n\n{'='*60}")
    print(f"  SUMMARY")
    print(f"{'='*60}")
    for qname, (ms, rows) in sorted(results.items()):
        status = "[ERROR]" if isinstance(rows, str) else "[WIN]"
        print(f"  {qname}: {ms}ms  rows={rows}  {status}")

if __name__ == "__main__":
    main()
