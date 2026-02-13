"""Batch 4: date-CTE rewrites for Q48, Q50, Q65, Q66, Q85, Q87, Q97 + single-pass for Q9, Q88."""
import snowflake.connector
import time
from urllib.parse import urlparse, parse_qs, unquote

DSN = 'snowflake://jkdl:QGdg%2A%24WC%25O62xS71@CVRYJTF-AW47074/SNOWFLAKE_SAMPLE_DATA/TPCDS_SF10TCL?warehouse=COMPUTE_WH&role=ACCOUNTADMIN'

REWRITES = {

    "query_9": """
WITH aggs AS (
    SELECT
        COUNT(CASE WHEN ss_quantity BETWEEN  1 AND 20 THEN 1 END) AS cnt1,
        AVG(CASE WHEN ss_quantity BETWEEN  1 AND 20 THEN ss_ext_sales_price END) AS avg_ep1,
        AVG(CASE WHEN ss_quantity BETWEEN  1 AND 20 THEN ss_net_profit END) AS avg_np1,
        COUNT(CASE WHEN ss_quantity BETWEEN 21 AND 40 THEN 1 END) AS cnt2,
        AVG(CASE WHEN ss_quantity BETWEEN 21 AND 40 THEN ss_ext_sales_price END) AS avg_ep2,
        AVG(CASE WHEN ss_quantity BETWEEN 21 AND 40 THEN ss_net_profit END) AS avg_np2,
        COUNT(CASE WHEN ss_quantity BETWEEN 41 AND 60 THEN 1 END) AS cnt3,
        AVG(CASE WHEN ss_quantity BETWEEN 41 AND 60 THEN ss_ext_sales_price END) AS avg_ep3,
        AVG(CASE WHEN ss_quantity BETWEEN 41 AND 60 THEN ss_net_profit END) AS avg_np3,
        COUNT(CASE WHEN ss_quantity BETWEEN 61 AND 80 THEN 1 END) AS cnt4,
        AVG(CASE WHEN ss_quantity BETWEEN 61 AND 80 THEN ss_ext_sales_price END) AS avg_ep4,
        AVG(CASE WHEN ss_quantity BETWEEN 61 AND 80 THEN ss_net_profit END) AS avg_np4,
        COUNT(CASE WHEN ss_quantity BETWEEN 81 AND 100 THEN 1 END) AS cnt5,
        AVG(CASE WHEN ss_quantity BETWEEN 81 AND 100 THEN ss_ext_sales_price END) AS avg_ep5,
        AVG(CASE WHEN ss_quantity BETWEEN 81 AND 100 THEN ss_net_profit END) AS avg_np5
    FROM store_sales
    WHERE ss_quantity BETWEEN 1 AND 100
)
SELECT
    CASE WHEN cnt1 > 2972190 THEN avg_ep1 ELSE avg_np1 END AS bucket1,
    CASE WHEN cnt2 > 4505785 THEN avg_ep2 ELSE avg_np2 END AS bucket2,
    CASE WHEN cnt3 > 1575726 THEN avg_ep3 ELSE avg_np3 END AS bucket3,
    CASE WHEN cnt4 > 3188917 THEN avg_ep4 ELSE avg_np4 END AS bucket4,
    CASE WHEN cnt5 > 3525216 THEN avg_ep5 ELSE avg_np5 END AS bucket5
FROM aggs, reason WHERE r_reason_sk = 1
""",

    "query_48": """
WITH date_filter AS (
    SELECT d_date_sk FROM date_dim WHERE d_year = 1999
)
SELECT SUM(ss_quantity)
FROM store_sales
    JOIN date_filter ON ss_sold_date_sk = d_date_sk
    JOIN store ON s_store_sk = ss_store_sk
    JOIN customer_demographics ON cd_demo_sk = ss_cdemo_sk
    JOIN customer_address ON ss_addr_sk = ca_address_sk
WHERE (
    (cd_marital_status = 'U' AND cd_education_status = 'Primary'
     AND ss_sales_price BETWEEN 100.00 AND 150.00)
    OR (cd_marital_status = 'W' AND cd_education_status = 'College'
        AND ss_sales_price BETWEEN 50.00 AND 100.00)
    OR (cd_marital_status = 'D' AND cd_education_status = '2 yr Degree'
        AND ss_sales_price BETWEEN 150.00 AND 200.00)
)
AND (
    (ca_country = 'United States' AND ca_state IN ('MD','MN','IA')
     AND ss_net_profit BETWEEN 0 AND 2000)
    OR (ca_country = 'United States' AND ca_state IN ('VA','IL','TX')
        AND ss_net_profit BETWEEN 150 AND 3000)
    OR (ca_country = 'United States' AND ca_state IN ('MI','WI','IN')
        AND ss_net_profit BETWEEN 50 AND 25000)
)
""",

    "query_50": """
WITH d_return AS (
    SELECT d_date_sk FROM date_dim WHERE d_year = 2001 AND d_moy = 8
)
SELECT s_store_name, s_company_id, s_street_number, s_street_name, s_street_type,
       s_suite_number, s_city, s_county, s_state, s_zip,
       SUM(CASE WHEN (sr_returned_date_sk - ss_sold_date_sk <= 30) THEN 1 ELSE 0 END) AS "30 days",
       SUM(CASE WHEN (sr_returned_date_sk - ss_sold_date_sk > 30) AND (sr_returned_date_sk - ss_sold_date_sk <= 60) THEN 1 ELSE 0 END) AS "31-60 days",
       SUM(CASE WHEN (sr_returned_date_sk - ss_sold_date_sk > 60) AND (sr_returned_date_sk - ss_sold_date_sk <= 90) THEN 1 ELSE 0 END) AS "61-90 days",
       SUM(CASE WHEN (sr_returned_date_sk - ss_sold_date_sk > 90) AND (sr_returned_date_sk - ss_sold_date_sk <= 120) THEN 1 ELSE 0 END) AS "91-120 days",
       SUM(CASE WHEN (sr_returned_date_sk - ss_sold_date_sk > 120) THEN 1 ELSE 0 END) AS ">120 days"
FROM store_returns
    JOIN d_return ON sr_returned_date_sk = d_date_sk
    JOIN store_sales ON ss_ticket_number = sr_ticket_number AND ss_item_sk = sr_item_sk AND ss_customer_sk = sr_customer_sk
    JOIN store ON ss_store_sk = s_store_sk
GROUP BY s_store_name, s_company_id, s_street_number, s_street_name, s_street_type,
         s_suite_number, s_city, s_county, s_state, s_zip
ORDER BY s_store_name, s_company_id, s_street_number, s_street_name, s_street_type,
         s_suite_number, s_city, s_county, s_state, s_zip
LIMIT 100
""",

    "query_65": """
WITH date_filter AS (
    SELECT d_date_sk FROM date_dim WHERE d_month_seq BETWEEN 1221 AND 1232
),
sc AS (
    SELECT ss_store_sk, ss_item_sk, SUM(ss_sales_price) AS revenue
    FROM store_sales JOIN date_filter ON ss_sold_date_sk = d_date_sk
    GROUP BY ss_store_sk, ss_item_sk
),
sb AS (
    SELECT ss_store_sk, AVG(revenue) AS ave FROM sc GROUP BY ss_store_sk
)
SELECT s_store_name, i_item_desc, sc.revenue, i_current_price, i_wholesale_cost, i_brand
FROM store
    JOIN sc ON s_store_sk = sc.ss_store_sk
    JOIN sb ON sc.ss_store_sk = sb.ss_store_sk
    JOIN item ON i_item_sk = sc.ss_item_sk
WHERE sc.revenue <= 0.1 * sb.ave
ORDER BY s_store_name, i_item_desc
LIMIT 100
""",

    "query_66": """
WITH date_filter AS (
    SELECT d_date_sk, d_moy FROM date_dim WHERE d_year = 1998
),
time_filter AS (
    SELECT t_time_sk FROM time_dim WHERE t_time BETWEEN 48821 AND 48821 + 28800
),
carrier_filter AS (
    SELECT sm_ship_mode_sk FROM ship_mode WHERE sm_carrier IN ('GREAT EASTERN','LATVIAN')
)
SELECT w_warehouse_name, w_warehouse_sq_ft, w_city, w_county, w_state, w_country,
       ship_carriers, year,
       SUM(jan_sales) AS jan_sales, SUM(feb_sales) AS feb_sales, SUM(mar_sales) AS mar_sales,
       SUM(apr_sales) AS apr_sales, SUM(may_sales) AS may_sales, SUM(jun_sales) AS jun_sales,
       SUM(jul_sales) AS jul_sales, SUM(aug_sales) AS aug_sales, SUM(sep_sales) AS sep_sales,
       SUM(oct_sales) AS oct_sales, SUM(nov_sales) AS nov_sales, SUM(dec_sales) AS dec_sales,
       SUM(jan_sales/w_warehouse_sq_ft) AS jan_sales_per_sq_foot, SUM(feb_sales/w_warehouse_sq_ft) AS feb_sales_per_sq_foot,
       SUM(mar_sales/w_warehouse_sq_ft) AS mar_sales_per_sq_foot, SUM(apr_sales/w_warehouse_sq_ft) AS apr_sales_per_sq_foot,
       SUM(may_sales/w_warehouse_sq_ft) AS may_sales_per_sq_foot, SUM(jun_sales/w_warehouse_sq_ft) AS jun_sales_per_sq_foot,
       SUM(jul_sales/w_warehouse_sq_ft) AS jul_sales_per_sq_foot, SUM(aug_sales/w_warehouse_sq_ft) AS aug_sales_per_sq_foot,
       SUM(sep_sales/w_warehouse_sq_ft) AS sep_sales_per_sq_foot, SUM(oct_sales/w_warehouse_sq_ft) AS oct_sales_per_sq_foot,
       SUM(nov_sales/w_warehouse_sq_ft) AS nov_sales_per_sq_foot, SUM(dec_sales/w_warehouse_sq_ft) AS dec_sales_per_sq_foot,
       SUM(jan_net) AS jan_net, SUM(feb_net) AS feb_net, SUM(mar_net) AS mar_net,
       SUM(apr_net) AS apr_net, SUM(may_net) AS may_net, SUM(jun_net) AS jun_net,
       SUM(jul_net) AS jul_net, SUM(aug_net) AS aug_net, SUM(sep_net) AS sep_net,
       SUM(oct_net) AS oct_net, SUM(nov_net) AS nov_net, SUM(dec_net) AS dec_net
FROM (
    SELECT w_warehouse_name, w_warehouse_sq_ft, w_city, w_county, w_state, w_country,
           'GREAT EASTERN' || ',' || 'LATVIAN' AS ship_carriers, 1998 AS year,
           SUM(CASE WHEN d_moy=1 THEN ws_ext_sales_price*ws_quantity ELSE 0 END) AS jan_sales,
           SUM(CASE WHEN d_moy=2 THEN ws_ext_sales_price*ws_quantity ELSE 0 END) AS feb_sales,
           SUM(CASE WHEN d_moy=3 THEN ws_ext_sales_price*ws_quantity ELSE 0 END) AS mar_sales,
           SUM(CASE WHEN d_moy=4 THEN ws_ext_sales_price*ws_quantity ELSE 0 END) AS apr_sales,
           SUM(CASE WHEN d_moy=5 THEN ws_ext_sales_price*ws_quantity ELSE 0 END) AS may_sales,
           SUM(CASE WHEN d_moy=6 THEN ws_ext_sales_price*ws_quantity ELSE 0 END) AS jun_sales,
           SUM(CASE WHEN d_moy=7 THEN ws_ext_sales_price*ws_quantity ELSE 0 END) AS jul_sales,
           SUM(CASE WHEN d_moy=8 THEN ws_ext_sales_price*ws_quantity ELSE 0 END) AS aug_sales,
           SUM(CASE WHEN d_moy=9 THEN ws_ext_sales_price*ws_quantity ELSE 0 END) AS sep_sales,
           SUM(CASE WHEN d_moy=10 THEN ws_ext_sales_price*ws_quantity ELSE 0 END) AS oct_sales,
           SUM(CASE WHEN d_moy=11 THEN ws_ext_sales_price*ws_quantity ELSE 0 END) AS nov_sales,
           SUM(CASE WHEN d_moy=12 THEN ws_ext_sales_price*ws_quantity ELSE 0 END) AS dec_sales,
           SUM(CASE WHEN d_moy=1 THEN ws_net_paid_inc_ship_tax*ws_quantity ELSE 0 END) AS jan_net,
           SUM(CASE WHEN d_moy=2 THEN ws_net_paid_inc_ship_tax*ws_quantity ELSE 0 END) AS feb_net,
           SUM(CASE WHEN d_moy=3 THEN ws_net_paid_inc_ship_tax*ws_quantity ELSE 0 END) AS mar_net,
           SUM(CASE WHEN d_moy=4 THEN ws_net_paid_inc_ship_tax*ws_quantity ELSE 0 END) AS apr_net,
           SUM(CASE WHEN d_moy=5 THEN ws_net_paid_inc_ship_tax*ws_quantity ELSE 0 END) AS may_net,
           SUM(CASE WHEN d_moy=6 THEN ws_net_paid_inc_ship_tax*ws_quantity ELSE 0 END) AS jun_net,
           SUM(CASE WHEN d_moy=7 THEN ws_net_paid_inc_ship_tax*ws_quantity ELSE 0 END) AS jul_net,
           SUM(CASE WHEN d_moy=8 THEN ws_net_paid_inc_ship_tax*ws_quantity ELSE 0 END) AS aug_net,
           SUM(CASE WHEN d_moy=9 THEN ws_net_paid_inc_ship_tax*ws_quantity ELSE 0 END) AS sep_net,
           SUM(CASE WHEN d_moy=10 THEN ws_net_paid_inc_ship_tax*ws_quantity ELSE 0 END) AS oct_net,
           SUM(CASE WHEN d_moy=11 THEN ws_net_paid_inc_ship_tax*ws_quantity ELSE 0 END) AS nov_net,
           SUM(CASE WHEN d_moy=12 THEN ws_net_paid_inc_ship_tax*ws_quantity ELSE 0 END) AS dec_net
    FROM web_sales
        JOIN date_filter ON ws_sold_date_sk = d_date_sk
        JOIN time_filter ON ws_sold_time_sk = t_time_sk
        JOIN carrier_filter ON ws_ship_mode_sk = sm_ship_mode_sk
        JOIN warehouse ON ws_warehouse_sk = w_warehouse_sk
    GROUP BY w_warehouse_name, w_warehouse_sq_ft, w_city, w_county, w_state, w_country
    UNION ALL
    SELECT w_warehouse_name, w_warehouse_sq_ft, w_city, w_county, w_state, w_country,
           'GREAT EASTERN' || ',' || 'LATVIAN' AS ship_carriers, 1998 AS year,
           SUM(CASE WHEN d_moy=1 THEN cs_ext_list_price*cs_quantity ELSE 0 END) AS jan_sales,
           SUM(CASE WHEN d_moy=2 THEN cs_ext_list_price*cs_quantity ELSE 0 END) AS feb_sales,
           SUM(CASE WHEN d_moy=3 THEN cs_ext_list_price*cs_quantity ELSE 0 END) AS mar_sales,
           SUM(CASE WHEN d_moy=4 THEN cs_ext_list_price*cs_quantity ELSE 0 END) AS apr_sales,
           SUM(CASE WHEN d_moy=5 THEN cs_ext_list_price*cs_quantity ELSE 0 END) AS may_sales,
           SUM(CASE WHEN d_moy=6 THEN cs_ext_list_price*cs_quantity ELSE 0 END) AS jun_sales,
           SUM(CASE WHEN d_moy=7 THEN cs_ext_list_price*cs_quantity ELSE 0 END) AS jul_sales,
           SUM(CASE WHEN d_moy=8 THEN cs_ext_list_price*cs_quantity ELSE 0 END) AS aug_sales,
           SUM(CASE WHEN d_moy=9 THEN cs_ext_list_price*cs_quantity ELSE 0 END) AS sep_sales,
           SUM(CASE WHEN d_moy=10 THEN cs_ext_list_price*cs_quantity ELSE 0 END) AS oct_sales,
           SUM(CASE WHEN d_moy=11 THEN cs_ext_list_price*cs_quantity ELSE 0 END) AS nov_sales,
           SUM(CASE WHEN d_moy=12 THEN cs_ext_list_price*cs_quantity ELSE 0 END) AS dec_sales,
           SUM(CASE WHEN d_moy=1 THEN cs_net_paid_inc_ship_tax*cs_quantity ELSE 0 END) AS jan_net,
           SUM(CASE WHEN d_moy=2 THEN cs_net_paid_inc_ship_tax*cs_quantity ELSE 0 END) AS feb_net,
           SUM(CASE WHEN d_moy=3 THEN cs_net_paid_inc_ship_tax*cs_quantity ELSE 0 END) AS mar_net,
           SUM(CASE WHEN d_moy=4 THEN cs_net_paid_inc_ship_tax*cs_quantity ELSE 0 END) AS apr_net,
           SUM(CASE WHEN d_moy=5 THEN cs_net_paid_inc_ship_tax*cs_quantity ELSE 0 END) AS may_net,
           SUM(CASE WHEN d_moy=6 THEN cs_net_paid_inc_ship_tax*cs_quantity ELSE 0 END) AS jun_net,
           SUM(CASE WHEN d_moy=7 THEN cs_net_paid_inc_ship_tax*cs_quantity ELSE 0 END) AS jul_net,
           SUM(CASE WHEN d_moy=8 THEN cs_net_paid_inc_ship_tax*cs_quantity ELSE 0 END) AS aug_net,
           SUM(CASE WHEN d_moy=9 THEN cs_net_paid_inc_ship_tax*cs_quantity ELSE 0 END) AS sep_net,
           SUM(CASE WHEN d_moy=10 THEN cs_net_paid_inc_ship_tax*cs_quantity ELSE 0 END) AS oct_net,
           SUM(CASE WHEN d_moy=11 THEN cs_net_paid_inc_ship_tax*cs_quantity ELSE 0 END) AS nov_net,
           SUM(CASE WHEN d_moy=12 THEN cs_net_paid_inc_ship_tax*cs_quantity ELSE 0 END) AS dec_net
    FROM catalog_sales
        JOIN date_filter ON cs_sold_date_sk = d_date_sk
        JOIN time_filter ON cs_sold_time_sk = t_time_sk
        JOIN carrier_filter ON cs_ship_mode_sk = sm_ship_mode_sk
        JOIN warehouse ON cs_warehouse_sk = w_warehouse_sk
    GROUP BY w_warehouse_name, w_warehouse_sq_ft, w_city, w_county, w_state, w_country
) x
GROUP BY w_warehouse_name, w_warehouse_sq_ft, w_city, w_county, w_state, w_country, ship_carriers, year
ORDER BY w_warehouse_name
LIMIT 100
""",

    "query_85": """
WITH date_filter AS (
    SELECT d_date_sk FROM date_dim WHERE d_year = 2000
)
SELECT SUBSTR(r_reason_desc,1,20), AVG(ws_quantity), AVG(wr_refunded_cash), AVG(wr_fee)
FROM web_sales
    JOIN date_filter ON ws_sold_date_sk = d_date_sk
    JOIN web_returns ON ws_item_sk = wr_item_sk AND ws_order_number = wr_order_number
    JOIN web_page ON ws_web_page_sk = wp_web_page_sk
    JOIN customer_demographics cd1 ON cd1.cd_demo_sk = wr_refunded_cdemo_sk
    JOIN customer_demographics cd2 ON cd2.cd_demo_sk = wr_returning_cdemo_sk
    JOIN customer_address ON ca_address_sk = wr_refunded_addr_sk
    JOIN reason ON r_reason_sk = wr_reason_sk
WHERE (
    (cd1.cd_marital_status = 'M' AND cd1.cd_marital_status = cd2.cd_marital_status
     AND cd1.cd_education_status = '4 yr Degree' AND cd1.cd_education_status = cd2.cd_education_status
     AND ws_sales_price BETWEEN 100.00 AND 150.00)
    OR (cd1.cd_marital_status = 'S' AND cd1.cd_marital_status = cd2.cd_marital_status
        AND cd1.cd_education_status = 'Secondary' AND cd1.cd_education_status = cd2.cd_education_status
        AND ws_sales_price BETWEEN 50.00 AND 100.00)
    OR (cd1.cd_marital_status = 'W' AND cd1.cd_marital_status = cd2.cd_marital_status
        AND cd1.cd_education_status = 'Advanced Degree' AND cd1.cd_education_status = cd2.cd_education_status
        AND ws_sales_price BETWEEN 150.00 AND 200.00)
)
AND (
    (ca_country = 'United States' AND ca_state IN ('FL','TX','DE') AND ws_net_profit BETWEEN 100 AND 200)
    OR (ca_country = 'United States' AND ca_state IN ('IN','ND','ID') AND ws_net_profit BETWEEN 150 AND 300)
    OR (ca_country = 'United States' AND ca_state IN ('MT','IL','OH') AND ws_net_profit BETWEEN 50 AND 250)
)
GROUP BY r_reason_desc
ORDER BY SUBSTR(r_reason_desc,1,20), AVG(ws_quantity), AVG(wr_refunded_cash), AVG(wr_fee)
LIMIT 100
""",

    "query_87": """
WITH date_filter AS (
    SELECT d_date_sk, d_date FROM date_dim WHERE d_month_seq BETWEEN 1184 AND 1195
)
SELECT COUNT(*) FROM (
    (SELECT DISTINCT c_last_name, c_first_name, d_date
     FROM store_sales JOIN date_filter ON ss_sold_date_sk = d_date_sk
          JOIN customer ON ss_customer_sk = c_customer_sk)
    EXCEPT
    (SELECT DISTINCT c_last_name, c_first_name, d_date
     FROM catalog_sales JOIN date_filter ON cs_sold_date_sk = d_date_sk
          JOIN customer ON cs_bill_customer_sk = c_customer_sk)
    EXCEPT
    (SELECT DISTINCT c_last_name, c_first_name, d_date
     FROM web_sales JOIN date_filter ON ws_sold_date_sk = d_date_sk
          JOIN customer ON ws_bill_customer_sk = c_customer_sk)
) cool_cust
""",

    "query_97": """
WITH date_filter AS (
    SELECT d_date_sk FROM date_dim WHERE d_month_seq BETWEEN 1214 AND 1225
),
ssci AS (
    SELECT ss_customer_sk AS customer_sk, ss_item_sk AS item_sk
    FROM store_sales JOIN date_filter ON ss_sold_date_sk = d_date_sk
    GROUP BY ss_customer_sk, ss_item_sk
),
csci AS (
    SELECT cs_bill_customer_sk AS customer_sk, cs_item_sk AS item_sk
    FROM catalog_sales JOIN date_filter ON cs_sold_date_sk = d_date_sk
    GROUP BY cs_bill_customer_sk, cs_item_sk
)
SELECT SUM(CASE WHEN ssci.customer_sk IS NOT NULL AND csci.customer_sk IS NULL THEN 1 ELSE 0 END) store_only,
       SUM(CASE WHEN ssci.customer_sk IS NULL AND csci.customer_sk IS NOT NULL THEN 1 ELSE 0 END) catalog_only,
       SUM(CASE WHEN ssci.customer_sk IS NOT NULL AND csci.customer_sk IS NOT NULL THEN 1 ELSE 0 END) store_and_catalog
FROM ssci FULL OUTER JOIN csci ON (ssci.customer_sk = csci.customer_sk AND ssci.item_sk = csci.item_sk)
LIMIT 100
""",
}


def connect():
    parsed = urlparse(DSN)
    return snowflake.connector.connect(
        user=unquote(parsed.username),
        password=unquote(parsed.password),
        account=parsed.hostname,
        database=parsed.path.split('/')[1],
        schema=parsed.path.split('/')[2],
        warehouse=parse_qs(parsed.query).get('warehouse', [''])[0],
        role=parse_qs(parsed.query).get('role', [''])[0],
    )


def run_timed(cur, sql, timeout=300):
    cur.execute("ALTER SESSION SET USE_CACHED_RESULT = FALSE")
    cur.execute(f"ALTER SESSION SET STATEMENT_TIMEOUT_IN_SECONDS = {timeout}")
    t0 = time.time()
    try:
        cur.execute(sql)
        rows = cur.fetchall()
        elapsed = (time.time() - t0) * 1000
        return elapsed, len(rows)
    except Exception as e:
        elapsed = (time.time() - t0) * 1000
        return elapsed, f"ERROR: {e}"


if __name__ == "__main__":
    conn = connect()
    cur = conn.cursor()

    results = {}
    for qname, sql in REWRITES.items():
        print(f"\n{'='*60}")
        print(f"  {qname}")
        print(f"{'='*60}")

        print("  Warmup...", end=" ", flush=True)
        tw, rw = run_timed(cur, sql)
        print(f"{tw:.0f}ms, rows={rw}")

        if isinstance(rw, str) and "ERROR" in rw:
            results[qname] = {"avg_ms": tw, "rows": rw, "runs": [tw]}
            continue

        print("  Run 1...", end=" ", flush=True)
        t1, r1 = run_timed(cur, sql)
        print(f"{t1:.0f}ms, rows={r1}")

        print("  Run 2...", end=" ", flush=True)
        t2, r2 = run_timed(cur, sql)
        print(f"{t2:.0f}ms, rows={r2}")

        avg = (t1 + t2) / 2
        results[qname] = {"avg_ms": avg, "rows": r1, "runs": [t1, t2]}
        print(f"  >> Average: {avg:.0f}ms (was TIMEOUT >300s)")

    conn.close()

    print(f"\n\n{'='*60}")
    print("  SUMMARY")
    print(f"{'='*60}")
    for qname, r in sorted(results.items(), key=lambda x: x[0]):
        status = "WIN" if isinstance(r["rows"], int) else "ERROR"
        print(f"  {qname}: {r['avg_ms']:.0f}ms  rows={r['rows']}  [{status}]")
