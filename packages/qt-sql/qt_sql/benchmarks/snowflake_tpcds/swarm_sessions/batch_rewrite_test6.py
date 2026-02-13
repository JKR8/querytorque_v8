"""Batch 6: date-CTE rewrites for Q47, Q57, Q89, Q70, Q51, Q79, Q76, Q71."""
import snowflake.connector
import time
from urllib.parse import urlparse, parse_qs, unquote

DSN = 'snowflake://jkdl:QGdg%2A%24WC%25O62xS71@CVRYJTF-AW47074/SNOWFLAKE_SAMPLE_DATA/TPCDS_SF10TCL?warehouse=COMPUTE_WH&role=ACCOUNTADMIN'

REWRITES = {

    "query_47": """
WITH date_filter AS (
    SELECT d_date_sk, d_year, d_moy FROM date_dim
    WHERE d_year = 2001
       OR (d_year = 2000 AND d_moy = 12)
       OR (d_year = 2002 AND d_moy = 1)
),
v1 AS (
    SELECT i_category, i_brand,
           s_store_name, s_company_name,
           d_year, d_moy,
           SUM(ss_sales_price) sum_sales,
           AVG(SUM(ss_sales_price)) OVER
             (PARTITION BY i_category, i_brand, s_store_name, s_company_name, d_year)
             avg_monthly_sales,
           RANK() OVER
             (PARTITION BY i_category, i_brand, s_store_name, s_company_name
              ORDER BY d_year, d_moy) rn
    FROM store_sales
        JOIN date_filter ON ss_sold_date_sk = d_date_sk
        JOIN item ON ss_item_sk = i_item_sk
        JOIN store ON ss_store_sk = s_store_sk
    GROUP BY i_category, i_brand, s_store_name, s_company_name, d_year, d_moy
),
v2 AS (
    SELECT v1.s_store_name, v1.d_year, v1.avg_monthly_sales,
           v1.sum_sales, v1_lag.sum_sales psum, v1_lead.sum_sales nsum
    FROM v1
        JOIN v1 v1_lag ON v1.i_category = v1_lag.i_category
            AND v1.i_brand = v1_lag.i_brand
            AND v1.s_store_name = v1_lag.s_store_name
            AND v1.s_company_name = v1_lag.s_company_name
            AND v1.rn = v1_lag.rn + 1
        JOIN v1 v1_lead ON v1.i_category = v1_lead.i_category
            AND v1.i_brand = v1_lead.i_brand
            AND v1.s_store_name = v1_lead.s_store_name
            AND v1.s_company_name = v1_lead.s_company_name
            AND v1.rn = v1_lead.rn - 1
)
SELECT * FROM v2
WHERE d_year = 2001
  AND avg_monthly_sales > 0
  AND CASE WHEN avg_monthly_sales > 0 THEN ABS(sum_sales - avg_monthly_sales) / avg_monthly_sales ELSE NULL END > 0.1
ORDER BY sum_sales - avg_monthly_sales, nsum
LIMIT 100
""",

    "query_57": """
WITH date_filter AS (
    SELECT d_date_sk, d_year, d_moy FROM date_dim
    WHERE d_year = 1999
       OR (d_year = 1998 AND d_moy = 12)
       OR (d_year = 2000 AND d_moy = 1)
),
v1 AS (
    SELECT i_category, i_brand,
           cc_name,
           d_year, d_moy,
           SUM(cs_sales_price) sum_sales,
           AVG(SUM(cs_sales_price)) OVER
             (PARTITION BY i_category, i_brand, cc_name, d_year)
             avg_monthly_sales,
           RANK() OVER
             (PARTITION BY i_category, i_brand, cc_name
              ORDER BY d_year, d_moy) rn
    FROM catalog_sales
        JOIN date_filter ON cs_sold_date_sk = d_date_sk
        JOIN item ON cs_item_sk = i_item_sk
        JOIN call_center ON cc_call_center_sk = cs_call_center_sk
    GROUP BY i_category, i_brand, cc_name, d_year, d_moy
),
v2 AS (
    SELECT v1.i_brand, v1.d_year, v1.avg_monthly_sales,
           v1.sum_sales, v1_lag.sum_sales psum, v1_lead.sum_sales nsum
    FROM v1
        JOIN v1 v1_lag ON v1.i_category = v1_lag.i_category
            AND v1.i_brand = v1_lag.i_brand
            AND v1.cc_name = v1_lag.cc_name
            AND v1.rn = v1_lag.rn + 1
        JOIN v1 v1_lead ON v1.i_category = v1_lead.i_category
            AND v1.i_brand = v1_lead.i_brand
            AND v1.cc_name = v1_lead.cc_name
            AND v1.rn = v1_lead.rn - 1
)
SELECT * FROM v2
WHERE d_year = 1999
  AND avg_monthly_sales > 0
  AND CASE WHEN avg_monthly_sales > 0 THEN ABS(sum_sales - avg_monthly_sales) / avg_monthly_sales ELSE NULL END > 0.1
ORDER BY sum_sales - avg_monthly_sales, nsum
LIMIT 100
""",

    "query_89": """
WITH date_filter AS (
    SELECT d_date_sk, d_moy FROM date_dim WHERE d_year = 1999
)
SELECT * FROM (
    SELECT i_category, i_class, i_brand,
           s_store_name, s_company_name,
           d_moy,
           SUM(ss_sales_price) sum_sales,
           AVG(SUM(ss_sales_price)) OVER
             (PARTITION BY i_category, i_brand, s_store_name, s_company_name)
             avg_monthly_sales
    FROM store_sales
        JOIN date_filter ON ss_sold_date_sk = d_date_sk
        JOIN item ON ss_item_sk = i_item_sk
        JOIN store ON ss_store_sk = s_store_sk
    WHERE ((i_category IN ('Jewelry','Shoes','Electronics') AND i_class IN ('semi-precious','athletic','portable'))
        OR (i_category IN ('Men','Music','Women') AND i_class IN ('accessories','rock','maternity')))
    GROUP BY i_category, i_class, i_brand, s_store_name, s_company_name, d_moy
) tmp1
WHERE CASE WHEN avg_monthly_sales <> 0 THEN ABS(sum_sales - avg_monthly_sales) / avg_monthly_sales ELSE NULL END > 0.1
ORDER BY sum_sales - avg_monthly_sales, s_store_name
LIMIT 100
""",

    "query_70": """
WITH date_filter AS (
    SELECT d_date_sk FROM date_dim WHERE d_month_seq BETWEEN 1213 AND 1213+11
)
SELECT
    SUM(ss_net_profit) AS total_sum,
    s_state, s_county,
    GROUPING(s_state) + GROUPING(s_county) AS lochierarchy,
    RANK() OVER (
        PARTITION BY GROUPING(s_state) + GROUPING(s_county),
        CASE WHEN GROUPING(s_county) = 0 THEN s_state END
        ORDER BY SUM(ss_net_profit) DESC) AS rank_within_parent
FROM store_sales
    JOIN date_filter d1 ON d1.d_date_sk = ss_sold_date_sk
    JOIN store ON s_store_sk = ss_store_sk
WHERE s_state IN (
    SELECT s_state FROM (
        SELECT s_state,
               RANK() OVER (PARTITION BY s_state ORDER BY SUM(ss_net_profit) DESC) AS ranking
        FROM store_sales
            JOIN date_filter ON date_filter.d_date_sk = ss_sold_date_sk
            JOIN store ON s_store_sk = ss_store_sk
        GROUP BY s_state
    ) tmp1
    WHERE ranking <= 5
)
GROUP BY ROLLUP(s_state, s_county)
ORDER BY lochierarchy DESC,
    CASE WHEN lochierarchy = 0 THEN s_state END,
    rank_within_parent
LIMIT 100
""",

    "query_51": """
WITH date_filter AS (
    SELECT d_date_sk, d_date FROM date_dim WHERE d_month_seq BETWEEN 1216 AND 1216+11
),
web_v1 AS (
    SELECT ws_item_sk item_sk, d_date,
           SUM(SUM(ws_sales_price))
               OVER (PARTITION BY ws_item_sk ORDER BY d_date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) cume_sales
    FROM web_sales
        JOIN date_filter ON ws_sold_date_sk = d_date_sk
    WHERE ws_item_sk IS NOT NULL
    GROUP BY ws_item_sk, d_date
),
store_v1 AS (
    SELECT ss_item_sk item_sk, d_date,
           SUM(SUM(ss_sales_price))
               OVER (PARTITION BY ss_item_sk ORDER BY d_date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) cume_sales
    FROM store_sales
        JOIN date_filter ON ss_sold_date_sk = d_date_sk
    WHERE ss_item_sk IS NOT NULL
    GROUP BY ss_item_sk, d_date
)
SELECT * FROM (
    SELECT item_sk, d_date, web_sales, store_sales,
           MAX(web_sales) OVER (PARTITION BY item_sk ORDER BY d_date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) web_cumulative,
           MAX(store_sales) OVER (PARTITION BY item_sk ORDER BY d_date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) store_cumulative
    FROM (
        SELECT CASE WHEN web.item_sk IS NOT NULL THEN web.item_sk ELSE store.item_sk END item_sk,
               CASE WHEN web.d_date IS NOT NULL THEN web.d_date ELSE store.d_date END d_date,
               web.cume_sales web_sales,
               store.cume_sales store_sales
        FROM web_v1 web FULL OUTER JOIN store_v1 store
            ON web.item_sk = store.item_sk AND web.d_date = store.d_date
    ) x
) y
WHERE web_cumulative > store_cumulative
ORDER BY item_sk, d_date
LIMIT 100
""",

    "query_79": """
WITH date_filter AS (
    SELECT d_date_sk FROM date_dim WHERE d_dow = 1 AND d_year IN (1998, 1999, 2000)
)
SELECT c_last_name, c_first_name, SUBSTR(s_city,1,30), ss_ticket_number, amt, profit
FROM (
    SELECT ss_ticket_number, ss_customer_sk, store.s_city,
           SUM(ss_coupon_amt) amt, SUM(ss_net_profit) profit
    FROM store_sales
        JOIN date_filter ON ss_sold_date_sk = d_date_sk
        JOIN store ON ss_store_sk = s_store_sk
        JOIN household_demographics ON ss_hdemo_sk = hd_demo_sk
    WHERE (hd_dep_count = 5 OR hd_vehicle_count > 4)
      AND s_number_employees BETWEEN 200 AND 295
    GROUP BY ss_ticket_number, ss_customer_sk, ss_addr_sk, store.s_city
) ms
JOIN customer ON ss_customer_sk = c_customer_sk
ORDER BY c_last_name, c_first_name, SUBSTR(s_city,1,30), profit
LIMIT 100
""",

    "query_61": """
WITH date_filter AS (
    SELECT d_date_sk FROM date_dim WHERE d_year = 1999 AND d_moy = 11
)
SELECT promotions, total,
       CAST(promotions AS DECIMAL(15,4)) / CAST(total AS DECIMAL(15,4)) * 100
FROM
  (SELECT SUM(ss_ext_sales_price) promotions
   FROM store_sales
       JOIN date_filter ON ss_sold_date_sk = d_date_sk
       JOIN store ON ss_store_sk = s_store_sk
       JOIN promotion ON ss_promo_sk = p_promo_sk
       JOIN customer ON ss_customer_sk = c_customer_sk
       JOIN customer_address ON ca_address_sk = c_current_addr_sk
       JOIN item ON ss_item_sk = i_item_sk
   WHERE ca_gmt_offset = -7
     AND i_category = 'Jewelry'
     AND (p_channel_dmail = 'Y' OR p_channel_email = 'Y' OR p_channel_tv = 'Y')
     AND s_gmt_offset = -7) promotional_sales,
  (SELECT SUM(ss_ext_sales_price) total
   FROM store_sales
       JOIN date_filter ON ss_sold_date_sk = d_date_sk
       JOIN store ON ss_store_sk = s_store_sk
       JOIN customer ON ss_customer_sk = c_customer_sk
       JOIN customer_address ON ca_address_sk = c_current_addr_sk
       JOIN item ON ss_item_sk = i_item_sk
   WHERE ca_gmt_offset = -7
     AND i_category = 'Jewelry'
     AND s_gmt_offset = -7) all_sales
ORDER BY promotions, total
LIMIT 100
""",

    "query_67": """
WITH date_filter AS (
    SELECT d_date_sk, d_year, d_qoy, d_moy FROM date_dim
    WHERE d_month_seq BETWEEN 1206 AND 1206+11
)
SELECT * FROM (
    SELECT i_category, i_class, i_brand, i_product_name,
           d_year, d_qoy, d_moy, s_store_id, sumsales,
           RANK() OVER (PARTITION BY i_category ORDER BY sumsales DESC) rk
    FROM (
        SELECT i_category, i_class, i_brand, i_product_name,
               d_year, d_qoy, d_moy, s_store_id,
               SUM(COALESCE(ss_sales_price * ss_quantity, 0)) sumsales
        FROM store_sales
            JOIN date_filter ON ss_sold_date_sk = d_date_sk
            JOIN store ON ss_store_sk = s_store_sk
            JOIN item ON ss_item_sk = i_item_sk
        GROUP BY ROLLUP(i_category, i_class, i_brand, i_product_name, d_year, d_qoy, d_moy, s_store_id)
    ) dw1
) dw2
WHERE rk <= 100
ORDER BY i_category, i_class, i_brand, i_product_name,
         d_year, d_qoy, d_moy, s_store_id, sumsales, rk
LIMIT 100
""",

    "query_71": """
WITH date_filter AS (
    SELECT d_date_sk FROM date_dim WHERE d_moy = 12 AND d_year = 1998
)
SELECT i_brand_id brand_id, i_brand brand, t_hour, t_minute,
       SUM(ext_price) ext_price
FROM item,
    (SELECT ws_ext_sales_price AS ext_price,
            ws_sold_date_sk AS sold_date_sk,
            ws_item_sk AS sold_item_sk,
            ws_sold_time_sk AS time_sk
     FROM web_sales JOIN date_filter ON d_date_sk = ws_sold_date_sk
     UNION ALL
     SELECT cs_ext_sales_price AS ext_price,
            cs_sold_date_sk AS sold_date_sk,
            cs_item_sk AS sold_item_sk,
            cs_sold_time_sk AS time_sk
     FROM catalog_sales JOIN date_filter ON d_date_sk = cs_sold_date_sk
     UNION ALL
     SELECT ss_ext_sales_price AS ext_price,
            ss_sold_date_sk AS sold_date_sk,
            ss_item_sk AS sold_item_sk,
            ss_sold_time_sk AS time_sk
     FROM store_sales JOIN date_filter ON d_date_sk = ss_sold_date_sk
    ) tmp
    JOIN time_dim ON time_sk = t_time_sk
WHERE sold_item_sk = i_item_sk
  AND i_manager_id = 1
  AND (t_meal_time = 'breakfast' OR t_meal_time = 'dinner')
GROUP BY i_brand, i_brand_id, t_hour, t_minute
ORDER BY ext_price DESC, i_brand_id
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
