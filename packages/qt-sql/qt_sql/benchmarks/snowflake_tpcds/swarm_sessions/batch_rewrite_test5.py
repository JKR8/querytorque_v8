"""Batch 5: EXISTS-pattern queries Q10, Q35, Q69 + Q72, Q88, Q93."""
import snowflake.connector
import time
from urllib.parse import urlparse, parse_qs, unquote

DSN = 'snowflake://jkdl:QGdg%2A%24WC%25O62xS71@CVRYJTF-AW47074/SNOWFLAKE_SAMPLE_DATA/TPCDS_SF10TCL?warehouse=COMPUTE_WH&role=ACCOUNTADMIN'

REWRITES = {

    "query_10": """
WITH d_ss AS (SELECT d_date_sk FROM date_dim WHERE d_year = 2001 AND d_moy BETWEEN 1 AND 4),
     d_ws AS (SELECT d_date_sk FROM date_dim WHERE d_year = 2001 AND d_moy BETWEEN 1 AND 4),
     d_cs AS (SELECT d_date_sk FROM date_dim WHERE d_year = 2001 AND d_moy BETWEEN 1 AND 4)
SELECT cd_gender, cd_marital_status, cd_education_status,
       COUNT(*) cnt1, cd_purchase_estimate, COUNT(*) cnt2,
       cd_credit_rating, COUNT(*) cnt3, cd_dep_count, COUNT(*) cnt4,
       cd_dep_employed_count, COUNT(*) cnt5, cd_dep_college_count, COUNT(*) cnt6
FROM customer c
    JOIN customer_address ca ON c.c_current_addr_sk = ca.ca_address_sk
    JOIN customer_demographics ON cd_demo_sk = c.c_current_cdemo_sk
WHERE ca_county IN ('Storey County','Marquette County','Warren County','Cochran County','Kandiyohi County')
  AND EXISTS (SELECT 1 FROM store_sales JOIN d_ss ON ss_sold_date_sk = d_date_sk
              WHERE c.c_customer_sk = ss_customer_sk)
  AND (EXISTS (SELECT 1 FROM web_sales JOIN d_ws ON ws_sold_date_sk = d_date_sk
               WHERE c.c_customer_sk = ws_bill_customer_sk)
       OR EXISTS (SELECT 1 FROM catalog_sales JOIN d_cs ON cs_sold_date_sk = d_date_sk
                  WHERE c.c_customer_sk = cs_ship_customer_sk))
GROUP BY cd_gender, cd_marital_status, cd_education_status, cd_purchase_estimate,
         cd_credit_rating, cd_dep_count, cd_dep_employed_count, cd_dep_college_count
ORDER BY cd_gender, cd_marital_status, cd_education_status, cd_purchase_estimate,
         cd_credit_rating, cd_dep_count, cd_dep_employed_count, cd_dep_college_count
LIMIT 100
""",

    "query_35": """
WITH d_ss AS (SELECT d_date_sk FROM date_dim WHERE d_year = 2001 AND d_qoy < 4),
     d_ws AS (SELECT d_date_sk FROM date_dim WHERE d_year = 2001 AND d_qoy < 4),
     d_cs AS (SELECT d_date_sk FROM date_dim WHERE d_year = 2001 AND d_qoy < 4)
SELECT ca_state, cd_gender, cd_marital_status, cd_dep_count,
       COUNT(*) cnt1, MAX(cd_dep_count), SUM(cd_dep_count), MAX(cd_dep_count),
       cd_dep_employed_count, COUNT(*) cnt2, MAX(cd_dep_employed_count),
       SUM(cd_dep_employed_count), MAX(cd_dep_employed_count),
       cd_dep_college_count, COUNT(*) cnt3, MAX(cd_dep_college_count),
       SUM(cd_dep_college_count), MAX(cd_dep_college_count)
FROM customer c
    JOIN customer_address ca ON c.c_current_addr_sk = ca.ca_address_sk
    JOIN customer_demographics ON cd_demo_sk = c.c_current_cdemo_sk
WHERE EXISTS (SELECT 1 FROM store_sales JOIN d_ss ON ss_sold_date_sk = d_date_sk
              WHERE c.c_customer_sk = ss_customer_sk)
  AND (EXISTS (SELECT 1 FROM web_sales JOIN d_ws ON ws_sold_date_sk = d_date_sk
               WHERE c.c_customer_sk = ws_bill_customer_sk)
       OR EXISTS (SELECT 1 FROM catalog_sales JOIN d_cs ON cs_sold_date_sk = d_date_sk
                  WHERE c.c_customer_sk = cs_ship_customer_sk))
GROUP BY ca_state, cd_gender, cd_marital_status, cd_dep_count,
         cd_dep_employed_count, cd_dep_college_count
ORDER BY ca_state, cd_gender, cd_marital_status, cd_dep_count,
         cd_dep_employed_count, cd_dep_college_count
LIMIT 100
""",

    "query_69": """
WITH d_ss AS (SELECT d_date_sk FROM date_dim WHERE d_year = 2000 AND d_moy BETWEEN 1 AND 3),
     d_ws AS (SELECT d_date_sk FROM date_dim WHERE d_year = 2000 AND d_moy BETWEEN 1 AND 3),
     d_cs AS (SELECT d_date_sk FROM date_dim WHERE d_year = 2000 AND d_moy BETWEEN 1 AND 3)
SELECT cd_gender, cd_marital_status, cd_education_status,
       COUNT(*) cnt1, cd_purchase_estimate, COUNT(*) cnt2,
       cd_credit_rating, COUNT(*) cnt3
FROM customer c
    JOIN customer_address ca ON c.c_current_addr_sk = ca.ca_address_sk
    JOIN customer_demographics ON cd_demo_sk = c.c_current_cdemo_sk
WHERE ca_state IN ('TX','VA','MI')
  AND EXISTS (SELECT 1 FROM store_sales JOIN d_ss ON ss_sold_date_sk = d_date_sk
              WHERE c.c_customer_sk = ss_customer_sk)
  AND NOT EXISTS (SELECT 1 FROM web_sales JOIN d_ws ON ws_sold_date_sk = d_date_sk
                  WHERE c.c_customer_sk = ws_bill_customer_sk)
  AND NOT EXISTS (SELECT 1 FROM catalog_sales JOIN d_cs ON cs_sold_date_sk = d_date_sk
                  WHERE c.c_customer_sk = cs_ship_customer_sk)
GROUP BY cd_gender, cd_marital_status, cd_education_status,
         cd_purchase_estimate, cd_credit_rating
ORDER BY cd_gender, cd_marital_status, cd_education_status,
         cd_purchase_estimate, cd_credit_rating
LIMIT 100
""",

    "query_72": """
WITH d1_filter AS (SELECT d_date_sk, d_date, d_week_seq FROM date_dim WHERE d_year = 2002)
SELECT i_item_desc, w_warehouse_name, d1.d_week_seq,
       SUM(CASE WHEN p_promo_sk IS NULL THEN 1 ELSE 0 END) no_promo,
       SUM(CASE WHEN p_promo_sk IS NOT NULL THEN 1 ELSE 0 END) promo,
       COUNT(*) total_cnt
FROM catalog_sales
    JOIN d1_filter d1 ON cs_sold_date_sk = d1.d_date_sk
    JOIN inventory ON cs_item_sk = inv_item_sk
    JOIN warehouse ON w_warehouse_sk = inv_warehouse_sk
    JOIN item ON i_item_sk = cs_item_sk
    JOIN customer_demographics ON cs_bill_cdemo_sk = cd_demo_sk
    JOIN household_demographics ON cs_bill_hdemo_sk = hd_demo_sk
    JOIN date_dim d2 ON inv_date_sk = d2.d_date_sk AND d1.d_week_seq = d2.d_week_seq
    JOIN date_dim d3 ON cs_ship_date_sk = d3.d_date_sk AND d3.d_date > DATEADD(DAY, 5, d1.d_date)
    LEFT OUTER JOIN promotion ON cs_promo_sk = p_promo_sk
    LEFT OUTER JOIN catalog_returns ON cr_item_sk = cs_item_sk AND cr_order_number = cs_order_number
WHERE inv_quantity_on_hand < cs_quantity
  AND hd_buy_potential = '501-1000'
  AND cd_marital_status = 'W'
GROUP BY i_item_desc, w_warehouse_name, d1.d_week_seq
ORDER BY total_cnt DESC, i_item_desc, w_warehouse_name, d1.d_week_seq
LIMIT 100
""",

    "query_88": """
WITH time_slots AS (
    SELECT t_time_sk,
           CASE WHEN t_hour = 8 AND t_minute >= 30 THEN 1
                WHEN t_hour = 9 AND t_minute < 30 THEN 2
                WHEN t_hour = 9 AND t_minute >= 30 THEN 3
                WHEN t_hour = 10 AND t_minute < 30 THEN 4
                WHEN t_hour = 10 AND t_minute >= 30 THEN 5
                WHEN t_hour = 11 AND t_minute < 30 THEN 6
                WHEN t_hour = 11 AND t_minute >= 30 THEN 7
                WHEN t_hour = 12 AND t_minute < 30 THEN 8
           END AS slot
    FROM time_dim
    WHERE t_hour BETWEEN 8 AND 12
),
base AS (
    SELECT slot
    FROM store_sales
        JOIN time_slots ON ss_sold_time_sk = t_time_sk
        JOIN household_demographics ON ss_hdemo_sk = hd_demo_sk
        JOIN store ON ss_store_sk = s_store_sk
    WHERE s_store_name = 'ese'
      AND ((hd_dep_count = -1 AND hd_vehicle_count <= 1)
           OR (hd_dep_count = 4 AND hd_vehicle_count <= 6)
           OR (hd_dep_count = 3 AND hd_vehicle_count <= 5))
      AND slot IS NOT NULL
)
SELECT
    SUM(CASE WHEN slot = 1 THEN 1 ELSE 0 END) AS h8_30_to_9,
    SUM(CASE WHEN slot = 2 THEN 1 ELSE 0 END) AS h9_to_9_30,
    SUM(CASE WHEN slot = 3 THEN 1 ELSE 0 END) AS h9_30_to_10,
    SUM(CASE WHEN slot = 4 THEN 1 ELSE 0 END) AS h10_to_10_30,
    SUM(CASE WHEN slot = 5 THEN 1 ELSE 0 END) AS h10_30_to_11,
    SUM(CASE WHEN slot = 6 THEN 1 ELSE 0 END) AS h11_to_11_30,
    SUM(CASE WHEN slot = 7 THEN 1 ELSE 0 END) AS h11_30_to_12,
    SUM(CASE WHEN slot = 8 THEN 1 ELSE 0 END) AS h12_to_12_30
FROM base
""",

    "query_93": """
SELECT ss_customer_sk, SUM(act_sales) sumsales
FROM (
    SELECT ss_item_sk, ss_ticket_number, ss_customer_sk,
           CASE WHEN sr_return_quantity IS NOT NULL
                THEN (ss_quantity - sr_return_quantity) * ss_sales_price
                ELSE ss_quantity * ss_sales_price END AS act_sales
    FROM store_sales
        LEFT OUTER JOIN store_returns ON sr_item_sk = ss_item_sk AND sr_ticket_number = ss_ticket_number
        JOIN reason ON sr_reason_sk = r_reason_sk
    WHERE r_reason_desc = 'duplicate purchase'
) t
GROUP BY ss_customer_sk
ORDER BY sumsales, ss_customer_sk
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
