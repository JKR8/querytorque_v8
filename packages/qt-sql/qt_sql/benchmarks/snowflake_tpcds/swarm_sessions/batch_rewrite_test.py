"""Batch test date-CTE rewrites for timeout queries."""
import snowflake.connector
import time
import json
from urllib.parse import urlparse, parse_qs, unquote

DSN = 'snowflake://jkdl:QGdg%2A%24WC%25O62xS71@CVRYJTF-AW47074/SNOWFLAKE_SAMPLE_DATA/TPCDS_SF10TCL?warehouse=COMPUTE_WH&role=ACCOUNTADMIN'

REWRITES = {
    "query_55": """
WITH date_filter AS (
    SELECT d_date_sk FROM date_dim WHERE d_moy = 12 AND d_year = 2000
)
SELECT i_brand_id brand_id, i_brand brand,
    SUM(ss_ext_sales_price) ext_price
FROM store_sales
    JOIN date_filter ON d_date_sk = ss_sold_date_sk
    JOIN item ON ss_item_sk = i_item_sk
WHERE i_manager_id = 100
GROUP BY i_brand, i_brand_id
ORDER BY ext_price DESC, i_brand_id
LIMIT 100
""",

    "query_36": """
WITH date_filter AS (
    SELECT d_date_sk FROM date_dim WHERE d_year = 2002
)
SELECT
    sum(ss_net_profit)/sum(ss_ext_sales_price) as gross_margin
   ,i_category
   ,i_class
   ,grouping(i_category)+grouping(i_class) as lochierarchy
   ,rank() over (
    partition by grouping(i_category)+grouping(i_class),
    case when grouping(i_class) = 0 then i_category end
    order by sum(ss_net_profit)/sum(ss_ext_sales_price) asc) as rank_within_parent
FROM store_sales
    JOIN date_filter ON d_date_sk = ss_sold_date_sk
    JOIN item ON i_item_sk = ss_item_sk
    JOIN store ON s_store_sk = ss_store_sk
WHERE s_state IN ('SD','TN','GA','SC','MO','AL','MI','OH')
GROUP BY ROLLUP(i_category, i_class)
ORDER BY
   lochierarchy DESC
  ,case when lochierarchy = 0 then i_category end
  ,rank_within_parent
LIMIT 100
""",

    "query_53": """
WITH date_filter AS (
    SELECT d_date_sk, d_qoy FROM date_dim
    WHERE d_month_seq IN (1200,1201,1202,1203,1204,1205,1206,1207,1208,1209,1210,1211)
)
SELECT * FROM
(SELECT i_manufact_id,
    SUM(ss_sales_price) sum_sales,
    AVG(SUM(ss_sales_price)) OVER (PARTITION BY i_manufact_id) avg_quarterly_sales
FROM store_sales
    JOIN date_filter ON ss_sold_date_sk = d_date_sk
    JOIN item ON ss_item_sk = i_item_sk
    JOIN store ON ss_store_sk = s_store_sk
WHERE ((i_category IN ('Books','Children','Electronics') AND
        i_class IN ('personal','portable','reference','self-help') AND
        i_brand IN ('scholaramalgamalg #14','scholaramalgamalg #7',
                    'exportiunivamalg #9','scholaramalgamalg #9'))
    OR (i_category IN ('Women','Music','Men') AND
        i_class IN ('accessories','classical','fragrances','pants') AND
        i_brand IN ('amalgimporto #1','edu packscholar #1','exportiimporto #1',
                    'importoamalg #1')))
GROUP BY i_manufact_id, d_qoy) tmp1
WHERE CASE WHEN avg_quarterly_sales > 0
    THEN ABS(sum_sales - avg_quarterly_sales) / avg_quarterly_sales
    ELSE NULL END > 0.1
ORDER BY avg_quarterly_sales, sum_sales, i_manufact_id
LIMIT 100
""",

    "query_21": """
WITH date_filter AS (
    SELECT d_date_sk, d_date FROM date_dim
    WHERE d_date BETWEEN DATEADD(DAY, -30, '2002-02-27'::DATE) AND DATEADD(DAY, 30, '2002-02-27'::DATE)
)
SELECT * FROM (
    SELECT w_warehouse_name
          ,i_item_id
          ,SUM(CASE WHEN d_date < '2002-02-27'::DATE THEN inv_quantity_on_hand ELSE 0 END) AS inv_before
          ,SUM(CASE WHEN d_date >= '2002-02-27'::DATE THEN inv_quantity_on_hand ELSE 0 END) AS inv_after
    FROM inventory
        JOIN date_filter ON inv_date_sk = d_date_sk
        JOIN warehouse ON inv_warehouse_sk = w_warehouse_sk
        JOIN item ON i_item_sk = inv_item_sk
    WHERE i_current_price BETWEEN 0.99 AND 1.49
    GROUP BY w_warehouse_name, i_item_id
) x
WHERE (CASE WHEN inv_before > 0 THEN inv_after / inv_before ELSE NULL END)
      BETWEEN 2.0/3.0 AND 3.0/2.0
ORDER BY w_warehouse_name, i_item_id
LIMIT 100
""",

    "query_32": """
WITH date_filter AS (
    SELECT d_date_sk FROM date_dim
    WHERE d_date BETWEEN '1999-01-07' AND DATEADD(DAY, 90, '1999-01-07'::DATE)
)
SELECT SUM(cs_ext_discount_amt) AS "excess discount amount"
FROM catalog_sales
    JOIN date_filter ON d_date_sk = cs_sold_date_sk
    JOIN item ON i_item_sk = cs_item_sk
WHERE i_manufact_id = 29
  AND cs_ext_discount_amt > (
      SELECT 1.3 * AVG(cs2.cs_ext_discount_amt)
      FROM catalog_sales cs2
          JOIN date_filter df2 ON df2.d_date_sk = cs2.cs_sold_date_sk
      WHERE cs2.cs_item_sk = item.i_item_sk
  )
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

        # Warmup
        print("  Warmup...", end=" ", flush=True)
        tw, rw = run_timed(cur, sql)
        print(f"{tw:.0f}ms, rows={rw}")

        # Measure 1
        print("  Run 1...", end=" ", flush=True)
        t1, r1 = run_timed(cur, sql)
        print(f"{t1:.0f}ms, rows={r1}")

        # Measure 2
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
    for qname, r in sorted(results.items()):
        status = "WIN" if isinstance(r["rows"], int) else "ERROR"
        print(f"  {qname}: {r['avg_ms']:.0f}ms  rows={r['rows']}  [{status}]")
