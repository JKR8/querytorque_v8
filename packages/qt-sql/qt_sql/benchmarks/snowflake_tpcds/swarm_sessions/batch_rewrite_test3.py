"""Batch test date-CTE rewrites for triple-date-dim and Q16 timeout queries."""
import snowflake.connector
import time
from urllib.parse import urlparse, parse_qs, unquote

DSN = 'snowflake://jkdl:QGdg%2A%24WC%25O62xS71@CVRYJTF-AW47074/SNOWFLAKE_SAMPLE_DATA/TPCDS_SF10TCL?warehouse=COMPUTE_WH&role=ACCOUNTADMIN'

REWRITES = {
    "query_25": """
WITH d1 AS (SELECT d_date_sk FROM date_dim WHERE d_moy = 4 AND d_year = 2000),
     d2 AS (SELECT d_date_sk FROM date_dim WHERE d_moy BETWEEN 4 AND 10 AND d_year = 2000),
     d3 AS (SELECT d_date_sk FROM date_dim WHERE d_moy BETWEEN 4 AND 10 AND d_year = 2000)
SELECT i_item_id, i_item_desc, s_store_id, s_store_name,
       SUM(ss_net_profit) AS store_sales_profit,
       SUM(sr_net_loss) AS store_returns_loss,
       SUM(cs_net_profit) AS catalog_sales_profit
FROM store_sales
    JOIN d1 ON d1.d_date_sk = ss_sold_date_sk
    JOIN store_returns ON ss_customer_sk = sr_customer_sk AND ss_item_sk = sr_item_sk AND ss_ticket_number = sr_ticket_number
    JOIN d2 ON sr_returned_date_sk = d2.d_date_sk
    JOIN catalog_sales ON sr_customer_sk = cs_bill_customer_sk AND sr_item_sk = cs_item_sk
    JOIN d3 ON cs_sold_date_sk = d3.d_date_sk
    JOIN store ON s_store_sk = ss_store_sk
    JOIN item ON i_item_sk = ss_item_sk
GROUP BY i_item_id, i_item_desc, s_store_id, s_store_name
ORDER BY i_item_id, i_item_desc, s_store_id, s_store_name
LIMIT 100
""",

    "query_29": """
WITH d1 AS (SELECT d_date_sk FROM date_dim WHERE d_moy = 4 AND d_year = 1999),
     d2 AS (SELECT d_date_sk FROM date_dim WHERE d_moy BETWEEN 4 AND 7 AND d_year = 1999),
     d3 AS (SELECT d_date_sk FROM date_dim WHERE d_year IN (1999, 2000, 2001))
SELECT i_item_id, i_item_desc, s_store_id, s_store_name,
       AVG(ss_quantity) AS store_sales_quantity,
       AVG(sr_return_quantity) AS store_returns_quantity,
       AVG(cs_quantity) AS catalog_sales_quantity
FROM store_sales
    JOIN d1 ON d1.d_date_sk = ss_sold_date_sk
    JOIN store_returns ON ss_customer_sk = sr_customer_sk AND ss_item_sk = sr_item_sk AND ss_ticket_number = sr_ticket_number
    JOIN d2 ON sr_returned_date_sk = d2.d_date_sk
    JOIN catalog_sales ON sr_customer_sk = cs_bill_customer_sk AND sr_item_sk = cs_item_sk
    JOIN d3 ON cs_sold_date_sk = d3.d_date_sk
    JOIN store ON s_store_sk = ss_store_sk
    JOIN item ON i_item_sk = ss_item_sk
GROUP BY i_item_id, i_item_desc, s_store_id, s_store_name
ORDER BY i_item_id, i_item_desc, s_store_id, s_store_name
LIMIT 100
""",

    "query_17": """
WITH d1 AS (SELECT d_date_sk FROM date_dim WHERE d_quarter_name = '2001Q1'),
     d2 AS (SELECT d_date_sk FROM date_dim WHERE d_quarter_name IN ('2001Q1','2001Q2','2001Q3')),
     d3 AS (SELECT d_date_sk FROM date_dim WHERE d_quarter_name IN ('2001Q1','2001Q2','2001Q3'))
SELECT i_item_id, i_item_desc, s_state,
       COUNT(ss_quantity) AS store_sales_quantitycount,
       AVG(ss_quantity) AS store_sales_quantityave,
       STDDEV_SAMP(ss_quantity) AS store_sales_quantitystdev,
       STDDEV_SAMP(ss_quantity)/AVG(ss_quantity) AS store_sales_quantitycov,
       COUNT(sr_return_quantity) AS store_returns_quantitycount,
       AVG(sr_return_quantity) AS store_returns_quantityave,
       STDDEV_SAMP(sr_return_quantity) AS store_returns_quantitystdev,
       STDDEV_SAMP(sr_return_quantity)/AVG(sr_return_quantity) AS store_returns_quantitycov,
       COUNT(cs_quantity) AS catalog_sales_quantitycount,
       AVG(cs_quantity) AS catalog_sales_quantityave,
       STDDEV_SAMP(cs_quantity) AS catalog_sales_quantitystdev,
       STDDEV_SAMP(cs_quantity)/AVG(cs_quantity) AS catalog_sales_quantitycov
FROM store_sales
    JOIN d1 ON d1.d_date_sk = ss_sold_date_sk
    JOIN store_returns ON ss_customer_sk = sr_customer_sk AND ss_item_sk = sr_item_sk AND ss_ticket_number = sr_ticket_number
    JOIN d2 ON sr_returned_date_sk = d2.d_date_sk
    JOIN catalog_sales ON sr_customer_sk = cs_bill_customer_sk AND sr_item_sk = cs_item_sk
    JOIN d3 ON cs_sold_date_sk = d3.d_date_sk
    JOIN store ON s_store_sk = ss_store_sk
    JOIN item ON i_item_sk = ss_item_sk
GROUP BY i_item_id, i_item_desc, s_state
ORDER BY i_item_id, i_item_desc, s_state
LIMIT 100
""",

    "query_16": """
WITH date_filter AS (
    SELECT d_date_sk FROM date_dim
    WHERE d_date BETWEEN '2002-04-01'::DATE AND DATEADD(DAY, 60, '2002-04-01'::DATE)
)
SELECT COUNT(DISTINCT cs_order_number) AS "order count",
       SUM(cs_ext_ship_cost) AS "total shipping cost",
       SUM(cs_net_profit) AS "total net profit"
FROM catalog_sales cs1
    JOIN date_filter ON cs1.cs_ship_date_sk = d_date_sk
    JOIN customer_address ON cs1.cs_ship_addr_sk = ca_address_sk
    JOIN call_center ON cs1.cs_call_center_sk = cc_call_center_sk
WHERE ca_state = 'WV'
  AND cc_county IN ('Ziebach County','Luce County','Richland County','Daviess County','Barrow County')
  AND EXISTS (SELECT 1 FROM catalog_sales cs2
              WHERE cs1.cs_order_number = cs2.cs_order_number
                AND cs1.cs_warehouse_sk <> cs2.cs_warehouse_sk)
  AND NOT EXISTS (SELECT 1 FROM catalog_returns cr1
                  WHERE cs1.cs_order_number = cr1.cr_order_number)
ORDER BY 1
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
    for qname, r in sorted(results.items()):
        status = "WIN" if isinstance(r["rows"], int) else "ERROR"
        print(f"  {qname}: {r['avg_ms']:.0f}ms  rows={r['rows']}  [{status}]")
