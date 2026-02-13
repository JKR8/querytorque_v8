"""Batch test date-CTE rewrites for multi-channel timeout queries."""
import snowflake.connector
import time
from urllib.parse import urlparse, parse_qs, unquote

DSN = 'snowflake://jkdl:QGdg%2A%24WC%25O62xS71@CVRYJTF-AW47074/SNOWFLAKE_SAMPLE_DATA/TPCDS_SF10TCL?warehouse=COMPUTE_WH&role=ACCOUNTADMIN'

REWRITES = {
    "query_49": """
WITH date_filter AS (
    SELECT d_date_sk FROM date_dim WHERE d_year = 1999 AND d_moy = 12
)
SELECT channel, item, return_ratio, return_rank, currency_rank FROM (
  SELECT 'web' AS channel, item, return_ratio, return_rank, currency_rank FROM (
    SELECT item, return_ratio, currency_ratio,
           RANK() OVER (ORDER BY return_ratio) AS return_rank,
           RANK() OVER (ORDER BY currency_ratio) AS currency_rank
    FROM (
      SELECT ws.ws_item_sk AS item,
             CAST(SUM(COALESCE(wr.wr_return_quantity,0)) AS DECIMAL(15,4)) /
             CAST(SUM(COALESCE(ws.ws_quantity,0)) AS DECIMAL(15,4)) AS return_ratio,
             CAST(SUM(COALESCE(wr.wr_return_amt,0)) AS DECIMAL(15,4)) /
             CAST(SUM(COALESCE(ws.ws_net_paid,0)) AS DECIMAL(15,4)) AS currency_ratio
      FROM web_sales ws
           LEFT OUTER JOIN web_returns wr ON (ws.ws_order_number = wr.wr_order_number AND ws.ws_item_sk = wr.wr_item_sk)
           JOIN date_filter ON ws.ws_sold_date_sk = d_date_sk
      WHERE wr.wr_return_amt > 10000 AND ws.ws_net_profit > 1 AND ws.ws_net_paid > 0 AND ws.ws_quantity > 0
      GROUP BY ws.ws_item_sk
    )
  ) WHERE return_rank <= 10 OR currency_rank <= 10
  UNION
  SELECT 'catalog' AS channel, item, return_ratio, return_rank, currency_rank FROM (
    SELECT item, return_ratio, currency_ratio,
           RANK() OVER (ORDER BY return_ratio) AS return_rank,
           RANK() OVER (ORDER BY currency_ratio) AS currency_rank
    FROM (
      SELECT cs.cs_item_sk AS item,
             CAST(SUM(COALESCE(cr.cr_return_quantity,0)) AS DECIMAL(15,4)) /
             CAST(SUM(COALESCE(cs.cs_quantity,0)) AS DECIMAL(15,4)) AS return_ratio,
             CAST(SUM(COALESCE(cr.cr_return_amount,0)) AS DECIMAL(15,4)) /
             CAST(SUM(COALESCE(cs.cs_net_paid,0)) AS DECIMAL(15,4)) AS currency_ratio
      FROM catalog_sales cs
           LEFT OUTER JOIN catalog_returns cr ON (cs.cs_order_number = cr.cr_order_number AND cs.cs_item_sk = cr.cr_item_sk)
           JOIN date_filter ON cs.cs_sold_date_sk = d_date_sk
      WHERE cr.cr_return_amount > 10000 AND cs.cs_net_profit > 1 AND cs.cs_net_paid > 0 AND cs.cs_quantity > 0
      GROUP BY cs.cs_item_sk
    )
  ) WHERE return_rank <= 10 OR currency_rank <= 10
  UNION
  SELECT 'store' AS channel, item, return_ratio, return_rank, currency_rank FROM (
    SELECT item, return_ratio, currency_ratio,
           RANK() OVER (ORDER BY return_ratio) AS return_rank,
           RANK() OVER (ORDER BY currency_ratio) AS currency_rank
    FROM (
      SELECT sts.ss_item_sk AS item,
             CAST(SUM(COALESCE(sr.sr_return_quantity,0)) AS DECIMAL(15,4)) /
             CAST(SUM(COALESCE(sts.ss_quantity,0)) AS DECIMAL(15,4)) AS return_ratio,
             CAST(SUM(COALESCE(sr.sr_return_amt,0)) AS DECIMAL(15,4)) /
             CAST(SUM(COALESCE(sts.ss_net_paid,0)) AS DECIMAL(15,4)) AS currency_ratio
      FROM store_sales sts
           LEFT OUTER JOIN store_returns sr ON (sts.ss_ticket_number = sr.sr_ticket_number AND sts.ss_item_sk = sr.sr_item_sk)
           JOIN date_filter ON sts.ss_sold_date_sk = d_date_sk
      WHERE sr.sr_return_amt > 10000 AND sts.ss_net_profit > 1 AND sts.ss_net_paid > 0 AND sts.ss_quantity > 0
      GROUP BY sts.ss_item_sk
    )
  ) WHERE return_rank <= 10 OR currency_rank <= 10
)
ORDER BY 1,4,5,2
LIMIT 100
""",

    "query_77": """
WITH date_filter AS (
    SELECT d_date_sk FROM date_dim
    WHERE d_date BETWEEN '1998-08-05'::DATE AND DATEADD(DAY, 30, '1998-08-05'::DATE)
),
ss AS (
    SELECT s_store_sk, SUM(ss_ext_sales_price) AS sales, SUM(ss_net_profit) AS profit
    FROM store_sales JOIN date_filter ON ss_sold_date_sk = d_date_sk JOIN store ON ss_store_sk = s_store_sk
    GROUP BY s_store_sk
),
sr AS (
    SELECT s_store_sk, SUM(sr_return_amt) AS "returns", SUM(sr_net_loss) AS profit_loss
    FROM store_returns JOIN date_filter ON sr_returned_date_sk = d_date_sk JOIN store ON sr_store_sk = s_store_sk
    GROUP BY s_store_sk
),
cs AS (
    SELECT cs_call_center_sk, SUM(cs_ext_sales_price) AS sales, SUM(cs_net_profit) AS profit
    FROM catalog_sales JOIN date_filter ON cs_sold_date_sk = d_date_sk
    GROUP BY cs_call_center_sk
),
cr AS (
    SELECT cr_call_center_sk, SUM(cr_return_amount) AS "returns", SUM(cr_net_loss) AS profit_loss
    FROM catalog_returns JOIN date_filter ON cr_returned_date_sk = d_date_sk
    GROUP BY cr_call_center_sk
),
ws AS (
    SELECT wp_web_page_sk, SUM(ws_ext_sales_price) AS sales, SUM(ws_net_profit) AS profit
    FROM web_sales JOIN date_filter ON ws_sold_date_sk = d_date_sk JOIN web_page ON ws_web_page_sk = wp_web_page_sk
    GROUP BY wp_web_page_sk
),
wr AS (
    SELECT wp_web_page_sk, SUM(wr_return_amt) AS "returns", SUM(wr_net_loss) AS profit_loss
    FROM web_returns JOIN date_filter ON wr_returned_date_sk = d_date_sk JOIN web_page ON wr_web_page_sk = wp_web_page_sk
    GROUP BY wp_web_page_sk
)
SELECT channel, id, SUM(sales) AS sales, SUM("returns") AS "returns", SUM(profit) AS profit
FROM (
    SELECT 'store channel' AS channel, ss.s_store_sk AS id, sales,
           COALESCE("returns", 0) AS "returns", (profit - COALESCE(profit_loss,0)) AS profit
    FROM ss LEFT JOIN sr ON ss.s_store_sk = sr.s_store_sk
    UNION ALL
    SELECT 'catalog channel', cs_call_center_sk, sales, "returns", (profit - profit_loss)
    FROM cs, cr
    UNION ALL
    SELECT 'web channel', ws.wp_web_page_sk, sales,
           COALESCE("returns", 0), (profit - COALESCE(profit_loss,0))
    FROM ws LEFT JOIN wr ON ws.wp_web_page_sk = wr.wp_web_page_sk
) x
GROUP BY ROLLUP(channel, id)
ORDER BY channel, id
LIMIT 100
""",

    "query_5": """
WITH date_filter AS (
    SELECT d_date_sk FROM date_dim
    WHERE d_date BETWEEN '2000-08-19'::DATE AND DATEADD(DAY, 14, '2000-08-19'::DATE)
),
ssr AS (
    SELECT s_store_id, SUM(sales_price) AS sales, SUM(profit) AS profit,
           SUM(return_amt) AS "returns", SUM(net_loss) AS profit_loss
    FROM (
        SELECT ss_store_sk AS store_sk, ss_sold_date_sk AS date_sk,
               ss_ext_sales_price AS sales_price, ss_net_profit AS profit,
               CAST(0 AS DECIMAL(7,2)) AS return_amt, CAST(0 AS DECIMAL(7,2)) AS net_loss
        FROM store_sales
        UNION ALL
        SELECT sr_store_sk, sr_returned_date_sk, CAST(0 AS DECIMAL(7,2)),
               CAST(0 AS DECIMAL(7,2)), sr_return_amt, sr_net_loss
        FROM store_returns
    ) salesreturns
    JOIN date_filter ON date_sk = d_date_sk
    JOIN store ON store_sk = s_store_sk
    GROUP BY s_store_id
),
csr AS (
    SELECT cp_catalog_page_id, SUM(sales_price) AS sales, SUM(profit) AS profit,
           SUM(return_amt) AS "returns", SUM(net_loss) AS profit_loss
    FROM (
        SELECT cs_catalog_page_sk AS page_sk, cs_sold_date_sk AS date_sk,
               cs_ext_sales_price AS sales_price, cs_net_profit AS profit,
               CAST(0 AS DECIMAL(7,2)) AS return_amt, CAST(0 AS DECIMAL(7,2)) AS net_loss
        FROM catalog_sales
        UNION ALL
        SELECT cr_catalog_page_sk, cr_returned_date_sk, CAST(0 AS DECIMAL(7,2)),
               CAST(0 AS DECIMAL(7,2)), cr_return_amount, cr_net_loss
        FROM catalog_returns
    ) salesreturns
    JOIN date_filter ON date_sk = d_date_sk
    JOIN catalog_page ON page_sk = cp_catalog_page_sk
    GROUP BY cp_catalog_page_id
),
wsr AS (
    SELECT web_site_id, SUM(sales_price) AS sales, SUM(profit) AS profit,
           SUM(return_amt) AS "returns", SUM(net_loss) AS profit_loss
    FROM (
        SELECT ws_web_site_sk AS wsr_web_site_sk, ws_sold_date_sk AS date_sk,
               ws_ext_sales_price AS sales_price, ws_net_profit AS profit,
               CAST(0 AS DECIMAL(7,2)) AS return_amt, CAST(0 AS DECIMAL(7,2)) AS net_loss
        FROM web_sales
        UNION ALL
        SELECT ws_web_site_sk AS wsr_web_site_sk, wr_returned_date_sk AS date_sk,
               CAST(0 AS DECIMAL(7,2)), CAST(0 AS DECIMAL(7,2)), wr_return_amt, wr_net_loss
        FROM web_returns LEFT OUTER JOIN web_sales ON (wr_item_sk = ws_item_sk AND wr_order_number = ws_order_number)
    ) salesreturns
    JOIN date_filter ON date_sk = d_date_sk
    JOIN web_site ON wsr_web_site_sk = web_site_sk
    GROUP BY web_site_id
)
SELECT channel, id, SUM(sales) AS sales, SUM("returns") AS "returns", SUM(profit) AS profit
FROM (
    SELECT 'store channel' AS channel, 'store' || s_store_id AS id, sales, "returns", (profit - profit_loss) AS profit FROM ssr
    UNION ALL
    SELECT 'catalog channel', 'catalog_page' || cp_catalog_page_id, sales, "returns", (profit - profit_loss) FROM csr
    UNION ALL
    SELECT 'web channel', 'web_site' || web_site_id, sales, "returns", (profit - profit_loss) FROM wsr
) x
GROUP BY ROLLUP(channel, id)
ORDER BY channel, id
LIMIT 100
""",

    "query_80": """
WITH date_filter AS (
    SELECT d_date_sk FROM date_dim
    WHERE d_date BETWEEN '1998-08-28'::DATE AND DATEADD(DAY, 30, '1998-08-28'::DATE)
),
ssr AS (
    SELECT s_store_id AS store_id,
           SUM(ss_ext_sales_price) AS sales,
           SUM(COALESCE(sr_return_amt, 0)) AS "returns",
           SUM(ss_net_profit - COALESCE(sr_net_loss, 0)) AS profit
    FROM store_sales LEFT OUTER JOIN store_returns ON (ss_item_sk = sr_item_sk AND ss_ticket_number = sr_ticket_number)
    JOIN date_filter ON ss_sold_date_sk = d_date_sk
    JOIN store ON ss_store_sk = s_store_sk
    JOIN item ON ss_item_sk = i_item_sk
    JOIN promotion ON ss_promo_sk = p_promo_sk
    WHERE i_current_price > 50 AND p_channel_tv = 'N'
    GROUP BY s_store_id
),
csr AS (
    SELECT cp_catalog_page_id AS catalog_page_id,
           SUM(cs_ext_sales_price) AS sales,
           SUM(COALESCE(cr_return_amount, 0)) AS "returns",
           SUM(cs_net_profit - COALESCE(cr_net_loss, 0)) AS profit
    FROM catalog_sales LEFT OUTER JOIN catalog_returns ON (cs_item_sk = cr_item_sk AND cs_order_number = cr_order_number)
    JOIN date_filter ON cs_sold_date_sk = d_date_sk
    JOIN catalog_page ON cs_catalog_page_sk = cp_catalog_page_sk
    JOIN item ON cs_item_sk = i_item_sk
    JOIN promotion ON cs_promo_sk = p_promo_sk
    WHERE i_current_price > 50 AND p_channel_tv = 'N'
    GROUP BY cp_catalog_page_id
),
wsr AS (
    SELECT web_site_id,
           SUM(ws_ext_sales_price) AS sales,
           SUM(COALESCE(wr_return_amt, 0)) AS "returns",
           SUM(ws_net_profit - COALESCE(wr_net_loss, 0)) AS profit
    FROM web_sales LEFT OUTER JOIN web_returns ON (ws_item_sk = wr_item_sk AND ws_order_number = wr_order_number)
    JOIN date_filter ON ws_sold_date_sk = d_date_sk
    JOIN web_site ON ws_web_site_sk = web_site_sk
    JOIN item ON ws_item_sk = i_item_sk
    JOIN promotion ON ws_promo_sk = p_promo_sk
    WHERE i_current_price > 50 AND p_channel_tv = 'N'
    GROUP BY web_site_id
)
SELECT channel, id, SUM(sales) AS sales, SUM("returns") AS "returns", SUM(profit) AS profit
FROM (
    SELECT 'store channel' AS channel, 'store' || store_id AS id, sales, "returns", profit FROM ssr
    UNION ALL
    SELECT 'catalog channel', 'catalog_page' || catalog_page_id, sales, "returns", profit FROM csr
    UNION ALL
    SELECT 'web channel', 'web_site' || web_site_id, sales, "returns", profit FROM wsr
) x
GROUP BY ROLLUP(channel, id)
ORDER BY channel, id
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

        if isinstance(rw, str) and "ERROR" in rw:
            results[qname] = {"avg_ms": tw, "rows": rw, "runs": [tw]}
            continue

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
