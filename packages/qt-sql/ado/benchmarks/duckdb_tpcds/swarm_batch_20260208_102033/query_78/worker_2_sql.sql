WITH filtered_date AS (
  SELECT d_date_sk, d_year
  FROM date_dim
  WHERE d_year = 2000
),
ws AS (
  SELECT
    fd.d_year AS ws_sold_year,
    ws.ws_item_sk,
    ws.ws_bill_customer_sk AS ws_customer_sk,
    SUM(ws.ws_quantity) AS ws_qty,
    SUM(ws.ws_wholesale_cost) AS ws_wc,
    SUM(ws.ws_sales_price) AS ws_sp
  FROM web_sales ws
  LEFT JOIN web_returns wr
    ON wr.wr_order_number = ws.ws_order_number
    AND ws.ws_item_sk = wr.wr_item_sk
  JOIN filtered_date fd
    ON ws.ws_sold_date_sk = fd.d_date_sk
  WHERE wr.wr_order_number IS NULL
  GROUP BY
    fd.d_year,
    ws.ws_item_sk,
    ws.ws_bill_customer_sk
),
cs AS (
  SELECT
    fd.d_year AS cs_sold_year,
    cs.cs_item_sk,
    cs.cs_bill_customer_sk AS cs_customer_sk,
    SUM(cs.cs_quantity) AS cs_qty,
    SUM(cs.cs_wholesale_cost) AS cs_wc,
    SUM(cs.cs_sales_price) AS cs_sp
  FROM catalog_sales cs
  LEFT JOIN catalog_returns cr
    ON cr.cr_order_number = cs.cs_order_number
    AND cs.cs_item_sk = cr.cr_item_sk
  JOIN filtered_date fd
    ON cs.cs_sold_date_sk = fd.d_date_sk
  WHERE cr.cr_order_number IS NULL
  GROUP BY
    fd.d_year,
    cs.cs_item_sk,
    cs.cs_bill_customer_sk
),
ss AS (
  SELECT
    fd.d_year AS ss_sold_year,
    ss.ss_item_sk,
    ss.ss_customer_sk,
    SUM(ss.ss_quantity) AS ss_qty,
    SUM(ss.ss_wholesale_cost) AS ss_wc,
    SUM(ss.ss_sales_price) AS ss_sp
  FROM store_sales ss
  LEFT JOIN store_returns sr
    ON sr.sr_ticket_number = ss.ss_ticket_number
    AND ss.ss_item_sk = sr.sr_item_sk
  JOIN filtered_date fd
    ON ss.ss_sold_date_sk = fd.d_date_sk
  WHERE sr.sr_ticket_number IS NULL
  GROUP BY
    fd.d_year,
    ss.ss_item_sk,
    ss.ss_customer_sk
)
SELECT
  ss.ss_item_sk,
  ROUND(ss.ss_qty / (
    COALESCE(ws.ws_qty, 0) + COALESCE(cs.cs_qty, 0)
  ), 2) AS ratio,
  ss.ss_qty AS store_qty,
  ss.ss_wc AS store_wholesale_cost,
  ss.ss_sp AS store_sales_price,
  COALESCE(ws.ws_qty, 0) + COALESCE(cs.cs_qty, 0) AS other_chan_qty,
  COALESCE(ws.ws_wc, 0) + COALESCE(cs.cs_wc, 0) AS other_chan_wholesale_cost,
  COALESCE(ws.ws_sp, 0) + COALESCE(cs.cs_sp, 0) AS other_chan_sales_price
FROM ss
LEFT JOIN ws
  ON ws.ws_sold_year = ss.ss_sold_year
  AND ws.ws_item_sk = ss.ss_item_sk
  AND ws.ws_customer_sk = ss.ss_customer_sk
LEFT JOIN cs
  ON cs.cs_sold_year = ss.ss_sold_year
  AND cs.cs_item_sk = ss.ss_item_sk
  AND cs.cs_customer_sk = ss.ss_customer_sk
WHERE
  COALESCE(ws.ws_qty, 0) > 0
  OR COALESCE(cs.cs_qty, 0) > 0
ORDER BY
  ss.ss_item_sk,
  ss.ss_qty DESC,
  ss.ss_wc DESC,
  ss.ss_sp DESC,
  other_chan_qty,
  other_chan_wholesale_cost,
  other_chan_sales_price,
  ratio
LIMIT 100