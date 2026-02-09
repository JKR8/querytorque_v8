WITH filtered_dates AS (
  SELECT d_date_sk
  FROM date_dim
  WHERE d_year = 1999
    AND d_moy = 12
),
web_prejoin AS (
  SELECT
    ws.ws_item_sk AS item,
    ws.ws_quantity,
    ws.ws_net_paid,
    COALESCE(wr.wr_return_quantity, 0) AS wr_return_quantity,
    COALESCE(wr.wr_return_amt, 0) AS wr_return_amt
  FROM web_sales AS ws
  INNER JOIN filtered_dates AS d ON ws.ws_sold_date_sk = d.d_date_sk
  LEFT OUTER JOIN web_returns AS wr
    ON ws.ws_order_number = wr.wr_order_number
    AND ws.ws_item_sk = wr.wr_item_sk
    AND wr.wr_return_amt > 10000
  WHERE ws.ws_net_profit > 1
    AND ws.ws_net_paid > 0
    AND ws.ws_quantity > 0
),
catalog_prejoin AS (
  SELECT
    cs.cs_item_sk AS item,
    cs.cs_quantity,
    cs.cs_net_paid,
    COALESCE(cr.cr_return_quantity, 0) AS cr_return_quantity,
    COALESCE(cr.cr_return_amount, 0) AS cr_return_amount
  FROM catalog_sales AS cs
  INNER JOIN filtered_dates AS d ON cs.cs_sold_date_sk = d.d_date_sk
  LEFT OUTER JOIN catalog_returns AS cr
    ON cs.cs_order_number = cr.cr_order_number
    AND cs.cs_item_sk = cr.cr_item_sk
    AND cr.cr_return_amount > 10000
  WHERE cs.cs_net_profit > 1
    AND cs.cs_net_paid > 0
    AND cs.cs_quantity > 0
),
store_prejoin AS (
  SELECT
    sts.ss_item_sk AS item,
    sts.ss_quantity,
    sts.ss_net_paid,
    COALESCE(sr.sr_return_quantity, 0) AS sr_return_quantity,
    COALESCE(sr.sr_return_amt, 0) AS sr_return_amt
  FROM store_sales AS sts
  INNER JOIN filtered_dates AS d ON sts.ss_sold_date_sk = d.d_date_sk
  LEFT OUTER JOIN store_returns AS sr
    ON sts.ss_ticket_number = sr.sr_ticket_number
    AND sts.ss_item_sk = sr.sr_item_sk
    AND sr.sr_return_amt > 10000
  WHERE sts.ss_net_profit > 1
    AND sts.ss_net_paid > 0
    AND sts.ss_quantity > 0
),
web_aggregated AS (
  SELECT
    item,
    CAST(SUM(wr_return_quantity) AS DECIMAL(15,4)) / CAST(SUM(ws_quantity) AS DECIMAL(15,4)) AS return_ratio,
    CAST(SUM(wr_return_amt) AS DECIMAL(15,4)) / CAST(SUM(ws_net_paid) AS DECIMAL(15,4)) AS currency_ratio
  FROM web_prejoin
  GROUP BY item
),
catalog_aggregated AS (
  SELECT
    item,
    CAST(SUM(cr_return_quantity) AS DECIMAL(15,4)) / CAST(SUM(cs_quantity) AS DECIMAL(15,4)) AS return_ratio,
    CAST(SUM(cr_return_amount) AS DECIMAL(15,4)) / CAST(SUM(cs_net_paid) AS DECIMAL(15,4)) AS currency_ratio
  FROM catalog_prejoin
  GROUP BY item
),
store_aggregated AS (
  SELECT
    item,
    CAST(SUM(sr_return_quantity) AS DECIMAL(15,4)) / CAST(SUM(ss_quantity) AS DECIMAL(15,4)) AS return_ratio,
    CAST(SUM(sr_return_amt) AS DECIMAL(15,4)) / CAST(SUM(ss_net_paid) AS DECIMAL(15,4)) AS currency_ratio
  FROM store_prejoin
  GROUP BY item
),
web_ranked AS (
  SELECT
    'web' AS channel,
    item,
    return_ratio,
    currency_ratio,
    RANK() OVER (ORDER BY return_ratio) AS return_rank,
    RANK() OVER (ORDER BY currency_ratio) AS currency_rank
  FROM web_aggregated
),
catalog_ranked AS (
  SELECT
    'catalog' AS channel,
    item,
    return_ratio,
    currency_ratio,
    RANK() OVER (ORDER BY return_ratio) AS return_rank,
    RANK() OVER (ORDER BY currency_ratio) AS currency_rank
  FROM catalog_aggregated
),
store_ranked AS (
  SELECT
    'store' AS channel,
    item,
    return_ratio,
    currency_ratio,
    RANK() OVER (ORDER BY return_ratio) AS return_rank,
    RANK() OVER (ORDER BY currency_ratio) AS currency_rank
  FROM store_aggregated
)
SELECT channel, item, return_ratio, return_rank, currency_rank
FROM (
  SELECT channel, item, return_ratio, return_rank, currency_rank
  FROM web_ranked
  WHERE return_rank <= 10 OR currency_rank <= 10
  UNION ALL
  SELECT channel, item, return_ratio, return_rank, currency_rank
  FROM catalog_ranked
  WHERE return_rank <= 10 OR currency_rank <= 10
  UNION ALL
  SELECT channel, item, return_ratio, return_rank, currency_rank
  FROM store_ranked
  WHERE return_rank <= 10 OR currency_rank <= 10
)
ORDER BY 1, 4, 5, 2
LIMIT 100;