WITH filtered_dates AS (
  SELECT d_date_sk
  FROM date_dim
  WHERE d_date BETWEEN CAST('1998-08-28' AS DATE) 
    AND (CAST('1998-08-28' AS DATE) + INTERVAL '30' DAY)
),
filtered_items AS (
  SELECT i_item_sk
  FROM item
  WHERE i_current_price > 50
),
filtered_promotions AS (
  SELECT p_promo_sk
  FROM promotion
  WHERE p_channel_tv = 'N'
),
-- Store channel: pre-filter returns, then aggregate sales and returns separately
filtered_store_returns AS (
  SELECT
    sr_item_sk,
    sr_ticket_number,
    SUM(sr_return_amt) AS return_amt_sum,
    SUM(sr_net_loss) AS net_loss_sum
  FROM store_returns
  JOIN filtered_items ON sr_item_sk = i_item_sk
  GROUP BY sr_item_sk, sr_ticket_number
),
store_sales_agg AS (
  SELECT
    s_store_id,
    ss_item_sk,
    ss_ticket_number,
    SUM(ss_ext_sales_price) AS sales_price_sum,
    SUM(ss_net_profit) AS net_profit_sum
  FROM store_sales
  JOIN filtered_dates ON ss_sold_date_sk = d_date_sk
  JOIN filtered_items ON ss_item_sk = i_item_sk
  JOIN filtered_promotions ON ss_promo_sk = p_promo_sk
  JOIN store ON ss_store_sk = s_store_sk
  GROUP BY s_store_id, ss_item_sk, ss_ticket_number
),
ssr AS (
  SELECT
    s_store_id AS store_id,
    SUM(sales_price_sum) AS sales,
    SUM(COALESCE(return_amt_sum, 0)) AS "returns",
    SUM(net_profit_sum - COALESCE(net_loss_sum, 0)) AS profit
  FROM store_sales_agg
  LEFT JOIN filtered_store_returns
    ON ss_item_sk = sr_item_sk AND ss_ticket_number = sr_ticket_number
  GROUP BY s_store_id
),
-- Catalog channel: same pattern
filtered_catalog_returns AS (
  SELECT
    cr_item_sk,
    cr_order_number,
    SUM(cr_return_amount) AS return_amount_sum,
    SUM(cr_net_loss) AS net_loss_sum
  FROM catalog_returns
  JOIN filtered_items ON cr_item_sk = i_item_sk
  GROUP BY cr_item_sk, cr_order_number
),
catalog_sales_agg AS (
  SELECT
    cp_catalog_page_id,
    cs_item_sk,
    cs_order_number,
    SUM(cs_ext_sales_price) AS sales_price_sum,
    SUM(cs_net_profit) AS net_profit_sum
  FROM catalog_sales
  JOIN filtered_dates ON cs_sold_date_sk = d_date_sk
  JOIN filtered_items ON cs_item_sk = i_item_sk
  JOIN filtered_promotions ON cs_promo_sk = p_promo_sk
  JOIN catalog_page ON cs_catalog_page_sk = cp_catalog_page_sk
  GROUP BY cp_catalog_page_id, cs_item_sk, cs_order_number
),
csr AS (
  SELECT
    cp_catalog_page_id AS catalog_page_id,
    SUM(sales_price_sum) AS sales,
    SUM(COALESCE(return_amount_sum, 0)) AS "returns",
    SUM(net_profit_sum - COALESCE(net_loss_sum, 0)) AS profit
  FROM catalog_sales_agg
  LEFT JOIN filtered_catalog_returns
    ON cs_item_sk = cr_item_sk AND cs_order_number = cr_order_number
  GROUP BY cp_catalog_page_id
),
-- Web channel: same pattern
filtered_web_returns AS (
  SELECT
    wr_item_sk,
    wr_order_number,
    SUM(wr_return_amt) AS return_amt_sum,
    SUM(wr_net_loss) AS net_loss_sum
  FROM web_returns
  JOIN filtered_items ON wr_item_sk = i_item_sk
  GROUP BY wr_item_sk, wr_order_number
),
web_sales_agg AS (
  SELECT
    web_site_id,
    ws_item_sk,
    ws_order_number,
    SUM(ws_ext_sales_price) AS sales_price_sum,
    SUM(ws_net_profit) AS net_profit_sum
  FROM web_sales
  JOIN filtered_dates ON ws_sold_date_sk = d_date_sk
  JOIN filtered_items ON ws_item_sk = i_item_sk
  JOIN filtered_promotions ON ws_promo_sk = p_promo_sk
  JOIN web_site ON ws_web_site_sk = web_site_sk
  GROUP BY web_site_id, ws_item_sk, ws_order_number
),
wsr AS (
  SELECT
    web_site_id,
    SUM(sales_price_sum) AS sales,
    SUM(COALESCE(return_amt_sum, 0)) AS "returns",
    SUM(net_profit_sum - COALESCE(net_loss_sum, 0)) AS profit
  FROM web_sales_agg
  LEFT JOIN filtered_web_returns
    ON ws_item_sk = wr_item_sk AND ws_order_number = wr_order_number
  GROUP BY web_site_id
)
SELECT
  channel,
  id,
  SUM(sales) AS sales,
  SUM("returns") AS "returns",
  SUM(profit) AS profit
FROM (
  SELECT
    'store channel' AS channel,
    'store' || store_id AS id,
    sales,
    "returns",
    profit
  FROM ssr
  UNION ALL
  SELECT
    'catalog channel' AS channel,
    'catalog_page' || catalog_page_id AS id,
    sales,
    "returns",
    profit
  FROM csr
  UNION ALL
  SELECT
    'web channel' AS channel,
    'web_site' || web_site_id AS id,
    sales,
    "returns",
    profit
  FROM wsr
) AS x
GROUP BY
  ROLLUP (
    channel,
    id
  )
ORDER BY
  channel,
  id
LIMIT 100;