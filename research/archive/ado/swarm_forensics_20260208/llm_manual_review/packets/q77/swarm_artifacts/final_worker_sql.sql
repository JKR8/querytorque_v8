WITH date_range AS (
  SELECT d_date_sk
  FROM date_dim
  WHERE d_date BETWEEN CAST('1998-08-05' AS DATE) AND (
    CAST('1998-08-05' AS DATE) + INTERVAL '30' DAY
  )
), store_channel AS (
  SELECT
    COALESCE(ss_store_sk, sr_store_sk) AS store_sk,
    SUM(CASE WHEN type = 'sale' THEN ss_ext_sales_price ELSE 0 END) AS sales,
    SUM(CASE WHEN type = 'sale' THEN ss_net_profit ELSE 0 END) AS profit,
    SUM(CASE WHEN type = 'return' THEN sr_return_amt ELSE 0 END) AS returns,
    SUM(CASE WHEN type = 'return' THEN sr_net_loss ELSE 0 END) AS profit_loss
  FROM (
    SELECT
      ss_store_sk,
      ss_ext_sales_price,
      ss_net_profit,
      NULL AS sr_return_amt,
      NULL AS sr_net_loss,
      'sale' AS type
    FROM store_sales
    INNER JOIN date_range ON ss_sold_date_sk = d_date_sk
    UNION ALL
    SELECT
      sr_store_sk,
      NULL,
      NULL,
      sr_return_amt,
      sr_net_loss,
      'return' AS type
    FROM store_returns
    INNER JOIN date_range ON sr_returned_date_sk = d_date_sk
  ) AS combined
  GROUP BY store_sk
), catalog_channel AS (
  SELECT
    COALESCE(cs_call_center_sk, cr_call_center_sk) AS call_center_sk,
    SUM(CASE WHEN type = 'sale' THEN cs_ext_sales_price ELSE 0 END) AS sales,
    SUM(CASE WHEN type = 'sale' THEN cs_net_profit ELSE 0 END) AS profit,
    SUM(CASE WHEN type = 'return' THEN cr_return_amount ELSE 0 END) AS returns,
    SUM(CASE WHEN type = 'return' THEN cr_net_loss ELSE 0 END) AS profit_loss
  FROM (
    SELECT
      cs_call_center_sk,
      cs_ext_sales_price,
      cs_net_profit,
      NULL AS cr_return_amount,
      NULL AS cr_net_loss,
      'sale' AS type
    FROM catalog_sales
    INNER JOIN date_range ON cs_sold_date_sk = d_date_sk
    UNION ALL
    SELECT
      cr_call_center_sk,
      NULL,
      NULL,
      cr_return_amount,
      cr_net_loss,
      'return' AS type
    FROM catalog_returns
    INNER JOIN date_range ON cr_returned_date_sk = d_date_sk
  ) AS combined
  GROUP BY call_center_sk
), web_channel AS (
  SELECT
    COALESCE(ws_web_page_sk, wr_web_page_sk) AS web_page_sk,
    SUM(CASE WHEN type = 'sale' THEN ws_ext_sales_price ELSE 0 END) AS sales,
    SUM(CASE WHEN type = 'sale' THEN ws_net_profit ELSE 0 END) AS profit,
    SUM(CASE WHEN type = 'return' THEN wr_return_amt ELSE 0 END) AS returns,
    SUM(CASE WHEN type = 'return' THEN wr_net_loss ELSE 0 END) AS profit_loss
  FROM (
    SELECT
      ws_web_page_sk,
      ws_ext_sales_price,
      ws_net_profit,
      NULL AS wr_return_amt,
      NULL AS wr_net_loss,
      'sale' AS type
    FROM web_sales
    INNER JOIN date_range ON ws_sold_date_sk = d_date_sk
    UNION ALL
    SELECT
      wr_web_page_sk,
      NULL,
      NULL,
      wr_return_amt,
      wr_net_loss,
      'return' AS type
    FROM web_returns
    INNER JOIN date_range ON wr_returned_date_sk = d_date_sk
  ) AS combined
  GROUP BY web_page_sk
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
    s.s_store_sk AS id,
    sc.sales AS sales,
    COALESCE(sc.returns, 0) AS "returns",
    (sc.profit - COALESCE(sc.profit_loss, 0)) AS profit
  FROM store_channel sc
  INNER JOIN store s ON sc.store_sk = s.s_store_sk
  UNION ALL
  SELECT
    'catalog channel' AS channel,
    cc.call_center_sk AS id,
    cc.sales AS sales,
    cc.returns AS "returns",
    (cc.profit - cc.profit_loss) AS profit
  FROM catalog_channel cc
  UNION ALL
  SELECT
    'web channel' AS channel,
    wp.wp_web_page_sk AS id,
    wc.sales AS sales,
    COALESCE(wc.returns, 0) AS "returns",
    (wc.profit - COALESCE(wc.profit_loss, 0)) AS profit
  FROM web_channel wc
  INNER JOIN web_page wp ON wc.web_page_sk = wp.wp_web_page_sk
) AS x
GROUP BY ROLLUP(channel, id)
ORDER BY channel, id
LIMIT 100