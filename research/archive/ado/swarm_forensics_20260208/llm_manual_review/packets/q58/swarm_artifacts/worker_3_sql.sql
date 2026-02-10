WITH filtered_dates AS (
  SELECT d_date_sk, d_date
  FROM date_dim
  WHERE d_week_seq = (
    SELECT d_week_seq
    FROM date_dim
    WHERE d_date = '2001-03-24'
  )
),
all_sales AS (
  SELECT 
    i.i_item_id AS item_id,
    'store' AS channel,
    ss.ss_ext_sales_price AS sales_price
  FROM store_sales ss
  JOIN filtered_dates d ON ss.ss_sold_date_sk = d.d_date_sk
  JOIN item i ON ss.ss_item_sk = i.i_item_sk
  
  UNION ALL
  
  SELECT 
    i.i_item_id AS item_id,
    'catalog' AS channel,
    cs.cs_ext_sales_price AS sales_price
  FROM catalog_sales cs
  JOIN filtered_dates d ON cs.cs_sold_date_sk = d.d_date_sk
  JOIN item i ON cs.cs_item_sk = i.i_item_sk
  
  UNION ALL
  
  SELECT 
    i.i_item_id AS item_id,
    'web' AS channel,
    ws.ws_ext_sales_price AS sales_price
  FROM web_sales ws
  JOIN filtered_dates d ON ws.ws_sold_date_sk = d.d_date_sk
  JOIN item i ON ws.ws_item_sk = i.i_item_sk
),
item_revenues AS (
  SELECT
    item_id,
    SUM(CASE WHEN channel = 'store' THEN sales_price ELSE 0 END) AS ss_item_rev,
    SUM(CASE WHEN channel = 'catalog' THEN sales_price ELSE 0 END) AS cs_item_rev,
    SUM(CASE WHEN channel = 'web' THEN sales_price ELSE 0 END) AS ws_item_rev
  FROM all_sales
  GROUP BY item_id
  HAVING
    COUNT(CASE WHEN channel = 'store' AND sales_price IS NOT NULL THEN 1 END) > 0
    AND COUNT(CASE WHEN channel = 'catalog' AND sales_price IS NOT NULL THEN 1 END) > 0
    AND COUNT(CASE WHEN channel = 'web' AND sales_price IS NOT NULL THEN 1 END) > 0
)
SELECT
  item_id,
  ss_item_rev,
  ss_item_rev / ((ss_item_rev + cs_item_rev + ws_item_rev) / 3) * 100 AS ss_dev,
  cs_item_rev,
  cs_item_rev / ((ss_item_rev + cs_item_rev + ws_item_rev) / 3) * 100 AS cs_dev,
  ws_item_rev,
  ws_item_rev / ((ss_item_rev + cs_item_rev + ws_item_rev) / 3) * 100 AS ws_dev,
  (ss_item_rev + cs_item_rev + ws_item_rev) / 3 AS average
FROM item_revenues
WHERE
  ss_item_rev BETWEEN 0.9 * cs_item_rev AND 1.1 * cs_item_rev
  AND ss_item_rev BETWEEN 0.9 * ws_item_rev AND 1.1 * ws_item_rev
  AND cs_item_rev BETWEEN 0.9 * ss_item_rev AND 1.1 * ss_item_rev
  AND cs_item_rev BETWEEN 0.9 * ws_item_rev AND 1.1 * ws_item_rev
  AND ws_item_rev BETWEEN 0.9 * ss_item_rev AND 1.1 * ss_item_rev
  AND ws_item_rev BETWEEN 0.9 * cs_item_rev AND 1.1 * cs_item_rev
ORDER BY
  item_id,
  ss_item_rev
LIMIT 100;