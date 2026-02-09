WITH filtered_dates AS (
  SELECT d_date_sk
  FROM date_dim
  WHERE d_date BETWEEN '2002-02-26' AND (
    CAST('2002-02-26' AS DATE) + INTERVAL '90' DAY
  )
),
filtered_items AS (
  SELECT i_item_sk
  FROM item
  WHERE i_manufact_id = 320
),
item_avg_discount AS (
  SELECT 
    ws_item_sk,
    1.3 * AVG(ws_ext_discount_amt) AS avg_threshold
  FROM web_sales
  JOIN filtered_dates ON ws_sold_date_sk = d_date_sk
  GROUP BY ws_item_sk
)
SELECT
  SUM(ws.ws_ext_discount_amt) AS "Excess Discount Amount"
FROM web_sales ws
JOIN filtered_items fi ON ws.ws_item_sk = fi.i_item_sk
JOIN filtered_dates fd ON ws.ws_sold_date_sk = fd.d_date_sk
JOIN item_avg_discount iad ON ws.ws_item_sk = iad.ws_item_sk
WHERE ws.ws_ext_discount_amt > iad.avg_threshold
ORDER BY SUM(ws.ws_ext_discount_amt)
LIMIT 100