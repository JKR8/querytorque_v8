WITH filtered_dates AS (
  SELECT d_date_sk
  FROM date_dim
  WHERE d_date BETWEEN '2002-02-26' AND (CAST('2002-02-26' AS DATE) + INTERVAL '90' DAY)
),
item_avg_discount AS (
  SELECT 
    ws_item_sk,
    AVG(ws_ext_discount_amt) * 1.3 AS threshold
  FROM web_sales
  JOIN filtered_dates ON d_date_sk = ws_sold_date_sk
  GROUP BY ws_item_sk
)
SELECT
  SUM(ws_ext_discount_amt) AS "Excess Discount Amount"
FROM web_sales
JOIN item ON i_item_sk = ws_item_sk
JOIN filtered_dates ON d_date_sk = ws_sold_date_sk
JOIN item_avg_discount ON web_sales.ws_item_sk = item_avg_discount.ws_item_sk
WHERE
  i_manufact_id = 320
  AND ws_ext_discount_amt > item_avg_discount.threshold
ORDER BY
  SUM(ws_ext_discount_amt)
LIMIT 100