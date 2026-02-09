WITH filtered_dates AS (
  SELECT d_date_sk
  FROM date_dim
  WHERE d_date BETWEEN '1999-01-07' AND (
    CAST('1999-01-07' AS DATE) + INTERVAL '90' DAY
  )
),
filtered_items AS (
  SELECT i_item_sk
  FROM item
  WHERE i_manufact_id = 29
),
item_avg_discount AS (
  SELECT
    cs_item_sk,
    1.3 * AVG(cs_ext_discount_amt) AS threshold
  FROM catalog_sales
  JOIN filtered_dates ON d_date_sk = cs_sold_date_sk
  GROUP BY cs_item_sk
)
SELECT
  SUM(cs_ext_discount_amt) AS "excess discount amount"
FROM catalog_sales
JOIN filtered_items ON i_item_sk = cs_item_sk
JOIN filtered_dates ON d_date_sk = cs_sold_date_sk
JOIN item_avg_discount ON catalog_sales.cs_item_sk = item_avg_discount.cs_item_sk
WHERE cs_ext_discount_amt > threshold
LIMIT 100