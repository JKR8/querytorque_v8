WITH filtered_dates AS (
  SELECT d_date_sk
  FROM date_dim
  WHERE d_moy = 12
    AND d_year = 2000
),
filtered_items AS (
  SELECT i_item_sk, i_brand_id, i_brand
  FROM item
  WHERE i_manager_id = 100
)
SELECT
  i_brand_id AS brand_id,
  i_brand AS brand,
  SUM(ss_ext_sales_price) AS ext_price
FROM store_sales
JOIN filtered_dates ON store_sales.ss_sold_date_sk = filtered_dates.d_date_sk
JOIN filtered_items ON store_sales.ss_item_sk = filtered_items.i_item_sk
GROUP BY
  i_brand,
  i_brand_id
ORDER BY
  ext_price DESC,
  i_brand_id
LIMIT 100