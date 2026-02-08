WITH filtered_dates AS (
  SELECT d_date_sk, d_year
  FROM date_dim
  WHERE d_moy = 11
    AND d_year = 2002
),
filtered_items AS (
  SELECT i_item_sk, i_category_id, i_category
  FROM item
  WHERE i_manager_id = 1
),
filtered_sales AS (
  SELECT ss_ext_sales_price, ss_item_sk, d_year
  FROM store_sales
  JOIN filtered_dates ON store_sales.ss_sold_date_sk = filtered_dates.d_date_sk
)
SELECT
  filtered_sales.d_year,
  filtered_items.i_category_id,
  filtered_items.i_category,
  SUM(filtered_sales.ss_ext_sales_price)
FROM filtered_sales
JOIN filtered_items ON filtered_sales.ss_item_sk = filtered_items.i_item_sk
GROUP BY
  filtered_sales.d_year,
  filtered_items.i_category_id,
  filtered_items.i_category
ORDER BY
  SUM(filtered_sales.ss_ext_sales_price) DESC,
  filtered_sales.d_year,
  filtered_items.i_category_id,
  filtered_items.i_category
LIMIT 100