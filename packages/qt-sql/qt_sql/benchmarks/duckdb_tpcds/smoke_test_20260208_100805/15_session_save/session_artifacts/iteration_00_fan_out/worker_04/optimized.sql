WITH filtered_date AS (
  SELECT d_date_sk, d_year
  FROM date_dim
  WHERE d_moy = 11
    AND d_year = 2002
),
filtered_item AS (
  SELECT i_item_sk, i_category_id, i_category
  FROM item
  WHERE i_manager_id = 1
)
SELECT
  dt.d_year,
  item.i_category_id,
  item.i_category,
  SUM(ss_ext_sales_price)
FROM store_sales
JOIN filtered_date AS dt ON store_sales.ss_sold_date_sk = dt.d_date_sk
JOIN filtered_item AS item ON store_sales.ss_item_sk = item.i_item_sk
GROUP BY
  dt.d_year,
  item.i_category_id,
  item.i_category
ORDER BY
  SUM(ss_ext_sales_price) DESC,
  dt.d_year,
  item.i_category_id,
  item.i_category
LIMIT 100