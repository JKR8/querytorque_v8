WITH filtered_date AS (
    SELECT d_date_sk
    FROM date_dim
    WHERE d_moy = 12
      AND d_year = 2000
),
filtered_item AS (
    SELECT i_item_sk, i_brand_id, i_brand
    FROM item
    WHERE i_manager_id = 100
)
SELECT
    filtered_item.i_brand_id AS brand_id,
    filtered_item.i_brand AS brand,
    SUM(store_sales.ss_ext_sales_price) AS ext_price
FROM store_sales
JOIN filtered_date ON store_sales.ss_sold_date_sk = filtered_date.d_date_sk
JOIN filtered_item ON store_sales.ss_item_sk = filtered_item.i_item_sk
GROUP BY
    filtered_item.i_brand,
    filtered_item.i_brand_id
ORDER BY
    ext_price DESC,
    filtered_item.i_brand_id
LIMIT 100