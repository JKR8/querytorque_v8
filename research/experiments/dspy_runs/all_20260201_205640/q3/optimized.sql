WITH filtered_date AS (
    SELECT d_date_sk, d_year
    FROM date_dim
    WHERE d_moy = 11
), filtered_item AS (
    SELECT i_item_sk, i_brand_id, i_brand
    FROM item
    WHERE i_manufact_id = 816
), joined_data AS (
    SELECT dt.d_year,
           item.i_brand_id,
           item.i_brand,
           ss.ss_sales_price
    FROM filtered_date dt
    JOIN store_sales ss ON dt.d_date_sk = ss.ss_sold_date_sk
    JOIN filtered_item item ON ss.ss_item_sk = item.i_item_sk
)
SELECT d_year,
       i_brand_id AS brand_id,
       i_brand AS brand,
       SUM(ss_sales_price) AS sum_agg
FROM joined_data
GROUP BY d_year, i_brand, i_brand_id
ORDER BY d_year, sum_agg DESC, i_brand_id
LIMIT 100;