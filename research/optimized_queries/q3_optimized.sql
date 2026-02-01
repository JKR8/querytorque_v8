-- Q3: Gemini's predicate pushdown with JOIN syntax
-- Sample DB: 1.20x speedup
-- Pattern: Filter dimensions into CTEs, use explicit JOIN

WITH filtered_date AS (
    SELECT d_date_sk, d_year FROM date_dim WHERE d_moy = 11
),
filtered_item AS (
    SELECT i_item_sk, i_brand, i_brand_id FROM item WHERE i_manufact_id = 816
)
SELECT dt.d_year, item.i_brand_id brand_id, item.i_brand brand, sum(ss_sales_price) sum_agg
FROM filtered_date dt
JOIN store_sales ON dt.d_date_sk = store_sales.ss_sold_date_sk
JOIN filtered_item item ON store_sales.ss_item_sk = item.i_item_sk
GROUP BY dt.d_year, item.i_brand, item.i_brand_id
ORDER BY dt.d_year, sum_agg desc, brand_id
LIMIT 100;
