-- Q22 current state: optimized (retry3w_2, 1.69x)
-- Source: /mnt/c/Users/jakc9/Documents/QueryTorque_V8/retry_collect/q22/w3_optimized.sql
-- Best speedup: 1.69x

WITH filtered_dates AS (SELECT d_date_sk FROM date_dim WHERE d_month_seq BETWEEN 1200 AND 1200 + 11)
SELECT i_product_name, i_brand, i_class, i_category, AVG(inv_quantity_on_hand) AS qoh FROM inventory JOIN filtered_dates ON inv_date_sk = d_date_sk JOIN item ON inv_item_sk = i_item_sk GROUP BY ROLLUP (i_product_name, i_brand, i_class, i_category) ORDER BY qoh NULLS FIRST, i_product_name NULLS FIRST, i_brand NULLS FIRST, i_class NULLS FIRST, i_category NULLS FIRST LIMIT 100