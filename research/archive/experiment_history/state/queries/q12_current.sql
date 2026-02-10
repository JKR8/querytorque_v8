-- Q12 current state: optimized (retry3w_3, 1.23x)
-- Source: /mnt/c/Users/jakc9/Documents/QueryTorque_V8/retry_collect/q12/w3_optimized.sql
-- Best speedup: 1.23x

WITH filtered_dates AS (SELECT d_date_sk FROM date_dim WHERE d_date BETWEEN CAST('1999-02-22' AS DATE) AND CAST('1999-03-24' AS DATE)), filtered_items AS (SELECT i_item_sk, i_item_id, i_item_desc, i_category, i_class, i_current_price FROM item WHERE i_category IN ('Sports', 'Books', 'Home'))
SELECT i.i_item_id, i.i_item_desc, i.i_category, i.i_class, i.i_current_price, SUM(ws.ws_ext_sales_price) AS itemrevenue, SUM(ws.ws_ext_sales_price) * 100.0000 / SUM(SUM(ws.ws_ext_sales_price)) OVER (PARTITION BY i.i_class) AS revenueratio FROM web_sales AS ws JOIN filtered_dates AS d ON ws.ws_sold_date_sk = d.d_date_sk JOIN filtered_items AS i ON ws.ws_item_sk = i.i_item_sk GROUP BY i.i_item_id, i.i_item_desc, i.i_category, i.i_class, i.i_current_price ORDER BY i.i_category, i.i_class, i.i_item_id, i.i_item_desc, revenueratio LIMIT 100