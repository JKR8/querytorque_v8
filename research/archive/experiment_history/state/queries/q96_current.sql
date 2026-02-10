-- Q96 current state: optimized (retry3w_2, 1.64x)
-- Source: /mnt/c/Users/jakc9/Documents/QueryTorque_V8/retry_collect/q96/w3_optimized.sql
-- Best speedup: 1.64x

WITH filtered_store AS (SELECT s_store_sk FROM store WHERE s_store_name = 'ese'), filtered_time AS (SELECT t_time_sk FROM time_dim WHERE t_hour = 20 AND t_minute >= 30), filtered_household AS (SELECT hd_demo_sk FROM household_demographics WHERE hd_dep_count = 7), filtered_sales AS (SELECT 1 FROM store_sales WHERE ss_store_sk IN (SELECT s_store_sk FROM filtered_store) AND ss_sold_time_sk IN (SELECT t_time_sk FROM filtered_time) AND ss_hdemo_sk IN (SELECT hd_demo_sk FROM filtered_household))
SELECT COUNT(*) FROM filtered_sales ORDER BY COUNT(*) LIMIT 100