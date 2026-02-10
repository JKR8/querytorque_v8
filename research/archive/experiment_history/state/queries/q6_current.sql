-- Q6 current state: optimized (kimi, 1.33x)
-- Source: /mnt/c/Users/jakc9/Documents/QueryTorque_V8/research/CONSOLIDATED_BENCHMARKS/kimi_q1-q30_optimization/q6/output_optimized.sql
-- Best speedup: 1.33x

WITH target_month AS (SELECT DISTINCT d_month_seq FROM date_dim WHERE d_year = 2002 AND d_moy = 3), category_avg AS (SELECT i_category, AVG(i_current_price) * 1.2 AS price_threshold FROM item GROUP BY i_category)
SELECT a.ca_state AS state, COUNT(*) AS cnt FROM customer_address AS a JOIN customer AS c ON a.ca_address_sk = c.c_current_addr_sk JOIN store_sales AS s ON c.c_customer_sk = s.ss_customer_sk JOIN date_dim AS d ON s.ss_sold_date_sk = d.d_date_sk JOIN target_month AS tm ON d.d_month_seq = tm.d_month_seq JOIN item AS i ON s.ss_item_sk = i.i_item_sk JOIN category_avg AS ca ON i.i_category = ca.i_category WHERE i.i_current_price > ca.price_threshold GROUP BY a.ca_state HAVING COUNT(*) >= 10 ORDER BY cnt, a.ca_state LIMIT 100