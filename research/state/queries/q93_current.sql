-- Q93 current state: optimized (kimi, 2.73x)
-- Source: /mnt/c/Users/jakc9/Documents/QueryTorque_V8/research/CONSOLIDATED_BENCHMARKS/kimi_q31-q99_optimization/q93/output_optimized.sql
-- Best speedup: 2.73x

WITH filtered_reason AS (SELECT r_reason_sk FROM reason WHERE r_reason_desc = 'duplicate purchase'), filtered_returns AS (SELECT sr_item_sk, sr_ticket_number, sr_return_quantity FROM store_returns JOIN filtered_reason ON sr_reason_sk = r_reason_sk)
SELECT ss_customer_sk, SUM(act_sales) AS sumsales FROM (SELECT ss.ss_customer_sk, CASE WHEN NOT fr.sr_return_quantity IS NULL THEN (ss.ss_quantity - fr.sr_return_quantity) * ss.ss_sales_price ELSE (ss.ss_quantity * ss.ss_sales_price) END AS act_sales FROM store_sales AS ss JOIN filtered_returns AS fr ON (fr.sr_item_sk = ss.ss_item_sk AND fr.sr_ticket_number = ss.ss_ticket_number)) AS t GROUP BY ss_customer_sk ORDER BY sumsales, ss_customer_sk LIMIT 100