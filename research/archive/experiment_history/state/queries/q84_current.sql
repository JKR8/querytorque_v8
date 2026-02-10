-- Q84 current state: optimized (kimi, 1.22x)
-- Source: /mnt/c/Users/jakc9/Documents/QueryTorque_V8/research/CONSOLIDATED_BENCHMARKS/kimi_q31-q99_optimization/q84/output_optimized.sql
-- Best speedup: 1.22x

WITH filtered_income_band AS (SELECT ib_income_band_sk FROM income_band WHERE ib_lower_bound >= 5806 AND ib_upper_bound <= 5806 + 50000), filtered_address AS (SELECT ca_address_sk FROM customer_address WHERE ca_city = 'Oakwood')
SELECT c.c_customer_id AS customer_id, COALESCE(c.c_last_name, '') || ', ' || COALESCE(c.c_first_name, '') AS customername FROM customer AS c JOIN filtered_address AS a ON c.c_current_addr_sk = a.ca_address_sk JOIN household_demographics AS hd ON c.c_current_hdemo_sk = hd.hd_demo_sk JOIN filtered_income_band AS ib ON hd.hd_income_band_sk = ib.ib_income_band_sk JOIN customer_demographics AS cd ON cd.cd_demo_sk = c.c_current_cdemo_sk JOIN store_returns AS sr ON sr.sr_cdemo_sk = cd.cd_demo_sk ORDER BY c_customer_id LIMIT 100