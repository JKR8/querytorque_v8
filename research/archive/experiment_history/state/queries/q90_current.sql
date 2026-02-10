-- Q90 current state: optimized (kimi, 1.57x)
-- Source: /mnt/c/Users/jakc9/Documents/QueryTorque_V8/research/CONSOLIDATED_BENCHMARKS/kimi_q31-q99_optimization/q90/output_optimized.sql
-- Best speedup: 1.57x

WITH filtered_web_data AS (SELECT CASE WHEN t.t_hour BETWEEN 10 AND 11 THEN 1 END AS am_flag, CASE WHEN t.t_hour BETWEEN 16 AND 17 THEN 1 END AS pm_flag FROM web_sales AS ws JOIN household_demographics AS hd ON ws.ws_ship_hdemo_sk = hd.hd_demo_sk JOIN time_dim AS t ON ws.ws_sold_time_sk = t.t_time_sk JOIN web_page AS wp ON ws.ws_web_page_sk = wp.wp_web_page_sk WHERE hd.hd_dep_count = 2 AND wp.wp_char_count BETWEEN 5000 AND 5200 AND (t.t_hour BETWEEN 10 AND 11 OR t.t_hour BETWEEN 16 AND 17)), counts AS (SELECT COUNT(am_flag) AS amc, COUNT(pm_flag) AS pmc FROM filtered_web_data)
SELECT CAST(amc AS DECIMAL(15, 4)) / CAST(pmc AS DECIMAL(15, 4)) AS am_pm_ratio FROM counts ORDER BY am_pm_ratio LIMIT 100