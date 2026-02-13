WITH filtered_sales AS (SELECT ss.ss_sold_time_sk
FROM store_sales ss
JOIN store s ON ss.ss_store_sk = s.s_store_sk
JOIN household_demographics hd ON ss.ss_hdemo_sk = hd.hd_demo_sk
WHERE s.s_store_name = 'ese'
  AND (
        (hd.hd_dep_count = -1 AND hd.hd_vehicle_count <= 1)
        OR (hd.hd_dep_count = 3 AND hd.hd_vehicle_count <= 5)
        OR (hd.hd_dep_count = 4 AND hd.hd_vehicle_count <= 6)
      )),
time_join AS (SELECT fs.ss_sold_time_sk, t.t_hour, t.t_minute
FROM filtered_sales fs
JOIN time_dim t ON fs.ss_sold_time_sk = t.t_time_sk),
interval_agg AS (SELECT
  COUNT_IF(t_hour=8 AND t_minute>=30) AS h8_30_to_9,
  COUNT_IF(t_hour=9 AND t_minute<30) AS h9_to_9_30,
  COUNT_IF(t_hour=9 AND t_minute>=30) AS h9_30_to_10,
  COUNT_IF(t_hour=10 AND t_minute<30) AS h10_to_10_30,
  COUNT_IF(t_hour=10 AND t_minute>=30) AS h10_30_to_11,
  COUNT_IF(t_hour=11 AND t_minute<30) AS h11_to_11_30,
  COUNT_IF(t_hour=11 AND t_minute>=30) AS h11_30_to_12,
  COUNT_IF(t_hour=12 AND t_minute<30) AS h12_to_12_30
FROM time_join)
SELECT * FROM interval_agg