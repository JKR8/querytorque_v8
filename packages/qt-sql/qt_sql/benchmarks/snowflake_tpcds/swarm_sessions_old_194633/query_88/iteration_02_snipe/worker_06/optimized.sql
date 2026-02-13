WITH filtered_store AS (
  SELECT s_store_sk 
  FROM store 
  WHERE s_store_name = 'ese'
), 
filtered_household AS (
  SELECT hd_demo_sk 
  FROM household_demographics 
  WHERE 
    (hd_dep_count = -1 AND hd_vehicle_count <= 1) OR
    (hd_dep_count = 4 AND hd_vehicle_count <= 6) OR
    (hd_dep_count = 3 AND hd_vehicle_count <= 5)
), 
time_buckets AS (
  SELECT t_time_sk, t_hour, t_minute 
  FROM time_dim 
  WHERE 
    (t_hour = 8 AND t_minute >= 30) OR
    (t_hour = 9 AND t_minute < 30) OR
    (t_hour = 9 AND t_minute >= 30) OR
    (t_hour = 10 AND t_minute < 30) OR
    (t_hour = 10 AND t_minute >= 30) OR
    (t_hour = 11 AND t_minute < 30) OR
    (t_hour = 11 AND t_minute >= 30) OR
    (t_hour = 12 AND t_minute < 30)
), 
sales_joined AS (
  SELECT t.t_hour, t.t_minute 
  FROM store_sales ss
  JOIN filtered_store s ON ss.ss_store_sk = s.s_store_sk
  JOIN filtered_household h ON ss.ss_hdemo_sk = h.hd_demo_sk
  JOIN time_buckets t ON ss.ss_sold_time_sk = t.t_time_sk
)
SELECT
  COUNT_IF(t_hour=8 AND t_minute>=30) AS h8_30_to_9,
  COUNT_IF(t_hour=9 AND t_minute<30) AS h9_to_9_30,
  COUNT_IF(t_hour=9 AND t_minute>=30) AS h9_30_to_10,
  COUNT_IF(t_hour=10 AND t_minute<30) AS h10_to_10_30,
  COUNT_IF(t_hour=10 AND t_minute>=30) AS h10_30_to_11,
  COUNT_IF(t_hour=11 AND t_minute<30) AS h11_to_11_30,
  COUNT_IF(t_hour=11 AND t_minute>=30) AS h11_30_to_12,
  COUNT_IF(t_hour=12 AND t_minute<30) AS h12_to_12_30
FROM sales_joined;