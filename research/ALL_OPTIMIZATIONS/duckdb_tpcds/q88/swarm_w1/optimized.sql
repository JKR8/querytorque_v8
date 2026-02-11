WITH filtered_store AS (
    SELECT s_store_sk
    FROM store
    WHERE s_store_name = 'ese'
),
filtered_household AS (
    SELECT hd_demo_sk
    FROM household_demographics
    WHERE (
        (hd_dep_count = -1 AND hd_vehicle_count <= 1)  -- -1 + 2 = 1
        OR (hd_dep_count = 4 AND hd_vehicle_count <= 6)  -- 4 + 2 = 6
        OR (hd_dep_count = 3 AND hd_vehicle_count <= 5)  -- 3 + 2 = 5
    )
),
filtered_time AS (
    SELECT t_time_sk, t_hour, t_minute
    FROM time_dim
    WHERE t_hour IN (8, 9, 10, 11, 12)
),
qualified_sales AS (
    SELECT 
        ss_sold_time_sk,
        t_hour,
        t_minute
    FROM store_sales
    INNER JOIN filtered_time ON ss_sold_time_sk = t_time_sk
    INNER JOIN filtered_household ON ss_hdemo_sk = hd_demo_sk
    INNER JOIN filtered_store ON ss_store_sk = s_store_sk
)
SELECT
    (SELECT COUNT(*) FROM qualified_sales WHERE t_hour = 8 AND t_minute >= 30) AS h8_30_to_9,
    (SELECT COUNT(*) FROM qualified_sales WHERE t_hour = 9 AND t_minute < 30) AS h9_to_9_30,
    (SELECT COUNT(*) FROM qualified_sales WHERE t_hour = 9 AND t_minute >= 30) AS h9_30_to_10,
    (SELECT COUNT(*) FROM qualified_sales WHERE t_hour = 10 AND t_minute < 30) AS h10_to_10_30,
    (SELECT COUNT(*) FROM qualified_sales WHERE t_hour = 10 AND t_minute >= 30) AS h10_30_to_11,
    (SELECT COUNT(*) FROM qualified_sales WHERE t_hour = 11 AND t_minute < 30) AS h11_to_11_30,
    (SELECT COUNT(*) FROM qualified_sales WHERE t_hour = 11 AND t_minute >= 30) AS h11_30_to_12,
    (SELECT COUNT(*) FROM qualified_sales WHERE t_hour = 12 AND t_minute < 30) AS h12_to_12_30