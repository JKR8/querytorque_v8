WITH filtered_time AS (
    SELECT t_time_sk
    FROM time_dim
    WHERE t_hour = 8
      AND t_minute >= 30
),
filtered_household AS (
    SELECT hd_demo_sk
    FROM household_demographics
    WHERE hd_dep_count = 3
),
filtered_store AS (
    SELECT s_store_sk
    FROM store
    WHERE s_store_name = 'ese'
),
dimension_keys AS (
    SELECT
        t.t_time_sk,
        h.hd_demo_sk,
        s.s_store_sk
    FROM filtered_time t
    CROSS JOIN filtered_household h
    CROSS JOIN filtered_store s
)
SELECT
    COUNT(*)
FROM store_sales ss
JOIN dimension_keys dk ON 
    ss.ss_sold_time_sk = dk.t_time_sk
    AND ss.ss_hdemo_sk = dk.hd_demo_sk
    AND ss.ss_store_sk = dk.s_store_sk
ORDER BY
    COUNT(*)
LIMIT 100