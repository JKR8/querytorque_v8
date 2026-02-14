-- TPC-DS Query 88 (Optimized - Single-pass scan consolidation)
-- Transform: single_pass_aggregation + dimension_prefetch (DuckDB 5.25x winner)
-- Key changes:
--   1. Pre-filter small dimension tables into CTEs (store, household_demographics, time_dim)
--   2. Classify time_dim rows into 8 time slots (1-8) via CASE
--   3. Single JOIN of store_sales with all 3 pre-filtered dimensions
--   4. COUNT(CASE WHEN slot=N) for all 8 buckets in ONE pass
-- Original: 8 separate 4-table joins = 8 full scans of 28.8B rows
-- Optimized: 1 scan with pre-filtered dimension CTEs
WITH filtered_store AS (
    SELECT s_store_sk
    FROM store
    WHERE s_store_name = 'ese'
),
filtered_hd AS (
    SELECT hd_demo_sk
    FROM household_demographics
    WHERE (hd_dep_count = -1 AND hd_vehicle_count <= 1)
       OR (hd_dep_count = 4 AND hd_vehicle_count <= 6)
       OR (hd_dep_count = 3 AND hd_vehicle_count <= 5)
),
time_slots AS (
    SELECT
        t_time_sk,
        CASE
            WHEN t_hour = 8  AND t_minute >= 30 THEN 1
            WHEN t_hour = 9  AND t_minute <  30 THEN 2
            WHEN t_hour = 9  AND t_minute >= 30 THEN 3
            WHEN t_hour = 10 AND t_minute <  30 THEN 4
            WHEN t_hour = 10 AND t_minute >= 30 THEN 5
            WHEN t_hour = 11 AND t_minute <  30 THEN 6
            WHEN t_hour = 11 AND t_minute >= 30 THEN 7
            WHEN t_hour = 12 AND t_minute <  30 THEN 8
        END AS slot
    FROM time_dim
    WHERE t_hour BETWEEN 8 AND 12
)
SELECT
    COUNT(CASE WHEN ts.slot = 1 THEN 1 END) AS h8_30_to_9,
    COUNT(CASE WHEN ts.slot = 2 THEN 1 END) AS h9_to_9_30,
    COUNT(CASE WHEN ts.slot = 3 THEN 1 END) AS h9_30_to_10,
    COUNT(CASE WHEN ts.slot = 4 THEN 1 END) AS h10_to_10_30,
    COUNT(CASE WHEN ts.slot = 5 THEN 1 END) AS h10_30_to_11,
    COUNT(CASE WHEN ts.slot = 6 THEN 1 END) AS h11_to_11_30,
    COUNT(CASE WHEN ts.slot = 7 THEN 1 END) AS h11_30_to_12,
    COUNT(CASE WHEN ts.slot = 8 THEN 1 END) AS h12_to_12_30
FROM store_sales ss
    JOIN time_slots ts ON ss.ss_sold_time_sk = ts.t_time_sk
    JOIN filtered_hd hd ON ss.ss_hdemo_sk = hd.hd_demo_sk
    JOIN filtered_store fs ON ss.ss_store_sk = fs.s_store_sk
WHERE ts.slot IS NOT NULL;
