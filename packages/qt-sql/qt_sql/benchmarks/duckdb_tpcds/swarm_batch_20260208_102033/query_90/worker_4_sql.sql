WITH time_ranges AS (
    SELECT 
        ws_sold_time_sk,
        CASE 
            WHEN t_hour BETWEEN 10 AND 11 THEN 1
            WHEN t_hour BETWEEN 16 AND 17 THEN 2
            ELSE 0 
        END AS time_period
    FROM time_dim
    WHERE t_hour BETWEEN 10 AND 11 OR t_hour BETWEEN 16 AND 17
),
filtered_web AS (
    SELECT 
        wp_web_page_sk,
        wp_char_count
    FROM web_page
    WHERE wp_char_count BETWEEN 5000 AND 5200
),
filtered_hh AS (
    SELECT 
        hd_demo_sk
    FROM household_demographics
    WHERE hd_dep_count = 2
),
qualified_sales AS (
    SELECT 
        ws.ws_sold_time_sk,
        ws.ws_web_page_sk,
        ws.ws_ship_hdemo_sk,
        tr.time_period
    FROM web_sales ws
    INNER JOIN time_ranges tr ON ws.ws_sold_time_sk = tr.ws_sold_time_sk
    INNER JOIN filtered_web w ON ws.ws_web_page_sk = w.wp_web_page_sk
    INNER JOIN filtered_hh h ON ws.ws_ship_hdemo_sk = h.hd_demo_sk
),
counts AS (
    SELECT 
        COUNT(CASE WHEN time_period = 1 THEN 1 END) AS amc,
        COUNT(CASE WHEN time_period = 2 THEN 1 END) AS pmc
    FROM qualified_sales
)
SELECT
    CAST(amc AS DECIMAL(15, 4)) / CAST(pmc AS DECIMAL(15, 4)) AS am_pm_ratio
FROM counts
ORDER BY am_pm_ratio
LIMIT 100;