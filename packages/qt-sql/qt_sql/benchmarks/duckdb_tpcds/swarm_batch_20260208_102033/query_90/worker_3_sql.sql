WITH filtered_dimensions AS (
  SELECT 
    hd.hd_demo_sk,
    td.t_time_sk,
    td.t_hour,
    wp.wp_web_page_sk
  FROM household_demographics hd
  CROSS JOIN time_dim td
  CROSS JOIN web_page wp
  WHERE hd.hd_dep_count = 2
    AND wp.wp_char_count BETWEEN 5000 AND 5200
    AND td.t_hour IN (10, 11, 16, 17)
),
joined_data AS (
  SELECT 
    fd.t_hour
  FROM web_sales ws
  INNER JOIN filtered_dimensions fd
    ON ws.ws_sold_time_sk = fd.t_time_sk
    AND ws.ws_ship_hdemo_sk = fd.hd_demo_sk
    AND ws.ws_web_page_sk = fd.wp_web_page_sk
),
counts AS (
  SELECT
    COUNT(CASE WHEN t_hour BETWEEN 10 AND 11 THEN 1 END) AS amc,
    COUNT(CASE WHEN t_hour BETWEEN 16 AND 17 THEN 1 END) AS pmc
  FROM joined_data
)
SELECT
  CAST(amc AS DECIMAL(15,4)) / CAST(pmc AS DECIMAL(15,4)) AS am_pm_ratio
FROM counts
ORDER BY am_pm_ratio
LIMIT 100