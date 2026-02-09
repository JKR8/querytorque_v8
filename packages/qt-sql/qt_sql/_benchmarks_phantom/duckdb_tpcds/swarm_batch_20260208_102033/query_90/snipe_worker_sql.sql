WITH filtered_dims AS (
  SELECT 
    hd.hd_demo_sk,
    wp.wp_web_page_sk,
    t.t_time_sk,
    t.t_hour
  FROM household_demographics hd
  CROSS JOIN web_page wp
  CROSS JOIN time_dim t
  WHERE hd.hd_dep_count = 2
    AND wp.wp_char_count BETWEEN 5000 AND 5200
    AND (t.t_hour BETWEEN 10 AND 11 OR t.t_hour BETWEEN 16 AND 17)
),
filtered_sales AS (
  SELECT 
    fd.t_hour,
    COUNT(*) AS cnt
  FROM web_sales ws
  INNER JOIN filtered_dims fd
    ON ws.ws_sold_time_sk = fd.t_time_sk
    AND ws.ws_ship_hdemo_sk = fd.hd_demo_sk
    AND ws.ws_web_page_sk = fd.wp_web_page_sk
  GROUP BY fd.t_hour
)
SELECT
  CAST(
    COALESCE(SUM(CASE WHEN t_hour BETWEEN 10 AND 11 THEN cnt END), 0)
    AS DECIMAL(15, 4)
  ) /
  CAST(
    COALESCE(SUM(CASE WHEN t_hour BETWEEN 16 AND 17 THEN cnt END), 0)
    AS DECIMAL(15, 4)
  ) AS am_pm_ratio
FROM filtered_sales
ORDER BY am_pm_ratio
LIMIT 100