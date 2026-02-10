SELECT 
    CAST(SUM(CASE WHEN t_hour BETWEEN 10 AND 11 THEN 1 ELSE 0 END) AS DECIMAL(15,4)) / 
    CAST(SUM(CASE WHEN t_hour BETWEEN 16 AND 17 THEN 1 ELSE 0 END) AS DECIMAL(15,4)) AS am_pm_ratio
FROM web_sales ws
JOIN household_demographics hd ON ws.ws_ship_hdemo_sk = hd.hd_demo_sk
JOIN time_dim t ON ws.ws_sold_time_sk = t.t_time_sk
JOIN web_page wp ON ws.ws_web_page_sk = wp.wp_web_page_sk
WHERE hd.hd_dep_count = 2
  AND wp.wp_char_count BETWEEN 5000 AND 5200
  AND t.t_hour IN (10, 11, 16, 17)
ORDER BY am_pm_ratio
LIMIT 100;