SELECT CASE WHEN t6.PMC = 0 THEN NULL ELSE CAST(t2.AMC AS DECIMAL(15, 4)) / CAST(t6.PMC AS DECIMAL(15, 4)) END AS AM_PM_RATIO
FROM (SELECT COUNT(*) AS AMC
FROM web_sales
INNER JOIN (SELECT *
FROM household_demographics
WHERE hd_dep_count = 6) AS t ON web_sales.ws_ship_hdemo_sk = t.hd_demo_sk
INNER JOIN (SELECT *
FROM time_dim
WHERE t_hour >= 8 AND t_hour <= 8 + 1) AS t0 ON web_sales.ws_sold_time_sk = t0.t_time_sk
INNER JOIN (SELECT *
FROM web_page
WHERE wp_char_count >= 5000 AND wp_char_count <= 5200) AS t1 ON web_sales.ws_web_page_sk = t1.wp_web_page_sk) AS t2,
(SELECT COUNT(*) AS PMC
FROM web_sales AS web_sales0
INNER JOIN (SELECT *
FROM household_demographics
WHERE hd_dep_count = 6) AS t3 ON web_sales0.ws_ship_hdemo_sk = t3.hd_demo_sk
INNER JOIN (SELECT *
FROM time_dim
WHERE t_hour >= 19 AND t_hour <= 19 + 1) AS t4 ON web_sales0.ws_sold_time_sk = t4.t_time_sk
INNER JOIN (SELECT *
FROM web_page
WHERE wp_char_count >= 5000 AND wp_char_count <= 5200) AS t5 ON web_sales0.ws_web_page_sk = t5.wp_web_page_sk) AS t6
ORDER BY 1
FETCH NEXT 100 ROWS ONLY