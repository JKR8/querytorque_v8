SELECT 
    COUNT(CASE WHEN t_hour = 8 AND t_minute >= 30 THEN 1 END) AS h8_30_to_9,
    COUNT(CASE WHEN t_hour = 9 AND t_minute < 30 THEN 1 END) AS h9_to_9_30,
    COUNT(CASE WHEN t_hour = 9 AND t_minute >= 30 THEN 1 END) AS h9_30_to_10,
    COUNT(CASE WHEN t_hour = 10 AND t_minute < 30 THEN 1 END) AS h10_to_10_30,
    COUNT(CASE WHEN t_hour = 10 AND t_minute >= 30 THEN 1 END) AS h10_30_to_11,
    COUNT(CASE WHEN t_hour = 11 AND t_minute < 30 THEN 1 END) AS h11_to_11_30,
    COUNT(CASE WHEN t_hour = 11 AND t_minute >= 30 THEN 1 END) AS h11_30_to_12,
    COUNT(CASE WHEN t_hour = 12 AND t_minute < 30 THEN 1 END) AS h12_to_12_30
FROM store_sales
JOIN household_demographics ON ss_hdemo_sk = hd_demo_sk
JOIN time_dim ON ss_sold_time_sk = t_time_sk
JOIN store ON ss_store_sk = s_store_sk
WHERE s_store_name = 'ese'
    AND t_hour BETWEEN 8 AND 12
    AND (
        (t_hour = 8 AND t_minute >= 30) OR
        (t_hour = 9) OR
        (t_hour = 10) OR
        (t_hour = 11) OR
        (t_hour = 12 AND t_minute < 30)
    )
    AND hd_dep_count IN (-1, 3, 4)
    AND hd_vehicle_count <= hd_dep_count + 2;