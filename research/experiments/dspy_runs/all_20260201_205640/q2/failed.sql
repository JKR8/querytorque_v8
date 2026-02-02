-- start query 2 in stream 0 using template query2.tpl
WITH weekly_sales_by_year AS (
    SELECT 
        d.d_week_seq,
        d.d_year,
        SUM(CASE WHEN d.d_day_name = 'Sunday' THEN s.sales_price ELSE NULL END) AS sun_sales,
        SUM(CASE WHEN d.d_day_name = 'Monday' THEN s.sales_price ELSE NULL END) AS mon_sales,
        SUM(CASE WHEN d.d_day_name = 'Tuesday' THEN s.sales_price ELSE NULL END) AS tue_sales,
        SUM(CASE WHEN d.d_day_name = 'Wednesday' THEN s.sales_price ELSE NULL END) AS wed_sales,
        SUM(CASE WHEN d.d_day_name = 'Thursday' THEN s.sales_price ELSE NULL END) AS thu_sales,
        SUM(CASE WHEN d.d_day_name = 'Friday' THEN s.sales_price ELSE NULL END) AS fri_sales,
        SUM(CASE WHEN d.d_day_name = 'Saturday' THEN s.sales_price ELSE NULL END) AS sat_sales
    FROM (
        SELECT ws_sold_date_sk AS sold_date_sk, ws_ext_sales_price AS sales_price
        FROM web_sales
        UNION ALL
        SELECT cs_sold_date_sk AS sold_date_sk, cs_ext_sales_price AS sales_price
        FROM catalog_sales
    ) s
    JOIN date_dim d ON d.d_date_sk = s.sold_date_sk
    WHERE d.d_year IN (1998, 1999)
    GROUP BY d.d_week_seq, d.d_year
)
SELECT 
    y.d_week_seq AS d_week_seq1,
    ROUND(y.sun_sales / z.sun_sales, 2),
    ROUND(y.mon_sales / z.mon_sales, 2),
    ROUND(y.tue_sales / z.tue_sales, 2),
    ROUND(y.wed_sales / z.wed_sales, 2),
    ROUND(y.thu_sales / z.thu_sales, 2),
    ROUND(y.fri_sales / z.fri_sales, 2),
    ROUND(y.sat_sales / z.sat_sales, 2)
FROM weekly_sales_by_year y
JOIN weekly_sales_by_year z ON y.d_week_seq = z.d_week_seq - 53
WHERE y.d_year = 1998
  AND z.d_year = 1999
ORDER BY y.d_week_seq;

-- end query 2 in stream 0 using template query2.tpl