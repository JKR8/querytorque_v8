WITH filtered_date AS (
    SELECT d_date_sk, d_day_name
    FROM date_dim
    WHERE d_year = 2000
), 
pre_aggregated AS (
    SELECT 
        ss_store_sk,
        d_day_name,
        SUM(ss_sales_price) AS daily_sales
    FROM store_sales
    JOIN filtered_date ON ss_sold_date_sk = d_date_sk
    GROUP BY ss_store_sk, d_day_name
),
pivoted AS (
    SELECT 
        ss_store_sk,
        SUM(CASE WHEN d_day_name = 'Sunday' THEN daily_sales ELSE NULL END) AS sun_sales,
        SUM(CASE WHEN d_day_name = 'Monday' THEN daily_sales ELSE NULL END) AS mon_sales,
        SUM(CASE WHEN d_day_name = 'Tuesday' THEN daily_sales ELSE NULL END) AS tue_sales,
        SUM(CASE WHEN d_day_name = 'Wednesday' THEN daily_sales ELSE NULL END) AS wed_sales,
        SUM(CASE WHEN d_day_name = 'Thursday' THEN daily_sales ELSE NULL END) AS thu_sales,
        SUM(CASE WHEN d_day_name = 'Friday' THEN daily_sales ELSE NULL END) AS fri_sales,
        SUM(CASE WHEN d_day_name = 'Saturday' THEN daily_sales ELSE NULL END) AS sat_sales
    FROM pre_aggregated
    GROUP BY ss_store_sk
)
SELECT 
    s_store_name,
    s_store_id,
    sun_sales,
    mon_sales,
    tue_sales,
    wed_sales,
    thu_sales,
    fri_sales,
    sat_sales
FROM store
JOIN pivoted ON s_store_sk = ss_store_sk
WHERE s_gmt_offset = -5
GROUP BY 
    s_store_name,
    s_store_id,
    sun_sales,
    mon_sales,
    tue_sales,
    wed_sales,
    thu_sales,
    fri_sales,
    sat_sales
ORDER BY 
    s_store_name,
    s_store_id,
    sun_sales,
    mon_sales,
    tue_sales,
    wed_sales,
    thu_sales,
    fri_sales,
    sat_sales
LIMIT 100;