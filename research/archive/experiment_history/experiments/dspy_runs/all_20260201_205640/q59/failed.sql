WITH wss AS (
    SELECT 
        d.d_week_seq,
        ss_store_sk,
        SUM(CASE WHEN d.d_day_name = 'Sunday' THEN ss_sales_price END) AS sun_sales,
        SUM(CASE WHEN d.d_day_name = 'Monday' THEN ss_sales_price END) AS mon_sales,
        SUM(CASE WHEN d.d_day_name = 'Tuesday' THEN ss_sales_price END) AS tue_sales,
        SUM(CASE WHEN d.d_day_name = 'Wednesday' THEN ss_sales_price END) AS wed_sales,
        SUM(CASE WHEN d.d_day_name = 'Thursday' THEN ss_sales_price END) AS thu_sales,
        SUM(CASE WHEN d.d_day_name = 'Friday' THEN ss_sales_price END) AS fri_sales,
        SUM(CASE WHEN d.d_day_name = 'Saturday' THEN ss_sales_price END) AS sat_sales
    FROM store_sales
    JOIN date_dim d ON d.d_date_sk = ss_sold_date_sk
    GROUP BY d.d_week_seq, ss_store_sk
),
filtered_data AS (
    SELECT 
        s.s_store_name,
        s.s_store_id,
        w.d_week_seq,
        d1.d_month_seq AS month_seq1,
        d2.d_month_seq AS month_seq2,
        w.sun_sales,
        w.mon_sales,
        w.tue_sales,
        w.wed_sales,
        w.thu_sales,
        w.fri_sales,
        w.sat_sales
    FROM wss w
    JOIN store s ON w.ss_store_sk = s.s_store_sk
    LEFT JOIN date_dim d1 ON d1.d_week_seq = w.d_week_seq 
        AND d1.d_month_seq BETWEEN 1196 AND 1196 + 11
    LEFT JOIN date_dim d2 ON d2.d_week_seq = w.d_week_seq 
        AND d2.d_month_seq BETWEEN 1196 + 12 AND 1196 + 23
    WHERE d1.d_month_seq IS NOT NULL OR d2.d_month_seq IS NOT NULL
)
SELECT 
    y.s_store_name AS s_store_name1,
    y.s_store_id AS s_store_id1,
    y.d_week_seq AS d_week_seq1,
    y.sun_sales / NULLIF(x.sun_sales, 0) AS sun_ratio,
    y.mon_sales / NULLIF(x.mon_sales, 0) AS mon_ratio,
    y.tue_sales / NULLIF(x.tue_sales, 0) AS tue_ratio,
    y.wed_sales / NULLIF(x.wed_sales, 0) AS wed_ratio,
    y.thu_sales / NULLIF(x.thu_sales, 0) AS thu_ratio,
    y.fri_sales / NULLIF(x.fri_sales, 0) AS fri_ratio,
    y.sat_sales / NULLIF(x.sat_sales, 0) AS sat_ratio
FROM filtered_data y
JOIN filtered_data x ON y.s_store_id = x.s_store_id
    AND y.d_week_seq = x.d_week_seq - 52
    AND y.month_seq1 IS NOT NULL
    AND x.month_seq2 IS NOT NULL
ORDER BY y.s_store_name, y.s_store_id, y.d_week_seq
LIMIT 100;