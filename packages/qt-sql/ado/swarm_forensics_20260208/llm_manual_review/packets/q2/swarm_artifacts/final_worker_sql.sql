WITH filtered_dates AS (
    SELECT 
        d_date_sk,
        d_week_seq,
        d_year,
        d_day_name
    FROM date_dim
    WHERE d_year IN (1998, 1998+1)
),

web_sales_filtered AS (
    SELECT
        fd.d_week_seq,
        fd.d_year,
        fd.d_day_name,
        ws.ws_ext_sales_price AS sales_price
    FROM web_sales ws
    INNER JOIN filtered_dates fd ON ws.ws_sold_date_sk = fd.d_date_sk
),

catalog_sales_filtered AS (
    SELECT
        fd.d_week_seq,
        fd.d_year,
        fd.d_day_name,
        cs.cs_ext_sales_price AS sales_price
    FROM catalog_sales cs
    INNER JOIN filtered_dates fd ON cs.cs_sold_date_sk = fd.d_date_sk
),

combined_sales AS (
    SELECT * FROM web_sales_filtered
    UNION ALL
    SELECT * FROM catalog_sales_filtered
),

weekly_aggregated AS (
    SELECT
        d_week_seq,
        d_year,
        SUM(CASE WHEN d_day_name = 'Sunday' THEN sales_price END) AS sun_sales,
        SUM(CASE WHEN d_day_name = 'Monday' THEN sales_price END) AS mon_sales,
        SUM(CASE WHEN d_day_name = 'Tuesday' THEN sales_price END) AS tue_sales,
        SUM(CASE WHEN d_day_name = 'Wednesday' THEN sales_price END) AS wed_sales,
        SUM(CASE WHEN d_day_name = 'Thursday' THEN sales_price END) AS thu_sales,
        SUM(CASE WHEN d_day_name = 'Friday' THEN sales_price END) AS fri_sales,
        SUM(CASE WHEN d_day_name = 'Saturday' THEN sales_price END) AS sat_sales
    FROM combined_sales
    GROUP BY d_week_seq, d_year
),

yearly_paired AS (
    SELECT
        y1998.d_week_seq AS d_week_seq1,
        y1998.sun_sales AS sun_sales1,
        y1998.mon_sales AS mon_sales1,
        y1998.tue_sales AS tue_sales1,
        y1998.wed_sales AS wed_sales1,
        y1998.thu_sales AS thu_sales1,
        y1998.fri_sales AS fri_sales1,
        y1998.sat_sales AS sat_sales1,
        y1999.sun_sales AS sun_sales2,
        y1999.mon_sales AS mon_sales2,
        y1999.tue_sales AS tue_sales2,
        y1999.wed_sales AS wed_sales2,
        y1999.thu_sales AS thu_sales2,
        y1999.fri_sales AS fri_sales2,
        y1999.sat_sales AS sat_sales2
    FROM weekly_aggregated y1998
    JOIN weekly_aggregated y1999 
        ON y1998.d_week_seq = y1999.d_week_seq - 53
        AND y1998.d_year = 1998
        AND y1999.d_year = 1999
)

SELECT
    d_week_seq1,
    ROUND(sun_sales1 / NULLIF(sun_sales2, 0), 2),
    ROUND(mon_sales1 / NULLIF(mon_sales2, 0), 2),
    ROUND(tue_sales1 / NULLIF(tue_sales2, 0), 2),
    ROUND(wed_sales1 / NULLIF(wed_sales2, 0), 2),
    ROUND(thu_sales1 / NULLIF(thu_sales2, 0), 2),
    ROUND(fri_sales1 / NULLIF(fri_sales2, 0), 2),
    ROUND(sat_sales1 / NULLIF(sat_sales2, 0), 2)
FROM yearly_paired
ORDER BY d_week_seq1;