WITH wscs AS (
  SELECT
    ws_sold_date_sk AS sold_date_sk,
    ws_ext_sales_price AS sales_price
  FROM web_sales
  UNION ALL
  SELECT
    cs_sold_date_sk AS sold_date_sk,
    cs_ext_sales_price AS sales_price
  FROM catalog_sales
),
date_1998 AS (
  SELECT d_date_sk, d_week_seq, d_day_name
  FROM date_dim
  WHERE d_year = 1998
),
date_1999 AS (
  SELECT d_date_sk, d_week_seq, d_day_name
  FROM date_dim
  WHERE d_year = 1999
),
wswscs_1998 AS (
  SELECT
    d_week_seq,
    SUM(CASE WHEN d_day_name = 'Sunday' THEN sales_price ELSE NULL END) AS sun_sales,
    SUM(CASE WHEN d_day_name = 'Monday' THEN sales_price ELSE NULL END) AS mon_sales,
    SUM(CASE WHEN d_day_name = 'Tuesday' THEN sales_price ELSE NULL END) AS tue_sales,
    SUM(CASE WHEN d_day_name = 'Wednesday' THEN sales_price ELSE NULL END) AS wed_sales,
    SUM(CASE WHEN d_day_name = 'Thursday' THEN sales_price ELSE NULL END) AS thu_sales,
    SUM(CASE WHEN d_day_name = 'Friday' THEN sales_price ELSE NULL END) AS fri_sales,
    SUM(CASE WHEN d_day_name = 'Saturday' THEN sales_price ELSE NULL END) AS sat_sales
  FROM wscs
  JOIN date_1998 ON d_date_sk = sold_date_sk
  GROUP BY d_week_seq
),
wswscs_1999 AS (
  SELECT
    d_week_seq,
    SUM(CASE WHEN d_day_name = 'Sunday' THEN sales_price ELSE NULL END) AS sun_sales,
    SUM(CASE WHEN d_day_name = 'Monday' THEN sales_price ELSE NULL END) AS mon_sales,
    SUM(CASE WHEN d_day_name = 'Tuesday' THEN sales_price ELSE NULL END) AS tue_sales,
    SUM(CASE WHEN d_day_name = 'Wednesday' THEN sales_price ELSE NULL END) AS wed_sales,
    SUM(CASE WHEN d_day_name = 'Thursday' THEN sales_price ELSE NULL END) AS thu_sales,
    SUM(CASE WHEN d_day_name = 'Friday' THEN sales_price ELSE NULL END) AS fri_sales,
    SUM(CASE WHEN d_day_name = 'Saturday' THEN sales_price ELSE NULL END) AS sat_sales
  FROM wscs
  JOIN date_1999 ON d_date_sk = sold_date_sk
  GROUP BY d_week_seq
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
FROM wswscs_1998 y
JOIN wswscs_1999 z ON y.d_week_seq = z.d_week_seq - 53
ORDER BY y.d_week_seq