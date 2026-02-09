WITH filtered_dates AS (
  SELECT 
    d_date_sk,
    d_week_seq,
    d_year,
    d_day_name
  FROM date_dim
  WHERE d_year IN (1998, 1999)
),
wscs AS (
  SELECT
    sold_date_sk,
    sales_price
  FROM (
    SELECT
      ws_sold_date_sk AS sold_date_sk,
      ws_ext_sales_price AS sales_price
    FROM web_sales
    UNION ALL
    SELECT
      cs_sold_date_sk AS sold_date_sk,
      cs_ext_sales_price AS sales_price
    FROM catalog_sales
  )
),
wswscs_filtered AS (
  SELECT
    fd.d_week_seq,
    fd.d_year,
    SUM(CASE WHEN fd.d_day_name = 'Sunday' THEN wscs.sales_price END) AS sun_sales,
    SUM(CASE WHEN fd.d_day_name = 'Monday' THEN wscs.sales_price END) AS mon_sales,
    SUM(CASE WHEN fd.d_day_name = 'Tuesday' THEN wscs.sales_price END) AS tue_sales,
    SUM(CASE WHEN fd.d_day_name = 'Wednesday' THEN wscs.sales_price END) AS wed_sales,
    SUM(CASE WHEN fd.d_day_name = 'Thursday' THEN wscs.sales_price END) AS thu_sales,
    SUM(CASE WHEN fd.d_day_name = 'Friday' THEN wscs.sales_price END) AS fri_sales,
    SUM(CASE WHEN fd.d_day_name = 'Saturday' THEN wscs.sales_price END) AS sat_sales
  FROM wscs
  JOIN filtered_dates fd ON wscs.sold_date_sk = fd.d_date_sk
  GROUP BY fd.d_week_seq, fd.d_year
)
SELECT
  s98.d_week_seq AS d_week_seq1,
  ROUND(s98.sun_sales / s99.sun_sales, 2),
  ROUND(s98.mon_sales / s99.mon_sales, 2),
  ROUND(s98.tue_sales / s99.tue_sales, 2),
  ROUND(s98.wed_sales / s99.wed_sales, 2),
  ROUND(s98.thu_sales / s99.thu_sales, 2),
  ROUND(s98.fri_sales / s99.fri_sales, 2),
  ROUND(s98.sat_sales / s99.sat_sales, 2)
FROM wswscs_filtered s98
JOIN wswscs_filtered s99 
  ON s98.d_week_seq = s99.d_week_seq - 53
  AND s98.d_year = 1998
  AND s99.d_year = 1999
ORDER BY d_week_seq1