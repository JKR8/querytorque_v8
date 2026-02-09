WITH wss AS (
  SELECT
    d_week_seq,
    ss_store_sk,
    SUM(CASE WHEN d_day_name = 'Sunday' THEN ss_sales_price END) AS sun_sales,
    SUM(CASE WHEN d_day_name = 'Monday' THEN ss_sales_price END) AS mon_sales,
    SUM(CASE WHEN d_day_name = 'Tuesday' THEN ss_sales_price END) AS tue_sales,
    SUM(CASE WHEN d_day_name = 'Wednesday' THEN ss_sales_price END) AS wed_sales,
    SUM(CASE WHEN d_day_name = 'Thursday' THEN ss_sales_price END) AS thu_sales,
    SUM(CASE WHEN d_day_name = 'Friday' THEN ss_sales_price END) AS fri_sales,
    SUM(CASE WHEN d_day_name = 'Saturday' THEN ss_sales_price END) AS sat_sales
  FROM store_sales
  JOIN date_dim ON d_date_sk = ss_sold_date_sk
  WHERE ss_sales_price / ss_list_price BETWEEN 0.11 AND 0.31
  GROUP BY d_week_seq, ss_store_sk
),
filtered_stores AS (
  SELECT s_store_sk, s_store_id, s_store_name
  FROM store
  WHERE s_state IN ('GA', 'IA', 'LA', 'MO', 'SD', 'TN', 'TX', 'VA')
),
period_data AS (
  SELECT
    wss.d_week_seq,
    wss.ss_store_sk,
    fs.s_store_id,
    fs.s_store_name,
    d.d_month_seq,
    wss.sun_sales,
    wss.mon_sales,
    wss.tue_sales,
    wss.wed_sales,
    wss.thu_sales,
    wss.fri_sales,
    wss.sat_sales
  FROM wss
  JOIN filtered_stores fs ON wss.ss_store_sk = fs.s_store_sk
  JOIN date_dim d ON wss.d_week_seq = d.d_week_seq
  WHERE d.d_month_seq BETWEEN 1208 AND 1208 + 23
),
aligned_periods AS (
  SELECT
    s_store_name,
    s_store_id,
    d_week_seq,
    SUM(CASE WHEN d_month_seq BETWEEN 1208 AND 1208 + 11 THEN sun_sales END) AS sun_sales1,
    SUM(CASE WHEN d_month_seq BETWEEN 1208 AND 1208 + 11 THEN mon_sales END) AS mon_sales1,
    SUM(CASE WHEN d_month_seq BETWEEN 1208 AND 1208 + 11 THEN tue_sales END) AS tue_sales1,
    SUM(CASE WHEN d_month_seq BETWEEN 1208 AND 1208 + 11 THEN wed_sales END) AS wed_sales1,
    SUM(CASE WHEN d_month_seq BETWEEN 1208 AND 1208 + 11 THEN thu_sales END) AS thu_sales1,
    SUM(CASE WHEN d_month_seq BETWEEN 1208 AND 1208 + 11 THEN fri_sales END) AS fri_sales1,
    SUM(CASE WHEN d_month_seq BETWEEN 1208 AND 1208 + 11 THEN sat_sales END) AS sat_sales1,
    SUM(CASE WHEN d_month_seq BETWEEN 1208 + 12 AND 1208 + 23 THEN sun_sales END) AS sun_sales2,
    SUM(CASE WHEN d_month_seq BETWEEN 1208 + 12 AND 1208 + 23 THEN mon_sales END) AS mon_sales2,
    SUM(CASE WHEN d_month_seq BETWEEN 1208 + 12 AND 1208 + 23 THEN tue_sales END) AS tue_sales2,
    SUM(CASE WHEN d_month_seq BETWEEN 1208 + 12 AND 1208 + 23 THEN wed_sales END) AS wed_sales2,
    SUM(CASE WHEN d_month_seq BETWEEN 1208 + 12 AND 1208 + 23 THEN thu_sales END) AS thu_sales2,
    SUM(CASE WHEN d_month_seq BETWEEN 1208 + 12 AND 1208 + 23 THEN fri_sales END) AS fri_sales2,
    SUM(CASE WHEN d_month_seq BETWEEN 1208 + 12 AND 1208 + 23 THEN sat_sales END) AS sat_sales2
  FROM period_data
  GROUP BY s_store_id, s_store_name, d_week_seq
  HAVING COUNT(DISTINCT CASE WHEN d_month_seq BETWEEN 1208 AND 1208 + 11 THEN d_week_seq END) > 0
     AND COUNT(DISTINCT CASE WHEN d_month_seq BETWEEN 1208 + 12 AND 1208 + 23 THEN d_week_seq + 52 END) > 0
)
SELECT
  s_store_name AS s_store_name1,
  s_store_id AS s_store_id1,
  d_week_seq AS d_week_seq1,
  sun_sales1 / NULLIF(sun_sales2, 0) AS "sun_sales1 / sun_sales2",
  mon_sales1 / NULLIF(mon_sales2, 0) AS "mon_sales1 / mon_sales2",
  tue_sales1 / NULLIF(tue_sales2, 0) AS "tue_sales1 / tue_sales2",
  wed_sales1 / NULLIF(wed_sales2, 0) AS "wed_sales1 / wed_sales2",
  thu_sales1 / NULLIF(thu_sales2, 0) AS "thu_sales1 / thu_sales2",
  fri_sales1 / NULLIF(fri_sales2, 0) AS "fri_sales1 / fri_sales2",
  sat_sales1 / NULLIF(sat_sales2, 0) AS "sat_sales1 / sat_sales2"
FROM aligned_periods
WHERE sun_sales2 IS NOT NULL
  AND mon_sales2 IS NOT NULL
  AND tue_sales2 IS NOT NULL
  AND wed_sales2 IS NOT NULL
  AND thu_sales2 IS NOT NULL
  AND fri_sales2 IS NOT NULL
  AND sat_sales2 IS NOT NULL
ORDER BY s_store_name1, s_store_id1, d_week_seq1
LIMIT 100;