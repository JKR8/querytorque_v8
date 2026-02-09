WITH filtered_stores AS (
  SELECT s_store_sk, s_store_id, s_store_name
  FROM store
  WHERE s_state IN ('GA', 'IA', 'LA', 'MO', 'SD', 'TN', 'TX', 'VA')
),
date_range1 AS (
  SELECT d_week_seq, d_date_sk
  FROM date_dim
  WHERE d_month_seq BETWEEN 1208 AND 1208 + 11
),
date_range2 AS (
  SELECT d_week_seq, d_date_sk
  FROM date_dim
  WHERE d_month_seq BETWEEN 1208 + 12 AND 1208 + 23
),
wss1 AS (
  SELECT
    d1.d_week_seq AS d_week_seq1,
    fs.s_store_id AS s_store_id1,
    fs.s_store_name AS s_store_name1,
    SUM(CASE WHEN d_day_name = 'Sunday' THEN ss_sales_price END) AS sun_sales1,
    SUM(CASE WHEN d_day_name = 'Monday' THEN ss_sales_price END) AS mon_sales1,
    SUM(CASE WHEN d_day_name = 'Tuesday' THEN ss_sales_price END) AS tue_sales1,
    SUM(CASE WHEN d_day_name = 'Wednesday' THEN ss_sales_price END) AS wed_sales1,
    SUM(CASE WHEN d_day_name = 'Thursday' THEN ss_sales_price END) AS thu_sales1,
    SUM(CASE WHEN d_day_name = 'Friday' THEN ss_sales_price END) AS fri_sales1,
    SUM(CASE WHEN d_day_name = 'Saturday' THEN ss_sales_price END) AS sat_sales1
  FROM store_sales
  JOIN date_dim ON d_date_sk = ss_sold_date_sk
  JOIN date_range1 d1 ON d1.d_date_sk = date_dim.d_date_sk
  JOIN filtered_stores fs ON ss_store_sk = fs.s_store_sk
  WHERE ss_sales_price / ss_list_price BETWEEN 11 * 0.01 AND 31 * 0.01
  GROUP BY d1.d_week_seq, fs.s_store_id, fs.s_store_name
),
wss2 AS (
  SELECT
    d2.d_week_seq AS d_week_seq2,
    fs.s_store_id AS s_store_id2,
    fs.s_store_name AS s_store_name2,
    SUM(CASE WHEN d_day_name = 'Sunday' THEN ss_sales_price END) AS sun_sales2,
    SUM(CASE WHEN d_day_name = 'Monday' THEN ss_sales_price END) AS mon_sales2,
    SUM(CASE WHEN d_day_name = 'Tuesday' THEN ss_sales_price END) AS tue_sales2,
    SUM(CASE WHEN d_day_name = 'Wednesday' THEN ss_sales_price END) AS wed_sales2,
    SUM(CASE WHEN d_day_name = 'Thursday' THEN ss_sales_price END) AS thu_sales2,
    SUM(CASE WHEN d_day_name = 'Friday' THEN ss_sales_price END) AS fri_sales2,
    SUM(CASE WHEN d_day_name = 'Saturday' THEN ss_sales_price END) AS sat_sales2
  FROM store_sales
  JOIN date_dim ON d_date_sk = ss_sold_date_sk
  JOIN date_range2 d2 ON d2.d_date_sk = date_dim.d_date_sk
  JOIN filtered_stores fs ON ss_store_sk = fs.s_store_sk
  WHERE ss_sales_price / ss_list_price BETWEEN 11 * 0.01 AND 31 * 0.01
  GROUP BY d2.d_week_seq, fs.s_store_id, fs.s_store_name
)
SELECT
  wss1.s_store_name1,
  wss1.s_store_id1,
  wss1.d_week_seq1,
  wss1.sun_sales1 / wss2.sun_sales2,
  wss1.mon_sales1 / wss2.mon_sales2,
  wss1.tue_sales1 / wss2.tue_sales2,
  wss1.wed_sales1 / wss2.wed_sales2,
  wss1.thu_sales1 / wss2.thu_sales2,
  wss1.fri_sales1 / wss2.fri_sales2,
  wss1.sat_sales1 / wss2.sat_sales2
FROM wss1
JOIN wss2 ON wss1.s_store_id1 = wss2.s_store_id2 
  AND wss1.d_week_seq1 = wss2.d_week_seq2 - 52
ORDER BY wss1.s_store_name1, wss1.s_store_id1, wss1.d_week_seq1
LIMIT 100