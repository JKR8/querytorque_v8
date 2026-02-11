WITH 
-- Isolate selective date filter for period 1
date_filter1 AS (
    SELECT d_week_seq
    FROM date_dim
    WHERE d_month_seq BETWEEN 1208 AND 1208 + 11
),
-- Isolate selective date filter for period 2
date_filter2 AS (
    SELECT d_week_seq
    FROM date_dim
    WHERE d_month_seq BETWEEN 1208 + 12 AND 1208 + 23
),
-- Isolate selective store filter
store_filter AS (
    SELECT s_store_sk, s_store_id, s_store_name
    FROM store
    WHERE s_state IN ('GA', 'IA', 'LA', 'MO', 'SD', 'TN', 'TX', 'VA')
),
-- Original wss CTE (unchanged)
wss AS (
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
-- Period 1 using explicit joins with filtered dimensions
period1 AS (
    SELECT
        sf.s_store_name AS s_store_name1,
        wss.d_week_seq AS d_week_seq1,
        sf.s_store_id AS s_store_id1,
        wss.sun_sales AS sun_sales1,
        wss.mon_sales AS mon_sales1,
        wss.tue_sales AS tue_sales1,
        wss.wed_sales AS wed_sales1,
        wss.thu_sales AS thu_sales1,
        wss.fri_sales AS fri_sales1,
        wss.sat_sales AS sat_sales1
    FROM wss
    JOIN store_filter sf ON wss.ss_store_sk = sf.s_store_sk
    JOIN date_filter1 df1 ON wss.d_week_seq = df1.d_week_seq
),
-- Period 2 using explicit joins with filtered dimensions
period2 AS (
    SELECT
        sf.s_store_name AS s_store_name2,
        wss.d_week_seq AS d_week_seq2,
        sf.s_store_id AS s_store_id2,
        wss.sun_sales AS sun_sales2,
        wss.mon_sales AS mon_sales2,
        wss.tue_sales AS tue_sales2,
        wss.wed_sales AS wed_sales2,
        wss.thu_sales AS thu_sales2,
        wss.fri_sales AS fri_sales2,
        wss.sat_sales AS sat_sales2
    FROM wss
    JOIN store_filter sf ON wss.ss_store_sk = sf.s_store_sk
    JOIN date_filter2 df2 ON wss.d_week_seq = df2.d_week_seq
)
SELECT
    p1.s_store_name1,
    p1.s_store_id1,
    p1.d_week_seq1,
    p1.sun_sales1 / NULLIF(p2.sun_sales2, 0) AS "sun_sales1 / sun_sales2",
    p1.mon_sales1 / NULLIF(p2.mon_sales2, 0) AS "mon_sales1 / mon_sales2",
    p1.tue_sales1 / NULLIF(p2.tue_sales2, 0) AS "tue_sales1 / tue_sales2",
    p1.wed_sales1 / NULLIF(p2.wed_sales2, 0) AS "wed_sales1 / wed_sales2",
    p1.thu_sales1 / NULLIF(p2.thu_sales2, 0) AS "thu_sales1 / thu_sales2",
    p1.fri_sales1 / NULLIF(p2.fri_sales2, 0) AS "fri_sales1 / fri_sales2",
    p1.sat_sales1 / NULLIF(p2.sat_sales2, 0) AS "sat_sales1 / sat_sales2"
FROM period1 p1
JOIN period2 p2 ON p1.s_store_id1 = p2.s_store_id2 
    AND p1.d_week_seq1 = p2.d_week_seq2 - 52
ORDER BY p1.s_store_name1, p1.s_store_id1, p1.d_week_seq1
LIMIT 100;