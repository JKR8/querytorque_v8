WITH filtered_returns AS (
    SELECT 
        sr_ticket_number,
        sr_item_sk,
        sr_customer_sk,
        sr_returned_date_sk
    FROM store_returns
    WHERE sr_returned_date_sk IN (
        SELECT d_date_sk 
        FROM date_dim 
        WHERE d_year = 2002 AND d_moy = 8
    )
),
sales_with_dates AS (
    SELECT 
        ss_ticket_number,
        ss_item_sk,
        ss_customer_sk,
        ss_store_sk,
        ss_sold_date_sk,
        d1.d_date AS sold_date,
        d2.d_date AS return_date,
        d2.d_date_sk AS return_date_sk
    FROM store_sales
    JOIN date_dim d1 ON ss_sold_date_sk = d1.d_date_sk
    JOIN filtered_returns fr ON ss_ticket_number = fr.sr_ticket_number 
        AND ss_item_sk = fr.sr_item_sk 
        AND ss_customer_sk = fr.sr_customer_sk
    JOIN date_dim d2 ON fr.sr_returned_date_sk = d2.d_date_sk
    WHERE d1.d_date BETWEEN (d2.d_date - INTERVAL '120 DAY') AND d2.d_date
),
return_latency_buckets AS (
    SELECT 
        ss_store_sk,
        return_date_sk - ss_sold_date_sk AS day_diff,
        COUNT(*) AS cnt
    FROM sales_with_dates
    GROUP BY ss_store_sk, return_date_sk - ss_sold_date_sk
)
SELECT
    s_store_name,
    s_company_id,
    s_street_number,
    s_street_name,
    s_street_type,
    s_suite_number,
    s_city,
    s_county,
    s_state,
    s_zip,
    COALESCE(SUM(CASE WHEN day_diff <= 30 THEN cnt END), 0) AS "30 days",
    COALESCE(SUM(CASE WHEN day_diff > 30 AND day_diff <= 60 THEN cnt END), 0) AS "31-60 days",
    COALESCE(SUM(CASE WHEN day_diff > 60 AND day_diff <= 90 THEN cnt END), 0) AS "61-90 days",
    COALESCE(SUM(CASE WHEN day_diff > 90 AND day_diff <= 120 THEN cnt END), 0) AS "91-120 days",
    COALESCE(SUM(CASE WHEN day_diff > 120 THEN cnt END), 0) AS ">120 days"
FROM return_latency_buckets rlb
JOIN store ON rlb.ss_store_sk = s_store_sk
GROUP BY
    s_store_name,
    s_company_id,
    s_street_number,
    s_street_name,
    s_street_type,
    s_suite_number,
    s_city,
    s_county,
    s_state,
    s_zip
ORDER BY
    s_store_name,
    s_company_id,
    s_street_number,
    s_street_name,
    s_street_type,
    s_suite_number,
    s_city,
    s_county,
    s_state,
    s_zip
LIMIT 100;
