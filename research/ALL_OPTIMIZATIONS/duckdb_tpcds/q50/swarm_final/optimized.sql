WITH filtered_returns AS (
    SELECT 
        sr_ticket_number,
        sr_item_sk,
        sr_customer_sk,
        sr_returned_date_sk
    FROM store_returns
    JOIN date_dim d2 ON sr_returned_date_sk = d2.d_date_sk
    WHERE d2.d_year = 2001 AND d2.d_moy = 8
),
store_sales_filtered AS (
    SELECT 
        ss_store_sk,
        ss_sold_date_sk,
        sr_returned_date_sk - ss_sold_date_sk AS days_diff
    FROM store_sales
    WHERE EXISTS (
        SELECT 1
        FROM filtered_returns fr
        WHERE ss_ticket_number = fr.sr_ticket_number
          AND ss_item_sk = fr.sr_item_sk
          AND ss_customer_sk = fr.sr_customer_sk
    )
),
bucketed_counts AS (
    SELECT 
        ss_store_sk,
        COUNT(CASE WHEN days_diff <= 30 THEN 1 END) AS days_30,
        COUNT(CASE WHEN days_diff > 30 AND days_diff <= 60 THEN 1 END) AS days_31_60,
        COUNT(CASE WHEN days_diff > 60 AND days_diff <= 90 THEN 1 END) AS days_61_90,
        COUNT(CASE WHEN days_diff > 90 AND days_diff <= 120 THEN 1 END) AS days_91_120,
        COUNT(CASE WHEN days_diff > 120 THEN 1 END) AS days_over_120
    FROM store_sales_filtered
    GROUP BY ss_store_sk
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
    COALESCE(days_30, 0) AS "30 days",
    COALESCE(days_31_60, 0) AS "31-60 days",
    COALESCE(days_61_90, 0) AS "61-90 days",
    COALESCE(days_91_120, 0) AS "91-120 days",
    COALESCE(days_over_120, 0) AS ">120 days"
FROM store
LEFT JOIN bucketed_counts ON ss_store_sk = s_store_sk
WHERE s_store_sk IS NOT NULL
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