WITH august_returns AS (
    SELECT 
        sr_ticket_number,
        sr_item_sk,
        sr_customer_sk,
        sr_returned_date_sk,
        sr_store_sk
    FROM store_returns
    WHERE EXISTS (
        SELECT 1 FROM date_dim 
        WHERE d_date_sk = sr_returned_date_sk 
        AND d_year = 2002 
        AND d_moy = 8
    )
),
sales_returns_joined AS (
    SELECT 
        ss.ss_store_sk,
        ar.sr_returned_date_sk - ss.ss_sold_date_sk AS days_diff
    FROM store_sales ss
    JOIN august_returns ar ON 
        ss.ss_ticket_number = ar.sr_ticket_number
        AND ss.ss_item_sk = ar.sr_item_sk
        AND ss.ss_customer_sk = ar.sr_customer_sk
    WHERE EXISTS (
        SELECT 1 FROM date_dim d2
        WHERE d2.d_date_sk = ar.sr_returned_date_sk
        AND ss.ss_sold_date_sk BETWEEN d2.d_date_sk - 120 AND d2.d_date_sk
    )
),
store_aggregated AS (
    SELECT 
        ss_store_sk,
        SUM(CASE WHEN days_diff <= 30 THEN 1 ELSE 0 END) AS "30 days",
        SUM(CASE WHEN days_diff > 30 AND days_diff <= 60 THEN 1 ELSE 0 END) AS "31-60 days",
        SUM(CASE WHEN days_diff > 60 AND days_diff <= 90 THEN 1 ELSE 0 END) AS "61-90 days",
        SUM(CASE WHEN days_diff > 90 AND days_diff <= 120 THEN 1 ELSE 0 END) AS "91-120 days",
        SUM(CASE WHEN days_diff > 120 THEN 1 ELSE 0 END) AS ">120 days"
    FROM sales_returns_joined
    GROUP BY ss_store_sk
)
SELECT 
    s.s_store_name,
    s.s_company_id,
    s.s_street_number,
    s.s_street_name,
    s.s_street_type,
    s.s_suite_number,
    s.s_city,
    s.s_county,
    s.s_state,
    s.s_zip,
    sa."30 days",
    sa."31-60 days",
    sa."61-90 days",
    sa."91-120 days",
    sa.">120 days"
FROM store_aggregated sa
JOIN store s ON sa.ss_store_sk = s.s_store_sk
ORDER BY 
    s.s_store_name,
    s.s_company_id,
    s.s_street_number,
    s.s_street_name,
    s.s_street_type,
    s.s_suite_number,
    s.s_city,
    s.s_county,
    s.s_state,
    s.s_zip
LIMIT 100;