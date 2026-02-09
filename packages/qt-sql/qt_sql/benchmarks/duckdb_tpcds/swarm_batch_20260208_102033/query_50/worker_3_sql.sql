WITH filtered_returns AS (
    SELECT 
        sr_ticket_number,
        sr_item_sk,
        sr_customer_sk,
        sr_returned_date_sk
    FROM store_returns
    WHERE EXISTS (
        SELECT 1 
        FROM date_dim
        WHERE d_date_sk = sr_returned_date_sk
          AND d_year = 2001
          AND d_moy = 8
    )
),
sales_returns_joined AS (
    SELECT 
        ss_sold_date_sk,
        ss_store_sk,
        filtered_returns.sr_returned_date_sk,
        (filtered_returns.sr_returned_date_sk - ss_sold_date_sk) AS days_diff
    FROM store_sales
    JOIN filtered_returns ON 
        store_sales.ss_ticket_number = filtered_returns.sr_ticket_number
        AND store_sales.ss_item_sk = filtered_returns.sr_item_sk
        AND store_sales.ss_customer_sk = filtered_returns.sr_customer_sk
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
    SUM(CASE WHEN days_diff <= 30 THEN 1 ELSE 0 END) AS "30 days",
    SUM(CASE WHEN days_diff > 30 AND days_diff <= 60 THEN 1 ELSE 0 END) AS "31-60 days",
    SUM(CASE WHEN days_diff > 60 AND days_diff <= 90 THEN 1 ELSE 0 END) AS "61-90 days",
    SUM(CASE WHEN days_diff > 90 AND days_diff <= 120 THEN 1 ELSE 0 END) AS "91-120 days",
    SUM(CASE WHEN days_diff > 120 THEN 1 ELSE 0 END) AS ">120 days"
FROM sales_returns_joined
JOIN store s ON sales_returns_joined.ss_store_sk = s.s_store_sk
WHERE EXISTS (
    SELECT 1 
    FROM date_dim
    WHERE d_date_sk = sales_returns_joined.ss_sold_date_sk
)
GROUP BY
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