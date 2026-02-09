WITH filtered_d2 AS (
    SELECT d_date_sk
    FROM date_dim
    WHERE d_year = 2001
      AND d_moy = 8
),
filtered_d1 AS (
    SELECT d_date_sk
    FROM date_dim
),
sales_with_store AS (
    SELECT 
        ss_ticket_number,
        ss_item_sk,
        ss_customer_sk,
        ss_sold_date_sk,
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
    FROM store_sales
    JOIN store ON ss_store_sk = s_store_sk
    JOIN filtered_d1 ON ss_sold_date_sk = filtered_d1.d_date_sk
),
returns_with_date AS (
    SELECT 
        sr_ticket_number,
        sr_item_sk,
        sr_customer_sk,
        sr_returned_date_sk
    FROM store_returns
    JOIN filtered_d2 ON sr_returned_date_sk = filtered_d2.d_date_sk
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
    SUM(CASE WHEN (sr_returned_date_sk - ss_sold_date_sk <= 30) THEN 1 ELSE 0 END) AS "30 days",
    SUM(CASE WHEN (sr_returned_date_sk - ss_sold_date_sk > 30) AND (sr_returned_date_sk - ss_sold_date_sk <= 60) THEN 1 ELSE 0 END) AS "31-60 days",
    SUM(CASE WHEN (sr_returned_date_sk - ss_sold_date_sk > 60) AND (sr_returned_date_sk - ss_sold_date_sk <= 90) THEN 1 ELSE 0 END) AS "61-90 days",
    SUM(CASE WHEN (sr_returned_date_sk - ss_sold_date_sk > 90) AND (sr_returned_date_sk - ss_sold_date_sk <= 120) THEN 1 ELSE 0 END) AS "91-120 days",
    SUM(CASE WHEN (sr_returned_date_sk - ss_sold_date_sk > 120) THEN 1 ELSE 0 END) AS ">120 days"
FROM sales_with_store s
JOIN returns_with_date r 
  ON s.ss_ticket_number = r.sr_ticket_number
 AND s.ss_item_sk = r.sr_item_sk
 AND s.ss_customer_sk = r.sr_customer_sk
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