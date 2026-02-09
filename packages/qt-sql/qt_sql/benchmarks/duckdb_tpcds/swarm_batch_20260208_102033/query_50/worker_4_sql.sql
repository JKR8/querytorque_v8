WITH filtered_returns AS (
    SELECT 
        sr_ticket_number,
        sr_item_sk,
        sr_customer_sk,
        sr_returned_date_sk
    FROM store_returns
    INNER JOIN date_dim AS d2 ON store_returns.sr_returned_date_sk = d2.d_date_sk
    WHERE d2.d_year = 2001 AND d2.d_moy = 8
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
    SUM(
        CASE
            WHEN (sr_returned_date_sk - ss_sold_date_sk > 30)
                 AND (sr_returned_date_sk - ss_sold_date_sk <= 60) THEN 1
            ELSE 0
        END
    ) AS "31-60 days",
    SUM(
        CASE
            WHEN (sr_returned_date_sk - ss_sold_date_sk > 60)
                 AND (sr_returned_date_sk - ss_sold_date_sk <= 90) THEN 1
            ELSE 0
        END
    ) AS "61-90 days",
    SUM(
        CASE
            WHEN (sr_returned_date_sk - ss_sold_date_sk > 90)
                 AND (sr_returned_date_sk - ss_sold_date_sk <= 120) THEN 1
            ELSE 0
        END
    ) AS "91-120 days",
    SUM(CASE WHEN (sr_returned_date_sk - ss_sold_date_sk > 120) THEN 1 ELSE 0 END) AS ">120 days"
FROM store_sales
INNER JOIN filtered_returns ON store_sales.ss_ticket_number = filtered_returns.sr_ticket_number
                            AND store_sales.ss_item_sk = filtered_returns.sr_item_sk
                            AND store_sales.ss_customer_sk = filtered_returns.sr_customer_sk
INNER JOIN store ON store_sales.ss_store_sk = store.s_store_sk
INNER JOIN date_dim AS d1 ON store_sales.ss_sold_date_sk = d1.d_date_sk
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