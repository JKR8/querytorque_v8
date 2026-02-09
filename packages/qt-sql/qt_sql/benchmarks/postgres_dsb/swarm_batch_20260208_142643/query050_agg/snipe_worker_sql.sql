WITH d2_filtered AS (
    SELECT d_date_sk, d_date
    FROM date_dim
    WHERE d_year = 2002
      AND d_moy = 8
),
d2_date_range AS (
    SELECT MIN(d_date) AS min_d_date, MAX(d_date) AS max_d_date
    FROM d2_filtered
),
d1_filtered AS (
    SELECT d_date_sk, d_date
    FROM date_dim
    CROSS JOIN d2_date_range
    WHERE date_dim.d_date BETWEEN (d2_date_range.min_d_date - INTERVAL '120 DAY')
                              AND d2_date_range.max_d_date
),
filtered_store_sales AS (
    SELECT
        ss_ticket_number,
        ss_item_sk,
        ss_customer_sk,
        ss_store_sk,
        ss_sold_date_sk,
        d1_filtered.d_date AS sold_date
    FROM store_sales
    JOIN d1_filtered ON store_sales.ss_sold_date_sk = d1_filtered.d_date_sk
),
filtered_store_returns AS (
    SELECT
        sr_ticket_number,
        sr_item_sk,
        sr_customer_sk,
        sr_returned_date_sk,
        d2_filtered.d_date AS return_date
    FROM store_returns
    JOIN d2_filtered ON store_returns.sr_returned_date_sk = d2_filtered.d_date_sk
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
    SUM(CASE WHEN (return_date - sold_date <= 30) THEN 1 ELSE 0 END) AS "30 days",
    SUM(CASE WHEN (return_date - sold_date > 30 AND return_date - sold_date <= 60)
             THEN 1 ELSE 0 END) AS "31-60 days",
    SUM(CASE WHEN (return_date - sold_date > 60 AND return_date - sold_date <= 90)
             THEN 1 ELSE 0 END) AS "61-90 days",
    SUM(CASE WHEN (return_date - sold_date > 90 AND return_date - sold_date <= 120)
             THEN 1 ELSE 0 END) AS "91-120 days",
    SUM(CASE WHEN (return_date - sold_date > 120) THEN 1 ELSE 0 END) AS ">120 days"
FROM filtered_store_sales ss
JOIN filtered_store_returns sr
    ON ss.ss_ticket_number = sr.sr_ticket_number
   AND ss.ss_item_sk = sr.sr_item_sk
   AND ss.ss_customer_sk = sr.sr_customer_sk
JOIN store ON ss.ss_store_sk = store.s_store_sk
WHERE sold_date BETWEEN (return_date - INTERVAL '120 DAY') AND return_date
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