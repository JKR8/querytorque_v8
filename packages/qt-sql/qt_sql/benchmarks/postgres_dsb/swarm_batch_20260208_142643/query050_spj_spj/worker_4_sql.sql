WITH return_dates AS (
    SELECT d_date_sk, d_date
    FROM date_dim
    WHERE d_moy = 12
),
sales_dates AS (
    SELECT d_date_sk, d_date
    FROM date_dim
    WHERE d_dow = 1
),
filtered_store AS (
    SELECT s_store_sk, s_store_name, s_company_id, s_street_number,
           s_street_name, s_suite_number, s_city, s_zip
    FROM store
    WHERE s_state IN ('LA', 'TX', 'VA')
)
SELECT
    MIN(s.s_store_name),
    MIN(s.s_company_id),
    MIN(s.s_street_number),
    MIN(s.s_street_name),
    MIN(s.s_suite_number),
    MIN(s.s_city),
    MIN(s.s_zip),
    MIN(ss.ss_ticket_number),
    MIN(ss.ss_sold_date_sk),
    MIN(sr.sr_returned_date_sk),
    MIN(ss.ss_item_sk),
    MIN(sd.d_date_sk)
FROM store_sales ss
JOIN store_returns sr ON ss.ss_ticket_number = sr.sr_ticket_number
    AND ss.ss_item_sk = sr.sr_item_sk
    AND ss.ss_customer_sk = sr.sr_customer_sk
JOIN filtered_store s ON ss.ss_store_sk = s.s_store_sk
    AND sr.sr_store_sk = s.s_store_sk
JOIN sales_dates sd ON ss.ss_sold_date_sk = sd.d_date_sk
JOIN return_dates rd ON sr.sr_returned_date_sk = rd.d_date_sk
WHERE sd.d_date BETWEEN (rd.d_date - INTERVAL '120 DAY') AND rd.d_date;