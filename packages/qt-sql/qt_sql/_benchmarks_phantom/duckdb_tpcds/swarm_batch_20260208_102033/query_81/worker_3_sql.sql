WITH filtered_date AS (
    SELECT d_date_sk
    FROM date_dim
    WHERE d_year = 2002
),
filtered_ca AS (
    SELECT ca_address_sk, ca_state
    FROM customer_address
    WHERE ca_state = 'CA'
),
customer_total_return AS (
    SELECT
        cr.cr_returning_customer_sk AS ctr_customer_sk,
        ca.ca_state AS ctr_state,
        SUM(cr.cr_return_amt_inc_tax) AS ctr_total_return,
        AVG(SUM(cr.cr_return_amt_inc_tax)) OVER (PARTITION BY ca.ca_state) AS state_avg_return
    FROM catalog_returns cr
    JOIN filtered_date fd ON cr.cr_returned_date_sk = fd.d_date_sk
    JOIN customer_address ca ON cr.cr_returning_addr_sk = ca.ca_address_sk
    GROUP BY cr.cr_returning_customer_sk, ca.ca_state
)
SELECT
    c.c_customer_id,
    c.c_salutation,
    c.c_first_name,
    c.c_last_name,
    ca.ca_street_number,
    ca.ca_street_name,
    ca.ca_street_type,
    ca.ca_suite_number,
    ca.ca_city,
    ca.ca_county,
    ca.ca_state,
    ca.ca_zip,
    ca.ca_country,
    ca.ca_gmt_offset,
    ca.ca_location_type,
    ctr.ctr_total_return
FROM customer_total_return ctr
JOIN customer c ON ctr.ctr_customer_sk = c.c_customer_sk
JOIN filtered_ca ca ON c.c_current_addr_sk = ca.ca_address_sk
WHERE ctr.ctr_total_return > ctr.state_avg_return * 1.2
ORDER BY
    c.c_customer_id,
    c.c_salutation,
    c.c_first_name,
    c.c_last_name,
    ca.ca_street_number,
    ca.ca_street_name,
    ca.ca_street_type,
    ca.ca_suite_number,
    ca.ca_city,
    ca.ca_county,
    ca.ca_state,
    ca.ca_zip,
    ca.ca_country,
    ca.ca_gmt_offset,
    ca.ca_location_type,
    ctr.ctr_total_return
LIMIT 100;