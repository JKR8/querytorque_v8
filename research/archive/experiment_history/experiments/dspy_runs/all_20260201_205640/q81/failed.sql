WITH customer_total_return AS (
    SELECT 
        cr_returning_customer_sk AS ctr_customer_sk,
        ca_state AS ctr_state,
        SUM(cr_return_amt_inc_tax) AS ctr_total_return
    FROM catalog_returns
    INNER JOIN date_dim ON cr_returned_date_sk = d_date_sk
    INNER JOIN customer_address ON cr_returning_addr_sk = ca_address_sk
    WHERE d_year = 2002
    GROUP BY cr_returning_customer_sk, ca_state
),
state_avg_return AS (
    SELECT 
        ctr_state,
        AVG(ctr_total_return) * 1.2 AS avg_threshold
    FROM customer_total_return
    GROUP BY ctr_state
)
SELECT 
    c_customer_id, c_salutation, c_first_name, c_last_name,
    ca_street_number, ca_street_name, ca_street_type, ca_suite_number,
    ca_city, ca_county, ca_state, ca_zip, ca_country, ca_gmt_offset,
    ca_location_type, ctr.ctr_total_return
FROM customer_total_return ctr
INNER JOIN state_avg_return sar ON ctr.ctr_state = sar.ctr_state
INNER JOIN customer ON ctr.ctr_customer_sk = c_customer_sk
INNER JOIN customer_address ca ON ca_address_sk = c_current_addr_sk
WHERE ca.ca_state = 'CA'
  AND ctr.ctr_total_return > sar.avg_threshold
  AND ctr.ctr_state = 'CA'
ORDER BY 
    c_customer_id, c_salutation, c_first_name, c_last_name,
    ca_street_number, ca_street_name, ca_street_type, ca_suite_number,
    ca_city, ca_county, ca_state, ca_zip, ca_country, ca_gmt_offset,
    ca_location_type, ctr.ctr_total_return
LIMIT 100;