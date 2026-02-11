WITH filtered_date AS (
  SELECT d_date_sk
  FROM date_dim
  WHERE d_year = 1998
),
customer_total_return AS (
  SELECT
    cr_returning_customer_sk AS ctr_customer_sk,
    ca_state AS ctr_state,
    SUM(cr_return_amt_inc_tax) AS ctr_total_return
  FROM catalog_returns
  INNER JOIN filtered_date ON cr_returned_date_sk = filtered_date.d_date_sk
  INNER JOIN customer_address ON cr_returning_addr_sk = ca_address_sk
  GROUP BY cr_returning_customer_sk, ca_state
),
state_averages AS (
  SELECT
    ctr_state,
    AVG(ctr_total_return) * 1.2 AS state_avg_threshold
  FROM customer_total_return
  GROUP BY ctr_state
),
filtered_va_addresses AS (
  SELECT *
  FROM customer_address
  WHERE ca_state = 'VA'
)
SELECT
  c_customer_id,
  c_salutation,
  c_first_name,
  c_last_name,
  ca_street_number,
  ca_street_name,
  ca_street_type,
  ca_suite_number,
  ca_city,
  ca_county,
  ca_state,
  ca_zip,
  ca_country,
  ca_gmt_offset,
  ca_location_type,
  ctr_total_return
FROM customer_total_return AS ctr1
INNER JOIN state_averages AS sa ON ctr1.ctr_state = sa.ctr_state
INNER JOIN customer ON ctr1.ctr_customer_sk = c_customer_sk
INNER JOIN filtered_va_addresses ON ca_address_sk = c_current_addr_sk
WHERE ctr1.ctr_total_return > sa.state_avg_threshold
ORDER BY
  c_customer_id,
  c_salutation,
  c_first_name,
  c_last_name,
  ca_street_number,
  ca_street_name,
  ca_street_type,
  ca_suite_number,
  ca_city,
  ca_county,
  ca_state,
  ca_zip,
  ca_country,
  ca_gmt_offset,
  ca_location_type,
  ctr_total_return
LIMIT 100
