WITH filtered_customer_address AS (
  SELECT ca_address_sk, ca_state
  FROM customer_address
  WHERE ca_state = 'IN'
),
customer_total_return AS (
  SELECT
    wr_returning_customer_sk AS ctr_customer_sk,
    ca.ca_state AS ctr_state,
    SUM(wr_return_amt) AS ctr_total_return
  FROM web_returns
  JOIN date_dim ON wr_returned_date_sk = d_date_sk
  JOIN filtered_customer_address ca ON wr_returning_addr_sk = ca.ca_address_sk
  WHERE d_year = 2002
  GROUP BY
    wr_returning_customer_sk,
    ca.ca_state
),
state_avg_return AS (
  SELECT
    ctr_state,
    AVG(ctr_total_return) * 1.2 AS threshold
  FROM customer_total_return
  GROUP BY ctr_state
)
SELECT
  c_customer_id,
  c_salutation,
  c_first_name,
  c_last_name,
  c_preferred_cust_flag,
  c_birth_day,
  c_birth_month,
  c_birth_year,
  c_birth_country,
  c_login,
  c_email_address,
  c_last_review_date_sk,
  ctr.ctr_total_return
FROM customer_total_return ctr
JOIN state_avg_return sar ON ctr.ctr_state = sar.ctr_state
JOIN customer ON ctr.ctr_customer_sk = c_customer_sk
JOIN filtered_customer_address fca ON c_current_addr_sk = fca.ca_address_sk
WHERE ctr.ctr_total_return > sar.threshold
ORDER BY
  c_customer_id,
  c_salutation,
  c_first_name,
  c_last_name,
  c_preferred_cust_flag,
  c_birth_day,
  c_birth_month,
  c_birth_year,
  c_birth_country,
  c_login,
  c_email_address,
  c_last_review_date_sk,
  ctr_total_return
LIMIT 100