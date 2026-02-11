WITH filtered_date AS (
  SELECT d_date_sk
  FROM date_dim
  WHERE d_year = 2002
),
filtered_ca AS (
  SELECT ca_address_sk, ca_state
  FROM customer_address
  WHERE ca_state = 'IN'
),
customer_total_return AS (
  SELECT
    wr_returning_customer_sk AS ctr_customer_sk,
    ca_state AS ctr_state,
    SUM(wr_return_amt) AS ctr_total_return,
    AVG(SUM(wr_return_amt)) OVER (PARTITION BY ca_state) AS state_avg
  FROM web_returns
  WHERE wr_returned_date_sk IN (SELECT d_date_sk FROM filtered_date)
    AND wr_returning_addr_sk IN (SELECT ca_address_sk FROM customer_address)
  GROUP BY
    wr_returning_customer_sk,
    ca_state
),
indiana_customers AS (
  SELECT
    c_customer_sk,
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
    c_last_review_date_sk
  FROM customer
  WHERE c_current_addr_sk IN (SELECT ca_address_sk FROM filtered_ca)
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
  ctr_total_return
FROM customer_total_return ctr1
JOIN indiana_customers c ON ctr1.ctr_customer_sk = c.c_customer_sk
WHERE ctr_total_return > state_avg * 1.2
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