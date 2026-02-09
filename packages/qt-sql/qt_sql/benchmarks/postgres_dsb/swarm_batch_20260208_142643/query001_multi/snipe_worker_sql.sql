WITH filtered_store AS (
  SELECT s_store_sk
  FROM store
  WHERE s_state IN ('MI', 'ND', 'TX')
),
filtered_date AS (
  SELECT d_date_sk
  FROM date_dim
  WHERE d_year = 2000
),
customer_total_return AS (
  SELECT
    sr_customer_sk AS ctr_customer_sk,
    sr_store_sk AS ctr_store_sk,
    sr_reason_sk AS ctr_reason_sk,
    SUM(sr_refunded_cash) AS ctr_total_return
  FROM store_returns
  JOIN filtered_date ON sr_returned_date_sk = filtered_date.d_date_sk
  WHERE sr_return_amt / sr_return_quantity BETWEEN 16 AND 75
  GROUP BY
    sr_customer_sk,
    sr_store_sk,
    sr_reason_sk
),
store_thresholds AS (
  SELECT
    ctr_store_sk,
    AVG(ctr_total_return) * 1.2 AS avg_limit
  FROM customer_total_return
  GROUP BY ctr_store_sk
),
filtered_customer_demographics AS (
  SELECT cd_demo_sk
  FROM customer_demographics
  WHERE cd_marital_status IN ('M', 'M')
    AND cd_education_status IN ('College', 'College')
    AND cd_gender = 'M'
),
filtered_customer AS (
  SELECT c_customer_sk, c_customer_id
  FROM customer
  JOIN filtered_customer_demographics ON c_current_cdemo_sk = filtered_customer_demographics.cd_demo_sk
  WHERE c_birth_month = 9
    AND c_birth_year BETWEEN 1979 AND 1985
)
SELECT
  c_customer_id
FROM customer_total_return ctr1
JOIN store_thresholds st ON ctr1.ctr_store_sk = st.ctr_store_sk
JOIN filtered_store fs ON ctr1.ctr_store_sk = fs.s_store_sk
JOIN filtered_customer c ON ctr1.ctr_customer_sk = c.c_customer_sk
WHERE ctr1.ctr_total_return > st.avg_limit
  AND ctr1.ctr_reason_sk BETWEEN 25 AND 28
ORDER BY c_customer_id
LIMIT 100