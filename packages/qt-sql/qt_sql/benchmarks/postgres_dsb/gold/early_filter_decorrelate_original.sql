WITH customer_total_return AS (
  SELECT sr_customer_sk AS ctr_customer_sk,
         sr_store_sk AS ctr_store_sk,
         sr_reason_sk AS ctr_reason_sk,
         SUM(SR_REFUNDED_CASH) AS ctr_total_return
  FROM store_returns, date_dim
  WHERE sr_returned_date_sk = d_date_sk
    AND d_year = 2001
    AND sr_return_amt / sr_return_quantity BETWEEN 236 AND 295
  GROUP BY sr_customer_sk, sr_store_sk, sr_reason_sk
)
SELECT c_customer_id
FROM customer_total_return ctr1, store, customer, customer_demographics
WHERE ctr1.ctr_total_return > (
    SELECT AVG(ctr_total_return) * 1.2
    FROM customer_total_return ctr2
    WHERE ctr1.ctr_store_sk = ctr2.ctr_store_sk
  )
  AND ctr1.ctr_reason_sk BETWEEN 28 AND 31
  AND s_store_sk = ctr1.ctr_store_sk
  AND s_state IN ('MI', 'NC', 'WI')
  AND ctr1.ctr_customer_sk = c_customer_sk
  AND c_current_cdemo_sk = cd_demo_sk
  AND cd_marital_status IN ('W', 'W')
  AND cd_education_status IN ('4 yr Degree', 'College')
  AND cd_gender = 'M'
  AND c_birth_month = 5
  AND c_birth_year BETWEEN 1950 AND 1956
ORDER BY c_customer_id
LIMIT 100