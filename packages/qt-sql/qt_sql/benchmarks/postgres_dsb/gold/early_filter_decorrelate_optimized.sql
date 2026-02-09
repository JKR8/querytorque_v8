WITH customer_total_return AS (
    SELECT sr_customer_sk AS ctr_customer_sk,
           sr_store_sk AS ctr_store_sk,
           sr_reason_sk AS ctr_reason_sk,
           SUM(SR_REFUNDED_CASH) AS ctr_total_return
    FROM store_returns
    JOIN date_dim ON sr_returned_date_sk = d_date_sk
    JOIN store ON sr_store_sk = s_store_sk
    WHERE d_year = 2001
      AND s_state IN ('MI', 'NC', 'WI')
      AND sr_return_amt / sr_return_quantity BETWEEN 236 AND 295
    GROUP BY sr_customer_sk, sr_store_sk, sr_reason_sk
),
store_thresholds AS (
    SELECT ctr_store_sk,
           AVG(ctr_total_return) * 1.2 AS avg_limit
    FROM customer_total_return
    GROUP BY ctr_store_sk
)
SELECT c_customer_id
FROM customer_total_return ctr1
JOIN store_thresholds st ON ctr1.ctr_store_sk = st.ctr_store_sk
JOIN customer ON ctr1.ctr_customer_sk = c_customer_sk
JOIN customer_demographics ON c_current_cdemo_sk = cd_demo_sk
JOIN store s ON ctr1.ctr_store_sk = s.s_store_sk
WHERE ctr1.ctr_total_return > st.avg_limit
  AND ctr1.ctr_reason_sk BETWEEN 28 AND 31
  AND s.s_state IN ('MI', 'NC', 'WI')
  AND cd_marital_status = 'W'
  AND cd_education_status IN ('4 yr Degree', 'College')
  AND cd_gender = 'M'
  AND c_birth_month = 5
  AND c_birth_year BETWEEN 1950 AND 1956
ORDER BY c_customer_id
LIMIT 100