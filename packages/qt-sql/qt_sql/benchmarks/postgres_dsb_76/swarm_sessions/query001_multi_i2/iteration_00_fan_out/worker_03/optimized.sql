WITH transitive_filtered_returns AS (SELECT sr_customer_sk AS ctr_customer_sk,
       sr_store_sk AS ctr_store_sk,
       sr_reason_sk AS ctr_reason_sk,
       SUM(SR_RETURN_AMT_INC_TAX) AS ctr_total_return
FROM store_returns sr
JOIN date_dim ON sr_returned_date_sk = d_date_sk
JOIN store ON s_store_sk = sr_store_sk
JOIN customer ON c_customer_sk = sr_customer_sk
JOIN customer_demographics ON cd_demo_sk = c_current_cdemo_sk
WHERE d_year = 2000
  AND sr_return_amt / sr_return_quantity BETWEEN 115 AND 174
  AND s_state IN ('IA', 'KY', 'NE')
  AND cd_marital_status IN ('S', 'S')
  AND cd_education_status IN ('4 yr Degree', '4 yr Degree')
  AND cd_gender = 'M'
  AND c_birth_month = 4
  AND c_birth_year BETWEEN 1987 AND 1993
GROUP BY sr_customer_sk, sr_store_sk, sr_reason_sk),
     store_averages AS (SELECT ctr_store_sk,
       AVG(ctr_total_return) * 1.2 AS store_threshold
FROM transitive_filtered_returns
GROUP BY ctr_store_sk),
     dimension_joins AS (SELECT r.ctr_customer_sk,
       r.ctr_store_sk,
       r.ctr_reason_sk,
       r.ctr_total_return
FROM transitive_filtered_returns r
JOIN store_averages s ON r.ctr_store_sk = s.ctr_store_sk
WHERE r.ctr_total_return > s.store_threshold
  AND r.ctr_reason_sk BETWEEN 17 AND 20)
SELECT c_customer_id
FROM dimension_joins d
JOIN customer c ON d.ctr_customer_sk = c.c_customer_sk
ORDER BY c_customer_id
LIMIT 100