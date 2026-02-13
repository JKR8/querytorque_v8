WITH returns_with_store_avg AS (SELECT sr_customer_sk AS ctr_customer_sk,
       sr_store_sk AS ctr_store_sk,
       sr_reason_sk AS ctr_reason_sk,
       SUM(sr_return_amt_inc_tax) AS ctr_total_return,
       AVG(SUM(sr_return_amt_inc_tax)) OVER (PARTITION BY sr_store_sk) * 1.2 AS store_avg_threshold
FROM store_returns
INNER JOIN date_dim ON sr_returned_date_sk = d_date_sk
WHERE d_year = 2000
  AND sr_return_amt / sr_return_quantity BETWEEN 115 AND 174
GROUP BY sr_customer_sk, sr_store_sk, sr_reason_sk) SELECT c_customer_id
FROM returns_with_store_avg r
INNER JOIN store ON s_store_sk = r.ctr_store_sk
INNER JOIN customer ON c_customer_sk = r.ctr_customer_sk
INNER JOIN customer_demographics ON cd_demo_sk = c_current_cdemo_sk
WHERE r.ctr_total_return > r.store_avg_threshold
  AND r.ctr_reason_sk BETWEEN 17 AND 20
  AND s_state IN ('IA', 'KY', 'NE')
  AND cd_marital_status IN ('S', 'S')
  AND cd_education_status IN ('4 yr Degree', '4 yr Degree')
  AND cd_gender = 'M'
  AND c_birth_month = 4
  AND c_birth_year BETWEEN 1987 AND 1993
ORDER BY c_customer_id
LIMIT 100