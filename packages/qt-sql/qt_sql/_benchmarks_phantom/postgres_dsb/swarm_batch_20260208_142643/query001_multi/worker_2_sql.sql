WITH filtered_date AS (
    SELECT d_date_sk
    FROM date_dim
    WHERE d_year = 2000
),
filtered_store_returns AS (
    SELECT
        sr_customer_sk,
        sr_store_sk,
        sr_reason_sk,
        SUM(sr_refunded_cash) AS ctr_total_return
    FROM store_returns
    INNER JOIN filtered_date ON sr_returned_date_sk = d_date_sk
    WHERE sr_return_amt / sr_return_quantity BETWEEN 16 AND 75
    GROUP BY sr_customer_sk, sr_store_sk, sr_reason_sk
),
store_averages AS (
    SELECT
        sr_store_sk,
        AVG(ctr_total_return) * 1.2 AS store_avg_threshold
    FROM filtered_store_returns
    GROUP BY sr_store_sk
),
filtered_store AS (
    SELECT s_store_sk
    FROM store
    WHERE s_state IN ('MI', 'ND', 'TX')
),
filtered_customer_demographics AS (
    SELECT cd_demo_sk
    FROM customer_demographics
    WHERE cd_marital_status = 'M'
      AND cd_education_status = 'College'
      AND cd_gender = 'M'
),
filtered_customer AS (
    SELECT c_customer_sk, c_customer_id
    FROM customer
    WHERE c_birth_month = 9
      AND c_birth_year BETWEEN 1979 AND 1985
)
SELECT
    fc.c_customer_id
FROM filtered_store_returns AS ctr1
INNER JOIN store_averages AS sa ON ctr1.sr_store_sk = sa.sr_store_sk
INNER JOIN filtered_store AS fs ON ctr1.sr_store_sk = fs.s_store_sk
INNER JOIN filtered_customer AS fc ON ctr1.sr_customer_sk = fc.c_customer_sk
INNER JOIN filtered_customer_demographics AS fcd ON fc.c_current_cdemo_sk = fcd.cd_demo_sk
WHERE ctr1.sr_reason_sk BETWEEN 25 AND 28
  AND ctr1.ctr_total_return > sa.store_avg_threshold
ORDER BY fc.c_customer_id
LIMIT 100;