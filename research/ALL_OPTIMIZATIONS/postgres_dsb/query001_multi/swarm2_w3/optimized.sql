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
        SR_REFUNDED_CASH
    FROM store_returns
    INNER JOIN filtered_date ON sr_returned_date_sk = filtered_date.d_date_sk
    WHERE sr_return_amt / sr_return_quantity BETWEEN 16 AND 75
),
customer_total_return AS (
    SELECT
        sr_customer_sk AS ctr_customer_sk,
        sr_store_sk AS ctr_store_sk,
        sr_reason_sk AS ctr_reason_sk,
        SUM(SR_REFUNDED_CASH) AS ctr_total_return
    FROM filtered_store_returns
    GROUP BY
        sr_customer_sk,
        sr_store_sk,
        sr_reason_sk
),
store_avg_returns AS (
    SELECT
        ctr_store_sk,
        AVG(ctr_total_return) * 1.2 AS store_avg_threshold
    FROM customer_total_return
    GROUP BY ctr_store_sk
),
filtered_store AS (
    SELECT s_store_sk
    FROM store
    WHERE s_state IN ('MI', 'ND', 'TX')
),
filtered_customer_demographics AS (
    SELECT cd_demo_sk
    FROM customer_demographics
    WHERE cd_marital_status IN ('M', 'M')
      AND cd_education_status IN ('College', 'College')
      AND cd_gender = 'M'
),
filtered_customer AS (
    SELECT c_customer_sk, c_customer_id, c_current_cdemo_sk
    FROM customer
    WHERE c_birth_month = 9
      AND c_birth_year BETWEEN 1979 AND 1985
      AND c_current_cdemo_sk IN (SELECT cd_demo_sk FROM filtered_customer_demographics)
)
SELECT
    c.c_customer_id
FROM customer_total_return AS ctr1
INNER JOIN store_avg_returns AS sar ON ctr1.ctr_store_sk = sar.ctr_store_sk
INNER JOIN filtered_store AS fs ON ctr1.ctr_store_sk = fs.s_store_sk
INNER JOIN filtered_customer AS c ON ctr1.ctr_customer_sk = c.c_customer_sk
WHERE ctr1.ctr_total_return > sar.store_avg_threshold
  AND ctr1.ctr_reason_sk BETWEEN 25 AND 28
ORDER BY c.c_customer_id
LIMIT 100