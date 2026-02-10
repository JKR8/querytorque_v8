WITH store_avg_return AS (
    SELECT 
        sr_store_sk,
        AVG(sr_fee) * 1.2 AS avg_store_return
    FROM store_returns
    INNER JOIN date_dim ON sr_returned_date_sk = d_date_sk
    WHERE d_year = 2000
    GROUP BY sr_store_sk
),
customer_store_return AS (
    SELECT 
        sr_customer_sk,
        sr_store_sk,
        SUM(SR_FEE) AS total_return
    FROM store_returns
    INNER JOIN date_dim ON sr_returned_date_sk = d_date_sk
    WHERE d_year = 2000
    GROUP BY sr_customer_sk, sr_store_sk
)
SELECT 
    c_customer_id
FROM customer_store_return csr
INNER JOIN store_avg_return sar ON csr.sr_store_sk = sar.sr_store_sk
INNER JOIN store ON csr.sr_store_sk = s_store_sk
INNER JOIN customer ON csr.sr_customer_sk = c_customer_sk
WHERE csr.total_return > sar.avg_store_return
  AND s_state = 'SD'
ORDER BY c_customer_id
LIMIT 100;