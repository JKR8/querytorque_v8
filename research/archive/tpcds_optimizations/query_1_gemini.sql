WITH relevant_stores AS (
    -- 1. Identify only the stores we care about first (~41 rows)
    SELECT s_store_sk
    FROM store
    WHERE s_state = 'SD'
),
customer_total_return AS (
    -- 2. Filter the massive fact table immediately using the relevant stores
    --    This drastically reduces rows input to the GROUP BY.
    SELECT
        sr_customer_sk AS ctr_customer_sk,
        sr_store_sk AS ctr_store_sk,
        SUM(sr_fee) AS ctr_total_return
    FROM store_returns
    JOIN date_dim ON sr_returned_date_sk = d_date_sk
    JOIN relevant_stores ON sr_store_sk = s_store_sk -- Filter pushed down
    WHERE d_year = 2000
    GROUP BY sr_customer_sk, sr_store_sk
),
calc_averages AS (
    -- 3. Calculate the average per store using a Window Function
    --    This eliminates the correlated subquery/self-join.
    SELECT
        ctr_customer_sk,
        ctr_store_sk,
        ctr_total_return,
        AVG(ctr_total_return) OVER (PARTITION BY ctr_store_sk) as store_avg
    FROM customer_total_return
)
SELECT
    c_customer_id
FROM calc_averages
JOIN customer ON ctr_customer_sk = c_customer_sk
WHERE ctr_total_return > (store_avg * 1.2) -- 4. Apply the threshold filter
ORDER BY c_customer_id
LIMIT 100;
