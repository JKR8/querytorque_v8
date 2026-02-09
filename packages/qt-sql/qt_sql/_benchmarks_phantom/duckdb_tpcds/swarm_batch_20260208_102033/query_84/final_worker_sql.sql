WITH 
-- 1. Filtered dimension tables
filtered_address AS (
    SELECT ca_address_sk
    FROM customer_address
    WHERE ca_city = 'Oakwood'
),
filtered_income_band AS (
    SELECT ib_income_band_sk
    FROM income_band
    WHERE ib_lower_bound >= 5806 
      AND ib_upper_bound <= 5806 + 50000
),
-- 2. Pre-aggregated fact table: only need existence, but must preserve cardinality
--    Get unique sr_cdemo_sk values (for semi-join) and maintain count for ordering
sr_agg AS (
    SELECT sr_cdemo_sk
    FROM store_returns
    GROUP BY sr_cdemo_sk
),
-- 3. Candidate customers: join all filtered dimensions, ordered by c_customer_id
candidate_customers AS (
    SELECT 
        c_customer_id,
        c_last_name,
        c_first_name,
        cd_demo_sk
    FROM customer
    JOIN filtered_address ON c_current_addr_sk = ca_address_sk
    JOIN customer_demographics ON cd_demo_sk = c_current_cdemo_sk
    JOIN household_demographics ON hd_demo_sk = c_current_hdemo_sk
    JOIN filtered_income_band ON ib_income_band_sk = hd_income_band_sk
    ORDER BY c_customer_id
    LIMIT 100
)
-- 4. Final join: verify existence in store_returns using pre-aggregated fact data
SELECT
    c_customer_id AS customer_id,
    COALESCE(c_last_name, '') || ', ' || COALESCE(c_first_name, '') AS customername
FROM candidate_customers cc
JOIN sr_agg ON sr_agg.sr_cdemo_sk = cc.cd_demo_sk
ORDER BY c_customer_id
LIMIT 100;