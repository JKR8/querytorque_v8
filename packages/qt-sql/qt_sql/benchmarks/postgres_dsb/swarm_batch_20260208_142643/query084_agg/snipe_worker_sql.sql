WITH filtered_address AS (
    SELECT ca_address_sk
    FROM customer_address
    WHERE ca_city = 'Lakeview'
),
filtered_income_band AS (
    SELECT ib_income_band_sk
    FROM income_band
    WHERE ib_lower_bound >= 69452
      AND ib_upper_bound <= 69452 + 50000
),
filtered_household AS (
    SELECT hd_demo_sk
    FROM household_demographics hd
    JOIN filtered_income_band fib ON hd.hd_income_band_sk = fib.ib_income_band_sk
),
eligible_customers AS (
    SELECT 
        c_customer_id,
        c_first_name,
        c_last_name,
        c_current_cdemo_sk,
        c_current_hdemo_sk
    FROM customer c
    JOIN filtered_address fa ON c.c_current_addr_sk = fa.ca_address_sk
    JOIN filtered_household fh ON c.c_current_hdemo_sk = fh.hd_demo_sk
)
SELECT
    ec.c_customer_id AS customer_id,
    COALESCE(ec.c_last_name, '') || ', ' || COALESCE(ec.c_first_name, '') AS customername
FROM eligible_customers ec
JOIN customer_demographics cd ON ec.c_current_cdemo_sk = cd.cd_demo_sk
JOIN store_returns sr ON sr.sr_cdemo_sk = cd.cd_demo_sk
ORDER BY ec.c_customer_id
LIMIT 100