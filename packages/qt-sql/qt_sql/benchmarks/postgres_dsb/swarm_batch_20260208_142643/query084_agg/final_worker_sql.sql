WITH filtered_address AS (
    SELECT ca_address_sk
    FROM customer_address
    WHERE ca_city = 'Lakeview'
),
filtered_income AS (
    SELECT ib_income_band_sk
    FROM income_band
    WHERE ib_lower_bound >= 69452
      AND ib_upper_bound <= 69452 + 50000
),
filtered_household AS (
    SELECT hd_demo_sk
    FROM household_demographics
    JOIN filtered_income ON hd_income_band_sk = filtered_income.ib_income_band_sk
),
qualified_customers AS (
    SELECT c_customer_id, c_first_name, c_last_name
    FROM store_returns
    JOIN customer_demographics ON sr_cdemo_sk = cd_demo_sk
    JOIN customer ON cd_demo_sk = c_current_cdemo_sk
    WHERE EXISTS (
        SELECT 1
        FROM filtered_address
        WHERE c_current_addr_sk = filtered_address.ca_address_sk
    )
      AND EXISTS (
        SELECT 1
        FROM filtered_household
        WHERE c_current_hdemo_sk = filtered_household.hd_demo_sk
    )
    ORDER BY c_customer_id
    LIMIT 100
)
SELECT
    c_customer_id AS customer_id,
    COALESCE(c_last_name, '') || ', ' || COALESCE(c_first_name, '') AS customername
FROM qualified_customers;