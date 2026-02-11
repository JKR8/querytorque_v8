WITH filtered_customer_address AS (
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
joined_customers AS (
    SELECT
        c.c_customer_id,
        c.c_last_name,
        c.c_first_name,
        cd.cd_demo_sk
    FROM customer c
    JOIN filtered_customer_address ca ON c.c_current_addr_sk = ca.ca_address_sk
    JOIN customer_demographics cd ON cd.cd_demo_sk = c.c_current_cdemo_sk
    JOIN household_demographics hd ON hd.hd_demo_sk = c.c_current_hdemo_sk
    JOIN filtered_income_band ib ON ib.ib_income_band_sk = hd.hd_income_band_sk
)
SELECT
    jc.c_customer_id AS customer_id,
    COALESCE(jc.c_last_name, '') || ', ' || COALESCE(jc.c_first_name, '') AS customername
FROM joined_customers jc
JOIN store_returns sr ON sr.sr_cdemo_sk = jc.cd_demo_sk
ORDER BY jc.c_customer_id
LIMIT 100
