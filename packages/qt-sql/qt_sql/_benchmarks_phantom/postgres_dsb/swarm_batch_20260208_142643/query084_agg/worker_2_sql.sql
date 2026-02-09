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
)
SELECT
  c_customer_id AS customer_id,
  COALESCE(c_last_name, '') || ', ' || COALESCE(c_first_name, '') AS customername
FROM customer
JOIN filtered_address ON c_current_addr_sk = filtered_address.ca_address_sk
JOIN customer_demographics ON cd_demo_sk = c_current_cdemo_sk
JOIN household_demographics ON hd_demo_sk = c_current_hdemo_sk
JOIN filtered_income_band ON filtered_income_band.ib_income_band_sk = hd_income_band_sk
JOIN store_returns ON sr_cdemo_sk = cd_demo_sk
ORDER BY c_customer_id
LIMIT 100