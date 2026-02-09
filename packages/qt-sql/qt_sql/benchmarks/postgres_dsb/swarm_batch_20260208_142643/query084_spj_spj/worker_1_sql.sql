WITH filtered_customer_address AS (
    SELECT ca_address_sk
    FROM customer_address
    WHERE ca_city = 'Lakeview'
),
filtered_income_band AS (
    SELECT ib_income_band_sk
    FROM income_band
    WHERE ib_lower_bound >= 7 * 10000
      AND ib_upper_bound <= 7 * 10000 + 50000
)
SELECT
    MIN(c.c_customer_id),
    MIN(sr.sr_ticket_number),
    MIN(sr.sr_item_sk)
FROM store_returns sr
JOIN customer_demographics cd ON sr.sr_cdemo_sk = cd.cd_demo_sk
JOIN customer c ON cd.cd_demo_sk = c.c_current_cdemo_sk
JOIN filtered_customer_address fca ON c.c_current_addr_sk = fca.ca_address_sk
JOIN household_demographics hd ON c.c_current_hdemo_sk = hd.hd_demo_sk
JOIN filtered_income_band fib ON hd.hd_income_band_sk = fib.ib_income_band_sk;