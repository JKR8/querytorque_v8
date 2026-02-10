WITH filtered_income_band AS (
    SELECT ib_income_band_sk 
    FROM income_band 
    WHERE ib_lower_bound >= 5806 
      AND ib_upper_bound <= 5806 + 50000
),
filtered_customer_address AS (
    SELECT ca_address_sk 
    FROM customer_address 
    WHERE ca_city = 'Oakwood'
)
SELECT c.c_customer_id as customer_id,
       COALESCE(c.c_last_name, '') || ', ' || COALESCE(c.c_first_name, '') as customername
FROM filtered_customer_address fca
JOIN customer c ON c.c_current_addr_sk = fca.ca_address_sk
JOIN customer_demographics cd ON cd.cd_demo_sk = c.c_current_cdemo_sk
JOIN household_demographics hd ON hd.hd_demo_sk = c.c_current_hdemo_sk
JOIN filtered_income_band fib ON fib.ib_income_band_sk = hd.hd_income_band_sk
JOIN store_returns sr ON sr.sr_cdemo_sk = cd.cd_demo_sk
ORDER BY c.c_customer_id
LIMIT 100;