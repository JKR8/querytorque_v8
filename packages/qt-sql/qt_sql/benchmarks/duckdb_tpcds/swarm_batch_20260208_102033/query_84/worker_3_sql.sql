WITH filtered_income_band AS (
    SELECT ib_income_band_sk
    FROM income_band
    WHERE ib_lower_bound >= 5806
      AND ib_upper_bound <= 5806 + 50000
),
filtered_household AS (
    SELECT hd_demo_sk, hd_income_band_sk
    FROM household_demographics
    INNER JOIN filtered_income_band 
        ON household_demographics.hd_income_band_sk = filtered_income_band.ib_income_band_sk
),
filtered_customer_address AS (
    SELECT ca_address_sk
    FROM customer_address
    WHERE ca_city = 'Oakwood'
),
prejoined_customer AS (
    SELECT 
        c_customer_id,
        c_current_cdemo_sk,
        c_last_name,
        c_first_name,
        c_current_hdemo_sk,
        c_current_addr_sk
    FROM customer
    INNER JOIN filtered_customer_address 
        ON customer.c_current_addr_sk = filtered_customer_address.ca_address_sk
    INNER JOIN filtered_household 
        ON customer.c_current_hdemo_sk = filtered_household.hd_demo_sk
),
prejoined_fact AS (
    SELECT 
        prejoined_customer.c_customer_id,
        prejoined_customer.c_last_name,
        prejoined_customer.c_first_name,
        customer_demographics.cd_demo_sk
    FROM prejoined_customer
    INNER JOIN customer_demographics 
        ON prejoined_customer.c_current_cdemo_sk = customer_demographics.cd_demo_sk
    INNER JOIN store_returns 
        ON store_returns.sr_cdemo_sk = customer_demographics.cd_demo_sk
)
SELECT
    c_customer_id AS customer_id,
    COALESCE(c_last_name, '') || ', ' || COALESCE(c_first_name, '') AS customername
FROM prejoined_fact
GROUP BY 1, 2, c_last_name, c_first_name
ORDER BY c_customer_id
LIMIT 100;