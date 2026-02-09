WITH 
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
household_with_income AS (
    SELECT hd_demo_sk
    FROM household_demographics hd
    JOIN filtered_income_band fib ON hd.hd_income_band_sk = fib.ib_income_band_sk
),
qualified_customers AS (
    SELECT 
        c.c_customer_id,
        c.c_last_name,
        c.c_first_name,
        c.c_current_cdemo_sk
    FROM customer c
    JOIN filtered_address fa ON c.c_current_addr_sk = fa.ca_address_sk
    JOIN household_with_income hwi ON c.c_current_hdemo_sk = hwi.hd_demo_sk
),
customer_demographic_matches AS (
    SELECT cd.cd_demo_sk
    FROM qualified_customers qc
    JOIN customer_demographics cd ON qc.c_current_cdemo_sk = cd.cd_demo_sk
)
SELECT
    qc.c_customer_id AS customer_id,
    COALESCE(qc.c_last_name, '') || ', ' || COALESCE(qc.c_first_name, '') AS customername
FROM qualified_customers qc
JOIN customer_demographic_matches cdm ON qc.c_current_cdemo_sk = cdm.cd_demo_sk
JOIN store_returns sr ON sr.sr_cdemo_sk = cdm.cd_demo_sk
ORDER BY qc.c_customer_id
LIMIT 100;