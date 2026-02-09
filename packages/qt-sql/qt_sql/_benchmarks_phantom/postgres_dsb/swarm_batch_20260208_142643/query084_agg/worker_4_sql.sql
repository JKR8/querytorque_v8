WITH filtered_ca AS (
    SELECT ca_address_sk
    FROM customer_address
    WHERE ca_city = 'Lakeview'
),
filtered_ib AS (
    SELECT ib_income_band_sk
    FROM income_band
    WHERE ib_lower_bound >= 69452
      AND ib_upper_bound <= 69452 + 50000
),
customer_base AS (
    SELECT 
        c_customer_id,
        c_first_name,
        c_last_name,
        c_current_cdemo_sk,
        c_current_hdemo_sk
    FROM customer
    WHERE EXISTS (
        SELECT 1
        FROM filtered_ca
        WHERE filtered_ca.ca_address_sk = customer.c_current_addr_sk
    )
)
SELECT
    c.c_customer_id AS customer_id,
    COALESCE(c.c_last_name, '') || ', ' || COALESCE(c.c_first_name, '') AS customername
FROM customer_base c
JOIN LATERAL (
    SELECT 1
    FROM customer_demographics cd
    JOIN household_demographics hd ON hd.hd_demo_sk = c.c_current_hdemo_sk
    JOIN filtered_ib ON filtered_ib.ib_income_band_sk = hd.hd_income_band_sk
    WHERE cd.cd_demo_sk = c.c_current_cdemo_sk
      AND EXISTS (
          SELECT 1
          FROM store_returns sr
          WHERE sr.sr_cdemo_sk = cd.cd_demo_sk
      )
) AS valid ON true
ORDER BY c.c_customer_id
LIMIT 100;