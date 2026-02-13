WITH prejoined_dims AS (SELECT
    c.c_customer_id,
    c.c_first_name,
    c.c_last_name,
    cd.cd_demo_sk
FROM customer c
INNER JOIN customer_address ca ON c.c_current_addr_sk = ca.ca_address_sk
INNER JOIN household_demographics hd ON c.c_current_hdemo_sk = hd.hd_demo_sk
INNER JOIN income_band ib ON hd.hd_income_band_sk = ib.ib_income_band_sk
INNER JOIN customer_demographics cd ON c.c_current_cdemo_sk = cd.cd_demo_sk
WHERE ca.ca_city = 'Jackson'
    AND ib.ib_lower_bound >= 23567
    AND ib.ib_upper_bound <= 73567), store_returns_join AS (SELECT
    pd.c_customer_id,
    COALESCE(pd.c_last_name, '') || ', ' || COALESCE(pd.c_first_name, '') AS customername
FROM prejoined_dims pd
INNER JOIN store_returns sr ON sr.sr_cdemo_sk = pd.cd_demo_sk) SELECT
    c_customer_id AS customer_id,
    customername
FROM store_returns_join
ORDER BY c_customer_id
LIMIT 100