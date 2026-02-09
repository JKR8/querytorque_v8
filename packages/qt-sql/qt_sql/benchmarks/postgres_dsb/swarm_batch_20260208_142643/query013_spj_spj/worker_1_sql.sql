WITH filtered_date AS (
    SELECT d_date_sk
    FROM date_dim
    WHERE d_year = 2001
),
filtered_customer_address AS (
    SELECT ca_address_sk, ca_state
    FROM customer_address
    WHERE ca_country = 'United States'
      AND ca_state IN ('IA', 'MO', 'TX', 'GA', 'LA', 'SD', 'TN', 'TX', 'VA')
),
filtered_customer_demographics AS (
    SELECT cd_demo_sk, cd_marital_status, cd_education_status
    FROM customer_demographics
    WHERE (cd_marital_status = 'U' AND cd_education_status = 'College')
       OR (cd_marital_status = 'W' AND cd_education_status = 'Secondary')
       OR (cd_marital_status = 'D' AND cd_education_status = 'Secondary')
),
filtered_household_demographics AS (
    SELECT hd_demo_sk, hd_dep_count
    FROM household_demographics
    WHERE hd_dep_count IN (1, 3)
)
SELECT
    MIN(ss_quantity),
    MIN(ss_ext_sales_price),
    MIN(ss_ext_wholesale_cost),
    MIN(ss_ext_wholesale_cost)
FROM store_sales
JOIN filtered_date ON ss_sold_date_sk = filtered_date.d_date_sk
JOIN store ON s_store_sk = ss_store_sk
JOIN filtered_customer_demographics ON cd_demo_sk = ss_cdemo_sk
JOIN filtered_household_demographics ON hd_demo_sk = ss_hdemo_sk
JOIN filtered_customer_address ON ca_address_sk = ss_addr_sk
WHERE (
    (
        cd_marital_status = 'U'
        AND cd_education_status = 'College'
        AND ss_sales_price BETWEEN 100.00 AND 150.00
        AND hd_dep_count = 3
    )
    OR (
        cd_marital_status = 'W'
        AND cd_education_status = 'Secondary'
        AND ss_sales_price BETWEEN 50.00 AND 100.00
        AND hd_dep_count = 1
    )
    OR (
        cd_marital_status = 'D'
        AND cd_education_status = 'Secondary'
        AND ss_sales_price BETWEEN 150.00 AND 200.00
        AND hd_dep_count = 1
    )
)
AND (
    (
        ca_state IN ('IA', 'MO', 'TX')
        AND ss_net_profit BETWEEN 100 AND 200
    )
    OR (
        ca_state IN ('GA', 'LA', 'SD')
        AND ss_net_profit BETWEEN 150 AND 300
    )
    OR (
        ca_state IN ('TN', 'TX', 'VA')
        AND ss_net_profit BETWEEN 50 AND 250
    )
);