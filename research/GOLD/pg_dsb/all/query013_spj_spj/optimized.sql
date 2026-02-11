WITH filtered_date AS (
    SELECT d_date_sk
    FROM date_dim
    WHERE d_year = 2001
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
),
filtered_customer_address AS (
    SELECT ca_address_sk, ca_state
    FROM customer_address
    WHERE ca_country = 'United States'
      AND ca_state IN ('IA', 'MO', 'TX', 'GA', 'LA', 'SD', 'TN', 'TX', 'VA')
),
filtered_store_sales AS (
    SELECT
        ss_quantity,
        ss_ext_sales_price,
        ss_ext_wholesale_cost,
        ss_sales_price,
        ss_net_profit,
        ss_store_sk,
        ss_sold_date_sk,
        ss_hdemo_sk,
        ss_cdemo_sk,
        ss_addr_sk
    FROM store_sales
    WHERE (ss_sales_price BETWEEN 100.00 AND 150.00)
       OR (ss_sales_price BETWEEN 50.00 AND 100.00)
       OR (ss_sales_price BETWEEN 150.00 AND 200.00)
)
SELECT
    MIN(ss_quantity),
    MIN(ss_ext_sales_price),
    MIN(ss_ext_wholesale_cost),
    MIN(ss_ext_wholesale_cost)
FROM filtered_store_sales ss
JOIN store s ON s.s_store_sk = ss.ss_store_sk
JOIN filtered_date d ON ss.ss_sold_date_sk = d.d_date_sk
JOIN filtered_customer_demographics cd ON ss.ss_cdemo_sk = cd.cd_demo_sk
JOIN filtered_household_demographics hd ON ss.ss_hdemo_sk = hd.hd_demo_sk
JOIN filtered_customer_address ca ON ss.ss_addr_sk = ca.ca_address_sk
WHERE (
    (cd.cd_marital_status = 'U' AND cd.cd_education_status = 'College' 
     AND ss.ss_sales_price BETWEEN 100.00 AND 150.00 AND hd.hd_dep_count = 3)
    OR (cd.cd_marital_status = 'W' AND cd.cd_education_status = 'Secondary' 
        AND ss.ss_sales_price BETWEEN 50.00 AND 100.00 AND hd.hd_dep_count = 1)
    OR (cd.cd_marital_status = 'D' AND cd.cd_education_status = 'Secondary' 
        AND ss.ss_sales_price BETWEEN 150.00 AND 200.00 AND hd.hd_dep_count = 1)
)
AND (
    (ca.ca_state IN ('IA', 'MO', 'TX') AND ss.ss_net_profit BETWEEN 100 AND 200)
    OR (ca.ca_state IN ('GA', 'LA', 'SD') AND ss.ss_net_profit BETWEEN 150 AND 300)
    OR (ca.ca_state IN ('TN', 'TX', 'VA') AND ss.ss_net_profit BETWEEN 50 AND 250)
);
