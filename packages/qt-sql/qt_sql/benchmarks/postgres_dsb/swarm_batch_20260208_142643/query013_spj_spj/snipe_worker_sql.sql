WITH filtered_date AS (
    SELECT d_date_sk
    FROM date_dim
    WHERE d_year = 2001
),
filtered_store AS (
    SELECT s_store_sk
    FROM store
),
filtered_demographics AS (
    SELECT cd_demo_sk, hd_demo_sk
    FROM customer_demographics
    CROSS JOIN household_demographics
    WHERE (
        (cd_marital_status = 'U' AND cd_education_status = 'College' AND hd_dep_count = 3)
        OR (cd_marital_status = 'W' AND cd_education_status = 'Secondary' AND hd_dep_count = 1)
        OR (cd_marital_status = 'D' AND cd_education_status = 'Secondary' AND hd_dep_count = 1)
    )
),
filtered_address AS (
    SELECT ca_address_sk
    FROM customer_address
    WHERE ca_country = 'United States'
      AND (
          (ca_state IN ('IA', 'MO', 'TX'))
          OR (ca_state IN ('GA', 'LA', 'SD'))
          OR (ca_state IN ('TN', 'TX', 'VA'))
      )
),
filtered_sales AS (
    SELECT 
        ss_quantity,
        ss_ext_sales_price,
        ss_ext_wholesale_cost,
        ss_net_profit,
        ss_sales_price,
        ss_store_sk,
        ss_sold_date_sk,
        ss_hdemo_sk,
        ss_cdemo_sk,
        ss_addr_sk
    FROM store_sales
    WHERE (
        (ss_sales_price BETWEEN 100.00 AND 150.00)
        OR (ss_sales_price BETWEEN 50.00 AND 100.00)
        OR (ss_sales_price BETWEEN 150.00 AND 200.00)
    )
    AND (
        (ss_net_profit BETWEEN 100 AND 200)
        OR (ss_net_profit BETWEEN 150 AND 300)
        OR (ss_net_profit BETWEEN 50 AND 250)
    )
)
SELECT
    MIN(ss_quantity),
    MIN(ss_ext_sales_price),
    MIN(ss_ext_wholesale_cost),
    MIN(ss_ext_wholesale_cost)
FROM filtered_sales s
JOIN filtered_store st ON s.ss_store_sk = st.s_store_sk
JOIN filtered_date d ON s.ss_sold_date_sk = d.d_date_sk
JOIN filtered_demographics dem ON s.ss_hdemo_sk = dem.hd_demo_sk 
    AND s.ss_cdemo_sk = dem.cd_demo_sk
JOIN filtered_address addr ON s.ss_addr_sk = addr.ca_address_sk