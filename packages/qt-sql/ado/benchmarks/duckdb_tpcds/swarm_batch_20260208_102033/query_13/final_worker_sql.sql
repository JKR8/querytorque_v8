WITH filtered_date AS (
    SELECT d_date_sk
    FROM date_dim 
    WHERE d_year = 2001
),
filtered_store AS (
    SELECT s_store_sk
    FROM store
),
demo_conditions AS (
    SELECT 
        cd_demo_sk,
        hd_demo_sk,
        CASE 
            WHEN cd_marital_status = 'D' AND cd_education_status = 'Unknown' AND hd_dep_count = 3 THEN 1
            WHEN cd_marital_status = 'S' AND cd_education_status = 'College' AND hd_dep_count = 1 THEN 2
            WHEN cd_marital_status = 'M' AND cd_education_status = '4 yr Degree' AND hd_dep_count = 1 THEN 3
            ELSE 0
        END AS demo_flag
    FROM customer_demographics
    JOIN household_demographics
    WHERE cd_marital_status IN ('D', 'S', 'M')
      AND hd_dep_count IN (1, 3)
),
addr_conditions AS (
    SELECT 
        ca_address_sk,
        CASE 
            WHEN ca_state IN ('SD', 'KS', 'MI') THEN 1
            WHEN ca_state IN ('MO', 'ND', 'CO') THEN 2
            WHEN ca_state IN ('NH', 'OH', 'TX') THEN 3
            ELSE 0
        END AS addr_flag
    FROM customer_address
    WHERE ca_country = 'United States'
      AND ca_state IN ('SD', 'KS', 'MI', 'MO', 'ND', 'CO', 'NH', 'OH', 'TX')
)
SELECT
    AVG(ss_quantity),
    AVG(ss_ext_sales_price),
    AVG(ss_ext_wholesale_cost),
    SUM(ss_ext_wholesale_cost)
FROM store_sales ss
JOIN filtered_date d ON ss.ss_sold_date_sk = d.d_date_sk
JOIN filtered_store s ON ss.ss_store_sk = s.s_store_sk
WHERE EXISTS (
    SELECT 1 
    FROM demo_conditions dc 
    WHERE dc.cd_demo_sk = ss.ss_cdemo_sk 
      AND dc.hd_demo_sk = ss.ss_hdemo_sk
      AND dc.demo_flag > 0
      AND (
          (dc.demo_flag = 1 AND ss.ss_sales_price BETWEEN 100.00 AND 150.00)
          OR (dc.demo_flag = 2 AND ss.ss_sales_price BETWEEN 50.00 AND 100.00)
          OR (dc.demo_flag = 3 AND ss.ss_sales_price BETWEEN 150.00 AND 200.00)
      )
)
AND EXISTS (
    SELECT 1 
    FROM addr_conditions ac 
    WHERE ac.ca_address_sk = ss.ss_addr_sk
      AND ac.addr_flag > 0
      AND (
          (ac.addr_flag = 1 AND ss.ss_net_profit BETWEEN 100 AND 200)
          OR (ac.addr_flag = 2 AND ss.ss_net_profit BETWEEN 150 AND 300)
          OR (ac.addr_flag = 3 AND ss.ss_net_profit BETWEEN 50 AND 250)
      )
);