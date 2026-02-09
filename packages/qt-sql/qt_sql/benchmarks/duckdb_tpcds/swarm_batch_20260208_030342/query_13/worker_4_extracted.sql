WITH filtered_dates AS (
    SELECT d_date_sk 
    FROM date_dim 
    WHERE d_year = 2001
),
-- First OR group (demographics)
demographics_filtered AS (
    SELECT ss.ss_item_sk, ss.ss_quantity, ss.ss_ext_sales_price, 
           ss.ss_ext_wholesale_cost, ss.ss_addr_sk, ss.ss_store_sk
    FROM store_sales ss
    JOIN filtered_dates ON ss.ss_sold_date_sk = filtered_dates.d_date_sk
    JOIN customer_demographics cd ON ss.ss_cdemo_sk = cd.cd_demo_sk
    JOIN household_demographics hd ON ss.ss_hdemo_sk = hd.hd_demo_sk
    WHERE (
        (cd.cd_marital_status = 'D' 
         AND cd.cd_education_status = 'Unknown' 
         AND ss.ss_sales_price BETWEEN 100.00 AND 150.00 
         AND hd.hd_dep_count = 3)
        OR
        (cd.cd_marital_status = 'S' 
         AND cd.cd_education_status = 'College' 
         AND ss.ss_sales_price BETWEEN 50.00 AND 100.00 
         AND hd.hd_dep_count = 1)
        OR
        (cd.cd_marital_status = 'M' 
         AND cd.cd_education_status = '4 yr Degree' 
         AND ss.ss_sales_price BETWEEN 150.00 AND 200.00 
         AND hd.hd_dep_count = 1)
    )
),
-- Second OR group (address and net profit) split into UNION ALL branches
address_branch_1 AS (
    SELECT ss.ss_item_sk, ss.ss_quantity, ss.ss_ext_sales_price,
           ss.ss_ext_wholesale_cost, ss.ss_store_sk
    FROM store_sales ss
    JOIN filtered_dates ON ss.ss_sold_date_sk = filtered_dates.d_date_sk
    JOIN customer_address ca ON ss.ss_addr_sk = ca.ca_address_sk
    WHERE ca.ca_country = 'United States'
      AND ca.ca_state IN ('SD', 'KS', 'MI')
      AND ss.ss_net_profit BETWEEN 100 AND 200
),
address_branch_2 AS (
    SELECT ss.ss_item_sk, ss.ss_quantity, ss.ss_ext_sales_price,
           ss.ss_ext_wholesale_cost, ss.ss_store_sk
    FROM store_sales ss
    JOIN filtered_dates ON ss.ss_sold_date_sk = filtered_dates.d_date_sk
    JOIN customer_address ca ON ss.ss_addr_sk = ca.ca_address_sk
    WHERE ca.ca_country = 'United States'
      AND ca.ca_state IN ('MO', 'ND', 'CO')
      AND ss.ss_net_profit BETWEEN 150 AND 300
),
address_branch_3 AS (
    SELECT ss.ss_item_sk, ss.ss_quantity, ss.ss_ext_sales_price,
           ss.ss_ext_wholesale_cost, ss.ss_store_sk
    FROM store_sales ss
    JOIN filtered_dates ON ss.ss_sold_date_sk = filtered_dates.d_date_sk
    JOIN customer_address ca ON ss.ss_addr_sk = ca.ca_address_sk
    WHERE ca.ca_country = 'United States'
      AND ca.ca_state IN ('NH', 'OH', 'TX')
      AND ss.ss_net_profit BETWEEN 50 AND 250
),
-- Combine address branches
address_filtered AS (
    SELECT * FROM address_branch_1
    UNION ALL
    SELECT * FROM address_branch_2
    UNION ALL
    SELECT * FROM address_branch_3
),
-- Join demographics and address filters
filtered_sales AS (
    SELECT DISTINCT 
        df.ss_quantity, 
        df.ss_ext_sales_price, 
        df.ss_ext_wholesale_cost
    FROM demographics_filtered df
    JOIN address_filtered af ON df.ss_item_sk = af.ss_item_sk 
                             AND df.ss_store_sk = af.ss_store_sk
    JOIN store s ON df.ss_store_sk = s.s_store_sk
)
SELECT
    AVG(ss_quantity) AS "AVG(ss_quantity)",
    AVG(ss_ext_sales_price) AS "AVG(ss_ext_sales_price)",
    AVG(ss_ext_wholesale_cost) AS "AVG(ss_ext_wholesale_cost)",
    SUM(ss_ext_wholesale_cost) AS "SUM(ss_ext_wholesale_cost)"
FROM filtered_sales;