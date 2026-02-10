-- start query 13 in stream 0 using template query13.tpl
SELECT 
    avg(ss_quantity) AS avg_ss_quantity,
    avg(ss_ext_sales_price) AS avg_ss_ext_sales_price,
    avg(ss_ext_wholesale_cost) AS avg_ss_ext_wholesale_cost,
    sum(ss_ext_wholesale_cost) AS sum_ss_ext_wholesale_cost
FROM (
    -- Branch 1: First set of demographic and address conditions
    SELECT ss_quantity, ss_ext_sales_price, ss_ext_wholesale_cost
    FROM store_sales
    JOIN store ON s_store_sk = ss_store_sk
    JOIN customer_demographics ON cd_demo_sk = ss_cdemo_sk
    JOIN household_demographics ON hd_demo_sk = ss_hdemo_sk
    JOIN customer_address ON ca_address_sk = ss_addr_sk
    JOIN date_dim ON d_date_sk = ss_sold_date_sk
    WHERE d_year = 2001
      AND cd_marital_status = 'D'
      AND cd_education_status = 'Unknown'
      AND ss_sales_price BETWEEN 100.00 AND 150.00
      AND hd_dep_count = 3
      AND ca_country = 'United States'
      AND ca_state IN ('SD', 'KS', 'MI')
      AND ss_net_profit BETWEEN 100 AND 200
    
    UNION ALL
    
    -- Branch 2: Second set of demographic and address conditions
    SELECT ss_quantity, ss_ext_sales_price, ss_ext_wholesale_cost
    FROM store_sales
    JOIN store ON s_store_sk = ss_store_sk
    JOIN customer_demographics ON cd_demo_sk = ss_cdemo_sk
    JOIN household_demographics ON hd_demo_sk = ss_hdemo_sk
    JOIN customer_address ON ca_address_sk = ss_addr_sk
    JOIN date_dim ON d_date_sk = ss_sold_date_sk
    WHERE d_year = 2001
      AND cd_marital_status = 'S'
      AND cd_education_status = 'College'
      AND ss_sales_price BETWEEN 50.00 AND 100.00
      AND hd_dep_count = 1
      AND ca_country = 'United States'
      AND ca_state IN ('MO', 'ND', 'CO')
      AND ss_net_profit BETWEEN 150 AND 300
    
    UNION ALL
    
    -- Branch 3: Third set of demographic and address conditions
    SELECT ss_quantity, ss_ext_sales_price, ss_ext_wholesale_cost
    FROM store_sales
    JOIN store ON s_store_sk = ss_store_sk
    JOIN customer_demographics ON cd_demo_sk = ss_cdemo_sk
    JOIN household_demographics ON hd_demo_sk = ss_hdemo_sk
    JOIN customer_address ON ca_address_sk = ss_addr_sk
    JOIN date_dim ON d_date_sk = ss_sold_date_sk
    WHERE d_year = 2001
      AND cd_marital_status = 'M'
      AND cd_education_status = '4 yr Degree'
      AND ss_sales_price BETWEEN 150.00 AND 200.00
      AND hd_dep_count = 1
      AND ca_country = 'United States'
      AND ca_state IN ('NH', 'OH', 'TX')
      AND ss_net_profit BETWEEN 50 AND 250
) AS combined_results;

-- end query 13 in stream 0 using template query13.tpl