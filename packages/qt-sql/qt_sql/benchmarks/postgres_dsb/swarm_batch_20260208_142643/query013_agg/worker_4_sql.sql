WITH filtered_date AS (
    SELECT d_date_sk
    FROM date_dim
    WHERE d_year = 2001
),
demographic_scenarios AS (
    -- Scenario 1
    SELECT 
        ss.ss_item_sk,
        ss.ss_quantity,
        ss.ss_ext_sales_price,
        ss.ss_ext_wholesale_cost,
        ss.ss_sales_price,
        ss.ss_net_profit,
        ss.ss_hdemo_sk,
        ss.ss_cdemo_sk,
        ss.ss_addr_sk,
        1 as scenario_id
    FROM store_sales ss
    JOIN filtered_date d ON ss.ss_sold_date_sk = d.d_date_sk
    JOIN store s ON s.s_store_sk = ss.ss_store_sk
    JOIN customer_demographics cd ON cd.cd_demo_sk = ss.ss_cdemo_sk
    JOIN household_demographics hd ON hd.hd_demo_sk = ss.ss_hdemo_sk
    WHERE cd.cd_marital_status = 'U'
        AND cd.cd_education_status = 'College'
        AND ss.ss_sales_price BETWEEN 100.00 AND 150.00
        AND hd.hd_dep_count = 3
    
    UNION ALL
    
    -- Scenario 2
    SELECT 
        ss.ss_item_sk,
        ss.ss_quantity,
        ss.ss_ext_sales_price,
        ss.ss_ext_wholesale_cost,
        ss.ss_sales_price,
        ss.ss_net_profit,
        ss.ss_hdemo_sk,
        ss.ss_cdemo_sk,
        ss.ss_addr_sk,
        2 as scenario_id
    FROM store_sales ss
    JOIN filtered_date d ON ss.ss_sold_date_sk = d.d_date_sk
    JOIN store s ON s.s_store_sk = ss.ss_store_sk
    JOIN customer_demographics cd ON cd.cd_demo_sk = ss.ss_cdemo_sk
    JOIN household_demographics hd ON hd.hd_demo_sk = ss.ss_hdemo_sk
    WHERE cd.cd_marital_status = 'W'
        AND cd.cd_education_status = 'Secondary'
        AND ss.ss_sales_price BETWEEN 50.00 AND 100.00
        AND hd.hd_dep_count = 1
    
    UNION ALL
    
    -- Scenario 3
    SELECT 
        ss.ss_item_sk,
        ss.ss_quantity,
        ss.ss_ext_sales_price,
        ss.ss_ext_wholesale_cost,
        ss.ss_sales_price,
        ss.ss_net_profit,
        ss.ss_hdemo_sk,
        ss.ss_cdemo_sk,
        ss.ss_addr_sk,
        3 as scenario_id
    FROM store_sales ss
    JOIN filtered_date d ON ss.ss_sold_date_sk = d.d_date_sk
    JOIN store s ON s.s_store_sk = ss.ss_store_sk
    JOIN customer_demographics cd ON cd.cd_demo_sk = ss.ss_cdemo_sk
    JOIN household_demographics hd ON hd.hd_demo_sk = ss.ss_hdemo_sk
    WHERE cd.cd_marital_status = 'D'
        AND cd.cd_education_status = 'Secondary'
        AND ss.ss_sales_price BETWEEN 150.00 AND 200.00
        AND hd.hd_dep_count = 1
),
address_filtered AS (
    SELECT ds.*
    FROM demographic_scenarios ds
    JOIN customer_address ca ON ca.ca_address_sk = ds.ss_addr_sk
    WHERE ca.ca_country = 'United States'
        AND (
            (ds.scenario_id = 1 AND ca.ca_state IN ('IA', 'MO', 'TX') AND ds.ss_net_profit BETWEEN 100 AND 200)
            OR (ds.scenario_id = 2 AND ca.ca_state IN ('GA', 'LA', 'SD') AND ds.ss_net_profit BETWEEN 150 AND 300)
            OR (ds.scenario_id = 3 AND ca.ca_state IN ('TN', 'TX', 'VA') AND ds.ss_net_profit BETWEEN 50 AND 250)
        )
)
SELECT
    AVG(ss_quantity),
    AVG(ss_ext_sales_price),
    AVG(ss_ext_wholesale_cost),
    SUM(ss_ext_wholesale_cost)
FROM address_filtered;