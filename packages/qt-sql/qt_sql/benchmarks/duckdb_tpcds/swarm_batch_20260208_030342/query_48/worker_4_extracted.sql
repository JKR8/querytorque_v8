WITH filtered_dates AS (
    SELECT d_date_sk
    FROM date_dim
    WHERE d_year = 1999
),
store_sales_filtered AS (
    SELECT 
        ss_quantity,
        ss_cdemo_sk,
        ss_addr_sk,
        ss_sales_price,
        ss_net_profit,
        ss_store_sk
    FROM store_sales
    JOIN filtered_dates ON ss_sold_date_sk = d_date_sk
),
-- Branch 1: First demographic condition with all three address conditions
branch1 AS (
    SELECT ss_quantity
    FROM store_sales_filtered
    JOIN store ON s_store_sk = ss_store_sk
    JOIN customer_demographics ON cd_demo_sk = ss_cdemo_sk
    JOIN customer_address ON ss_addr_sk = ca_address_sk
    WHERE cd_marital_status = 'U'
        AND cd_education_status = 'Primary'
        AND ss_sales_price BETWEEN 100.00 AND 150.00
        AND ca_country = 'United States'
        AND (
            (ca_state IN ('MD', 'MN', 'IA') AND ss_net_profit BETWEEN 0 AND 2000)
            OR (ca_state IN ('VA', 'IL', 'TX') AND ss_net_profit BETWEEN 150 AND 3000)
            OR (ca_state IN ('MI', 'WI', 'IN') AND ss_net_profit BETWEEN 50 AND 25000)
        )
),
-- Branch 2: Second demographic condition with all three address conditions
branch2 AS (
    SELECT ss_quantity
    FROM store_sales_filtered
    JOIN store ON s_store_sk = ss_store_sk
    JOIN customer_demographics ON cd_demo_sk = ss_cdemo_sk
    JOIN customer_address ON ss_addr_sk = ca_address_sk
    WHERE cd_marital_status = 'W'
        AND cd_education_status = 'College'
        AND ss_sales_price BETWEEN 50.00 AND 100.00
        AND ca_country = 'United States'
        AND (
            (ca_state IN ('MD', 'MN', 'IA') AND ss_net_profit BETWEEN 0 AND 2000)
            OR (ca_state IN ('VA', 'IL', 'TX') AND ss_net_profit BETWEEN 150 AND 3000)
            OR (ca_state IN ('MI', 'WI', 'IN') AND ss_net_profit BETWEEN 50 AND 25000)
        )
),
-- Branch 3: Third demographic condition with all three address conditions
branch3 AS (
    SELECT ss_quantity
    FROM store_sales_filtered
    JOIN store ON s_store_sk = ss_store_sk
    JOIN customer_demographics ON cd_demo_sk = ss_cdemo_sk
    JOIN customer_address ON ss_addr_sk = ca_address_sk
    WHERE cd_marital_status = 'D'
        AND cd_education_status = '2 yr Degree'
        AND ss_sales_price BETWEEN 150.00 AND 200.00
        AND ca_country = 'United States'
        AND (
            (ca_state IN ('MD', 'MN', 'IA') AND ss_net_profit BETWEEN 0 AND 2000)
            OR (ca_state IN ('VA', 'IL', 'TX') AND ss_net_profit BETWEEN 150 AND 3000)
            OR (ca_state IN ('MI', 'WI', 'IN') AND ss_net_profit BETWEEN 50 AND 25000)
        )
)
SELECT SUM(ss_quantity) AS "SUM(ss_quantity)"
FROM (
    SELECT ss_quantity FROM branch1
    UNION ALL
    SELECT ss_quantity FROM branch2
    UNION ALL
    SELECT ss_quantity FROM branch3
) combined;