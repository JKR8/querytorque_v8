WITH filtered_date AS (
    SELECT d_date_sk
    FROM date_dim
    WHERE d_year = 1999
),
-- Branch 1: Demo condition 1 with Address condition 1
branch1 AS (
    SELECT ss_quantity
    FROM store_sales
    JOIN store ON s_store_sk = ss_store_sk
    JOIN filtered_date ON ss_sold_date_sk = d_date_sk
    JOIN customer_demographics ON cd_demo_sk = ss_cdemo_sk
    JOIN customer_address ON ss_addr_sk = ca_address_sk
    WHERE cd_marital_status = 'U'
      AND cd_education_status = 'Primary'
      AND ss_sales_price BETWEEN 100.00 AND 150.00
      AND ca_country = 'United States'
      AND ca_state IN ('MD', 'MN', 'IA')
      AND ss_net_profit BETWEEN 0 AND 2000
),
-- Branch 2: Demo condition 1 with Address condition 2
branch2 AS (
    SELECT ss_quantity
    FROM store_sales
    JOIN store ON s_store_sk = ss_store_sk
    JOIN filtered_date ON ss_sold_date_sk = d_date_sk
    JOIN customer_demographics ON cd_demo_sk = ss_cdemo_sk
    JOIN customer_address ON ss_addr_sk = ca_address_sk
    WHERE cd_marital_status = 'U'
      AND cd_education_status = 'Primary'
      AND ss_sales_price BETWEEN 100.00 AND 150.00
      AND ca_country = 'United States'
      AND ca_state IN ('VA', 'IL', 'TX')
      AND ss_net_profit BETWEEN 150 AND 3000
),
-- Branch 3: Demo condition 1 with Address condition 3
branch3 AS (
    SELECT ss_quantity
    FROM store_sales
    JOIN store ON s_store_sk = ss_store_sk
    JOIN filtered_date ON ss_sold_date_sk = d_date_sk
    JOIN customer_demographics ON cd_demo_sk = ss_cdemo_sk
    JOIN customer_address ON ss_addr_sk = ca_address_sk
    WHERE cd_marital_status = 'U'
      AND cd_education_status = 'Primary'
      AND ss_sales_price BETWEEN 100.00 AND 150.00
      AND ca_country = 'United States'
      AND ca_state IN ('MI', 'WI', 'IN')
      AND ss_net_profit BETWEEN 50 AND 25000
),
-- Branch 4: Demo condition 2 with Address condition 1
branch4 AS (
    SELECT ss_quantity
    FROM store_sales
    JOIN store ON s_store_sk = ss_store_sk
    JOIN filtered_date ON ss_sold_date_sk = d_date_sk
    JOIN customer_demographics ON cd_demo_sk = ss_cdemo_sk
    JOIN customer_address ON ss_addr_sk = ca_address_sk
    WHERE cd_marital_status = 'W'
      AND cd_education_status = 'College'
      AND ss_sales_price BETWEEN 50.00 AND 100.00
      AND ca_country = 'United States'
      AND ca_state IN ('MD', 'MN', 'IA')
      AND ss_net_profit BETWEEN 0 AND 2000
),
-- Branch 5: Demo condition 2 with Address condition 2
branch5 AS (
    SELECT ss_quantity
    FROM store_sales
    JOIN store ON s_store_sk = ss_store_sk
    JOIN filtered_date ON ss_sold_date_sk = d_date_sk
    JOIN customer_demographics ON cd_demo_sk = ss_cdemo_sk
    JOIN customer_address ON ss_addr_sk = ca_address_sk
    WHERE cd_marital_status = 'W'
      AND cd_education_status = 'College'
      AND ss_sales_price BETWEEN 50.00 AND 100.00
      AND ca_country = 'United States'
      AND ca_state IN ('VA', 'IL', 'TX')
      AND ss_net_profit BETWEEN 150 AND 3000
),
-- Branch 6: Demo condition 2 with Address condition 3
branch6 AS (
    SELECT ss_quantity
    FROM store_sales
    JOIN store ON s_store_sk = ss_store_sk
    JOIN filtered_date ON ss_sold_date_sk = d_date_sk
    JOIN customer_demographics ON cd_demo_sk = ss_cdemo_sk
    JOIN customer_address ON ss_addr_sk = ca_address_sk
    WHERE cd_marital_status = 'W'
      AND cd_education_status = 'College'
      AND ss_sales_price BETWEEN 50.00 AND 100.00
      AND ca_country = 'United States'
      AND ca_state IN ('MI', 'WI', 'IN')
      AND ss_net_profit BETWEEN 50 AND 25000
),
-- Branch 7: Demo condition 3 with Address condition 1
branch7 AS (
    SELECT ss_quantity
    FROM store_sales
    JOIN store ON s_store_sk = ss_store_sk
    JOIN filtered_date ON ss_sold_date_sk = d_date_sk
    JOIN customer_demographics ON cd_demo_sk = ss_cdemo_sk
    JOIN customer_address ON ss_addr_sk = ca_address_sk
    WHERE cd_marital_status = 'D'
      AND cd_education_status = '2 yr Degree'
      AND ss_sales_price BETWEEN 150.00 AND 200.00
      AND ca_country = 'United States'
      AND ca_state IN ('MD', 'MN', 'IA')
      AND ss_net_profit BETWEEN 0 AND 2000
),
-- Branch 8: Demo condition 3 with Address condition 2
branch8 AS (
    SELECT ss_quantity
    FROM store_sales
    JOIN store ON s_store_sk = ss_store_sk
    JOIN filtered_date ON ss_sold_date_sk = d_date_sk
    JOIN customer_demographics ON cd_demo_sk = ss_cdemo_sk
    JOIN customer_address ON ss_addr_sk = ca_address_sk
    WHERE cd_marital_status = 'D'
      AND cd_education_status = '2 yr Degree'
      AND ss_sales_price BETWEEN 150.00 AND 200.00
      AND ca_country = 'United States'
      AND ca_state IN ('VA', 'IL', 'TX')
      AND ss_net_profit BETWEEN 150 AND 3000
),
-- Branch 9: Demo condition 3 with Address condition 3
branch9 AS (
    SELECT ss_quantity
    FROM store_sales
    JOIN store ON s_store_sk = ss_store_sk
    JOIN filtered_date ON ss_sold_date_sk = d_date_sk
    JOIN customer_demographics ON cd_demo_sk = ss_cdemo_sk
    JOIN customer_address ON ss_addr_sk = ca_address_sk
    WHERE cd_marital_status = 'D'
      AND cd_education_status = '2 yr Degree'
      AND ss_sales_price BETWEEN 150.00 AND 200.00
      AND ca_country = 'United States'
      AND ca_state IN ('MI', 'WI', 'IN')
      AND ss_net_profit BETWEEN 50 AND 25000
),
all_branches AS (
    SELECT ss_quantity FROM branch1
    UNION ALL
    SELECT ss_quantity FROM branch2
    UNION ALL
    SELECT ss_quantity FROM branch3
    UNION ALL
    SELECT ss_quantity FROM branch4
    UNION ALL
    SELECT ss_quantity FROM branch5
    UNION ALL
    SELECT ss_quantity FROM branch6
    UNION ALL
    SELECT ss_quantity FROM branch7
    UNION ALL
    SELECT ss_quantity FROM branch8
    UNION ALL
    SELECT ss_quantity FROM branch9
)
SELECT SUM(ss_quantity)
FROM all_branches;