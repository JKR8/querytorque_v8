WITH filtered_dates AS (
    SELECT d_date_sk
    FROM date_dim
    WHERE d_year = 1999
),
filtered_sales AS (
    SELECT 
        ss_quantity,
        ss_cdemo_sk,
        ss_addr_sk,
        ss_sales_price,
        ss_net_profit,
        -- Encode demographic conditions as bitmask flags
        CASE 
            WHEN cd.cd_marital_status = 'U' AND cd.cd_education_status = 'Primary' AND ss.ss_sales_price BETWEEN 100.00 AND 150.00 THEN 1
            ELSE 0
        END AS demo_flag1,
        CASE 
            WHEN cd.cd_marital_status = 'W' AND cd.cd_education_status = 'College' AND ss.ss_sales_price BETWEEN 50.00 AND 100.00 THEN 1
            ELSE 0
        END AS demo_flag2,
        CASE 
            WHEN cd.cd_marital_status = 'D' AND cd.cd_education_status = '2 yr Degree' AND ss.ss_sales_price BETWEEN 150.00 AND 200.00 THEN 1
            ELSE 0
        END AS demo_flag3,
        -- Encode address conditions as bitmask flags
        CASE 
            WHEN ca.ca_country = 'United States' AND ca.ca_state IN ('MD', 'MN', 'IA') AND ss.ss_net_profit BETWEEN 0 AND 2000 THEN 1
            ELSE 0
        END AS addr_flag1,
        CASE 
            WHEN ca.ca_country = 'United States' AND ca.ca_state IN ('VA', 'IL', 'TX') AND ss.ss_net_profit BETWEEN 150 AND 3000 THEN 1
            ELSE 0
        END AS addr_flag2,
        CASE 
            WHEN ca.ca_country = 'United States' AND ca.ca_state IN ('MI', 'WI', 'IN') AND ss.ss_net_profit BETWEEN 50 AND 25000 THEN 1
            ELSE 0
        END AS addr_flag3
    FROM store_sales ss
    JOIN filtered_dates fd ON ss.ss_sold_date_sk = fd.d_date_sk
    JOIN store s ON s.s_store_sk = ss.ss_store_sk
    LEFT JOIN customer_demographics cd ON cd.cd_demo_sk = ss.ss_cdemo_sk
    LEFT JOIN customer_address ca ON ca.ca_address_sk = ss.ss_addr_sk
)
SELECT SUM(ss_quantity)
FROM filtered_sales
WHERE (demo_flag1 + demo_flag2 + demo_flag3) > 0
  AND (addr_flag1 + addr_flag2 + addr_flag3) > 0