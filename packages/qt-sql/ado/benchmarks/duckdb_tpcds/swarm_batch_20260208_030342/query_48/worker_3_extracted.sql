WITH filtered_dates AS (
    SELECT d_date_sk
    FROM date_dim
    WHERE d_year = 1999
),
filtered_demographics AS (
    SELECT cd_demo_sk, cd_marital_status, cd_education_status
    FROM customer_demographics
    WHERE (cd_marital_status = 'U' AND cd_education_status = 'Primary')
       OR (cd_marital_status = 'W' AND cd_education_status = 'College')
       OR (cd_marital_status = 'D' AND cd_education_status = '2 yr Degree')
),
filtered_address AS (
    SELECT ca_address_sk, ca_state
    FROM customer_address
    WHERE ca_country = 'United States'
      AND (ca_state IN ('MD', 'MN', 'IA')
           OR ca_state IN ('VA', 'IL', 'TX')
           OR ca_state IN ('MI', 'WI', 'IN')
          )
),
prejoined_sales AS (
    SELECT 
        ss_quantity,
        ss_sales_price,
        ss_net_profit,
        cd_marital_status,
        cd_education_status,
        ca_state
    FROM store_sales
    JOIN filtered_dates ON ss_sold_date_sk = d_date_sk
    JOIN store ON s_store_sk = ss_store_sk
    JOIN filtered_demographics ON cd_demo_sk = ss_cdemo_sk
    JOIN filtered_address ON ca_address_sk = ss_addr_sk
)
SELECT SUM(ss_quantity)
FROM prejoined_sales
WHERE (
        (cd_marital_status = 'U' AND cd_education_status = 'Primary' AND ss_sales_price BETWEEN 100.00 AND 150.00)
        OR (cd_marital_status = 'W' AND cd_education_status = 'College' AND ss_sales_price BETWEEN 50.00 AND 100.00)
        OR (cd_marital_status = 'D' AND cd_education_status = '2 yr Degree' AND ss_sales_price BETWEEN 150.00 AND 200.00)
      )
      AND (
        (ca_state IN ('MD', 'MN', 'IA') AND ss_net_profit BETWEEN 0 AND 2000)
        OR (ca_state IN ('VA', 'IL', 'TX') AND ss_net_profit BETWEEN 150 AND 3000)
        OR (ca_state IN ('MI', 'WI', 'IN') AND ss_net_profit BETWEEN 50 AND 25000)
      )