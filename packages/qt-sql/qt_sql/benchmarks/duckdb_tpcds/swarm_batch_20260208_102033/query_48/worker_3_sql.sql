WITH filtered_dates AS (
    SELECT d_date_sk
    FROM date_dim
    WHERE d_year = 1999
),
demographic_conditions AS (
    SELECT cd_demo_sk
    FROM customer_demographics
    WHERE (cd_marital_status = 'U' AND cd_education_status = 'Primary')
       OR (cd_marital_status = 'W' AND cd_education_status = 'College')
       OR (cd_marital_status = 'D' AND cd_education_status = '2 yr Degree')
),
address_conditions AS (
    SELECT ca_address_sk
    FROM customer_address
    WHERE ca_country = 'United States'
      AND ca_state IN ('MD', 'MN', 'IA', 'VA', 'IL', 'TX', 'MI', 'WI', 'IN')
),
filtered_sales AS (
    SELECT 
        ss_quantity,
        ss_cdemo_sk,
        ss_addr_sk,
        ss_sales_price,
        ss_net_profit
    FROM store_sales
    INNER JOIN filtered_dates ON ss_sold_date_sk = d_date_sk
    INNER JOIN store ON s_store_sk = ss_store_sk
    WHERE ss_cdemo_sk IN (SELECT cd_demo_sk FROM demographic_conditions)
      AND ss_addr_sk IN (SELECT ca_address_sk FROM address_conditions)
)
SELECT SUM(ss_quantity)
FROM filtered_sales
WHERE (
    (
        ss_sales_price BETWEEN 100.00 AND 150.00
        AND ss_cdemo_sk IN (
            SELECT cd_demo_sk 
            FROM demographic_conditions 
            WHERE cd_marital_status = 'U' 
              AND cd_education_status = 'Primary'
        )
    )
    OR (
        ss_sales_price BETWEEN 50.00 AND 100.00
        AND ss_cdemo_sk IN (
            SELECT cd_demo_sk 
            FROM demographic_conditions 
            WHERE cd_marital_status = 'W' 
              AND cd_education_status = 'College'
        )
    )
    OR (
        ss_sales_price BETWEEN 150.00 AND 200.00
        AND ss_cdemo_sk IN (
            SELECT cd_demo_sk 
            FROM demographic_conditions 
            WHERE cd_marital_status = 'D' 
              AND cd_education_status = '2 yr Degree'
        )
    )
)
AND (
    (
        ss_net_profit BETWEEN 0 AND 2000
        AND ss_addr_sk IN (
            SELECT ca_address_sk 
            FROM address_conditions 
            WHERE ca_state IN ('MD', 'MN', 'IA')
        )
    )
    OR (
        ss_net_profit BETWEEN 150 AND 3000
        AND ss_addr_sk IN (
            SELECT ca_address_sk 
            FROM address_conditions 
            WHERE ca_state IN ('VA', 'IL', 'TX')
        )
    )
    OR (
        ss_net_profit BETWEEN 50 AND 25000
        AND ss_addr_sk IN (
            SELECT ca_address_sk 
            FROM address_conditions 
            WHERE ca_state IN ('MI', 'WI', 'IN')
        )
    )
)