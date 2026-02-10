WITH store_customers AS (
    SELECT DISTINCT ss_customer_sk
    FROM store_sales
    JOIN date_dim ON ss_sold_date_sk = d_date_sk
    WHERE d_year = 2000 
      AND d_moy BETWEEN 1 AND 3
),
web_customers AS (
    SELECT DISTINCT ws_bill_customer_sk
    FROM web_sales
    JOIN date_dim ON ws_sold_date_sk = d_date_sk
    WHERE d_year = 2000 
      AND d_moy BETWEEN 1 AND 3
),
catalog_customers AS (
    SELECT DISTINCT cs_ship_customer_sk
    FROM catalog_sales
    JOIN date_dim ON cs_sold_date_sk = d_date_sk
    WHERE d_year = 2000 
      AND d_moy BETWEEN 1 AND 3
)
SELECT 
    cd_gender,
    cd_marital_status,
    cd_education_status,
    COUNT(*) AS cnt1,
    cd_purchase_estimate,
    COUNT(*) AS cnt2,
    cd_credit_rating,
    COUNT(*) AS cnt3
FROM customer c
JOIN customer_address ca ON c.c_current_addr_sk = ca.ca_address_sk
JOIN customer_demographics cd ON cd_demo_sk = c.c_current_cdemo_sk
JOIN store_customers sc ON c.c_customer_sk = sc.ss_customer_sk
LEFT JOIN web_customers wc ON c.c_customer_sk = wc.ws_bill_customer_sk
LEFT JOIN catalog_customers cc ON c.c_customer_sk = cc.cs_ship_customer_sk
WHERE ca.ca_state IN ('TX', 'VA', 'MI')
  AND wc.ws_bill_customer_sk IS NULL
  AND cc.cs_ship_customer_sk IS NULL
GROUP BY cd_gender,
         cd_marital_status,
         cd_education_status,
         cd_purchase_estimate,
         cd_credit_rating
ORDER BY cd_gender,
         cd_marital_status,
         cd_education_status,
         cd_purchase_estimate,
         cd_credit_rating
LIMIT 100;