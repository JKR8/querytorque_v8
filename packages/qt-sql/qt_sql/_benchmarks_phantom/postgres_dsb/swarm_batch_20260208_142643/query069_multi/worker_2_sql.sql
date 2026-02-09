WITH filtered_date AS (
    SELECT d_date_sk
    FROM date_dim
    WHERE d_year = 2002
      AND d_moy BETWEEN 3 AND 5
),
filtered_address AS (
    SELECT ca_address_sk
    FROM customer_address
    WHERE ca_state IN ('IA', 'MO', 'TX')
),
filtered_demographics AS (
    SELECT cd_demo_sk, cd_gender, cd_marital_status, cd_education_status,
           cd_purchase_estimate, cd_credit_rating
    FROM customer_demographics
    WHERE cd_marital_status IN ('S', 'S', 'S')
      AND cd_education_status IN ('Primary', 'Secondary')
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
JOIN filtered_address ca ON c.c_current_addr_sk = ca.ca_address_sk
JOIN filtered_demographics ON cd_demo_sk = c.c_current_cdemo_sk
WHERE EXISTS (
    SELECT 1
    FROM store_sales
    JOIN filtered_date ON store_sales.ss_sold_date_sk = filtered_date.d_date_sk
    WHERE c.c_customer_sk = store_sales.ss_customer_sk
      AND store_sales.ss_list_price BETWEEN 100 AND 189
)
AND NOT EXISTS (
    SELECT 1
    FROM web_sales
    JOIN filtered_date ON web_sales.ws_sold_date_sk = filtered_date.d_date_sk
    WHERE c.c_customer_sk = web_sales.ws_bill_customer_sk
      AND web_sales.ws_list_price BETWEEN 100 AND 189
)
AND NOT EXISTS (
    SELECT 1
    FROM catalog_sales
    JOIN filtered_date ON catalog_sales.cs_sold_date_sk = filtered_date.d_date_sk
    WHERE c.c_customer_sk = catalog_sales.cs_ship_customer_sk
      AND catalog_sales.cs_list_price BETWEEN 100 AND 189
)
GROUP BY
    cd_gender,
    cd_marital_status,
    cd_education_status,
    cd_purchase_estimate,
    cd_credit_rating
ORDER BY
    cd_gender,
    cd_marital_status,
    cd_education_status,
    cd_purchase_estimate,
    cd_credit_rating
LIMIT 100;