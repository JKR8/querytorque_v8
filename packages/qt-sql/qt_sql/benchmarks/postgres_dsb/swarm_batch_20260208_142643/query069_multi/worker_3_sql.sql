WITH filtered_date AS (
    SELECT d_date_sk
    FROM date_dim
    WHERE d_year = 2002
      AND d_moy BETWEEN 3 AND 3 + 2
),
filtered_store_sales AS (
    SELECT ss_customer_sk
    FROM store_sales
    INNER JOIN filtered_date ON store_sales.ss_sold_date_sk = filtered_date.d_date_sk
    WHERE ss_list_price BETWEEN 100 AND 189
),
filtered_web_sales AS (
    SELECT ws_bill_customer_sk
    FROM web_sales
    INNER JOIN filtered_date ON web_sales.ws_sold_date_sk = filtered_date.d_date_sk
    WHERE ws_list_price BETWEEN 100 AND 189
),
filtered_catalog_sales AS (
    SELECT cs_ship_customer_sk
    FROM catalog_sales
    INNER JOIN filtered_date ON catalog_sales.cs_sold_date_sk = filtered_date.d_date_sk
    WHERE cs_list_price BETWEEN 100 AND 189
),
filtered_customer_address AS (
    SELECT ca_address_sk
    FROM customer_address
    WHERE ca_state IN ('IA', 'MO', 'TX')
),
filtered_customer_demographics AS (
    SELECT cd_demo_sk, cd_gender, cd_marital_status, cd_education_status, cd_purchase_estimate, cd_credit_rating
    FROM customer_demographics
    WHERE cd_marital_status IN ('S', 'S', 'S')
      AND cd_education_status IN ('Primary', 'Secondary')
)
SELECT
    cd.cd_gender,
    cd.cd_marital_status,
    cd.cd_education_status,
    COUNT(*) AS cnt1,
    cd.cd_purchase_estimate,
    COUNT(*) AS cnt2,
    cd.cd_credit_rating,
    COUNT(*) AS cnt3
FROM customer c
INNER JOIN filtered_customer_address ca ON c.c_current_addr_sk = ca.ca_address_sk
INNER JOIN filtered_customer_demographics cd ON cd.cd_demo_sk = c.c_current_cdemo_sk
WHERE EXISTS (
    SELECT 1 FROM filtered_store_sales ss WHERE ss.ss_customer_sk = c.c_customer_sk
)
AND NOT EXISTS (
    SELECT 1 FROM filtered_web_sales ws WHERE ws.ws_bill_customer_sk = c.c_customer_sk
)
AND NOT EXISTS (
    SELECT 1 FROM filtered_catalog_sales cs WHERE cs.cs_ship_customer_sk = c.c_customer_sk
)
GROUP BY
    cd.cd_gender,
    cd.cd_marital_status,
    cd.cd_education_status,
    cd.cd_purchase_estimate,
    cd.cd_credit_rating
ORDER BY
    cd.cd_gender,
    cd.cd_marital_status,
    cd.cd_education_status,
    cd.cd_purchase_estimate,
    cd.cd_credit_rating
LIMIT 100;