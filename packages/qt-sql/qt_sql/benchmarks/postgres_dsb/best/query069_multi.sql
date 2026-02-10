WITH filtered_date AS (
    SELECT d_date_sk
    FROM date_dim
    WHERE d_year = 2002
      AND d_moy BETWEEN 3 AND 3 + 2
),
filtered_customer_address AS (
    SELECT ca_address_sk
    FROM customer_address
    WHERE ca_state IN ('IA', 'MO', 'TX')
),
filtered_customer_demographics AS (
    SELECT cd_demo_sk,
           cd_gender,
           cd_marital_status,
           cd_education_status,
           cd_purchase_estimate,
           cd_credit_rating
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
JOIN filtered_customer_address ca ON c.c_current_addr_sk = ca.ca_address_sk
JOIN filtered_customer_demographics cd ON cd.cd_demo_sk = c.c_current_cdemo_sk
WHERE EXISTS (
    SELECT 1
    FROM store_sales ss
    JOIN filtered_date fd ON ss.ss_sold_date_sk = fd.d_date_sk
    WHERE ss.ss_customer_sk = c.c_customer_sk
      AND ss.ss_list_price BETWEEN 100 AND 189
)
AND NOT EXISTS (
    SELECT 1
    FROM web_sales ws
    JOIN filtered_date fd ON ws.ws_sold_date_sk = fd.d_date_sk
    WHERE ws.ws_bill_customer_sk = c.c_customer_sk
      AND ws.ws_list_price BETWEEN 100 AND 189
)
AND NOT EXISTS (
    SELECT 1
    FROM catalog_sales cs
    JOIN filtered_date fd ON cs.cs_sold_date_sk = fd.d_date_sk
    WHERE cs.cs_ship_customer_sk = c.c_customer_sk
      AND cs.cs_list_price BETWEEN 100 AND 189
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
LIMIT 100
