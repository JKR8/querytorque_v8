WITH filtered_date AS (
    SELECT d_date_sk
    FROM date_dim
    WHERE d_year = 2000
      AND d_moy BETWEEN 1 AND 3
),
filtered_address AS (
    SELECT ca_address_sk
    FROM customer_address
    WHERE ca_state IN ('TX', 'VA', 'MI')
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
FROM customer AS c
JOIN filtered_address AS ca ON c.c_current_addr_sk = ca.ca_address_sk
JOIN customer_demographics ON cd_demo_sk = c.c_current_cdemo_sk
WHERE EXISTS (
    SELECT 1
    FROM store_sales
    JOIN filtered_date AS d ON ss_sold_date_sk = d.d_date_sk
    WHERE c.c_customer_sk = ss_customer_sk
)
  AND NOT EXISTS (
    SELECT 1
    FROM web_sales
    JOIN filtered_date AS d ON ws_sold_date_sk = d.d_date_sk
    WHERE c.c_customer_sk = ws_bill_customer_sk
  )
  AND NOT EXISTS (
    SELECT 1
    FROM catalog_sales
    JOIN filtered_date AS d ON cs_sold_date_sk = d.d_date_sk
    WHERE c.c_customer_sk = cs_ship_customer_sk
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