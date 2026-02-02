-- start query 10 in stream 0 using template query10.tpl
WITH filtered_date AS (
    SELECT d_date_sk
    FROM date_dim
    WHERE d_year = 2001
      AND d_moy BETWEEN 1 AND 4
),
filtered_store_sales AS (
    SELECT DISTINCT ss_customer_sk
    FROM store_sales
    JOIN filtered_date ON ss_sold_date_sk = d_date_sk
),
filtered_web_sales AS (
    SELECT DISTINCT ws_bill_customer_sk
    FROM web_sales
    JOIN filtered_date ON ws_sold_date_sk = d_date_sk
),
filtered_catalog_sales AS (
    SELECT DISTINCT cs_ship_customer_sk
    FROM catalog_sales
    JOIN filtered_date ON cs_sold_date_sk = d_date_sk
),
filtered_customer_address AS (
    SELECT ca_address_sk
    FROM customer_address
    WHERE ca_county IN ('Storey County', 'Marquette County', 'Warren County', 'Cochran County', 'Kandiyohi County')
)
SELECT 
  cd_gender,
  cd_marital_status,
  cd_education_status,
  COUNT(*) cnt1,
  cd_purchase_estimate,
  COUNT(*) cnt2,
  cd_credit_rating,
  COUNT(*) cnt3,
  cd_dep_count,
  COUNT(*) cnt4,
  cd_dep_employed_count,
  COUNT(*) cnt5,
  cd_dep_college_count,
  COUNT(*) cnt6
FROM customer c
JOIN filtered_customer_address ca ON c.c_current_addr_sk = ca.ca_address_sk
JOIN customer_demographics cd ON cd.cd_demo_sk = c.c_current_cdemo_sk
WHERE EXISTS (SELECT 1 FROM filtered_store_sales fss WHERE fss.ss_customer_sk = c.c_customer_sk)
  AND (EXISTS (SELECT 1 FROM filtered_web_sales fws WHERE fws.ws_bill_customer_sk = c.c_customer_sk)
       OR EXISTS (SELECT 1 FROM filtered_catalog_sales fcs WHERE fcs.cs_ship_customer_sk = c.c_customer_sk))
GROUP BY cd_gender,
         cd_marital_status,
         cd_education_status,
         cd_purchase_estimate,
         cd_credit_rating,
         cd_dep_count,
         cd_dep_employed_count,
         cd_dep_college_count
ORDER BY cd_gender,
         cd_marital_status,
         cd_education_status,
         cd_purchase_estimate,
         cd_credit_rating,
         cd_dep_count,
         cd_dep_employed_count,
         cd_dep_college_count
LIMIT 100;
-- end query 10 in stream 0 using template query10.tpl