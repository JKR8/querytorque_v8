WITH date_cte AS (SELECT d_date_sk FROM date_dim WHERE d_year = 2001 AND d_qoy < 4),
     store_sales_cte AS (SELECT ss_customer_sk FROM store_sales JOIN date_cte ON store_sales.ss_sold_date_sk = date_cte.d_date_sk),
     web_sales_cte AS (SELECT ws_bill_customer_sk FROM web_sales JOIN date_cte ON web_sales.ws_sold_date_sk = date_cte.d_date_sk),
     catalog_sales_cte AS (SELECT cs_ship_customer_sk FROM catalog_sales JOIN date_cte ON catalog_sales.cs_sold_date_sk = date_cte.d_date_sk)
SELECT
  ca_state,
  cd_gender,
  cd_marital_status,
  cd_dep_count,
  COUNT(*) cnt1,
  MAX(cd_dep_count),
  SUM(cd_dep_count),
  MAX(cd_dep_count),
  cd_dep_employed_count,
  COUNT(*) cnt2,
  MAX(cd_dep_employed_count),
  SUM(cd_dep_employed_count),
  MAX(cd_dep_employed_count),
  cd_dep_college_count,
  COUNT(*) cnt3,
  MAX(cd_dep_college_count),
  SUM(cd_dep_college_count),
  MAX(cd_dep_college_count)
FROM customer c
JOIN customer_address ca ON c.c_current_addr_sk = ca.ca_address_sk
JOIN customer_demographics cd ON cd.cd_demo_sk = c.c_current_cdemo_sk
WHERE 
  c.c_customer_sk IN (SELECT ss_customer_sk FROM store_sales_cte)
  AND (
    c.c_customer_sk IN (SELECT ws_bill_customer_sk FROM web_sales_cte)
    OR c.c_customer_sk IN (SELECT cs_ship_customer_sk FROM catalog_sales_cte)
  )
GROUP BY 
  ca_state,
  cd_gender,
  cd_marital_status,
  cd_dep_count,
  cd_dep_employed_count,
  cd_dep_college_count
ORDER BY 
  ca_state,
  cd_gender,
  cd_marital_status,
  cd_dep_count,
  cd_dep_employed_count,
  cd_dep_college_count
LIMIT 100