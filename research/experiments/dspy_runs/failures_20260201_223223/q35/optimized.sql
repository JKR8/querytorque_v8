-- start query 35 in stream 0 using template query35.tpl
WITH filtered_dates AS (
    SELECT d_date_sk
    FROM date_dim
    WHERE d_year = 2001 AND d_qoy < 4
),
store_customers AS (
    SELECT DISTINCT ss_customer_sk
    FROM store_sales
    INNER JOIN filtered_dates ON store_sales.ss_sold_date_sk = filtered_dates.d_date_sk
),
web_customers AS (
    SELECT DISTINCT ws_bill_customer_sk
    FROM web_sales
    INNER JOIN filtered_dates ON web_sales.ws_sold_date_sk = filtered_dates.d_date_sk
),
catalog_customers AS (
    SELECT DISTINCT cs_ship_customer_sk
    FROM catalog_sales
    INNER JOIN filtered_dates ON catalog_sales.cs_sold_date_sk = filtered_dates.d_date_sk
)
SELECT  
  ca_state,
  cd_gender,
  cd_marital_status,
  cd_dep_count,
  count(*) cnt1,
  max(cd_dep_count),
  sum(cd_dep_count),
  max(cd_dep_count),
  cd_dep_employed_count,
  count(*) cnt2,
  max(cd_dep_employed_count),
  sum(cd_dep_employed_count),
  max(cd_dep_employed_count),
  cd_dep_college_count,
  count(*) cnt3,
  max(cd_dep_college_count),
  sum(cd_dep_college_count),
  max(cd_dep_college_count)
 FROM
  customer c
  INNER JOIN customer_address ca ON c.c_current_addr_sk = ca.ca_address_sk
  INNER JOIN customer_demographics cd ON cd.cd_demo_sk = c.c_current_cdemo_sk
 WHERE
  EXISTS (SELECT 1 FROM store_customers WHERE ss_customer_sk = c.c_customer_sk) AND
  (EXISTS (SELECT 1 FROM web_customers WHERE ws_bill_customer_sk = c.c_customer_sk) OR 
   EXISTS (SELECT 1 FROM catalog_customers WHERE cs_ship_customer_sk = c.c_customer_sk))
 GROUP BY ca_state,
          cd_gender,
          cd_marital_status,
          cd_dep_count,
          cd_dep_employed_count,
          cd_dep_college_count
 ORDER BY ca_state,
          cd_gender,
          cd_marital_status,
          cd_dep_count,
          cd_dep_employed_count,
          cd_dep_college_count
 LIMIT 100;

-- end query 35 in stream 0 using template query35.tpl