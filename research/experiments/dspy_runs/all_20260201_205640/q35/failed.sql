-- start query 35 in stream 0 using template query35.tpl
WITH sales_customers AS (
    SELECT ss_customer_sk AS customer_sk
    FROM store_sales
    JOIN date_dim ON ss_sold_date_sk = d_date_sk
    WHERE d_year = 2001 AND d_qoy < 4
    UNION
    SELECT ws_bill_customer_sk AS customer_sk
    FROM web_sales
    JOIN date_dim ON ws_sold_date_sk = d_date_sk
    WHERE d_year = 2001 AND d_qoy < 4
    UNION
    SELECT cs_ship_customer_sk AS customer_sk
    FROM catalog_sales
    JOIN date_dim ON cs_sold_date_sk = d_date_sk
    WHERE d_year = 2001 AND d_qoy < 4
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
FROM customer c
JOIN customer_address ca ON c.c_current_addr_sk = ca.ca_address_sk
JOIN customer_demographics cd ON cd.cd_demo_sk = c.c_current_cdemo_sk
WHERE EXISTS (
    SELECT 1 FROM sales_customers sc WHERE sc.customer_sk = c.c_customer_sk
)
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