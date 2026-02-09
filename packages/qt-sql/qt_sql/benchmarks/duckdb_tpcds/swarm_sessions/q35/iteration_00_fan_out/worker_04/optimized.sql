WITH filtered_dates AS (
    SELECT d_date_sk 
    FROM date_dim 
    WHERE d_year = 2001 
      AND d_qoy < 4
),
-- Get customers with store sales in the period
store_customers AS (
    SELECT DISTINCT ss_customer_sk AS customer_sk
    FROM store_sales
    JOIN filtered_dates ON ss_sold_date_sk = d_date_sk
),
-- Get customers with web sales OR catalog sales in the period
web_or_catalog_customers AS (
    SELECT ws_bill_customer_sk AS customer_sk
    FROM web_sales
    JOIN filtered_dates ON ws_sold_date_sk = d_date_sk
    UNION
    SELECT cs_ship_customer_sk AS customer_sk
    FROM catalog_sales
    JOIN filtered_dates ON cs_sold_date_sk = d_date_sk
)
SELECT
  ca_state,
  cd_gender,
  cd_marital_status,
  cd_dep_count,
  COUNT(*) AS cnt1,
  MAX(cd_dep_count),
  SUM(cd_dep_count),
  MAX(cd_dep_count),
  cd_dep_employed_count,
  COUNT(*) AS cnt2,
  MAX(cd_dep_employed_count),
  SUM(cd_dep_employed_count),
  MAX(cd_dep_employed_count),
  cd_dep_college_count,
  COUNT(*) AS cnt3,
  MAX(cd_dep_college_count),
  SUM(cd_dep_college_count),
  MAX(cd_dep_college_count)
FROM customer c
JOIN customer_address ca ON c.c_current_addr_sk = ca.ca_address_sk
JOIN customer_demographics cd ON cd.cd_demo_sk = c.c_current_cdemo_sk
JOIN store_customers sc ON c.c_customer_sk = sc.customer_sk
JOIN web_or_catalog_customers wcc ON c.c_customer_sk = wcc.customer_sk
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
LIMIT 100;