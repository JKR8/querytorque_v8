WITH filtered_dates AS (
    SELECT d_date_sk
    FROM date_dim
    WHERE d_year = 2001
      AND d_qoy < 4
),
store_customers AS (
    SELECT DISTINCT ss_customer_sk
    FROM store_sales
    JOIN filtered_dates ON ss_sold_date_sk = d_date_sk
),
web_customers AS (
    SELECT DISTINCT ws_bill_customer_sk
    FROM web_sales
    JOIN filtered_dates ON ws_sold_date_sk = d_date_sk
),
catalog_customers AS (
    SELECT DISTINCT cs_ship_customer_sk
    FROM catalog_sales
    JOIN filtered_dates ON cs_sold_date_sk = d_date_sk
)
SELECT
    ca.ca_state,
    cd.cd_gender,
    cd.cd_marital_status,
    cd.cd_dep_count,
    COUNT(*) AS cnt1,
    MAX(cd.cd_dep_count),
    SUM(cd.cd_dep_count),
    MAX(cd.cd_dep_count),
    cd.cd_dep_employed_count,
    COUNT(*) AS cnt2,
    MAX(cd.cd_dep_employed_count),
    SUM(cd.cd_dep_employed_count),
    MAX(cd.cd_dep_employed_count),
    cd.cd_dep_college_count,
    COUNT(*) AS cnt3,
    MAX(cd.cd_dep_college_count),
    SUM(cd.cd_dep_college_count),
    MAX(cd.cd_dep_college_count)
FROM customer c
JOIN customer_address ca ON c.c_current_addr_sk = ca.ca_address_sk
JOIN customer_demographics cd ON cd.cd_demo_sk = c.c_current_cdemo_sk
WHERE EXISTS (SELECT 1 FROM store_customers sc WHERE sc.ss_customer_sk = c.c_customer_sk)
  AND (EXISTS (SELECT 1 FROM web_customers wc WHERE wc.ws_bill_customer_sk = c.c_customer_sk)
       OR EXISTS (SELECT 1 FROM catalog_customers cc WHERE cc.cs_ship_customer_sk = c.c_customer_sk))
GROUP BY
    ca.ca_state,
    cd.cd_gender,
    cd.cd_marital_status,
    cd.cd_dep_count,
    cd.cd_dep_employed_count,
    cd.cd_dep_college_count
ORDER BY
    ca.ca_state,
    cd.cd_gender,
    cd.cd_marital_status,
    cd.cd_dep_count,
    cd.cd_dep_employed_count,
    cd.cd_dep_college_count
LIMIT 100;