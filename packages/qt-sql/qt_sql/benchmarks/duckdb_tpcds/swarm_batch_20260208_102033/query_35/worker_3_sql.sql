WITH filtered_dates AS (
    SELECT d_date_sk
    FROM date_dim
    WHERE d_year = 2001
      AND d_qoy < 4
),
store_customers AS (
    SELECT DISTINCT ss_customer_sk
    FROM store_sales
    JOIN filtered_dates ON store_sales.ss_sold_date_sk = filtered_dates.d_date_sk
),
web_customers AS (
    SELECT DISTINCT ws_bill_customer_sk
    FROM web_sales
    JOIN filtered_dates ON web_sales.ws_sold_date_sk = filtered_dates.d_date_sk
),
catalog_customers AS (
    SELECT DISTINCT cs_ship_customer_sk
    FROM catalog_sales
    JOIN filtered_dates ON catalog_sales.cs_sold_date_sk = filtered_dates.d_date_sk
),
multi_channel_customers AS (
    SELECT ss_customer_sk AS customer_sk
    FROM store_customers
    INTERSECT
    (
        SELECT ws_bill_customer_sk AS customer_sk
        FROM web_customers
        UNION
        SELECT cs_ship_customer_sk AS customer_sk
        FROM catalog_customers
    )
)
SELECT
  ca_state,
  cd_gender,
  cd_marital_status,
  cd_dep_count,
  COUNT(*) AS cnt1,
  MAX(cd_dep_count) AS "MAX(cd_dep_count)",
  SUM(cd_dep_count) AS "SUM(cd_dep_count)",
  MAX(cd_dep_count) AS "MAX(cd_dep_count)",
  cd_dep_employed_count,
  COUNT(*) AS cnt2,
  MAX(cd_dep_employed_count) AS "MAX(cd_dep_employed_count)",
  SUM(cd_dep_employed_count) AS "SUM(cd_dep_employed_count)",
  MAX(cd_dep_employed_count) AS "MAX(cd_dep_employed_count)",
  cd_dep_college_count,
  COUNT(*) AS cnt3,
  MAX(cd_dep_college_count) AS "MAX(cd_dep_college_count)",
  SUM(cd_dep_college_count) AS "SUM(cd_dep_college_count)",
  MAX(cd_dep_college_count) AS "MAX(cd_dep_college_count)"
FROM customer AS c
JOIN customer_address AS ca ON c.c_current_addr_sk = ca.ca_address_sk
JOIN customer_demographics cd ON cd.cd_demo_sk = c.c_current_cdemo_sk
JOIN multi_channel_customers mcc ON c.c_customer_sk = mcc.customer_sk
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