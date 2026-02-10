WITH filtered_dates AS (
  SELECT
    d_date_sk
  FROM date_dim
  WHERE
    d_year = 2001 AND d_qoy < 4
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
FROM customer AS c
JOIN customer_address AS ca
  ON c.c_current_addr_sk = ca.ca_address_sk
JOIN customer_demographics AS cd
  ON cd.cd_demo_sk = c.c_current_cdemo_sk
/* Convert EXISTS for store_sales to SEMI JOIN */
JOIN (
  SELECT DISTINCT
    ss_customer_sk
  FROM store_sales
  JOIN filtered_dates
    ON ss_sold_date_sk = d_date_sk
) AS ss
  ON c.c_customer_sk = ss.ss_customer_sk
/* Convert EXISTS for web_sales OR catalog_sales to LEFT JOIN with OR condition */
LEFT JOIN (
  SELECT DISTINCT
    ws_bill_customer_sk
  FROM web_sales
  JOIN filtered_dates
    ON ws_sold_date_sk = d_date_sk
) AS ws
  ON c.c_customer_sk = ws.ws_bill_customer_sk
LEFT JOIN (
  SELECT DISTINCT
    cs_ship_customer_sk
  FROM catalog_sales
  JOIN filtered_dates
    ON cs_sold_date_sk = d_date_sk
) AS cs
  ON c.c_customer_sk = cs.cs_ship_customer_sk
WHERE
  NOT ws.ws_bill_customer_sk IS NULL OR NOT cs.cs_ship_customer_sk IS NULL
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