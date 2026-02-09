WITH filtered_date AS (
    SELECT d_date_sk
    FROM date_dim
    WHERE d_year = 1999
      AND d_moy = 1
),
filtered_customer_demographics AS (
    SELECT cd_demo_sk, cd_marital_status, cd_education_status
    FROM customer_demographics
    WHERE (cd_marital_status = 'M' AND cd_education_status = 'Unknown')
       OR (cd_marital_status = 'W' AND cd_education_status = 'Advanced Degree')
),
filtered_household_demographics AS (
    SELECT hd_demo_sk
    FROM household_demographics
    WHERE hd_buy_potential LIKE '1001-5000%'
),
filtered_customer_address AS (
    SELECT ca_address_sk
    FROM customer_address
    WHERE ca_gmt_offset = -7
),
filtered_customer AS (
    SELECT c_customer_sk,
           c_current_cdemo_sk,
           c_current_hdemo_sk,
           c_current_addr_sk
    FROM customer
),
filtered_catalog_returns AS (
    SELECT cr_call_center_sk,
           cr_returned_date_sk,
           cr_returning_customer_sk,
           cr_net_loss
    FROM catalog_returns
)
SELECT
  cc_call_center_id AS Call_Center,
  cc_name AS Call_Center_Name,
  cc_manager AS Manager,
  SUM(cr_net_loss) AS Returns_Loss
FROM filtered_catalog_returns cr
JOIN call_center cc ON cr.cr_call_center_sk = cc.cc_call_center_sk
JOIN filtered_date d ON cr.cr_returned_date_sk = d.d_date_sk
JOIN filtered_customer c ON cr.cr_returning_customer_sk = c.c_customer_sk
JOIN filtered_customer_demographics cd ON c.c_current_cdemo_sk = cd.cd_demo_sk
JOIN filtered_household_demographics hd ON c.c_current_hdemo_sk = hd.hd_demo_sk
JOIN filtered_customer_address ca ON c.c_current_addr_sk = ca.ca_address_sk
GROUP BY
  cc_call_center_id,
  cc_name,
  cc_manager,
  cd_marital_status,
  cd_education_status
ORDER BY
  SUM(cr_net_loss) DESC