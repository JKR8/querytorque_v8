WITH filtered_date AS (
    SELECT d_date_sk
    FROM date_dim
    WHERE d_year = 2001
      AND d_moy = 11
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
    WHERE ca_gmt_offset = -6
),
filtered_customer AS (
    SELECT c_customer_sk, cd.cd_marital_status, cd.cd_education_status
    FROM customer
    INNER JOIN filtered_customer_demographics cd ON cd_demo_sk = c_current_cdemo_sk
    INNER JOIN filtered_household_demographics hd ON hd_demo_sk = c_current_hdemo_sk
    INNER JOIN filtered_customer_address ca ON ca_address_sk = c_current_addr_sk
)
SELECT
    cc_call_center_id AS Call_Center,
    cc_name AS Call_Center_Name,
    cc_manager AS Manager,
    SUM(cr_net_loss) AS Returns_Loss
FROM catalog_returns
INNER JOIN call_center ON cr_call_center_sk = cc_call_center_sk
INNER JOIN filtered_date ON cr_returned_date_sk = d_date_sk
INNER JOIN filtered_customer ON cr_returning_customer_sk = c_customer_sk
GROUP BY
    cc_call_center_id,
    cc_name,
    cc_manager,
    cd_marital_status,
    cd_education_status
ORDER BY
    SUM(cr_net_loss) DESC