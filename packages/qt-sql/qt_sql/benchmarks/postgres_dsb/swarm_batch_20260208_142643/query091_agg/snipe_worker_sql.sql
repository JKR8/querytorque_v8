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
qualified_customers AS (
    SELECT 
        c_customer_sk,
        c_current_cdemo_sk,
        c_current_hdemo_sk,
        c_current_addr_sk
    FROM customer
    WHERE c_current_cdemo_sk IN (SELECT cd_demo_sk FROM filtered_customer_demographics)
      AND c_current_hdemo_sk IN (SELECT hd_demo_sk FROM filtered_household_demographics)
      AND c_current_addr_sk IN (SELECT ca_address_sk FROM filtered_customer_address)
)
SELECT
    cc_call_center_id AS Call_Center,
    cc_name AS Call_Center_Name,
    cc_manager AS Manager,
    SUM(cr_net_loss) AS Returns_Loss
FROM call_center
JOIN catalog_returns ON cr_call_center_sk = cc_call_center_sk
JOIN filtered_date ON cr_returned_date_sk = d_date_sk
JOIN qualified_customers ON cr_returning_customer_sk = c_customer_sk
JOIN filtered_customer_demographics ON cd_demo_sk = c_current_cdemo_sk
GROUP BY
    cc_call_center_id,
    cc_name,
    cc_manager,
    cd_marital_status,
    cd_education_status
ORDER BY
    SUM(cr_net_loss) DESC;