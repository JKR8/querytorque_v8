-- start query 91 in stream 0 using template query91.tpl
WITH filtered_date AS (
    SELECT d_date_sk
    FROM date_dim
    WHERE d_year = 2001 AND d_moy = 11
),
filtered_customer_address AS (
    SELECT ca_address_sk
    FROM customer_address
    WHERE ca_gmt_offset = -6
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
filtered_customer AS (
    SELECT c_customer_sk, c_current_addr_sk, c_current_cdemo_sk, c_current_hdemo_sk
    FROM customer
)
SELECT  
    cc.cc_call_center_id AS Call_Center,
    cc.cc_name AS Call_Center_Name,
    cc.cc_manager AS Manager,
    SUM(cr.cr_net_loss) AS Returns_Loss
FROM call_center cc
JOIN catalog_returns cr ON cr.cr_call_center_sk = cc.cc_call_center_sk
JOIN filtered_date d ON cr.cr_returned_date_sk = d.d_date_sk
JOIN filtered_customer c ON cr.cr_returning_customer_sk = c.c_customer_sk
JOIN filtered_customer_address ca ON ca.ca_address_sk = c.c_current_addr_sk
JOIN filtered_customer_demographics cd ON cd.cd_demo_sk = c.c_current_cdemo_sk
JOIN filtered_household_demographics hd ON hd.hd_demo_sk = c.c_current_hdemo_sk
GROUP BY cc.cc_call_center_id, cc.cc_name, cc.cc_manager, cd.cd_marital_status, cd.cd_education_status
ORDER BY SUM(cr.cr_net_loss) DESC;

-- end query 91 in stream 0 using template query91.tpl