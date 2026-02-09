WITH filtered_date AS (
    SELECT d_date_sk
    FROM date_dim
    WHERE d_year = 1999
      AND d_moy = 1
),
filtered_cd AS (
    SELECT cd_demo_sk
    FROM customer_demographics
    WHERE (cd_marital_status = 'M' AND cd_education_status = 'Unknown')
       OR (cd_marital_status = 'W' AND cd_education_status = 'Advanced Degree')
),
filtered_hd AS (
    SELECT hd_demo_sk
    FROM household_demographics
    WHERE hd_buy_potential LIKE '1001-5000%'
),
filtered_ca AS (
    SELECT ca_address_sk
    FROM customer_address
    WHERE ca_gmt_offset = -7
),
filtered_customer AS (
    SELECT c_customer_sk, c_current_cdemo_sk, c_current_hdemo_sk, c_current_addr_sk
    FROM customer
    WHERE EXISTS (SELECT 1 FROM filtered_cd WHERE cd_demo_sk = c_current_cdemo_sk)
      AND EXISTS (SELECT 1 FROM filtered_hd WHERE hd_demo_sk = c_current_hdemo_sk)
      AND EXISTS (SELECT 1 FROM filtered_ca WHERE ca_address_sk = c_current_addr_sk)
),
filtered_catalog_returns AS (
    SELECT cr_call_center_sk, cr_net_loss, cr_item_sk, cr_order_number
    FROM catalog_returns
    WHERE cr_returned_date_sk IN (SELECT d_date_sk FROM filtered_date)
      AND cr_returning_customer_sk IN (SELECT c_customer_sk FROM filtered_customer)
)
SELECT
    MIN(cc_call_center_id),
    MIN(cc_name),
    MIN(cc_manager),
    MIN(cr_net_loss),
    MIN(cr_item_sk),
    MIN(cr_order_number)
FROM filtered_catalog_returns cr
JOIN call_center cc ON cr.cr_call_center_sk = cc.cc_call_center_sk;