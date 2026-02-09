WITH filtered_date AS (
    SELECT d_date_sk 
    FROM date_dim 
    WHERE d_year = 1999 
      AND d_moy = 1
),
filtered_customer_demographics AS (
    SELECT cd_demo_sk 
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
)
SELECT
  MIN(cc_call_center_id),
  MIN(cc_name),
  MIN(cc_manager),
  MIN(cr_net_loss),
  MIN(cr_item_sk),
  MIN(cr_order_number)
FROM catalog_returns
JOIN filtered_date ON cr_returned_date_sk = d_date_sk
JOIN customer ON cr_returning_customer_sk = c_customer_sk
JOIN filtered_customer_demographics ON cd_demo_sk = c_current_cdemo_sk
JOIN filtered_household_demographics ON hd_demo_sk = c_current_hdemo_sk
JOIN filtered_customer_address ON ca_address_sk = c_current_addr_sk
JOIN call_center ON cr_call_center_sk = cc_call_center_sk