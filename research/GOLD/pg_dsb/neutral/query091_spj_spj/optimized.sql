WITH filtered_date AS (
    SELECT d_date_sk
    FROM date_dim
    WHERE d_year = 1999
      AND d_moy = 1
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
filtered_cd AS (
    SELECT cd_demo_sk
    FROM customer_demographics
    WHERE (cd_marital_status = 'M' AND cd_education_status = 'Unknown')
       OR (cd_marital_status = 'W' AND cd_education_status = 'Advanced Degree')
),
qualified_customers AS (
    SELECT c_customer_sk
    FROM customer
    INNER JOIN filtered_cd ON cd_demo_sk = c_current_cdemo_sk
    INNER JOIN filtered_hd ON hd_demo_sk = c_current_hdemo_sk
    INNER JOIN filtered_ca ON ca_address_sk = c_current_addr_sk
)
SELECT
    MIN(cc_call_center_id),
    MIN(cc_name),
    MIN(cc_manager),
    MIN(cr_net_loss),
    MIN(cr_item_sk),
    MIN(cr_order_number)
FROM catalog_returns
INNER JOIN call_center ON cr_call_center_sk = cc_call_center_sk
INNER JOIN filtered_date ON cr_returned_date_sk = d_date_sk
INNER JOIN qualified_customers ON cr_returning_customer_sk = c_customer_sk
