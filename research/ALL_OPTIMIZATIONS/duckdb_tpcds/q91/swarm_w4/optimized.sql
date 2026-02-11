WITH filtered_dates AS (
    SELECT d_date_sk
    FROM date_dim
    WHERE d_year = 2001
      AND d_moy = 11
),
filtered_household AS (
    SELECT hd_demo_sk
    FROM household_demographics
    WHERE hd_buy_potential LIKE '1001-5000%'
),
filtered_address AS (
    SELECT ca_address_sk
    FROM customer_address
    WHERE ca_gmt_offset = -6
),
-- Branch 1: Marital=M, Education=Unknown
branch1 AS (
    SELECT 
        cr_net_loss,
        cr_call_center_sk,
        c_current_cdemo_sk
    FROM catalog_returns
    INNER JOIN filtered_dates ON cr_returned_date_sk = d_date_sk
    INNER JOIN customer ON cr_returning_customer_sk = c_customer_sk
    INNER JOIN filtered_address ON ca_address_sk = c_current_addr_sk
    INNER JOIN filtered_household ON hd_demo_sk = c_current_hdemo_sk
    INNER JOIN customer_demographics ON cd_demo_sk = c_current_cdemo_sk
    WHERE cd_marital_status = 'M'
      AND cd_education_status = 'Unknown'
),
-- Branch 2: Marital=W, Education=Advanced Degree
branch2 AS (
    SELECT 
        cr_net_loss,
        cr_call_center_sk,
        c_current_cdemo_sk
    FROM catalog_returns
    INNER JOIN filtered_dates ON cr_returned_date_sk = d_date_sk
    INNER JOIN customer ON cr_returning_customer_sk = c_customer_sk
    INNER JOIN filtered_address ON ca_address_sk = c_current_addr_sk
    INNER JOIN filtered_household ON hd_demo_sk = c_current_hdemo_sk
    INNER JOIN customer_demographics ON cd_demo_sk = c_current_cdemo_sk
    WHERE cd_marital_status = 'W'
      AND cd_education_status = 'Advanced Degree'
),
combined_returns AS (
    SELECT cr_net_loss, cr_call_center_sk FROM branch1
    UNION ALL
    SELECT cr_net_loss, cr_call_center_sk FROM branch2
)
SELECT
    cc_call_center_id AS Call_Center,
    cc_name AS Call_Center_Name,
    cc_manager AS Manager,
    SUM(cr_net_loss) AS Returns_Loss
FROM call_center
INNER JOIN combined_returns ON cr_call_center_sk = cc_call_center_sk
GROUP BY
    cc_call_center_id,
    cc_name,
    cc_manager
ORDER BY
    SUM(cr_net_loss) DESC;