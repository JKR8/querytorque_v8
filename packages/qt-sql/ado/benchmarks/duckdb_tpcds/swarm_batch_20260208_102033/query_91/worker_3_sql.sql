WITH filtered_date AS (
    SELECT d_date_sk
    FROM date_dim
    WHERE d_year = 2001
      AND d_moy = 11
),
filtered_cd_hd AS (
    SELECT cd_demo_sk, hd_demo_sk, cd_marital_status, cd_education_status
    FROM customer_demographics
    JOIN household_demographics
      ON hd_demo_sk IS NOT NULL
    WHERE (
        (cd_marital_status = 'M' AND cd_education_status = 'Unknown')
        OR (cd_marital_status = 'W' AND cd_education_status = 'Advanced Degree')
    )
      AND hd_buy_potential LIKE '1001-5000%'
),
filtered_customer AS (
    SELECT 
        c_customer_sk,
        c_current_cdemo_sk,
        c_current_hdemo_sk,
        c_current_addr_sk
    FROM customer
),
filtered_customer_dims AS (
    SELECT 
        fc.c_customer_sk,
        fcd.cd_marital_status,
        fcd.cd_education_status
    FROM filtered_customer fc
    JOIN filtered_cd_hd fcd
      ON fc.c_current_cdemo_sk = fcd.cd_demo_sk
     AND fc.c_current_hdemo_sk = fcd.hd_demo_sk
    JOIN customer_address ca
      ON fc.c_current_addr_sk = ca.ca_address_sk
     AND ca.ca_gmt_offset = -6
),
prejoined_fact AS (
    SELECT 
        cr.cr_net_loss,
        cr.cr_call_center_sk,
        fcd.cd_marital_status,
        fcd.cd_education_status
    FROM catalog_returns cr
    JOIN filtered_date fd
      ON cr.cr_returned_date_sk = fd.d_date_sk
    JOIN filtered_customer_dims fcd
      ON cr.cr_returning_customer_sk = fcd.c_customer_sk
)
SELECT 
    cc.cc_call_center_id AS Call_Center,
    cc.cc_name AS Call_Center_Name,
    cc.cc_manager AS Manager,
    SUM(pj.cr_net_loss) AS Returns_Loss
FROM prejoined_fact pj
JOIN call_center cc
  ON pj.cr_call_center_sk = cc.cc_call_center_sk
GROUP BY 
    cc.cc_call_center_id,
    cc.cc_name,
    cc.cc_manager,
    pj.cd_marital_status,
    pj.cd_education_status
ORDER BY 
    SUM(pj.cr_net_loss) DESC;