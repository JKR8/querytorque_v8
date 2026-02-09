WITH filtered_dates AS (
    SELECT d_date_sk
    FROM date_dim
    WHERE d_year = 1998
),
filtered_customer_demographics AS (
    SELECT cd_demo_sk, cd_marital_status, cd_education_status
    FROM customer_demographics
    WHERE (cd_marital_status = 'D' AND cd_education_status = 'Secondary')
       OR (cd_marital_status = 'M' AND cd_education_status = '4 yr Degree')
       OR (cd_marital_status = 'U' AND cd_education_status = 'Unknown')
),
filtered_customer_address AS (
    SELECT ca_address_sk, ca_state
    FROM customer_address
    WHERE ca_country = 'United States'
      AND (ca_state IN ('IA', 'PA', 'TX')
           OR ca_state IN ('GA', 'MO', 'SD')
           OR ca_state IN ('LA', 'TX', 'VA'))
)
SELECT
    MIN(ws_quantity),
    MIN(wr_refunded_cash),
    MIN(wr_fee),
    MIN(ws_item_sk),
    MIN(wr_order_number),
    MIN(cd1.cd_demo_sk),
    MIN(cd2.cd_demo_sk)
FROM web_sales
JOIN web_returns ON ws_item_sk = wr_item_sk 
                 AND ws_order_number = wr_order_number
JOIN web_page ON ws_web_page_sk = wp_web_page_sk
JOIN filtered_dates ON ws_sold_date_sk = d_date_sk
JOIN filtered_customer_demographics cd1 ON wr_refunded_cdemo_sk = cd1.cd_demo_sk
JOIN filtered_customer_demographics cd2 ON wr_returning_cdemo_sk = cd2.cd_demo_sk
JOIN filtered_customer_address ON ca_address_sk = wr_refunded_addr_sk
JOIN reason ON r_reason_sk = wr_reason_sk
WHERE (
    (cd1.cd_marital_status = 'D' 
     AND cd1.cd_marital_status = cd2.cd_marital_status
     AND cd1.cd_education_status = 'Secondary'
     AND cd1.cd_education_status = cd2.cd_education_status
     AND ws_sales_price BETWEEN 100.00 AND 150.00)
    OR
    (cd1.cd_marital_status = 'M'
     AND cd1.cd_marital_status = cd2.cd_marital_status
     AND cd1.cd_education_status = '4 yr Degree'
     AND cd1.cd_education_status = cd2.cd_education_status
     AND ws_sales_price BETWEEN 50.00 AND 100.00)
    OR
    (cd1.cd_marital_status = 'U'
     AND cd1.cd_marital_status = cd2.cd_marital_status
     AND cd1.cd_education_status = 'Unknown'
     AND cd1.cd_education_status = cd2.cd_education_status
     AND ws_sales_price BETWEEN 150.00 AND 200.00)
)
AND (
    (ca_state IN ('IA', 'PA', 'TX') AND ws_net_profit BETWEEN 100 AND 200)
    OR
    (ca_state IN ('GA', 'MO', 'SD') AND ws_net_profit BETWEEN 150 AND 300)
    OR
    (ca_state IN ('LA', 'TX', 'VA') AND ws_net_profit BETWEEN 50 AND 250)
)