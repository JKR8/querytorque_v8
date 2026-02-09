WITH filtered_date AS (
    SELECT d_date_sk
    FROM date_dim
    WHERE d_year = 1998
),
filtered_cd1 AS (
    SELECT cd_demo_sk, cd_marital_status, cd_education_status
    FROM customer_demographics
    WHERE (cd_marital_status = 'D' AND cd_education_status = 'Secondary')
       OR (cd_marital_status = 'M' AND cd_education_status = '4 yr Degree')
       OR (cd_marital_status = 'U' AND cd_education_status = 'Unknown')
),
filtered_cd2 AS (
    SELECT cd_demo_sk, cd_marital_status, cd_education_status
    FROM customer_demographics
    WHERE (cd_marital_status = 'D' AND cd_education_status = 'Secondary')
       OR (cd_marital_status = 'M' AND cd_education_status = '4 yr Degree')
       OR (cd_marital_status = 'U' AND cd_education_status = 'Unknown')
),
filtered_ca AS (
    SELECT ca_address_sk
    FROM customer_address
    WHERE ca_country = 'United States'
      AND (ca_state IN ('IA', 'PA', 'TX', 'GA', 'MO', 'SD', 'LA', 'TX', 'VA'))
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
JOIN web_returns ON web_sales.ws_item_sk = web_returns.wr_item_sk 
    AND web_sales.ws_order_number = web_returns.wr_order_number
JOIN filtered_date ON web_sales.ws_sold_date_sk = filtered_date.d_date_sk
JOIN web_page ON web_sales.ws_web_page_sk = web_page.wp_web_page_sk
JOIN filtered_cd1 cd1 ON web_returns.wr_refunded_cdemo_sk = cd1.cd_demo_sk
JOIN filtered_cd2 cd2 ON web_returns.wr_returning_cdemo_sk = cd2.cd_demo_sk
JOIN filtered_ca ON web_returns.wr_refunded_addr_sk = filtered_ca.ca_address_sk
JOIN reason ON web_returns.wr_reason_sk = reason.r_reason_sk
WHERE
    (
        (cd1.cd_marital_status = 'D' AND cd1.cd_marital_status = cd2.cd_marital_status
         AND cd1.cd_education_status = 'Secondary' AND cd1.cd_education_status = cd2.cd_education_status
         AND web_sales.ws_sales_price BETWEEN 100.00 AND 150.00)
        OR
        (cd1.cd_marital_status = 'M' AND cd1.cd_marital_status = cd2.cd_mar_status
         AND cd1.cd_education_status = '4 yr Degree' AND cd1.cd_education_status = cd2.cd_education_status
         AND web_sales.ws_sales_price BETWEEN 50.00 AND 100.00)
        OR
        (cd1.cd_marital_status = 'U' AND cd1.cd_marital_status = cd2.cd_marital_status
         AND cd1.cd_education_status = 'Unknown' AND cd1.cd_education_status = cd2.cd_education_status
         AND web_sales.ws_sales_price BETWEEN 150.00 AND 200.00)
    )
    AND
    (
        (web_sales.ws_net_profit BETWEEN 100 AND 200)
        OR
        (web_sales.ws_net_profit BETWEEN 150 AND 300)
        OR
        (web_sales.ws_net_profit BETWEEN 50 AND 250)
    );