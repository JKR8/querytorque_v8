WITH filtered_date AS (
    SELECT d_date_sk
    FROM date_dim
    WHERE d_year = 1998
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
JOIN filtered_date ON ws_sold_date_sk = filtered_date.d_date_sk
JOIN web_returns ON ws_item_sk = wr_item_sk 
    AND ws_order_number = wr_order_number
JOIN web_page ON ws_web_page_sk = wp_web_page_sk
JOIN customer_demographics AS cd1 ON cd1.cd_demo_sk = wr_refunded_cdemo_sk
JOIN customer_demographics AS cd2 ON cd2.cd_demo_sk = wr_returning_cdemo_sk
JOIN customer_address ON ca_address_sk = wr_refunded_addr_sk
JOIN reason ON r_reason_sk = wr_reason_sk
WHERE (
    (
        cd1.cd_marital_status = 'D'
        AND cd1.cd_marital_status = cd2.cd_marital_status
        AND cd1.cd_education_status = 'Secondary'
        AND cd1.cd_education_status = cd2.cd_education_status
        AND ws_sales_price BETWEEN 100.00 AND 150.00
    )
    OR (
        cd1.cd_marital_status = 'M'
        AND cd1.cd_marital_status = cd2.cd_marital_status
        AND cd1.cd_education_status = '4 yr Degree'
        AND cd1.cd_education_status = cd2.cd_education_status
        AND ws_sales_price BETWEEN 50.00 AND 100.00
    )
    OR (
        cd1.cd_marital_status = 'U'
        AND cd1.cd_marital_status = cd2.cd_marital_status
        AND cd1.cd_education_status = 'Unknown'
        AND cd1.cd_education_status = cd2.cd_education_status
        AND ws_sales_price BETWEEN 150.00 AND 200.00
    )
)
AND (
    (
        ca_country = 'United States'
        AND ca_state IN ('IA', 'PA', 'TX')
        AND ws_net_profit BETWEEN 100 AND 200
    )
    OR (
        ca_country = 'United States'
        AND ca_state IN ('GA', 'MO', 'SD')
        AND ws_net_profit BETWEEN 150 AND 300
    )
    OR (
        ca_country = 'United States'
        AND ca_state IN ('LA', 'TX', 'VA')
        AND ws_net_profit BETWEEN 50 AND 250
    )
)