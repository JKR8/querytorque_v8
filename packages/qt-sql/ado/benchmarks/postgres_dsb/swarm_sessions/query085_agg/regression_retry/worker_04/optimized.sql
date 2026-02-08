WITH filtered_date AS (
    SELECT d_date_sk
    FROM date_dim
    WHERE d_year = 1998
),
filtered_reason AS (
    SELECT r_reason_sk, r_reason_desc
    FROM reason
),
filtered_address AS (
    SELECT ca_address_sk
    FROM customer_address
    WHERE ca_country = 'United States'
      AND ca_state IN ('IA', 'PA', 'TX', 'GA', 'MO', 'SD', 'LA', 'VA')
),
sales_returns_base AS (
    SELECT 
        ws_item_sk,
        ws_order_number,
        ws_sold_date_sk,
        ws_web_page_sk,
        ws_quantity,
        ws_sales_price,
        ws_net_profit,
        wr_refunded_cash,
        wr_fee,
        wr_refunded_cdemo_sk,
        wr_returning_cdemo_sk,
        wr_refunded_addr_sk,
        wr_reason_sk
    FROM web_sales
    JOIN web_returns ON ws_item_sk = wr_item_sk 
        AND ws_order_number = wr_order_number
)
SELECT
    SUBSTRING(r_reason_desc FROM 1 FOR 20),
    AVG(ws_quantity),
    AVG(wr_refunded_cash),
    AVG(wr_fee)
FROM sales_returns_base
JOIN filtered_date ON ws_sold_date_sk = d_date_sk
JOIN web_page ON ws_web_page_sk = wp_web_page_sk
JOIN filtered_reason ON r_reason_sk = wr_reason_sk
JOIN filtered_address ON ca_address_sk = wr_refunded_addr_sk
JOIN customer_demographics cd1 ON cd1.cd_demo_sk = wr_refunded_cdemo_sk
JOIN customer_demographics cd2 ON cd2.cd_demo_sk = wr_returning_cdemo_sk
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
        ca_state IN ('IA', 'PA', 'TX')
        AND ws_net_profit BETWEEN 100 AND 200
    )
    OR (
        ca_state IN ('GA', 'MO', 'SD')
        AND ws_net_profit BETWEEN 150 AND 300
    )
    OR (
        ca_state IN ('LA', 'TX', 'VA')
        AND ws_net_profit BETWEEN 50 AND 250
    )
)
GROUP BY r_reason_desc
ORDER BY
    SUBSTRING(r_reason_desc FROM 1 FOR 20),
    AVG(ws_quantity),
    AVG(wr_refunded_cash),
    AVG(wr_fee)
LIMIT 100;