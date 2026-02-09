WITH filtered_date AS (
    SELECT d_date_sk
    FROM date_dim
    WHERE d_year = 1998
),
filtered_address AS (
    SELECT ca_address_sk
    FROM customer_address
    WHERE ca_country = 'United States'
      AND ca_state IN ('IA', 'PA', 'TX', 'GA', 'MO', 'SD', 'LA', 'TX', 'VA')
),
filtered_web_sales AS (
    SELECT 
        ws.ws_quantity,
        ws.ws_item_sk,
        ws.ws_order_number,
        ws.ws_sales_price,
        ws.ws_net_profit,
        ws.ws_web_page_sk,
        ws.ws_sold_date_sk
    FROM web_sales ws
    JOIN filtered_date fd ON ws.ws_sold_date_sk = fd.d_date_sk
    WHERE (ws.ws_sales_price BETWEEN 50.00 AND 200.00)
      AND (ws.ws_net_profit BETWEEN 50 AND 300)
),
filtered_web_returns AS (
    SELECT 
        wr.wr_refunded_cash,
        wr.wr_fee,
        wr.wr_item_sk,
        wr.wr_order_number,
        wr.wr_refunded_cdemo_sk,
        wr.wr_returning_cdemo_sk,
        wr.wr_refunded_addr_sk,
        wr.wr_reason_sk
    FROM web_returns wr
)
SELECT
    MIN(ws.ws_quantity),
    MIN(wr.wr_refunded_cash),
    MIN(wr.wr_fee),
    MIN(ws.ws_item_sk),
    MIN(wr.wr_order_number),
    MIN(cd1.cd_demo_sk),
    MIN(cd2.cd_demo_sk)
FROM filtered_web_sales ws
JOIN filtered_web_returns wr 
    ON ws.ws_item_sk = wr.wr_item_sk 
    AND ws.ws_order_number = wr.wr_order_number
JOIN web_page wp ON ws.ws_web_page_sk = wp.wp_web_page_sk
JOIN customer_demographics cd1 ON cd1.cd_demo_sk = wr.wr_refunded_cdemo_sk
JOIN customer_demographics cd2 ON cd2.cd_demo_sk = wr.wr_returning_cdemo_sk
JOIN filtered_address ca ON ca.ca_address_sk = wr.wr_refunded_addr_sk
JOIN reason r ON r.r_reason_sk = wr.wr_reason_sk
WHERE (
    (
        cd1.cd_marital_status = 'D'
        AND cd1.cd_marital_status = cd2.cd_marital_status
        AND cd1.cd_education_status = 'Secondary'
        AND cd1.cd_education_status = cd2.cd_education_status
        AND ws.ws_sales_price BETWEEN 100.00 AND 150.00
    )
    OR (
        cd1.cd_marital_status = 'M'
        AND cd1.cd_marital_status = cd2.cd_marital_status
        AND cd1.cd_education_status = '4 yr Degree'
        AND cd1.cd_education_status = cd2.cd_education_status
        AND ws.ws_sales_price BETWEEN 50.00 AND 100.00
    )
    OR (
        cd1.cd_marital_status = 'U'
        AND cd1.cd_marital_status = cd2.cd_marital_status
        AND cd1.cd_education_status = 'Unknown'
        AND cd1.cd_education_status = cd2.cd_education_status
        AND ws.ws_sales_price BETWEEN 150.00 AND 200.00
    )
)
AND (
    (
        ca.ca_address_sk = wr.wr_refunded_addr_sk
        AND ws.ws_net_profit BETWEEN 100 AND 200
    )
    OR (
        ca.ca_address_sk = wr.wr_refunded_addr_sk
        AND ws.ws_net_profit BETWEEN 150 AND 300
    )
    OR (
        ca.ca_address_sk = wr.wr_refunded_addr_sk
        AND ws.ws_net_profit BETWEEN 50 AND 250
    )
)