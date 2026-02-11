WITH filtered_dims AS (
    SELECT 
        d_date_sk,
        d_date
    FROM date_dim
    WHERE d_date BETWEEN '2000-2-01' AND (
        CAST('2000-2-01' AS DATE) + INTERVAL '60' DAY
    )
),
filtered_address AS (
    SELECT 
        ca_address_sk
    FROM customer_address
    WHERE ca_state = 'OK'
),
filtered_web_site AS (
    SELECT 
        web_site_sk
    FROM web_site
    WHERE web_company_name = 'pri'
)
SELECT
    COUNT(DISTINCT ws_order_number) AS "order count",
    SUM(ws_ext_ship_cost) AS "total shipping cost",
    SUM(ws_net_profit) AS "total net profit"
FROM web_sales AS ws1
JOIN filtered_dims ON ws1.ws_ship_date_sk = filtered_dims.d_date_sk
JOIN filtered_address ON ws1.ws_ship_addr_sk = filtered_address.ca_address_sk
JOIN filtered_web_site ON ws1.ws_web_site_sk = filtered_web_site.web_site_sk
WHERE EXISTS(
    SELECT 1
    FROM web_sales AS ws2
    WHERE ws1.ws_order_number = ws2.ws_order_number
      AND ws1.ws_warehouse_sk <> ws2.ws_warehouse_sk
)
AND NOT EXISTS(
    SELECT 1
    FROM web_returns AS wr1
    WHERE ws1.ws_order_number = wr1.wr_order_number
)
ORDER BY
    COUNT(DISTINCT ws_order_number)
LIMIT 100