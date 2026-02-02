WITH ws_wh AS (
    SELECT DISTINCT ws1.ws_order_number
    FROM web_sales ws1
    JOIN web_sales ws2 ON ws1.ws_order_number = ws2.ws_order_number
    WHERE ws1.ws_warehouse_sk <> ws2.ws_warehouse_sk
)
SELECT 
    COUNT(DISTINCT ws1.ws_order_number) AS "order count",
    SUM(ws1.ws_ext_ship_cost) AS "total shipping cost",
    SUM(ws1.ws_net_profit) AS "total net profit"
FROM web_sales ws1
JOIN date_dim ON ws1.ws_ship_date_sk = d_date_sk
JOIN customer_address ON ws1.ws_ship_addr_sk = ca_address_sk
JOIN web_site ON ws1.ws_web_site_sk = web_site_sk
WHERE d_date BETWEEN DATE '1999-02-01' AND (DATE '1999-02-01' + INTERVAL '60' DAY)
    AND ca_state = 'NC'
    AND web_company_name = 'pri'
    AND EXISTS (
        SELECT 1 
        FROM ws_wh 
        WHERE ws_wh.ws_order_number = ws1.ws_order_number
    )
    AND EXISTS (
        SELECT 1 
        FROM web_returns wr
        JOIN ws_wh ON wr.wr_order_number = ws_wh.ws_order_number
        WHERE wr.wr_order_number = ws1.ws_order_number
    )
LIMIT 100;