WITH filtered_data AS (
    SELECT 
        ws.ws_order_number,
        ws.ws_ext_ship_cost,
        ws.ws_net_profit,
        ws.ws_warehouse_sk
    FROM web_sales ws
    INNER JOIN date_dim dd ON ws.ws_ship_date_sk = dd.d_date_sk
    INNER JOIN customer_address ca ON ws.ws_ship_addr_sk = ca.ca_address_sk
    INNER JOIN web_site wsit ON ws.ws_web_site_sk = wsit.web_site_sk
    WHERE dd.d_date BETWEEN '2000-2-01' AND (CAST('2000-2-01' AS DATE) + INTERVAL 60 DAY)
        AND ca.ca_state = 'OK'
        AND wsit.web_company_name = 'pri'
)
SELECT 
    COUNT(DISTINCT fd1.ws_order_number) as "order count",
    SUM(fd1.ws_ext_ship_cost) as "total shipping cost",
    SUM(fd1.ws_net_profit) as "total net profit"
FROM filtered_data fd1
WHERE EXISTS (
    SELECT 1
    FROM filtered_data fd2
    WHERE fd1.ws_order_number = fd2.ws_order_number
        AND fd1.ws_warehouse_sk <> fd2.ws_warehouse_sk
)
AND NOT EXISTS (
    SELECT 1
    FROM web_returns wr
    WHERE fd1.ws_order_number = wr.wr_order_number
)
ORDER BY COUNT(DISTINCT fd1.ws_order_number)
LIMIT 100;