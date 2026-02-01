WITH filtered_sales AS (
    SELECT ws.ws_order_number, 
           ws.ws_warehouse_sk,
           ws.ws_ext_ship_cost, 
           ws.ws_net_profit
    FROM web_sales ws
    JOIN date_dim d ON ws.ws_ship_date_sk = d.d_date_sk
                   AND d.d_date BETWEEN '1999-2-01' AND (CAST('1999-2-01' AS DATE) + INTERVAL '60 DAY')
    JOIN customer_address ca ON ws.ws_ship_addr_sk = ca.ca_address_sk AND ca.ca_state = 'NC'
    JOIN web_site site ON ws.ws_web_site_sk = site.web_site_sk AND site.web_company_name = 'pri'
),
ws_wh AS (
    SELECT DISTINCT ws1.ws_order_number
    FROM filtered_sales ws1
    JOIN filtered_sales ws2 ON ws1.ws_order_number = ws2.ws_order_number
    WHERE ws1.ws_warehouse_sk <> ws2.ws_warehouse_sk
)
SELECT 
    COUNT(DISTINCT ws.ws_order_number) AS "order count",
    SUM(ws.ws_ext_ship_cost) AS "total shipping cost",
    SUM(ws.ws_net_profit) AS "total net profit"
FROM filtered_sales ws
WHERE ws.ws_order_number IN (SELECT ws_order_number FROM ws_wh)
  AND EXISTS (SELECT 1 FROM web_returns wr WHERE wr.wr_order_number = ws.ws_order_number)
ORDER BY 1
LIMIT 100;