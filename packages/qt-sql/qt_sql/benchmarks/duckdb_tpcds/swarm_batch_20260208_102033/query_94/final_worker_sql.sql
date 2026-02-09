WITH filtered_ws AS (
    SELECT 
        ws.ws_order_number,
        ws.ws_ext_ship_cost,
        ws.ws_net_profit,
        ws.ws_warehouse_sk
    FROM web_sales ws
    JOIN date_dim d ON ws.ws_ship_date_sk = d.d_date_sk
    JOIN customer_address ca ON ws.ws_ship_addr_sk = ca.ca_address_sk
    JOIN web_site web ON ws.ws_web_site_sk = web.web_site_sk
    WHERE d.d_date BETWEEN '2000-2-01' AND (
        CAST('2000-2-01' AS DATE) + INTERVAL '60' DAY
    )
    AND ca.ca_state = 'OK'
    AND web.web_company_name = 'pri'
),
multi_warehouse_orders AS (
    SELECT DISTINCT ws_order_number
    FROM filtered_ws
    GROUP BY ws_order_number
    HAVING COUNT(DISTINCT ws_warehouse_sk) > 1
),
returned_orders AS (
    SELECT DISTINCT wr_order_number
    FROM web_returns
)
SELECT
    COUNT(DISTINCT fws.ws_order_number) AS "order count",
    SUM(fws.ws_ext_ship_cost) AS "total shipping cost",
    SUM(fws.ws_net_profit) AS "total net profit"
FROM filtered_ws fws
JOIN multi_warehouse_orders mwo 
    ON fws.ws_order_number = mwo.ws_order_number
WHERE NOT EXISTS (
    SELECT 1
    FROM returned_orders ro
    WHERE fws.ws_order_number = ro.wr_order_number
)
ORDER BY
    COUNT(DISTINCT fws.ws_order_number)
LIMIT 100