-- start query 16 in stream 0 using template query16.tpl
SELECT 
   COUNT(DISTINCT cs1.cs_order_number) AS "order count",
   SUM(cs1.cs_ext_ship_cost) AS "total shipping cost",
   SUM(cs1.cs_net_profit) AS "total net profit"
FROM catalog_sales cs1
INNER JOIN date_dim ON cs1.cs_ship_date_sk = date_dim.d_date_sk
INNER JOIN customer_address ON cs1.cs_ship_addr_sk = customer_address.ca_address_sk
INNER JOIN call_center ON cs1.cs_call_center_sk = call_center.cc_call_center_sk
INNER JOIN catalog_sales cs2 ON cs1.cs_order_number = cs2.cs_order_number 
    AND cs1.cs_warehouse_sk <> cs2.cs_warehouse_sk
LEFT JOIN catalog_returns cr1 ON cs1.cs_order_number = cr1.cr_order_number
WHERE date_dim.d_date BETWEEN '2002-4-01' AND (CAST('2002-4-01' AS DATE) + INTERVAL 60 DAY)
    AND customer_address.ca_state = 'WV'
    AND call_center.cc_county IN ('Ziebach County', 'Luce County', 'Richland County', 
                                  'Daviess County', 'Barrow County')
    AND cr1.cr_order_number IS NULL
-- ORDER BY COUNT(DISTINCT cs_order_number)  -- Removed as unnecessary with LIMIT 100
LIMIT 100;

-- end query 16 in stream 0 using template query16.tpl