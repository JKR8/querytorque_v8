WITH filtered_items AS (
    SELECT i_item_sk, i_item_id
    FROM item
    WHERE i_item_sk IN (2, 3, 5, 7, 11, 13, 17, 19, 23, 29)
),
date_filter AS (
    SELECT d_date_sk
    FROM date_dim
    WHERE d_qoy = 2 AND d_year = 2000
)
SELECT ca_zip, ca_city, SUM(ws_sales_price)
FROM (
    -- First branch: ca_zip condition
    SELECT ws_sales_price, ca_zip, ca_city
    FROM web_sales ws
    JOIN date_filter df ON ws.ws_sold_date_sk = df.d_date_sk
    JOIN customer c ON ws.ws_bill_customer_sk = c.c_customer_sk
    JOIN customer_address ca ON c.c_current_addr_sk = ca.ca_address_sk
    JOIN item i ON ws.ws_item_sk = i.i_item_sk
    WHERE SUBSTR(ca.ca_zip, 1, 5) IN ('85669', '86197', '88274', '83405', '86475', 
                                      '85392', '85460', '80348', '81792')
    
    UNION ALL
    
    -- Second branch: item_id condition
    SELECT ws_sales_price, ca_zip, ca_city
    FROM web_sales ws
    JOIN date_filter df ON ws.ws_sold_date_sk = df.d_date_sk
    JOIN customer c ON ws.ws_bill_customer_sk = c.c_customer_sk
    JOIN customer_address ca ON c.c_current_addr_sk = ca.ca_address_sk
    JOIN filtered_items fi ON ws.ws_item_sk = fi.i_item_sk
) combined
GROUP BY ca_zip, ca_city
ORDER BY ca_zip, ca_city
LIMIT 100;