WITH filtered_dates AS (
  SELECT d_date_sk
  FROM date_dim
  WHERE d_qoy = 2
    AND d_year = 2000
),
filtered_customers AS (
  SELECT DISTINCT c_customer_sk
  FROM customer_address
  JOIN customer ON c_current_addr_sk = ca_address_sk
  WHERE SUBSTRING(ca_zip, 1, 5) IN ('85669', '86197', '88274', '83405', '86475', '85392', '85460', '80348', '81792')
),
filtered_items AS (
  SELECT DISTINCT i_item_sk
  FROM item
  WHERE i_item_sk IN (2, 3, 5, 7, 11, 13, 17, 19, 23, 29)
),
filtered_sales AS (
  SELECT ws_sales_price, ws_bill_customer_sk
  FROM web_sales
  JOIN filtered_dates ON ws_sold_date_sk = d_date_sk
  WHERE EXISTS (
    SELECT 1 FROM filtered_customers WHERE c_customer_sk = ws_bill_customer_sk
  )
  OR EXISTS (
    SELECT 1 FROM filtered_items WHERE i_item_sk = ws_item_sk
  )
)
SELECT
  ca_zip,
  ca_city,
  SUM(ws_sales_price) AS "SUM(ws_sales_price)"
FROM filtered_sales
JOIN customer ON ws_bill_customer_sk = c_customer_sk
JOIN customer_address ON c_current_addr_sk = ca_address_sk
GROUP BY
  ca_zip,
  ca_city
ORDER BY
  ca_zip,
  ca_city
LIMIT 100