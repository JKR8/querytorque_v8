SELECT t15.SEGMENT, COUNT(*) AS NUM_CUSTOMERS, t15.SEGMENT * 50 AS SEGMENT_BASE
FROM (SELECT CAST(ROUND(SUM(store_sales.ss_ext_sales_price) / 50) AS INTEGER) AS SEGMENT
FROM (SELECT customer.c_customer_sk AS C_CUSTOMER_SK, customer.c_current_addr_sk AS C_CURRENT_ADDR_SK
FROM (SELECT cs_sold_date_sk AS SOLD_DATE_SK, cs_bill_customer_sk AS CUSTOMER_SK, cs_item_sk AS ITEM_SK
FROM catalog_sales
UNION ALL
SELECT ws_sold_date_sk AS SOLD_DATE_SK, ws_bill_customer_sk AS CUSTOMER_SK, ws_item_sk AS ITEM_SK
FROM web_sales) AS t1
INNER JOIN (SELECT *
FROM item
WHERE i_category = 'Women' AND i_class = 'maternity') AS t2 ON t1.ITEM_SK = t2.i_item_sk
INNER JOIN (SELECT *
FROM date_dim
WHERE d_moy = 12 AND d_year = 1998) AS t3 ON t1.SOLD_DATE_SK = t3.d_date_sk
INNER JOIN customer ON t1.CUSTOMER_SK = customer.c_customer_sk
GROUP BY customer.c_customer_sk, customer.c_current_addr_sk) AS t5
INNER JOIN store_sales ON t5.C_CUSTOMER_SK = store_sales.ss_customer_sk
INNER JOIN customer_address ON t5.C_CURRENT_ADDR_SK = customer_address.ca_address_sk
INNER JOIN store ON customer_address.ca_county = store.s_county AND customer_address.ca_state = store.s_state
INNER JOIN (SELECT *
FROM date_dim
WHERE d_month_seq >= (((SELECT d_month_seq + 1
FROM date_dim
WHERE d_year = 1998 AND d_moy = 12
GROUP BY d_month_seq + 1))) AND d_month_seq <= (((SELECT d_month_seq + 3
FROM date_dim
WHERE d_year = 1998 AND d_moy = 12
GROUP BY d_month_seq + 3)))) AS t12 ON store_sales.ss_sold_date_sk = t12.d_date_sk
GROUP BY t5.C_CUSTOMER_SK) AS t15
GROUP BY t15.SEGMENT
ORDER BY t15.SEGMENT NULLS FIRST, 2 NULLS FIRST, 3
FETCH NEXT 100 ROWS ONLY