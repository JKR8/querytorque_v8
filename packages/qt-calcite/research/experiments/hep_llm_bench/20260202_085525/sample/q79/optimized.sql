SELECT customer.c_last_name AS C_LAST_NAME, customer.c_first_name AS C_FIRST_NAME, SUBSTRING(t4.S_CITY, 1, 30), t4.SS_TICKET_NUMBER, t4.AMT, t4.PROFIT
FROM (SELECT store_sales.ss_ticket_number AS SS_TICKET_NUMBER, store_sales.ss_customer_sk AS SS_CUSTOMER_SK, t0.s_city AS S_CITY, SUM(store_sales.ss_coupon_amt) AS AMT, SUM(store_sales.ss_net_profit) AS PROFIT
FROM store_sales
INNER JOIN (SELECT *
FROM date_dim
WHERE d_dow = 1 AND (d_year = 1999 OR d_year = CAST(1999 + 1 AS BIGINT) OR d_year = CAST(1999 + 2 AS BIGINT))) AS t ON store_sales.ss_sold_date_sk = t.d_date_sk
INNER JOIN (SELECT *
FROM store
WHERE s_number_employees >= 200 AND s_number_employees <= 295) AS t0 ON store_sales.ss_store_sk = t0.s_store_sk
INNER JOIN (SELECT *
FROM household_demographics
WHERE hd_dep_count = 6 OR hd_vehicle_count > 2) AS t1 ON store_sales.ss_hdemo_sk = t1.hd_demo_sk
GROUP BY store_sales.ss_ticket_number, store_sales.ss_customer_sk, store_sales.ss_addr_sk, t0.s_city) AS t4
INNER JOIN customer ON t4.SS_CUSTOMER_SK = customer.c_customer_sk
ORDER BY customer.c_last_name NULLS FIRST, customer.c_first_name NULLS FIRST, 3 NULLS FIRST, t4.PROFIT NULLS FIRST, t4.SS_TICKET_NUMBER
FETCH NEXT 100 ROWS ONLY