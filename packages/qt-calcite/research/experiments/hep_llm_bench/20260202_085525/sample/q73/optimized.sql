SELECT customer.c_last_name AS C_LAST_NAME, customer.c_first_name AS C_FIRST_NAME, customer.c_salutation AS C_SALUTATION, customer.c_preferred_cust_flag AS C_PREFERRED_CUST_FLAG, t5.SS_TICKET_NUMBER, t5.CNT
FROM (SELECT *
FROM (SELECT store_sales.ss_ticket_number AS SS_TICKET_NUMBER, store_sales.ss_customer_sk AS SS_CUSTOMER_SK, COUNT(*) AS CNT
FROM store_sales
INNER JOIN (SELECT *
FROM date_dim
WHERE d_dom >= 1 AND d_dom <= 2 AND (d_year = 1999 OR d_year = CAST(1999 + 1 AS BIGINT) OR d_year = CAST(1999 + 2 AS BIGINT))) AS t ON store_sales.ss_sold_date_sk = t.d_date_sk
INNER JOIN (SELECT *
FROM store
WHERE s_county IN ('Bronx County', 'Franklin Parish', 'Orange County', 'Williamson County')) AS t0 ON store_sales.ss_store_sk = t0.s_store_sk
INNER JOIN (SELECT *
FROM household_demographics
WHERE hd_buy_potential IN ('>10000', 'Unknown') AND hd_vehicle_count > 0 AND CASE WHEN hd_vehicle_count > 0 THEN CAST(hd_dep_count AS DECIMAL(19, 3)) / hd_vehicle_count ELSE NULL END > 1) AS t1 ON store_sales.ss_hdemo_sk = t1.hd_demo_sk
GROUP BY store_sales.ss_ticket_number, store_sales.ss_customer_sk) AS t4
WHERE t4.CNT >= 1 AND t4.CNT <= 5) AS t5
INNER JOIN customer ON t5.SS_CUSTOMER_SK = customer.c_customer_sk
ORDER BY t5.CNT DESC, customer.c_last_name