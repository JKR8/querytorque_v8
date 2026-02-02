SELECT t2.CA_COUNTY, t2.D_YEAR, CAST(t18.WEB_SALES AS DECIMAL(19, 4)) / t14.WEB_SALES AS WEB_Q1_Q2_INCREASE, CAST(t6.STORE_SALES AS DECIMAL(19, 4)) / t2.STORE_SALES AS STORE_Q1_Q2_INCREASE, CAST(t22.WEB_SALES AS DECIMAL(19, 4)) / t18.WEB_SALES AS WEB_Q2_Q3_INCREASE, CAST(t10.STORE_SALES AS DECIMAL(19, 4)) / t6.STORE_SALES AS STORE_Q2_Q3_INCREASE
FROM (SELECT customer_address.ca_county AS CA_COUNTY, date_dim.d_qoy AS D_QOY, date_dim.d_year AS D_YEAR, SUM(store_sales.ss_ext_sales_price) AS STORE_SALES
FROM store_sales
INNER JOIN date_dim ON store_sales.ss_sold_date_sk = date_dim.d_date_sk
INNER JOIN customer_address ON store_sales.ss_addr_sk = customer_address.ca_address_sk
GROUP BY customer_address.ca_county, date_dim.d_qoy, date_dim.d_year
HAVING date_dim.d_qoy = 1 AND date_dim.d_year = 2000) AS t2
INNER JOIN (SELECT customer_address0.ca_county AS CA_COUNTY, date_dim0.d_qoy AS D_QOY, date_dim0.d_year AS D_YEAR, SUM(store_sales0.ss_ext_sales_price) AS STORE_SALES
FROM store_sales AS store_sales0
INNER JOIN date_dim AS date_dim0 ON store_sales0.ss_sold_date_sk = date_dim0.d_date_sk
INNER JOIN customer_address AS customer_address0 ON store_sales0.ss_addr_sk = customer_address0.ca_address_sk
GROUP BY customer_address0.ca_county, date_dim0.d_qoy, date_dim0.d_year
HAVING date_dim0.d_qoy = 2 AND date_dim0.d_year = 2000) AS t6 ON t2.CA_COUNTY = t6.CA_COUNTY
INNER JOIN (SELECT customer_address1.ca_county AS CA_COUNTY, date_dim1.d_qoy AS D_QOY, date_dim1.d_year AS D_YEAR, SUM(store_sales1.ss_ext_sales_price) AS STORE_SALES
FROM store_sales AS store_sales1
INNER JOIN date_dim AS date_dim1 ON store_sales1.ss_sold_date_sk = date_dim1.d_date_sk
INNER JOIN customer_address AS customer_address1 ON store_sales1.ss_addr_sk = customer_address1.ca_address_sk
GROUP BY customer_address1.ca_county, date_dim1.d_qoy, date_dim1.d_year
HAVING date_dim1.d_qoy = 3 AND date_dim1.d_year = 2000) AS t10 ON t6.CA_COUNTY = t10.CA_COUNTY
INNER JOIN (SELECT customer_address2.ca_county AS CA_COUNTY, date_dim2.d_qoy AS D_QOY, date_dim2.d_year AS D_YEAR, SUM(web_sales.ws_ext_sales_price) AS WEB_SALES
FROM web_sales
INNER JOIN date_dim AS date_dim2 ON web_sales.ws_sold_date_sk = date_dim2.d_date_sk
INNER JOIN customer_address AS customer_address2 ON web_sales.ws_bill_addr_sk = customer_address2.ca_address_sk
GROUP BY customer_address2.ca_county, date_dim2.d_qoy, date_dim2.d_year
HAVING date_dim2.d_qoy = 1 AND date_dim2.d_year = 2000) AS t14 ON t2.CA_COUNTY = t14.CA_COUNTY
INNER JOIN (SELECT customer_address3.ca_county AS CA_COUNTY, date_dim3.d_qoy AS D_QOY, date_dim3.d_year AS D_YEAR, SUM(web_sales0.ws_ext_sales_price) AS WEB_SALES
FROM web_sales AS web_sales0
INNER JOIN date_dim AS date_dim3 ON web_sales0.ws_sold_date_sk = date_dim3.d_date_sk
INNER JOIN customer_address AS customer_address3 ON web_sales0.ws_bill_addr_sk = customer_address3.ca_address_sk
GROUP BY customer_address3.ca_county, date_dim3.d_qoy, date_dim3.d_year
HAVING date_dim3.d_qoy = 2 AND date_dim3.d_year = 2000) AS t18 ON t14.CA_COUNTY = t18.CA_COUNTY AND CASE WHEN t14.WEB_SALES > 0 THEN CAST(t18.WEB_SALES AS DECIMAL(19, 4)) / t14.WEB_SALES ELSE NULL END > CASE WHEN t2.STORE_SALES > 0 THEN CAST(t6.STORE_SALES AS DECIMAL(19, 4)) / t2.STORE_SALES ELSE NULL END
INNER JOIN (SELECT customer_address4.ca_county AS CA_COUNTY, date_dim4.d_qoy AS D_QOY, date_dim4.d_year AS D_YEAR, SUM(web_sales1.ws_ext_sales_price) AS WEB_SALES
FROM web_sales AS web_sales1
INNER JOIN date_dim AS date_dim4 ON web_sales1.ws_sold_date_sk = date_dim4.d_date_sk
INNER JOIN customer_address AS customer_address4 ON web_sales1.ws_bill_addr_sk = customer_address4.ca_address_sk
GROUP BY customer_address4.ca_county, date_dim4.d_qoy, date_dim4.d_year
HAVING date_dim4.d_qoy = 3 AND date_dim4.d_year = 2000) AS t22 ON t14.CA_COUNTY = t22.CA_COUNTY AND CASE WHEN t18.WEB_SALES > 0 THEN t22.WEB_SALES * 1.0000 / t18.WEB_SALES ELSE NULL END > CASE WHEN t6.STORE_SALES > 0 THEN t10.STORE_SALES * 1.0000 / t6.STORE_SALES ELSE NULL END
ORDER BY t2.CA_COUNTY