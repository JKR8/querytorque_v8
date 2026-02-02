SELECT SUBSTRING(reason.r_reason_desc, 1, 20), AVG(web_sales.ws_quantity) AS AVG1, AVG(web_returns.wr_refunded_cash) AS AVG2, AVG(web_returns.wr_fee)
FROM web_sales
INNER JOIN web_returns ON web_sales.ws_item_sk = web_returns.wr_item_sk AND web_sales.ws_order_number = web_returns.wr_order_number
INNER JOIN web_page ON web_sales.ws_web_page_sk = web_page.wp_web_page_sk
INNER JOIN customer_demographics ON web_returns.wr_refunded_cdemo_sk = customer_demographics.cd_demo_sk
INNER JOIN customer_demographics AS customer_demographics0 ON web_returns.wr_returning_cdemo_sk = customer_demographics0.cd_demo_sk AND (customer_demographics.cd_marital_status = 'M' AND customer_demographics.cd_marital_status = customer_demographics0.cd_marital_status AND customer_demographics.cd_education_status = 'Advanced Degree' AND customer_demographics.cd_education_status = customer_demographics0.cd_education_status AND (web_sales.ws_sales_price >= 100.00 AND web_sales.ws_sales_price <= 150.00) OR customer_demographics.cd_marital_status = 'S' AND customer_demographics.cd_marital_status = customer_demographics0.cd_marital_status AND customer_demographics.cd_education_status = 'College' AND customer_demographics.cd_education_status = customer_demographics0.cd_education_status AND (web_sales.ws_sales_price >= 50.00 AND web_sales.ws_sales_price <= 100.00) OR customer_demographics.cd_marital_status = 'W' AND customer_demographics.cd_marital_status = customer_demographics0.cd_marital_status AND customer_demographics.cd_education_status = '2 yr Degree' AND customer_demographics.cd_education_status = customer_demographics0.cd_education_status AND (web_sales.ws_sales_price >= 150.00 AND web_sales.ws_sales_price <= 200.00))
INNER JOIN customer_address ON web_returns.wr_refunded_addr_sk = customer_address.ca_address_sk AND (customer_address.ca_country = 'United States' AND customer_address.ca_state IN ('IN', 'NJ', 'OH') AND (web_sales.ws_net_profit >= 100 AND web_sales.ws_net_profit <= 200) OR customer_address.ca_country = 'United States' AND customer_address.ca_state IN ('CT', 'KY', 'WI') AND (web_sales.ws_net_profit >= 150 AND web_sales.ws_net_profit <= 300) OR customer_address.ca_country = 'United States' AND customer_address.ca_state IN ('AR', 'IA', 'LA') AND (web_sales.ws_net_profit >= 50 AND web_sales.ws_net_profit <= 250))
INNER JOIN (SELECT *
FROM date_dim
WHERE d_year = 2000) AS t ON web_sales.ws_sold_date_sk = t.d_date_sk
INNER JOIN reason ON web_returns.wr_reason_sk = reason.r_reason_sk
GROUP BY reason.r_reason_desc
ORDER BY 1, 2, 3, 4
FETCH NEXT 100 ROWS ONLY