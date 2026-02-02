SELECT customer_address.ca_zip AS CA_ZIP, SUM(catalog_sales.cs_sales_price)
FROM catalog_sales
INNER JOIN customer ON catalog_sales.cs_bill_customer_sk = customer.c_customer_sk
INNER JOIN customer_address ON customer.c_current_addr_sk = customer_address.ca_address_sk AND (SUBSTRING(customer_address.ca_zip, 1, 5) = '85669' OR SUBSTRING(customer_address.ca_zip, 1, 5) = '86197' OR (SUBSTRING(customer_address.ca_zip, 1, 5) = '88274' OR (SUBSTRING(customer_address.ca_zip, 1, 5) = '83405' OR SUBSTRING(customer_address.ca_zip, 1, 5) = '86475')) OR (SUBSTRING(customer_address.ca_zip, 1, 5) = '85392' OR (SUBSTRING(customer_address.ca_zip, 1, 5) = '85460' OR SUBSTRING(customer_address.ca_zip, 1, 5) = '80348') OR (SUBSTRING(customer_address.ca_zip, 1, 5) = '81792' OR (customer_address.ca_state IN ('CA', 'GA', 'WA') OR catalog_sales.cs_sales_price > 500))))
INNER JOIN (SELECT *
FROM date_dim
WHERE d_qoy = 2 AND d_year = 2001) AS t ON catalog_sales.cs_sold_date_sk = t.d_date_sk
GROUP BY customer_address.ca_zip
ORDER BY customer_address.ca_zip NULLS FIRST
FETCH NEXT 100 ROWS ONLY