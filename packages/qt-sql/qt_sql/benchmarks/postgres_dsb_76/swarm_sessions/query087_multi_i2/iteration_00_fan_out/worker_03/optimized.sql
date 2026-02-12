WITH filtered_dates AS (SELECT d_date_sk, d_date FROM date_dim WHERE d_month_seq BETWEEN 1213 AND 1224),
     filtered_customers AS (SELECT c_customer_sk, c_last_name, c_first_name FROM customer WHERE c_birth_year BETWEEN 1968 AND 1974),
     all_sales_joined AS (SELECT fd.d_date,
       (SELECT c_last_name FROM filtered_customers WHERE c_customer_sk = ss.ss_customer_sk) as store_last_name,
       (SELECT c_first_name FROM filtered_customers WHERE c_customer_sk = ss.ss_customer_sk) as store_first_name,
       (SELECT c_last_name FROM filtered_customers WHERE c_customer_sk = cs.cs_bill_customer_sk) as catalog_last_name,
       (SELECT c_first_name FROM filtered_customers WHERE c_customer_sk = cs.cs_bill_customer_sk) as catalog_first_name,
       (SELECT c_last_name FROM filtered_customers WHERE c_customer_sk = ws.ws_bill_customer_sk) as web_last_name,
       (SELECT c_first_name FROM filtered_customers WHERE c_customer_sk = ws.ws_bill_customer_sk) as web_first_name
FROM filtered_dates fd
LEFT JOIN store_sales ss ON ss.ss_sold_date_sk = fd.d_date_sk 
  AND ss.ss_customer_sk IN (SELECT c_customer_sk FROM filtered_customers)
  AND ss.ss_list_price BETWEEN 168 AND 197 
  AND ss.ss_wholesale_cost BETWEEN 76 AND 86
LEFT JOIN catalog_sales cs ON cs.cs_sold_date_sk = fd.d_date_sk 
  AND cs.cs_bill_customer_sk IN (SELECT c_customer_sk FROM filtered_customers)
  AND cs.cs_list_price BETWEEN 168 AND 197 
  AND cs.cs_wholesale_cost BETWEEN 76 AND 86
LEFT JOIN web_sales ws ON ws.ws_sold_date_sk = fd.d_date_sk 
  AND ws.ws_bill_customer_sk IN (SELECT c_customer_sk FROM filtered_customers)
  AND ws.ws_list_price BETWEEN 168 AND 197 
  AND ws.ws_wholesale_cost BETWEEN 76 AND 86),
     distinct_combinations AS (SELECT DISTINCT
       store_last_name as c_last_name,
       store_first_name as c_first_name,
       d_date,
       CASE WHEN catalog_last_name IS NOT NULL THEN 1 ELSE 0 END as in_catalog,
       CASE WHEN web_last_name IS NOT NULL THEN 1 ELSE 0 END as in_web
FROM all_sales_joined
WHERE store_last_name IS NOT NULL AND store_first_name IS NOT NULL),
     filter_by_presence AS (SELECT c_last_name, c_first_name, d_date
FROM distinct_combinations
WHERE in_catalog = 0 AND in_web = 0)
SELECT COUNT(*) FROM filter_by_presence