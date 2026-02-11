WITH filtered_date AS (
    SELECT 
        d_date_sk,
        d_date
    FROM date_dim
    WHERE d_month_seq BETWEEN 1184 AND 1184 + 11
),
store_customers AS (
    SELECT DISTINCT
        customer.c_last_name,
        customer.c_first_name,
        filtered_date.d_date
    FROM store_sales
    JOIN filtered_date ON store_sales.ss_sold_date_sk = filtered_date.d_date_sk
    JOIN customer ON store_sales.ss_customer_sk = customer.c_customer_sk
),
catalog_customers AS (
    SELECT DISTINCT
        customer.c_last_name,
        customer.c_first_name,
        filtered_date.d_date
    FROM catalog_sales
    JOIN filtered_date ON catalog_sales.cs_sold_date_sk = filtered_date.d_date_sk
    JOIN customer ON catalog_sales.cs_bill_customer_sk = customer.c_customer_sk
),
web_customers AS (
    SELECT DISTINCT
        customer.c_last_name,
        customer.c_first_name,
        filtered_date.d_date
    FROM web_sales
    JOIN filtered_date ON web_sales.ws_sold_date_sk = filtered_date.d_date_sk
    JOIN customer ON web_sales.ws_bill_customer_sk = customer.c_customer_sk
)
SELECT COUNT(*)
FROM store_customers sc
WHERE NOT EXISTS (
    SELECT 1 
    FROM catalog_customers cc 
    WHERE cc.c_last_name = sc.c_last_name 
      AND cc.c_first_name = sc.c_first_name 
      AND cc.d_date = sc.d_date
)
  AND NOT EXISTS (
    SELECT 1 
    FROM web_customers wc 
    WHERE wc.c_last_name = sc.c_last_name 
      AND wc.c_first_name = sc.c_first_name 
      AND wc.d_date = sc.d_date
);