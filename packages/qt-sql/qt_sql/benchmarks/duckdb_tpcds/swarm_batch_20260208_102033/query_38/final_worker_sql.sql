WITH filtered_dates AS (
    SELECT d_date_sk, d_date
    FROM date_dim
    WHERE d_month_seq BETWEEN 1183 AND 1183 + 11
),
customer_dates AS (
    -- Store sales
    SELECT DISTINCT 
        customer.c_customer_sk,
        filtered_dates.d_date,
        1 AS channel
    FROM store_sales
    JOIN filtered_dates ON store_sales.ss_sold_date_sk = filtered_dates.d_date_sk
    JOIN customer ON store_sales.ss_customer_sk = customer.c_customer_sk
    
    UNION ALL
    
    -- Catalog sales
    SELECT DISTINCT 
        customer.c_customer_sk,
        filtered_dates.d_date,
        2 AS channel
    FROM catalog_sales
    JOIN filtered_dates ON catalog_sales.cs_sold_date_sk = filtered_dates.d_date_sk
    JOIN customer ON catalog_sales.cs_bill_customer_sk = customer.c_customer_sk
    
    UNION ALL
    
    -- Web sales
    SELECT DISTINCT 
        customer.c_customer_sk,
        filtered_dates.d_date,
        3 AS channel
    FROM web_sales
    JOIN filtered_dates ON web_sales.ws_sold_date_sk = filtered_dates.d_date_sk
    JOIN customer ON web_sales.ws_bill_customer_sk = customer.c_customer_sk
),
triple_channel_customers AS (
    SELECT c_customer_sk, d_date
    FROM customer_dates
    GROUP BY c_customer_sk, d_date
    HAVING COUNT(DISTINCT channel) = 3
)
SELECT COUNT(*)
FROM (
    SELECT DISTINCT 
        customer.c_last_name,
        customer.c_first_name,
        triple_channel_customers.d_date
    FROM triple_channel_customers
    JOIN customer ON triple_channel_customers.c_customer_sk = customer.c_customer_sk
) AS hot_cust
LIMIT 100