WITH filtered_date AS (
    SELECT d_date_sk, d_date
    FROM date_dim
    WHERE d_month_seq BETWEEN 1189 AND 1189 + 11
),
filtered_customer AS (
    SELECT c_customer_sk, c_last_name, c_first_name
    FROM customer
    WHERE c_birth_month IN (4, 9, 10, 12)
),
all_sales AS (
    -- Store sales
    SELECT 
        cust.c_last_name,
        cust.c_first_name,
        date.d_date,
        1 AS channel_bit
    FROM store_sales sales
    INNER JOIN filtered_date date ON sales.ss_sold_date_sk = date.d_date_sk
    INNER JOIN filtered_customer cust ON sales.ss_customer_sk = cust.c_customer_sk
    WHERE sales.ss_list_price BETWEEN 25 AND 84
      AND sales.ss_wholesale_cost BETWEEN 34 AND 54
      
    UNION ALL
    
    -- Catalog sales  
    SELECT 
        cust.c_last_name,
        cust.c_first_name,
        date.d_date,
        2 AS channel_bit
    FROM catalog_sales sales
    INNER JOIN filtered_date date ON sales.cs_sold_date_sk = date.d_date_sk
    INNER JOIN filtered_customer cust ON sales.cs_bill_customer_sk = cust.c_customer_sk
    WHERE sales.cs_list_price BETWEEN 25 AND 84
      AND sales.cs_wholesale_cost BETWEEN 34 AND 54
      
    UNION ALL
    
    -- Web sales
    SELECT 
        cust.c_last_name,
        cust.c_first_name,
        date.d_date,
        4 AS channel_bit
    FROM web_sales sales
    INNER JOIN filtered_date date ON sales.ws_sold_date_sk = date.d_date_sk
    INNER JOIN filtered_customer cust ON sales.ws_bill_customer_sk = cust.c_customer_sk
    WHERE sales.ws_list_price BETWEEN 25 AND 84
      AND sales.ws_wholesale_cost BETWEEN 34 AND 54
)
SELECT COUNT(*)
FROM (
    SELECT c_last_name, c_first_name, d_date
    FROM all_sales
    GROUP BY c_last_name, c_first_name, d_date
    HAVING BIT_OR(channel_bit) = 7  -- 1|2|4 = 7 means present in all three channels
) AS hot_cust
LIMIT 100;