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
    SELECT 
        c_last_name,
        c_first_name,
        d_date,
        1 AS store_flag,
        0 AS catalog_flag,
        0 AS web_flag
    FROM store_sales
    JOIN filtered_date ON store_sales.ss_sold_date_sk = filtered_date.d_date_sk
    JOIN filtered_customer ON store_sales.ss_customer_sk = filtered_customer.c_customer_sk
    WHERE ss_list_price BETWEEN 25 AND 84
        AND ss_wholesale_cost BETWEEN 34 AND 54
    GROUP BY c_last_name, c_first_name, d_date
    
    UNION ALL
    
    SELECT 
        c_last_name,
        c_first_name,
        d_date,
        0 AS store_flag,
        1 AS catalog_flag,
        0 AS web_flag
    FROM catalog_sales
    JOIN filtered_date ON catalog_sales.cs_sold_date_sk = filtered_date.d_date_sk
    JOIN filtered_customer ON catalog_sales.cs_bill_customer_sk = filtered_customer.c_customer_sk
    WHERE cs_list_price BETWEEN 25 AND 84
        AND cs_wholesale_cost BETWEEN 34 AND 54
    GROUP BY c_last_name, c_first_name, d_date
    
    UNION ALL
    
    SELECT 
        c_last_name,
        c_first_name,
        d_date,
        0 AS store_flag,
        0 AS catalog_flag,
        1 AS web_flag
    FROM web_sales
    JOIN filtered_date ON web_sales.ws_sold_date_sk = filtered_date.d_date_sk
    JOIN filtered_customer ON web_sales.ws_bill_customer_sk = filtered_customer.c_customer_sk
    WHERE ws_list_price BETWEEN 25 AND 84
        AND ws_wholesale_cost BETWEEN 34 AND 54
    GROUP BY c_last_name, c_first_name, d_date
),
aggregated_sales AS (
    SELECT 
        c_last_name,
        c_first_name,
        d_date,
        MAX(store_flag) AS has_store,
        MAX(catalog_flag) AS has_catalog,
        MAX(web_flag) AS has_web
    FROM all_sales
    GROUP BY c_last_name, c_first_name, d_date
    HAVING MAX(store_flag) = 1 
        AND MAX(catalog_flag) = 1 
        AND MAX(web_flag) = 1
)
SELECT COUNT(*)
FROM aggregated_sales
LIMIT 100;