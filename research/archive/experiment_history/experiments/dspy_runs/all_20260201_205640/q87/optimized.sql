WITH all_sales AS (
    SELECT 
        c.c_last_name,
        c.c_first_name,
        d.d_date,
        'store' as sales_channel
    FROM store_sales ss
    JOIN date_dim d ON ss.ss_sold_date_sk = d.d_date_sk
    JOIN customer c ON ss.ss_customer_sk = c.c_customer_sk
    WHERE d.d_month_seq BETWEEN 1184 AND 1184+11
    
    UNION ALL
    
    SELECT 
        c.c_last_name,
        c.c_first_name,
        d.d_date,
        'catalog' as sales_channel
    FROM catalog_sales cs
    JOIN date_dim d ON cs.cs_sold_date_sk = d.d_date_sk
    JOIN customer c ON cs.cs_bill_customer_sk = c.c_customer_sk
    WHERE d.d_month_seq BETWEEN 1184 AND 1184+11
    
    UNION ALL
    
    SELECT 
        c.c_last_name,
        c.c_first_name,
        d.d_date,
        'web' as sales_channel
    FROM web_sales ws
    JOIN date_dim d ON ws.ws_sold_date_sk = d.d_date_sk
    JOIN customer c ON ws.ws_bill_customer_sk = c.c_customer_sk
    WHERE d.d_month_seq BETWEEN 1184 AND 1184+11
),
sales_counts AS (
    SELECT 
        c_last_name,
        c_first_name,
        d_date,
        COUNT(DISTINCT CASE WHEN sales_channel = 'store' THEN 1 END) as in_store,
        COUNT(DISTINCT CASE WHEN sales_channel = 'catalog' THEN 1 END) as in_catalog,
        COUNT(DISTINCT CASE WHEN sales_channel = 'web' THEN 1 END) as in_web
    FROM all_sales
    GROUP BY c_last_name, c_first_name, d_date
)
SELECT COUNT(*)
FROM sales_counts
WHERE in_store > 0 
  AND in_catalog = 0 
  AND in_web = 0;