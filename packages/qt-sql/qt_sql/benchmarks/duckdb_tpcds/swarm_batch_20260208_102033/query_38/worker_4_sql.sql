WITH date_filter AS (
    SELECT d_date_sk, d_date
    FROM date_dim
    WHERE d_month_seq BETWEEN 1183 AND 1183 + 11
),
store_customers AS (
    SELECT DISTINCT 
        c.c_last_name,
        c.c_first_name,
        df.d_date
    FROM store_sales ss
    JOIN date_filter df ON ss.ss_sold_date_sk = df.d_date_sk
    JOIN customer c ON ss.ss_customer_sk = c.c_customer_sk
),
catalog_customers AS (
    SELECT DISTINCT 
        c.c_last_name,
        c.c_first_name,
        df.d_date
    FROM catalog_sales cs
    JOIN date_filter df ON cs.cs_sold_date_sk = df.d_date_sk
    JOIN customer c ON cs.cs_bill_customer_sk = c.c_customer_sk
),
web_customers AS (
    SELECT DISTINCT 
        c.c_last_name,
        c.c_first_name,
        df.d_date
    FROM web_sales ws
    JOIN date_filter df ON ws.ws_sold_date_sk = df.d_date_sk
    JOIN customer c ON ws.ws_bill_customer_sk = c.c_customer_sk
)
SELECT COUNT(*)
FROM store_customers sc
WHERE EXISTS (
    SELECT 1
    FROM catalog_customers cc
    WHERE cc.c_last_name = sc.c_last_name
      AND cc.c_first_name = sc.c_first_name
      AND cc.d_date = sc.d_date
)
AND EXISTS (
    SELECT 1
    FROM web_customers wc
    WHERE wc.c_last_name = sc.c_last_name
      AND wc.c_first_name = sc.c_first_name
      AND wc.d_date = sc.d_date
)
LIMIT 100;