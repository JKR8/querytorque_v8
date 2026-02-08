WITH filtered_date AS (
    SELECT d_date_sk, d_date
    FROM date_dim
    WHERE d_month_seq BETWEEN 1183 AND 1183 + 11
),
store_customers AS (
    SELECT DISTINCT
        c.c_last_name,
        c.c_first_name,
        d.d_date
    FROM store_sales ss
    JOIN filtered_date d ON ss.ss_sold_date_sk = d.d_date_sk
    JOIN customer c ON ss.ss_customer_sk = c.c_customer_sk
),
catalog_customers AS (
    SELECT DISTINCT
        c.c_last_name,
        c.c_first_name,
        d.d_date
    FROM catalog_sales cs
    JOIN filtered_date d ON cs.cs_sold_date_sk = d.d_date_sk
    JOIN customer c ON cs.cs_bill_customer_sk = c.c_customer_sk
),
web_customers AS (
    SELECT DISTINCT
        c.c_last_name,
        c.c_first_name,
        d.d_date
    FROM web_sales ws
    JOIN filtered_date d ON ws.ws_sold_date_sk = d.d_date_sk
    JOIN customer c ON ws.ws_bill_customer_sk = c.c_customer_sk
)
SELECT COUNT(*)
FROM (
    SELECT * FROM store_customers
    INTERSECT
    SELECT * FROM catalog_customers
    INTERSECT
    SELECT * FROM web_customers
) AS hot_cust
LIMIT 100;