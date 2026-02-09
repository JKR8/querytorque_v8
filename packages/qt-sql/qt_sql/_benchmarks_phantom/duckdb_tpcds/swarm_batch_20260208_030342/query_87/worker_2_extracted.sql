WITH filtered_date AS (
    SELECT d_date_sk, d_date
    FROM date_dim
    WHERE d_month_seq BETWEEN 1184 AND 1184 + 11
),
filtered_customer AS (
    SELECT c_customer_sk, c_last_name, c_first_name
    FROM customer
),
store_combinations AS (
    SELECT DISTINCT c.c_last_name, c.c_first_name, d.d_date
    FROM store_sales ss
    JOIN filtered_date d ON ss.ss_sold_date_sk = d.d_date_sk
    JOIN filtered_customer c ON ss.ss_customer_sk = c.c_customer_sk
),
catalog_combinations AS (
    SELECT DISTINCT c.c_last_name, c.c_first_name, d.d_date
    FROM catalog_sales cs
    JOIN filtered_date d ON cs.cs_sold_date_sk = d.d_date_sk
    JOIN filtered_customer c ON cs.cs_bill_customer_sk = c.c_customer_sk
),
web_combinations AS (
    SELECT DISTINCT c.c_last_name, c.c_first_name, d.d_date
    FROM web_sales ws
    JOIN filtered_date d ON ws.ws_sold_date_sk = d.d_date_sk
    JOIN filtered_customer c ON ws.ws_bill_customer_sk = c.c_customer_sk
)
SELECT COUNT(*)
FROM (
    (SELECT * FROM store_combinations)
    EXCEPT
    (SELECT * FROM catalog_combinations)
    EXCEPT
    (SELECT * FROM web_combinations)
) AS cool_cust;