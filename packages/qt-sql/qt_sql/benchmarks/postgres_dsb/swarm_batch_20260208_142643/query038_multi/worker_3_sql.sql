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
store_sales_filtered AS (
    SELECT DISTINCT
        c.c_last_name,
        c.c_first_name,
        d.d_date
    FROM store_sales ss
    JOIN filtered_date d ON ss.ss_sold_date_sk = d.d_date_sk
    JOIN filtered_customer c ON ss.ss_customer_sk = c.c_customer_sk
    WHERE ss.ss_list_price BETWEEN 25 AND 84
      AND ss.ss_wholesale_cost BETWEEN 34 AND 54
),
catalog_sales_filtered AS (
    SELECT DISTINCT
        c.c_last_name,
        c.c_first_name,
        d.d_date
    FROM catalog_sales cs
    JOIN filtered_date d ON cs.cs_sold_date_sk = d.d_date_sk
    JOIN filtered_customer c ON cs.cs_bill_customer_sk = c.c_customer_sk
    WHERE cs.cs_list_price BETWEEN 25 AND 84
      AND cs.cs_wholesale_cost BETWEEN 34 AND 54
),
web_sales_filtered AS (
    SELECT DISTINCT
        c.c_last_name,
        c.c_first_name,
        d.d_date
    FROM web_sales ws
    JOIN filtered_date d ON ws.ws_sold_date_sk = d.d_date_sk
    JOIN filtered_customer c ON ws.ws_bill_customer_sk = c.c_customer_sk
    WHERE ws.ws_list_price BETWEEN 25 AND 84
      AND ws.ws_wholesale_cost BETWEEN 34 AND 54
)
SELECT COUNT(*)
FROM (
    SELECT * FROM store_sales_filtered
    INTERSECT
    SELECT * FROM catalog_sales_filtered
    INTERSECT
    SELECT * FROM web_sales_filtered
) AS hot_cust
LIMIT 100;