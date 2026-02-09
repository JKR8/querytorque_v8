WITH filtered_dates AS (
    SELECT d_date_sk, d_date
    FROM date_dim
    WHERE d_month_seq BETWEEN 1184 AND 1184 + 11
),
store_customers AS (
    SELECT DISTINCT
        customer.c_last_name,
        customer.c_first_name,
        filtered_dates.d_date
    FROM store_sales
    INNER JOIN filtered_dates 
        ON store_sales.ss_sold_date_sk = filtered_dates.d_date_sk
    INNER JOIN customer 
        ON store_sales.ss_customer_sk = customer.c_customer_sk
),
catalog_customers AS (
    SELECT DISTINCT
        customer.c_last_name,
        customer.c_first_name,
        filtered_dates.d_date
    FROM catalog_sales
    INNER JOIN filtered_dates 
        ON catalog_sales.cs_sold_date_sk = filtered_dates.d_date_sk
    INNER JOIN customer 
        ON catalog_sales.cs_bill_customer_sk = customer.c_customer_sk
),
web_customers AS (
    SELECT DISTINCT
        customer.c_last_name,
        customer.c_first_name,
        filtered_dates.d_date
    FROM web_sales
    INNER JOIN filtered_dates 
        ON web_sales.ws_sold_date_sk = filtered_dates.d_date_sk
    INNER JOIN customer 
        ON web_sales.ws_bill_customer_sk = customer.c_customer_sk
)
SELECT COUNT(*)
FROM (
    (SELECT * FROM store_customers)
    EXCEPT
    (SELECT * FROM catalog_customers)
    EXCEPT
    (SELECT * FROM web_customers)
) AS cool_cust;