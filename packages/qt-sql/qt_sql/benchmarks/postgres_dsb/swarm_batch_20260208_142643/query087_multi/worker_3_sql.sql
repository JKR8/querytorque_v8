WITH filtered_date AS (
    SELECT d_date_sk, d_date
    FROM date_dim
    WHERE d_month_seq BETWEEN 1194 AND 1194 + 11
),
filtered_customer AS (
    SELECT c_customer_sk, c_last_name, c_first_name
    FROM customer
    WHERE c_birth_year BETWEEN 1943 AND 1949
),
store_sales_data AS (
    SELECT DISTINCT
        cust.c_last_name,
        cust.c_first_name,
        dt.d_date
    FROM store_sales ss
    INNER JOIN filtered_date dt ON ss.ss_sold_date_sk = dt.d_date_sk
    INNER JOIN filtered_customer cust ON ss.ss_customer_sk = cust.c_customer_sk
    WHERE ss.ss_list_price BETWEEN 217 AND 246
      AND ss.ss_wholesale_cost BETWEEN 34 AND 44
),
catalog_sales_data AS (
    SELECT DISTINCT
        cust.c_last_name,
        cust.c_first_name,
        dt.d_date
    FROM catalog_sales cs
    INNER JOIN filtered_date dt ON cs.cs_sold_date_sk = dt.d_date_sk
    INNER JOIN filtered_customer cust ON cs.cs_bill_customer_sk = cust.c_customer_sk
    WHERE cs.cs_list_price BETWEEN 217 AND 246
      AND cs.cs_wholesale_cost BETWEEN 34 AND 44
),
web_sales_data AS (
    SELECT DISTINCT
        cust.c_last_name,
        cust.c_first_name,
        dt.d_date
    FROM web_sales ws
    INNER JOIN filtered_date dt ON ws.ws_sold_date_sk = dt.d_date_sk
    INNER JOIN filtered_customer cust ON ws.ws_bill_customer_sk = cust.c_customer_sk
    WHERE ws.ws_list_price BETWEEN 217 AND 246
      AND ws.ws_wholesale_cost BETWEEN 34 AND 44
)
SELECT COUNT(*)
FROM (
    (SELECT * FROM store_sales_data)
    EXCEPT
    (SELECT * FROM catalog_sales_data)
    EXCEPT
    (SELECT * FROM web_sales_data)
) AS cool_cust;