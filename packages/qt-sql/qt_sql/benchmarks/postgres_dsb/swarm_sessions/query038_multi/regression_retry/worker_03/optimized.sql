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
store_sales_distinct AS (
    SELECT DISTINCT
        c_last_name,
        c_first_name,
        d_date
    FROM store_sales
    JOIN filtered_date ON store_sales.ss_sold_date_sk = filtered_date.d_date_sk
    JOIN filtered_customer ON store_sales.ss_customer_sk = filtered_customer.c_customer_sk
    WHERE ss_list_price BETWEEN 25 AND 84
      AND ss_wholesale_cost BETWEEN 34 AND 54
),
catalog_sales_distinct AS (
    SELECT DISTINCT
        c_last_name,
        c_first_name,
        d_date
    FROM catalog_sales
    JOIN filtered_date ON catalog_sales.cs_sold_date_sk = filtered_date.d_date_sk
    JOIN filtered_customer ON catalog_sales.cs_bill_customer_sk = filtered_customer.c_customer_sk
    WHERE cs_list_price BETWEEN 25 AND 84
      AND cs_wholesale_cost BETWEEN 34 AND 54
),
web_sales_distinct AS (
    SELECT DISTINCT
        c_last_name,
        c_first_name,
        d_date
    FROM web_sales
    JOIN filtered_date ON web_sales.ws_sold_date_sk = filtered_date.d_date_sk
    JOIN filtered_customer ON web_sales.ws_bill_customer_sk = filtered_customer.c_customer_sk
    WHERE ws_list_price BETWEEN 25 AND 84
      AND ws_wholesale_cost BETWEEN 34 AND 54
)
SELECT COUNT(*)
FROM store_sales_distinct s
JOIN catalog_sales_distinct c
  ON s.c_last_name = c.c_last_name
 AND s.c_first_name = c.c_first_name
 AND s.d_date = c.d_date
JOIN web_sales_distinct w
  ON s.c_last_name = w.c_last_name
 AND s.c_first_name = w.c_first_name
 AND s.d_date = w.d_date
LIMIT 100;