WITH filtered_date AS (
    SELECT d_date_sk, d_date
    FROM date_dim
    WHERE d_month_seq BETWEEN 1189 AND 1189 + 11
),
filtered_customer AS (
    SELECT c_customer_sk, c_last_name, c_first_name
    FROM customer
    WHERE c_birth_month IN (4, 9, 10, 12)
)
SELECT COUNT(*)
FROM (
    SELECT DISTINCT
        fc.c_last_name,
        fc.c_first_name,
        fd.d_date
    FROM store_sales
    JOIN filtered_date fd ON store_sales.ss_sold_date_sk = fd.d_date_sk
    JOIN filtered_customer fc ON store_sales.ss_customer_sk = fc.c_customer_sk
    WHERE ss_list_price BETWEEN 25 AND 84
      AND ss_wholesale_cost BETWEEN 34 AND 54
    INTERSECT
    SELECT DISTINCT
        fc.c_last_name,
        fc.c_first_name,
        fd.d_date
    FROM catalog_sales
    JOIN filtered_date fd ON catalog_sales.cs_sold_date_sk = fd.d_date_sk
    JOIN filtered_customer fc ON catalog_sales.cs_bill_customer_sk = fc.c_customer_sk
    WHERE cs_list_price BETWEEN 25 AND 84
      AND cs_wholesale_cost BETWEEN 34 AND 54
    INTERSECT
    SELECT DISTINCT
        fc.c_last_name,
        fc.c_first_name,
        fd.d_date
    FROM web_sales
    JOIN filtered_date fd ON web_sales.ws_sold_date_sk = fd.d_date_sk
    JOIN filtered_customer fc ON web_sales.ws_bill_customer_sk = fc.c_customer_sk
    WHERE ws_list_price BETWEEN 25 AND 84
      AND ws_wholesale_cost BETWEEN 34 AND 54
) AS hot_cust
LIMIT 100