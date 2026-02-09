WITH filtered_dates AS (
    SELECT d_date_sk, d_date
    FROM date_dim
    WHERE d_month_seq BETWEEN 1189 AND 1189 + 11
)
SELECT COUNT(*)
FROM (
    SELECT DISTINCT c_last_name, c_first_name, d_date
    FROM store_sales
    JOIN filtered_dates ON store_sales.ss_sold_date_sk = filtered_dates.d_date_sk
    JOIN customer ON store_sales.ss_customer_sk = customer.c_customer_sk
    WHERE c_birth_month IN (4, 9, 10, 12)
        AND ss_list_price BETWEEN 25 AND 84
        AND ss_wholesale_cost BETWEEN 34 AND 54
    INTERSECT
    SELECT DISTINCT c_last_name, c_first_name, d_date
    FROM catalog_sales
    JOIN filtered_dates ON catalog_sales.cs_sold_date_sk = filtered_dates.d_date_sk
    JOIN customer ON catalog_sales.cs_bill_customer_sk = customer.c_customer_sk
    WHERE c_birth_month IN (4, 9, 10, 12)
        AND cs_list_price BETWEEN 25 AND 84
        AND cs_wholesale_cost BETWEEN 34 AND 54
    INTERSECT
    SELECT DISTINCT c_last_name, c_first_name, d_date
    FROM web_sales
    JOIN filtered_dates ON web_sales.ws_sold_date_sk = filtered_dates.d_date_sk
    JOIN customer ON web_sales.ws_bill_customer_sk = customer.c_customer_sk
    WHERE c_birth_month IN (4, 9, 10, 12)
        AND ws_list_price BETWEEN 25 AND 84
        AND ws_wholesale_cost BETWEEN 34 AND 54
) AS hot_cust
LIMIT 100;