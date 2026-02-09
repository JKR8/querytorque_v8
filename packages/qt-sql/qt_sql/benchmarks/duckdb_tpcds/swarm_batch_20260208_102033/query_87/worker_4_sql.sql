WITH store_set AS (
    SELECT DISTINCT
        c_last_name,
        c_first_name,
        d_date
    FROM store_sales
    JOIN date_dim ON store_sales.ss_sold_date_sk = date_dim.d_date_sk
    JOIN customer ON store_sales.ss_customer_sk = customer.c_customer_sk
    WHERE d_month_seq BETWEEN 1184 AND 1184 + 11
)
SELECT COUNT(*)
FROM store_set
WHERE NOT EXISTS (
    SELECT 1
    FROM catalog_sales
    JOIN date_dim ON catalog_sales.cs_sold_date_sk = date_dim.d_date_sk
    JOIN customer ON catalog_sales.cs_bill_customer_sk = customer.c_customer_sk
    WHERE d_month_seq BETWEEN 1184 AND 1184 + 11
      AND store_set.c_last_name = customer.c_last_name
      AND store_set.c_first_name = customer.c_first_name
      AND store_set.d_date = date_dim.d_date
)
AND NOT EXISTS (
    SELECT 1
    FROM web_sales
    JOIN date_dim ON web_sales.ws_sold_date_sk = date_dim.d_date_sk
    JOIN customer ON web_sales.ws_bill_customer_sk = customer.c_customer_sk
    WHERE d_month_seq BETWEEN 1184 AND 1184 + 11
      AND store_set.c_last_name = customer.c_last_name
      AND store_set.c_first_name = customer.c_first_name
      AND store_set.d_date = date_dim.d_date
)