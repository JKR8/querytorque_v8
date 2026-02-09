WITH filtered_dates AS (
    SELECT d_date_sk, d_date
    FROM date_dim
    WHERE d_month_seq BETWEEN 1184 AND 1184 + 11
),
sales_consolidated AS (
    SELECT 
        c_last_name,
        c_first_name,
        d_date,
        CASE 
            WHEN ss_customer_sk IS NOT NULL THEN 'store'
            WHEN cs_bill_customer_sk IS NOT NULL THEN 'catalog'
            WHEN ws_bill_customer_sk IS NOT NULL THEN 'web'
        END AS channel
    FROM customer
    LEFT JOIN store_sales 
        ON ss_customer_sk = c_customer_sk
    LEFT JOIN catalog_sales 
        ON cs_bill_customer_sk = c_customer_sk
    LEFT JOIN web_sales 
        ON ws_bill_customer_sk = c_customer_sk
    LEFT JOIN filtered_dates store_dates 
        ON ss_sold_date_sk = store_dates.d_date_sk
    LEFT JOIN filtered_dates catalog_dates 
        ON cs_sold_date_sk = catalog_dates.d_date_sk
    LEFT JOIN filtered_dates web_dates 
        ON ws_sold_date_sk = web_dates.d_date_sk
    WHERE (store_dates.d_date_sk IS NOT NULL)
        OR (catalog_dates.d_date_sk IS NOT NULL)
        OR (web_dates.d_date_sk IS NOT NULL)
)
SELECT COUNT(*)
FROM (
    SELECT DISTINCT c_last_name, c_first_name, d_date
    FROM sales_consolidated
    WHERE channel = 'store'
    EXCEPT
    SELECT DISTINCT c_last_name, c_first_name, d_date
    FROM sales_consolidated
    WHERE channel = 'catalog'
    EXCEPT
    SELECT DISTINCT c_last_name, c_first_name, d_date
    FROM sales_consolidated
    WHERE channel = 'web'
) AS cool_cust