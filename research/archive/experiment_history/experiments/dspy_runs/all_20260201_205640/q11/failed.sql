WITH store_sales_year AS (
    SELECT 
        c_customer_id AS customer_id,
        c_first_name AS customer_first_name,
        c_last_name AS customer_last_name,
        c_birth_country AS customer_birth_country,
        d_year AS dyear,
        SUM(ss_ext_list_price - ss_ext_discount_amt) AS year_total
    FROM customer
    INNER JOIN store_sales ON c_customer_sk = ss_customer_sk
    INNER JOIN date_dim ON ss_sold_date_sk = d_date_sk
    WHERE d_year IN (2001, 2002)
    GROUP BY 
        c_customer_id,
        c_first_name,
        c_last_name,
        c_birth_country,
        d_year
),
web_sales_year AS (
    SELECT 
        c_customer_id AS customer_id,
        c_first_name AS customer_first_name,
        c_last_name AS customer_last_name,
        c_birth_country AS customer_birth_country,
        d_year AS dyear,
        SUM(ws_ext_list_price - ws_ext_discount_amt) AS year_total
    FROM customer
    INNER JOIN web_sales ON c_customer_sk = ws_bill_customer_sk
    INNER JOIN date_dim ON ws_sold_date_sk = d_date_sk
    WHERE d_year IN (2001, 2002)
    GROUP BY 
        c_customer_id,
        c_first_name,
        c_last_name,
        c_birth_country,
        d_year
),
combined AS (
    SELECT 
        customer_id,
        customer_first_name,
        customer_last_name,
        customer_birth_country,
        MAX(CASE WHEN sale_type = 's' AND dyear = 2001 THEN year_total ELSE 0 END) AS s_firstyear,
        MAX(CASE WHEN sale_type = 's' AND dyear = 2002 THEN year_total ELSE 0 END) AS s_secyear,
        MAX(CASE WHEN sale_type = 'w' AND dyear = 2001 THEN year_total ELSE 0 END) AS w_firstyear,
        MAX(CASE WHEN sale_type = 'w' AND dyear = 2002 THEN year_total ELSE 0 END) AS w_secyear
    FROM (
        SELECT customer_id, customer_first_name, customer_last_name, customer_birth_country, dyear, year_total, 's' AS sale_type
        FROM store_sales_year
        UNION ALL
        SELECT customer_id, customer_first_name, customer_last_name, customer_birth_country, dyear, year_total, 'w' AS sale_type
        FROM web_sales_year
    ) AS all_sales
    GROUP BY 
        customer_id,
        customer_first_name,
        customer_last_name,
        customer_birth_country
)
SELECT 
    customer_id,
    customer_first_name,
    customer_last_name,
    customer_birth_country
FROM combined
WHERE s_firstyear > 0
    AND w_firstyear > 0
    AND CASE WHEN w_firstyear > 0 THEN w_secyear * 1.0 / w_firstyear ELSE 0.0 END
        > CASE WHEN s_firstyear > 0 THEN s_secyear * 1.0 / s_firstyear ELSE 0.0 END
ORDER BY 
    customer_id,
    customer_first_name,
    customer_last_name,
    customer_birth_country
LIMIT 100;