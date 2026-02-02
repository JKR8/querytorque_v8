WITH year_total AS (
    -- Store sales for years 1999 and 2000 only
    SELECT 
        c.c_customer_id AS customer_id,
        c.c_first_name AS customer_first_name,
        c.c_last_name AS customer_last_name,
        c.c_preferred_cust_flag AS customer_preferred_cust_flag,
        c.c_birth_country AS customer_birth_country,
        c.c_login AS customer_login,
        c.c_email_address AS customer_email_address,
        d.d_year AS dyear,
        SUM(((ss.ss_ext_list_price - ss.ss_ext_wholesale_cost - ss.ss_ext_discount_amt) + ss.ss_ext_sales_price) / 2) AS year_total,
        's' AS sale_type
    FROM customer c
    JOIN store_sales ss ON c.c_customer_sk = ss.ss_customer_sk
    JOIN date_dim d ON ss.ss_sold_date_sk = d.d_date_sk
    WHERE d.d_year IN (1999, 2000)
    GROUP BY c.c_customer_id, c.c_first_name, c.c_last_name, c.c_preferred_cust_flag,
             c.c_birth_country, c.c_login, c.c_email_address, d.d_year
    
    UNION ALL
    
    -- Catalog sales for years 1999 and 2000 only
    SELECT 
        c.c_customer_id AS customer_id,
        c.c_first_name AS customer_first_name,
        c.c_last_name AS customer_last_name,
        c.c_preferred_cust_flag AS customer_preferred_cust_flag,
        c.c_birth_country AS customer_birth_country,
        c.c_login AS customer_login,
        c.c_email_address AS customer_email_address,
        d.d_year AS dyear,
        SUM(((cs.cs_ext_list_price - cs.cs_ext_wholesale_cost - cs.cs_ext_discount_amt) + cs.cs_ext_sales_price) / 2) AS year_total,
        'c' AS sale_type
    FROM customer c
    JOIN catalog_sales cs ON c.c_customer_sk = cs.cs_bill_customer_sk
    JOIN date_dim d ON cs.cs_sold_date_sk = d.d_date_sk
    WHERE d.d_year IN (1999, 2000)
    GROUP BY c.c_customer_id, c.c_first_name, c.c_last_name, c.c_preferred_cust_flag,
             c.c_birth_country, c.c_login, c.c_email_address, d.d_year
    
    UNION ALL
    
    -- Web sales for years 1999 and 2000 only
    SELECT 
        c.c_customer_id AS customer_id,
        c.c_first_name AS customer_first_name,
        c.c_last_name AS customer_last_name,
        c.c_preferred_cust_flag AS customer_preferred_cust_flag,
        c.c_birth_country AS customer_birth_country,
        c.c_login AS customer_login,
        c.c_email_address AS customer_email_address,
        d.d_year AS dyear,
        SUM(((ws.ws_ext_list_price - ws.ws_ext_wholesale_cost - ws.ws_ext_discount_amt) + ws.ws_ext_sales_price) / 2) AS year_total,
        'w' AS sale_type
    FROM customer c
    JOIN web_sales ws ON c.c_customer_sk = ws.ws_bill_customer_sk
    JOIN date_dim d ON ws.ws_sold_date_sk = d.d_date_sk
    WHERE d.d_year IN (1999, 2000)
    GROUP BY c.c_customer_id, c.c_first_name, c.c_last_name, c.c_preferred_cust_flag,
             c.c_birth_country, c.c_login, c.c_email_address, d.d_year
),
pivoted_data AS (
    SELECT 
        customer_id,
        customer_first_name,
        customer_last_name,
        customer_birth_country,
        MAX(CASE WHEN sale_type = 's' AND dyear = 1999 THEN year_total END) AS s_1999,
        MAX(CASE WHEN sale_type = 's' AND dyear = 2000 THEN year_total END) AS s_2000,
        MAX(CASE WHEN sale_type = 'c' AND dyear = 1999 THEN year_total END) AS c_1999,
        MAX(CASE WHEN sale_type = 'c' AND dyear = 2000 THEN year_total END) AS c_2000,
        MAX(CASE WHEN sale_type = 'w' AND dyear = 1999 THEN year_total END) AS w_1999,
        MAX(CASE WHEN sale_type = 'w' AND dyear = 2000 THEN year_total END) AS w_2000
    FROM year_total
    GROUP BY customer_id, customer_first_name, customer_last_name, customer_birth_country
)
SELECT 
    customer_id,
    customer_first_name,
    customer_last_name,
    customer_birth_country
FROM pivoted_data
WHERE s_1999 > 0
    AND c_1999 > 0
    AND w_1999 > 0
    AND (c_2000 / NULLIF(c_1999, 0)) > (s_2000 / NULLIF(s_1999, 0))
    AND (c_2000 / NULLIF(c_1999, 0)) > (w_2000 / NULLIF(w_1999, 0))
ORDER BY customer_id, customer_first_name, customer_last_name, customer_birth_country
LIMIT 100;