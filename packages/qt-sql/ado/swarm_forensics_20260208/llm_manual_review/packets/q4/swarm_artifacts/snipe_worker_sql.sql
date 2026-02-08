WITH filtered_dates AS (
    SELECT d_date_sk, d_year
    FROM date_dim
    WHERE d_year IN (1999, 2000)
),
store_sales_1999 AS (
    SELECT
        c_customer_id AS customer_id,
        c_first_name AS customer_first_name,
        c_last_name AS customer_last_name,
        c_birth_country AS customer_birth_country,
        SUM(((ss_ext_list_price - ss_ext_wholesale_cost - ss_ext_discount_amt) + ss_ext_sales_price) / 2) AS year_total
    FROM customer
    JOIN store_sales ON c_customer_sk = ss_customer_sk
    JOIN filtered_dates ON ss_sold_date_sk = d_date_sk
    WHERE d_year = 1999
    GROUP BY
        c_customer_id,
        c_first_name,
        c_last_name,
        c_birth_country
),
store_sales_2000 AS (
    SELECT
        c_customer_id AS customer_id,
        c_first_name AS customer_first_name,
        c_last_name AS customer_last_name,
        c_birth_country AS customer_birth_country,
        SUM(((ss_ext_list_price - ss_ext_wholesale_cost - ss_ext_discount_amt) + ss_ext_sales_price) / 2) AS year_total
    FROM customer
    JOIN store_sales ON c_customer_sk = ss_customer_sk
    JOIN filtered_dates ON ss_sold_date_sk = d_date_sk
    WHERE d_year = 2000
    GROUP BY
        c_customer_id,
        c_first_name,
        c_last_name,
        c_birth_country
),
catalog_sales_1999 AS (
    SELECT
        c_customer_id AS customer_id,
        c_first_name AS customer_first_name,
        c_last_name AS customer_last_name,
        c_birth_country AS customer_birth_country,
        SUM(((cs_ext_list_price - cs_ext_wholesale_cost - cs_ext_discount_amt) + cs_ext_sales_price) / 2) AS year_total
    FROM customer
    JOIN catalog_sales ON c_customer_sk = cs_bill_customer_sk
    JOIN filtered_dates ON cs_sold_date_sk = d_date_sk
    WHERE d_year = 1999
    GROUP BY
        c_customer_id,
        c_first_name,
        c_last_name,
        c_birth_country
),
catalog_sales_2000 AS (
    SELECT
        c_customer_id AS customer_id,
        c_first_name AS customer_first_name,
        c_last_name AS customer_last_name,
        c_birth_country AS customer_birth_country,
        SUM(((cs_ext_list_price - cs_ext_wholesale_cost - cs_ext_discount_amt) + cs_ext_sales_price) / 2) AS year_total
    FROM customer
    JOIN catalog_sales ON c_customer_sk = cs_bill_customer_sk
    JOIN filtered_dates ON cs_sold_date_sk = d_date_sk
    WHERE d_year = 2000
    GROUP BY
        c_customer_id,
        c_first_name,
        c_last_name,
        c_birth_country
),
web_sales_1999 AS (
    SELECT
        c_customer_id AS customer_id,
        c_first_name AS customer_first_name,
        c_last_name AS customer_last_name,
        c_birth_country AS customer_birth_country,
        SUM(((ws_ext_list_price - ws_ext_wholesale_cost - ws_ext_discount_amt) + ws_ext_sales_price) / 2) AS year_total
    FROM customer
    JOIN web_sales ON c_customer_sk = ws_bill_customer_sk
    JOIN filtered_dates ON ws_sold_date_sk = d_date_sk
    WHERE d_year = 1999
    GROUP BY
        c_customer_id,
        c_first_name,
        c_last_name,
        c_birth_country
),
web_sales_2000 AS (
    SELECT
        c_customer_id AS customer_id,
        c_first_name AS customer_first_name,
        c_last_name AS customer_last_name,
        c_birth_country AS customer_birth_country,
        SUM(((ws_ext_list_price - ws_ext_wholesale_cost - ws_ext_discount_amt) + ws_ext_sales_price) / 2) AS year_total
    FROM customer
    JOIN web_sales ON c_customer_sk = ws_bill_customer_sk
    JOIN filtered_dates ON ws_sold_date_sk = d_date_sk
    WHERE d_year = 2000
    GROUP BY
        c_customer_id,
        c_first_name,
        c_last_name,
        c_birth_country
)
SELECT
    s2000.customer_id,
    s2000.customer_first_name,
    s2000.customer_last_name,
    s2000.customer_birth_country
FROM store_sales_1999 s1999
JOIN store_sales_2000 s2000 ON s1999.customer_id = s2000.customer_id
JOIN catalog_sales_1999 c1999 ON s1999.customer_id = c1999.customer_id
JOIN catalog_sales_2000 c2000 ON s1999.customer_id = c2000.customer_id
JOIN web_sales_1999 w1999 ON s1999.customer_id = w1999.customer_id
JOIN web_sales_2000 w2000 ON s1999.customer_id = w2000.customer_id
WHERE
    s1999.year_total > 0
    AND c1999.year_total > 0
    AND w1999.year_total > 0
    AND (c2000.year_total / c1999.year_total) > (s2000.year_total / s1999.year_total)
    AND (c2000.year_total / c1999.year_total) > (w2000.year_total / w1999.year_total)
ORDER BY
    s2000.customer_id,
    s2000.customer_first_name,
    s2000.customer_last_name,
    s2000.customer_birth_country
LIMIT 100