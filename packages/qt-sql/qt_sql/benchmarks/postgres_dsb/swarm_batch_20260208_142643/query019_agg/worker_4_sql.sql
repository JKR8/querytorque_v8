WITH filtered_date AS (
    SELECT d_date_sk
    FROM date_dim
    WHERE d_year = 2002
      AND d_moy = 12
),
filtered_item AS (
    SELECT i_item_sk,
           i_brand_id,
           i_brand,
           i_manufact_id,
           i_manufact
    FROM item
    WHERE i_category = 'Home'
),
filtered_customer AS (
    SELECT c_customer_sk,
           ca_zip,
           ca_address_sk
    FROM customer
    JOIN customer_address ON c_current_addr_sk = ca_address_sk
    WHERE ca_state = 'TX'
      AND c_birth_month = 1
),
filtered_store AS (
    SELECT s_store_sk,
           s_zip
    FROM store
),
filtered_sales AS (
    SELECT ss_item_sk,
           ss_customer_sk,
           ss_store_sk,
           ss_ext_sales_price
    FROM store_sales
    WHERE ss_wholesale_cost BETWEEN 34 AND 54
)
SELECT
    i.i_brand_id AS brand_id,
    i.i_brand AS brand,
    i.i_manufact_id,
    i.i_manufact,
    SUM(fs.ss_ext_sales_price) AS ext_price
FROM filtered_sales fs
JOIN filtered_date d ON fs.ss_sold_date_sk = d.d_date_sk
JOIN filtered_item i ON fs.ss_item_sk = i.i_item_sk
JOIN filtered_customer c ON fs.ss_customer_sk = c.c_customer_sk
JOIN filtered_store s ON fs.ss_store_sk = s.s_store_sk
WHERE SUBSTRING(c.ca_zip FROM 1 FOR 5) <> SUBSTRING(s.s_zip FROM 1 FOR 5)
GROUP BY
    i.i_brand,
    i.i_brand_id,
    i.i_manufact_id,
    i.i_manufact
ORDER BY
    ext_price DESC,
    i.i_brand,
    i.i_brand_id,
    i.i_manufact_id,
    i.i_manufact
LIMIT 100;