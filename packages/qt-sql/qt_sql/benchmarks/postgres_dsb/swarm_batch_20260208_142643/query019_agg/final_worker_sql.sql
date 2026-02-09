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
           c_current_addr_sk
    FROM customer
    WHERE c_birth_month = 1
),
filtered_customer_address AS (
    SELECT ca_address_sk,
           ca_zip
    FROM customer_address
    WHERE ca_state = 'TX'
),
filtered_store_sales AS (
    SELECT ss_sold_date_sk,
           ss_item_sk,
           ss_customer_sk,
           ss_store_sk,
           ss_ext_sales_price
    FROM store_sales
    WHERE ss_wholesale_cost BETWEEN 34 AND 54
),
pre_join AS (
    SELECT ss.ss_ext_sales_price,
           i.i_brand_id,
           i.i_brand,
           i.i_manufact_id,
           i.i_manufact,
           ss.ss_customer_sk,
           ss.ss_store_sk,
           c.c_current_addr_sk
    FROM filtered_store_sales ss
    JOIN filtered_date d ON ss.ss_sold_date_sk = d.d_date_sk
    JOIN filtered_item i ON ss.ss_item_sk = i.i_item_sk
    JOIN filtered_customer c ON ss.ss_customer_sk = c.c_customer_sk
)
SELECT i_brand_id AS brand_id,
       i_brand AS brand,
       i_manufact_id,
       i_manufact,
       SUM(ss_ext_sales_price) AS ext_price
FROM pre_join pj
JOIN filtered_customer_address ca ON pj.c_current_addr_sk = ca.ca_address_sk
JOIN store s ON pj.ss_store_sk = s.s_store_sk
WHERE SUBSTRING(ca.ca_zip FROM 1 FOR 5) <> SUBSTRING(s.s_zip FROM 1 FOR 5)
GROUP BY i_brand,
         i_brand_id,
         i_manufact_id,
         i_manufact
ORDER BY ext_price DESC,
         i_brand,
         i_brand_id,
         i_manufact_id,
         i_manufact
LIMIT 100;