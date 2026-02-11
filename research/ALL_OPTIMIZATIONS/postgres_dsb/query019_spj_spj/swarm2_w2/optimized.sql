WITH filtered_date AS (
    SELECT d_date_sk
    FROM date_dim
    WHERE d_year = 2002
      AND d_moy = 12
),
filtered_item AS (
    SELECT i_item_sk, i_brand_id, i_manufact_id
    FROM item
    WHERE i_category = 'Home'
),
filtered_customer AS (
    SELECT c_customer_sk, c_current_addr_sk
    FROM customer
    WHERE c_birth_month = 1
),
filtered_address AS (
    SELECT ca_address_sk, ca_zip
    FROM customer_address
    WHERE ca_state = 'TX'
)
SELECT
    MIN(i.i_brand_id),
    MIN(i.i_manufact_id),
    MIN(ss.ss_ext_sales_price)
FROM store_sales ss
JOIN filtered_date fd ON ss.ss_sold_date_sk = fd.d_date_sk
JOIN filtered_item i ON ss.ss_item_sk = i.i_item_sk
JOIN filtered_customer c ON ss.ss_customer_sk = c.c_customer_sk
JOIN filtered_address ca ON c.c_current_addr_sk = ca.ca_address_sk
JOIN store s ON ss.ss_store_sk = s.s_store_sk
WHERE ss.ss_wholesale_cost BETWEEN 34 AND 54
  AND SUBSTRING(ca.ca_zip FROM 1 FOR 5) <> SUBSTRING(s.s_zip FROM 1 FOR 5);