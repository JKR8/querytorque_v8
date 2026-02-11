WITH filtered_date AS (
    SELECT d_date_sk
    FROM date_dim
    WHERE d_year = 2002
      AND d_moy = 12
),
filtered_item AS (
    SELECT i_item_sk, i_brand_id, i_brand, i_manufact_id, i_manufact
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
    i_brand_id AS brand_id,
    i_brand AS brand,
    i_manufact_id,
    i_manufact,
    SUM(ss_ext_sales_price) AS ext_price
FROM store_sales
JOIN filtered_date ON ss_sold_date_sk = d_date_sk
JOIN filtered_item ON ss_item_sk = i_item_sk
JOIN filtered_customer ON ss_customer_sk = c_customer_sk
JOIN filtered_address ON c_current_addr_sk = ca_address_sk
JOIN store ON ss_store_sk = s_store_sk
WHERE ss_wholesale_cost BETWEEN 34 AND 54
  AND SUBSTRING(ca_zip FROM 1 FOR 5) <> SUBSTRING(s_zip FROM 1 FOR 5)
GROUP BY
    i_brand,
    i_brand_id,
    i_manufact_id,
    i_manufact
ORDER BY
    ext_price DESC,
    i_brand,
    i_brand_id,
    i_manufact_id,
    i_manufact
LIMIT 100;
