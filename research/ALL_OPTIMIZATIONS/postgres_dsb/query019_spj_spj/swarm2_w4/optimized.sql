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
)
SELECT
    MIN(i.i_brand_id),
    MIN(i.i_manufact_id),
    MIN(ss.ss_ext_sales_price)
FROM store_sales ss
JOIN filtered_item i ON ss.ss_item_sk = i.i_item_sk
WHERE ss.ss_wholesale_cost BETWEEN 34 AND 54
  AND EXISTS (
      SELECT 1
      FROM filtered_date d
      WHERE ss.ss_sold_date_sk = d.d_date_sk
  )
  AND EXISTS (
      SELECT 1
      FROM customer c
      JOIN customer_address ca ON c.c_current_addr_sk = ca.ca_address_sk
      JOIN store s ON ss.ss_store_sk = s.s_store_sk
      WHERE ss.ss_customer_sk = c.c_customer_sk
        AND c.c_birth_month = 1
        AND ca.ca_state = 'TX'
        AND SUBSTRING(ca.ca_zip FROM 1 FOR 5) <> SUBSTRING(s.s_zip FROM 1 FOR 5)
  );