WITH filtered_date AS (
    SELECT d_date_sk
    FROM date_dim
    WHERE d_year = 1999
      AND d_moy = 11
),
filtered_store AS (
    SELECT s_store_sk
    FROM store
    WHERE s_gmt_offset = -7
),
filtered_item AS (
    SELECT i_item_sk
    FROM item
    WHERE i_category = 'Jewelry'
),
filtered_customer_address AS (
    SELECT ca_address_sk, c_customer_sk
    FROM customer_address
    JOIN customer ON ca_address_sk = c_current_addr_sk
    WHERE ca_gmt_offset = -7
),
filtered_promotion AS (
    SELECT p_promo_sk
    FROM promotion
    WHERE p_channel_dmail = 'Y' 
       OR p_channel_email = 'Y' 
       OR p_channel_tv = 'Y'
),
promotional_sales AS (
    SELECT SUM(ss_ext_sales_price) AS promotions
    FROM store_sales
    JOIN filtered_date ON ss_sold_date_sk = d_date_sk
    JOIN filtered_store ON ss_store_sk = s_store_sk
    JOIN filtered_item ON ss_item_sk = i_item_sk
    JOIN filtered_customer_address ON ss_customer_sk = c_customer_sk
    JOIN filtered_promotion ON ss_promo_sk = p_promo_sk
),
all_sales AS (
    SELECT SUM(ss_ext_sales_price) AS total
    FROM store_sales
    JOIN filtered_date ON ss_sold_date_sk = d_date_sk
    JOIN filtered_store ON ss_store_sk = s_store_sk
    JOIN filtered_item ON ss_item_sk = i_item_sk
    JOIN filtered_customer_address ON ss_customer_sk = c_customer_sk
)
SELECT
    promotions,
    total,
    CAST(promotions AS DECIMAL(15, 4)) / CAST(total AS DECIMAL(15, 4)) * 100
FROM promotional_sales, all_sales
ORDER BY
    promotions,
    total
LIMIT 100