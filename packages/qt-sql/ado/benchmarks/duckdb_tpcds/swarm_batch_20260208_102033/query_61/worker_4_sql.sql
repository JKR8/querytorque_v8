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
filtered_customer_address AS (
    SELECT ca_address_sk
    FROM customer_address
    WHERE ca_gmt_offset = -7
),
filtered_item AS (
    SELECT i_item_sk
    FROM item
    WHERE i_category = 'Jewelry'
),
store_sales_base AS (
    SELECT
        ss_ext_sales_price,
        ss_promo_sk,
        ss_customer_sk,
        ss_item_sk
    FROM store_sales
    INNER JOIN filtered_date ON ss_sold_date_sk = d_date_sk
    INNER JOIN filtered_store ON ss_store_sk = s_store_sk
),
promotional_branches AS (
    -- Channel: dmail
    SELECT ss_ext_sales_price
    FROM store_sales_base ssb
    INNER JOIN promotion p ON ssb.ss_promo_sk = p_promo_sk
    INNER JOIN customer c ON ssb.ss_customer_sk = c_customer_sk
    INNER JOIN filtered_customer_address ca ON c_current_addr_sk = ca_address_sk
    INNER JOIN filtered_item i ON ssb.ss_item_sk = i_item_sk
    WHERE p_channel_dmail = 'Y'
    
    UNION ALL
    
    -- Channel: email
    SELECT ss_ext_sales_price
    FROM store_sales_base ssb
    INNER JOIN promotion p ON ssb.ss_promo_sk = p_promo_sk
    INNER JOIN customer c ON ssb.ss_customer_sk = c_customer_sk
    INNER JOIN filtered_customer_address ca ON c_current_addr_sk = ca_address_sk
    INNER JOIN filtered_item i ON ssb.ss_item_sk = i_item_sk
    WHERE p_channel_email = 'Y'
    
    UNION ALL
    
    -- Channel: tv
    SELECT ss_ext_sales_price
    FROM store_sales_base ssb
    INNER JOIN promotion p ON ssb.ss_promo_sk = p_promo_sk
    INNER JOIN customer c ON ssb.ss_customer_sk = c_customer_sk
    INNER JOIN filtered_customer_address ca ON c_current_addr_sk = ca_address_sk
    INNER JOIN filtered_item i ON ssb.ss_item_sk = i_item_sk
    WHERE p_channel_tv = 'Y'
),
all_sales_base AS (
    SELECT ss_ext_sales_price
    FROM store_sales_base ssb
    INNER JOIN customer c ON ssb.ss_customer_sk = c_customer_sk
    INNER JOIN filtered_customer_address ca ON c_current_addr_sk = ca_address_sk
    INNER JOIN filtered_item i ON ssb.ss_item_sk = i_item_sk
)
SELECT
    promotions,
    total,
    CAST(promotions AS DECIMAL(15, 4)) / CAST(total AS DECIMAL(15, 4)) * 100
FROM (
    SELECT SUM(ss_ext_sales_price) AS promotions
    FROM promotional_branches
) AS promotional_sales
CROSS JOIN (
    SELECT SUM(ss_ext_sales_price) AS total
    FROM all_sales_base
) AS all_sales
ORDER BY
    promotions,
    total
LIMIT 100