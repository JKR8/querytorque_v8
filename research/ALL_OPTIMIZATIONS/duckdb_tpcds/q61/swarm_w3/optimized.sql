WITH filtered_dates AS (
    SELECT d_date_sk
    FROM date_dim
    WHERE d_year = 1999
      AND d_moy = 11
),
filtered_stores AS (
    SELECT s_store_sk
    FROM store
    WHERE s_gmt_offset = -7
),
filtered_customer_address AS (
    SELECT ca_address_sk
    FROM customer_address
    WHERE ca_gmt_offset = -7
),
filtered_items AS (
    SELECT i_item_sk
    FROM item
    WHERE i_category = 'Jewelry'
),
filtered_promotions AS (
    SELECT p_promo_sk
    FROM promotion
    WHERE p_channel_dmail = 'Y'
       OR p_channel_email = 'Y'
       OR p_channel_tv = 'Y'
),
joined_sales AS (
    SELECT
        ss.ss_ext_sales_price,
        fp.p_promo_sk
    FROM store_sales ss
    INNER JOIN filtered_dates fd ON ss.ss_sold_date_sk = fd.d_date_sk
    INNER JOIN filtered_stores fs ON ss.ss_store_sk = fs.s_store_sk
    INNER JOIN customer c ON ss.ss_customer_sk = c.c_customer_sk
    INNER JOIN filtered_customer_address fca ON c.c_current_addr_sk = fca.ca_address_sk
    INNER JOIN filtered_items fi ON ss.ss_item_sk = fi.i_item_sk
    LEFT JOIN filtered_promotions fp ON ss.ss_promo_sk = fp.p_promo_sk
),
aggregated_sales AS (
    SELECT
        SUM(CASE WHEN p_promo_sk IS NOT NULL THEN ss_ext_sales_price ELSE 0 END) AS promotions,
        SUM(ss_ext_sales_price) AS total
    FROM joined_sales
)
SELECT
    promotions,
    total,
    CAST(promotions AS DECIMAL(15, 4)) / CAST(total AS DECIMAL(15, 4)) * 100
FROM aggregated_sales
ORDER BY
    promotions,
    total
LIMIT 100;