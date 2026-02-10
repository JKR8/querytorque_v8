-- start query 61 in stream 0 using template query61.tpl
SELECT 
    promotions,
    total,
    CAST(promotions AS DECIMAL(15,4)) / CAST(total AS DECIMAL(15,4)) * 100 AS promo_percentage
FROM (
    SELECT 
        SUM(CASE WHEN p_promo_sk IS NOT NULL AND (p_channel_dmail = 'Y' OR p_channel_email = 'Y' OR p_channel_tv = 'Y') THEN ss_ext_sales_price ELSE 0 END) AS promotions,
        SUM(ss_ext_sales_price) AS total
    FROM store_sales
    INNER JOIN date_dim ON ss_sold_date_sk = d_date_sk
    INNER JOIN store ON ss_store_sk = s_store_sk
    INNER JOIN customer ON ss_customer_sk = c_customer_sk
    INNER JOIN customer_address ON ca_address_sk = c_current_addr_sk
    INNER JOIN item ON ss_item_sk = i_item_sk
    LEFT JOIN promotion ON ss_promo_sk = p_promo_sk
    WHERE ca_gmt_offset = -7
        AND i_category = 'Jewelry'
        AND s_gmt_offset = -7
        AND d_year = 1999
        AND d_moy = 11
) aggregated_sales
LIMIT 100;

-- end query 61 in stream 0 using template query61.tpl