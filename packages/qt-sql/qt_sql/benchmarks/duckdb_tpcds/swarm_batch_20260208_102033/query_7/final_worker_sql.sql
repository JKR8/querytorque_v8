WITH filtered_promotion AS (
    SELECT p_promo_sk
    FROM promotion
    WHERE p_channel_email = 'N' OR p_channel_event = 'N'
),
filtered_store_sales AS (
    SELECT 
        ss_item_sk,
        ss_quantity,
        ss_list_price,
        ss_coupon_amt,
        ss_sales_price
    FROM store_sales
    WHERE EXISTS (
        SELECT 1 FROM date_dim 
        WHERE d_date_sk = store_sales.ss_sold_date_sk 
        AND d_year = 2001
    )
    AND EXISTS (
        SELECT 1 FROM customer_demographics 
        WHERE cd_demo_sk = store_sales.ss_cdemo_sk
        AND cd_gender = 'F'
        AND cd_marital_status = 'W'
        AND cd_education_status = 'College'
    )
    AND EXISTS (
        SELECT 1 FROM filtered_promotion 
        WHERE p_promo_sk = store_sales.ss_promo_sk
    )
),
aggregated_sales AS (
    SELECT
        ss_item_sk,
        AVG(ss_quantity) AS agg1,
        AVG(ss_list_price) AS agg2,
        AVG(ss_coupon_amt) AS agg3,
        AVG(ss_sales_price) AS agg4
    FROM filtered_store_sales
    GROUP BY ss_item_sk
)
SELECT
    i.i_item_id,
    a.agg1,
    a.agg2,
    a.agg3,
    a.agg4
FROM aggregated_sales a
JOIN item i ON a.ss_item_sk = i.i_item_sk
ORDER BY i.i_item_id
LIMIT 100;