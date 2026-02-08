WITH filtered_date AS (
    SELECT d_date_sk
    FROM date_dim
    WHERE d_year = 2001
),
filtered_customer_demographics AS (
    SELECT cd_demo_sk
    FROM customer_demographics
    WHERE cd_gender = 'M'
        AND cd_marital_status = 'S'
        AND cd_education_status = 'Unknown'
),
filtered_promotion AS (
    SELECT p_promo_sk
    FROM promotion
    WHERE p_channel_email = 'N' OR p_channel_event = 'N'
),
filtered_catalog_sales AS (
    SELECT
        cs_item_sk,
        cs_quantity,
        cs_list_price,
        cs_coupon_amt,
        cs_sales_price
    FROM catalog_sales
    WHERE EXISTS (
        SELECT 1 FROM filtered_date
        WHERE d_date_sk = cs_sold_date_sk
    )
    AND EXISTS (
        SELECT 1 FROM filtered_customer_demographics
        WHERE cd_demo_sk = cs_bill_cdemo_sk
    )
    AND EXISTS (
        SELECT 1 FROM filtered_promotion
        WHERE p_promo_sk = cs_promo_sk
    )
),
aggregated_items AS (
    SELECT
        cs_item_sk,
        AVG(cs_quantity) AS agg1,
        AVG(cs_list_price) AS agg2,
        AVG(cs_coupon_amt) AS agg3,
        AVG(cs_sales_price) AS agg4
    FROM filtered_catalog_sales
    GROUP BY cs_item_sk
)
SELECT
    i_item_id,
    agg1,
    agg2,
    agg3,
    agg4
FROM aggregated_items
JOIN item ON cs_item_sk = i_item_sk
ORDER BY i_item_id
LIMIT 100;