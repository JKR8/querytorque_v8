WITH filtered_dates AS (
    SELECT d_date_sk
    FROM date_dim
    WHERE d_year = 2001
),
filtered_cd AS (
    SELECT cd_demo_sk
    FROM customer_demographics
    WHERE cd_gender = 'F'
      AND cd_marital_status = 'W'
      AND cd_education_status = 'College'
),
filtered_promo_email AS (
    SELECT p_promo_sk
    FROM promotion
    WHERE p_channel_email = 'N'
),
filtered_promo_event AS (
    SELECT p_promo_sk
    FROM promotion
    WHERE p_channel_event = 'N'
      AND p_channel_email != 'N'
),
filtered_store_sales AS (
    -- Branch 1: email channel = 'N'
    SELECT ss_item_sk, ss_quantity, ss_list_price, ss_coupon_amt, ss_sales_price
    FROM store_sales
    JOIN filtered_dates ON ss_sold_date_sk = d_date_sk
    JOIN filtered_cd ON ss_cdemo_sk = cd_demo_sk
    JOIN filtered_promo_email ON ss_promo_sk = p_promo_sk
    UNION ALL
    -- Branch 2: event channel = 'N' and email channel != 'N'
    SELECT ss_item_sk, ss_quantity, ss_list_price, ss_coupon_amt, ss_sales_price
    FROM store_sales
    JOIN filtered_dates ON ss_sold_date_sk = d_date_sk
    JOIN filtered_cd ON ss_cdemo_sk = cd_demo_sk
    JOIN filtered_promo_event ON ss_promo_sk = p_promo_sk
)
SELECT
    i.i_item_id,
    AVG(ss_quantity) AS agg1,
    AVG(ss_list_price) AS agg2,
    AVG(ss_coupon_amt) AS agg3,
    AVG(ss_sales_price) AS agg4
FROM filtered_store_sales s
JOIN item i ON s.ss_item_sk = i.i_item_sk
GROUP BY i.i_item_id
ORDER BY i.i_item_id
LIMIT 100