WITH filtered_customer_demographics AS (
    SELECT cd_demo_sk
    FROM customer_demographics
    WHERE cd_gender = 'F'
      AND cd_marital_status = 'W'
      AND cd_education_status = 'College'
),
filtered_date_dim AS (
    SELECT d_date_sk
    FROM date_dim
    WHERE d_year = 2001
),
filtered_promotion AS (
    SELECT p_promo_sk
    FROM promotion
    WHERE p_channel_email = 'N' OR p_channel_event = 'N'
)
SELECT
    i.i_item_id,
    AVG(ss.ss_quantity) AS agg1,
    AVG(ss.ss_list_price) AS agg2,
    AVG(ss.ss_coupon_amt) AS agg3,
    AVG(ss.ss_sales_price) AS agg4
FROM store_sales ss
JOIN filtered_customer_demographics cd ON ss.ss_cdemo_sk = cd.cd_demo_sk
JOIN filtered_date_dim d ON ss.ss_sold_date_sk = d.d_date_sk
JOIN item i ON ss.ss_item_sk = i.i_item_sk
JOIN filtered_promotion p ON ss.ss_promo_sk = p.p_promo_sk
GROUP BY i.i_item_id
ORDER BY i.i_item_id
LIMIT 100