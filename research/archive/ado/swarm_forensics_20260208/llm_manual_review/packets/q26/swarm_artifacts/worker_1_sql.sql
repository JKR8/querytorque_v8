WITH filtered_date AS (
    SELECT d_date_sk
    FROM date_dim
    WHERE d_year = 2001
),
filtered_customer_demo AS (
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
)
SELECT
    i_item_id,
    AVG(cs_quantity) AS agg1,
    AVG(cs_list_price) AS agg2,
    AVG(cs_coupon_amt) AS agg3,
    AVG(cs_sales_price) AS agg4
FROM catalog_sales
JOIN filtered_date ON cs_sold_date_sk = d_date_sk
JOIN filtered_customer_demo ON cs_bill_cdemo_sk = cd_demo_sk
JOIN filtered_promotion ON cs_promo_sk = p_promo_sk
JOIN item ON cs_item_sk = i_item_sk
GROUP BY i_item_id
ORDER BY i_item_id
LIMIT 100