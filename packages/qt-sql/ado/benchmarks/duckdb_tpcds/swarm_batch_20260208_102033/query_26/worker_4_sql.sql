WITH filtered_date AS (
    SELECT d_date_sk
    FROM date_dim
    WHERE d_year = 2001
), filtered_customer_demographics AS (
    SELECT cd_demo_sk
    FROM customer_demographics
    WHERE cd_gender = 'M'
      AND cd_marital_status = 'S'
      AND cd_education_status = 'Unknown'
), promotion_branch1 AS (
    SELECT p_promo_sk
    FROM promotion
    WHERE p_channel_email = 'N'
), promotion_branch2 AS (
    SELECT p_promo_sk
    FROM promotion
    WHERE p_channel_event = 'N'
      AND (p_channel_email IS NULL OR p_channel_email != 'N')
), union_sales AS (
    SELECT
        i.i_item_id,
        cs.cs_quantity,
        cs.cs_list_price,
        cs.cs_coupon_amt,
        cs.cs_sales_price
    FROM catalog_sales cs
    INNER JOIN filtered_date d ON cs.cs_sold_date_sk = d.d_date_sk
    INNER JOIN filtered_customer_demographics cd ON cs.cs_bill_cdemo_sk = cd.cd_demo_sk
    INNER JOIN item i ON cs.cs_item_sk = i.i_item_sk
    INNER JOIN promotion_branch1 p ON cs.cs_promo_sk = p.p_promo_sk
    UNION ALL
    SELECT
        i.i_item_id,
        cs.cs_quantity,
        cs.cs_list_price,
        cs.cs_coupon_amt,
        cs.cs_sales_price
    FROM catalog_sales cs
    INNER JOIN filtered_date d ON cs.cs_sold_date_sk = d.d_date_sk
    INNER JOIN filtered_customer_demographics cd ON cs.cs_bill_cdemo_sk = cd.cd_demo_sk
    INNER JOIN item i ON cs.cs_item_sk = i.i_item_sk
    INNER JOIN promotion_branch2 p ON cs.cs_promo_sk = p.p_promo_sk
)
SELECT
    i_item_id,
    AVG(cs_quantity) AS agg1,
    AVG(cs_list_price) AS agg2,
    AVG(cs_coupon_amt) AS agg3,
    AVG(cs_sales_price) AS agg4
FROM union_sales
GROUP BY i_item_id
ORDER BY i_item_id
LIMIT 100