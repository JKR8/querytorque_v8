WITH filtered_dates AS (
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
filtered_promotions AS (
  SELECT p_promo_sk
  FROM promotion
  WHERE p_channel_email = 'N' OR p_channel_event = 'N'
),
joined_facts AS (
  SELECT 
    cs_item_sk,
    cs_quantity,
    cs_list_price,
    cs_coupon_amt,
    cs_sales_price
  FROM catalog_sales AS cs
  JOIN filtered_dates AS fd ON cs.cs_sold_date_sk = fd.d_date_sk
  JOIN filtered_customer_demographics AS fcd ON cs.cs_bill_cdemo_sk = fcd.cd_demo_sk
  JOIN filtered_promotions AS fp ON cs.cs_promo_sk = fp.p_promo_sk
)
SELECT
  i.i_item_id,
  AVG(jf.cs_quantity) AS agg1,
  AVG(jf.cs_list_price) AS agg2,
  AVG(jf.cs_coupon_amt) AS agg3,
  AVG(jf.cs_sales_price) AS agg4
FROM joined_facts AS jf
JOIN item AS i ON jf.cs_item_sk = i.i_item_sk
GROUP BY i.i_item_id
ORDER BY i.i_item_id
LIMIT 100;