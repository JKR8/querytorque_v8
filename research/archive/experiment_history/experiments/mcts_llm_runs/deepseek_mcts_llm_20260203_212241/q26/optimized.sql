WITH filtered_data AS (
  SELECT
    cs_item_sk,
    cs_quantity,
    cs_list_price,
    cs_coupon_amt,
    cs_sales_price
  FROM catalog_sales
  JOIN (
    SELECT
      cd_demo_sk
    FROM customer_demographics
    WHERE
      cd_gender = 'M' AND cd_marital_status = 'S' AND cd_education_status = 'Unknown'
  ) AS customer_demographics
    ON cs_bill_cdemo_sk = cd_demo_sk
  JOIN (
    SELECT
      d_date_sk
    FROM date_dim
    WHERE
      d_year = 2001
  ) AS date_dim
    ON cs_sold_date_sk = d_date_sk
  JOIN (
    SELECT
      p_promo_sk
    FROM promotion
    WHERE
      p_channel_email = 'N' OR p_channel_event = 'N'
  ) AS promotion
    ON cs_promo_sk = p_promo_sk
)
SELECT
  i_item_id,
  AVG(cs_quantity) AS agg1,
  AVG(cs_list_price) AS agg2,
  AVG(cs_coupon_amt) AS agg3,
  AVG(cs_sales_price) AS agg4
FROM filtered_data
JOIN item
  ON cs_item_sk = i_item_sk
GROUP BY
  i_item_id
ORDER BY
  i_item_id
LIMIT 100