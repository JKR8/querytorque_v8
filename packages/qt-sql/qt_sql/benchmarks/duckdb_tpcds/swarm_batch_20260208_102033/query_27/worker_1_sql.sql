WITH filtered_customer_demographics AS (
  SELECT cd_demo_sk
  FROM customer_demographics
  WHERE cd_gender = 'F'
    AND cd_marital_status = 'D'
    AND cd_education_status = 'Secondary'
),
filtered_date_dim AS (
  SELECT d_date_sk
  FROM date_dim
  WHERE d_year = 1999
),
filtered_store AS (
  SELECT s_store_sk, s_state
  FROM store
  WHERE s_state IN ('MO', 'AL', 'MI', 'TN', 'LA', 'SC')
)
SELECT
  i.i_item_id,
  fs.s_state,
  GROUPING(fs.s_state) AS g_state,
  AVG(ss.ss_quantity) AS agg1,
  AVG(ss.ss_list_price) AS agg2,
  AVG(ss.ss_coupon_amt) AS agg3,
  AVG(ss.ss_sales_price) AS agg4
FROM store_sales ss
JOIN filtered_customer_demographics fcd ON ss.ss_cdemo_sk = fcd.cd_demo_sk
JOIN filtered_date_dim fdd ON ss.ss_sold_date_sk = fdd.d_date_sk
JOIN filtered_store fs ON ss.ss_store_sk = fs.s_store_sk
JOIN item i ON ss.ss_item_sk = i.i_item_sk
GROUP BY ROLLUP (i.i_item_id, fs.s_state)
ORDER BY i.i_item_id, fs.s_state
LIMIT 100