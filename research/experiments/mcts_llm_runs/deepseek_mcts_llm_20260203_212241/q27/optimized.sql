SELECT
  i_item_id,
  s_state,
  GROUPING(s_state) AS g_state,
  AVG(ss_quantity) AS agg1,
  AVG(ss_list_price) AS agg2,
  AVG(ss_coupon_amt) AS agg3,
  AVG(ss_sales_price) AS agg4
FROM store_sales, (
  SELECT
    *
  FROM customer_demographics
  WHERE
    cd_gender = 'F' AND cd_marital_status = 'D' AND cd_education_status = 'Secondary'
) AS customer_demographics, (
  SELECT
    *
  FROM date_dim
  WHERE
    d_year = 1999
) AS date_dim, (
  SELECT
    *
  FROM store
  WHERE
    s_state IN ('MO', 'AL', 'MI', 'TN', 'LA', 'SC')
) AS store, item
WHERE
  ss_sold_date_sk = d_date_sk
  AND ss_item_sk = i_item_sk
  AND ss_store_sk = s_store_sk
  AND ss_cdemo_sk = cd_demo_sk
GROUP BY
  ROLLUP (
    i_item_id,
    s_state
  )
ORDER BY
  i_item_id,
  s_state
LIMIT 100