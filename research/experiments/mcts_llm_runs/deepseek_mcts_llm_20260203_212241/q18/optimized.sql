SELECT
  i_item_id,
  ca_country,
  ca_state,
  ca_county,
  AVG(cs_quantity) AS agg1,
  AVG(cs_list_price) AS agg2,
  AVG(cs_coupon_amt) AS agg3,
  AVG(cs_sales_price) AS agg4,
  AVG(cs_net_profit) AS agg5,
  AVG(c_birth_year) AS agg6,
  AVG(cd1.cd_dep_count) AS agg7
FROM catalog_sales, customer_demographics AS cd1, customer_demographics AS cd2, customer, customer_address, date_dim, item
WHERE
  cs_sold_date_sk = d_date_sk
  AND cs_item_sk = i_item_sk
  AND cs_bill_cdemo_sk = cd1.cd_demo_sk
  AND cs_bill_customer_sk = c_customer_sk
  AND cd1.cd_gender = 'F'
  AND cd1.cd_education_status = 'Advanced Degree'
  AND c_current_cdemo_sk = cd2.cd_demo_sk
  AND c_current_addr_sk = ca_address_sk
  AND c_birth_month IN (10, 7, 8, 4, 1, 2)
  AND d_year = 1998
  AND ca_state IN ('WA', 'GA', 'NC', 'ME', 'WY', 'OK', 'IN')
GROUP BY
  ROLLUP (
    i_item_id,
    ca_country,
    ca_state,
    ca_county
  )
ORDER BY
  ca_country,
  ca_state,
  ca_county,
  i_item_id
LIMIT 100