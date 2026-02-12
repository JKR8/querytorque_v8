WITH fact_dim_agg AS (SELECT 
  i_item_id,
  c_current_addr_sk,
  AVG(CAST(cs_quantity AS DECIMAL(12,2))) AS agg1,
  AVG(CAST(cs_list_price AS DECIMAL(12,2))) AS agg2,
  AVG(CAST(cs_coupon_amt AS DECIMAL(12,2))) AS agg3,
  AVG(CAST(cs_sales_price AS DECIMAL(12,2))) AS agg4,
  AVG(CAST(cs_net_profit AS DECIMAL(12,2))) AS agg5,
  AVG(CAST(c_birth_year AS DECIMAL(12,2))) AS agg6
FROM catalog_sales
INNER JOIN date_dim ON cs_sold_date_sk = d_date_sk
INNER JOIN item ON cs_item_sk = i_item_sk
INNER JOIN customer_demographics ON cs_bill_cdemo_sk = cd_demo_sk
INNER JOIN customer ON cs_bill_customer_sk = c_customer_sk
WHERE d_year = 1999
  AND i_category = 'Music'
  AND cd_gender = 'F'
  AND cd_education_status = '4 yr Degree'
  AND c_birth_month = 12
  AND cs_wholesale_cost BETWEEN 5 AND 10
GROUP BY i_item_id, c_current_addr_sk, c_birth_year), address_join AS (SELECT 
  f.i_item_id,
  ca.ca_country,
  ca.ca_state,
  ca.ca_county,
  f.agg1,
  f.agg2,
  f.agg3,
  f.agg4,
  f.agg5,
  f.agg6
FROM fact_dim_agg f
INNER JOIN customer_address ca ON f.c_current_addr_sk = ca.ca_address_sk
WHERE ca.ca_state IN ('AR', 'IN', 'VA')) SELECT 
  i_item_id,
  ca_country,
  ca_state,
  ca_county,
  CASE WHEN COUNT(*) > 0 THEN AVG(CAST(agg1 AS DECIMAL(12,2))) END AS agg1,
  CASE WHEN COUNT(*) > 0 THEN AVG(CAST(agg2 AS DECIMAL(12,2))) END AS agg2,
  CASE WHEN COUNT(*) > 0 THEN AVG(CAST(agg3 AS DECIMAL(12,2))) END AS agg3,
  CASE WHEN COUNT(*) > 0 THEN AVG(CAST(agg4 AS DECIMAL(12,2))) END AS agg4,
  CASE WHEN COUNT(*) > 0 THEN AVG(CAST(agg5 AS DECIMAL(12,2))) END AS agg5,
  CASE WHEN COUNT(*) > 0 THEN AVG(CAST(agg6 AS DECIMAL(12,2))) END AS agg6
FROM address_join
GROUP BY ROLLUP (i_item_id, ca_country, ca_state, ca_county)
ORDER BY ca_country, ca_state, ca_county, i_item_id
LIMIT 100