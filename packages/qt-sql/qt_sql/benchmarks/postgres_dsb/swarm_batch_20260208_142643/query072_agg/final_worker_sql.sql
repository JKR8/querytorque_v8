WITH date_triples AS (
  SELECT 
    d1.d_date_sk AS sold_date_sk,
    d2.d_date_sk AS inv_date_sk,
    d3.d_date_sk AS ship_date_sk,
    d1.d_week_seq
  FROM date_dim d1
  JOIN date_dim d2 ON d1.d_week_seq = d2.d_week_seq
  JOIN date_dim d3 ON d3.d_date > d1.d_date + INTERVAL '3 DAY'
  WHERE d1.d_year = 1998
),
filtered_item AS (
  SELECT i_item_sk, i_item_desc
  FROM item
  WHERE i_category IN ('Home', 'Men', 'Music')
),
filtered_customer_demographics AS (
  SELECT cd_demo_sk
  FROM customer_demographics
  WHERE cd_marital_status = 'M'
    AND cd_dep_count BETWEEN 9 AND 11
),
filtered_household_demographics AS (
  SELECT hd_demo_sk
  FROM household_demographics
  WHERE hd_buy_potential = '501-1000'
)
SELECT
  i.i_item_desc,
  w.w_warehouse_name,
  dt.d_week_seq,
  SUM(CASE WHEN p.p_promo_sk IS NULL THEN 1 ELSE 0 END) AS no_promo,
  SUM(CASE WHEN NOT p.p_promo_sk IS NULL THEN 1 ELSE 0 END) AS promo,
  COUNT(*) AS total_cnt
FROM catalog_sales cs
JOIN date_triples dt 
  ON cs.cs_sold_date_sk = dt.sold_date_sk 
 AND cs.cs_ship_date_sk = dt.ship_date_sk
JOIN filtered_item i ON cs.cs_item_sk = i.i_item_sk
JOIN filtered_customer_demographics cd ON cs.cs_bill_cdemo_sk = cd.cd_demo_sk
JOIN filtered_household_demographics hd ON cs.cs_bill_hdemo_sk = hd.hd_demo_sk
JOIN inventory inv 
  ON cs.cs_item_sk = inv.inv_item_sk 
 AND inv.inv_date_sk = dt.inv_date_sk
 AND inv.inv_quantity_on_hand < cs.cs_quantity
JOIN warehouse w ON w.w_warehouse_sk = inv.inv_warehouse_sk
LEFT OUTER JOIN promotion p ON cs.cs_promo_sk = p.p_promo_sk
LEFT OUTER JOIN catalog_returns cr 
  ON cr.cr_item_sk = cs.cs_item_sk 
 AND cr.cr_order_number = cs.cs_order_number
WHERE cs.cs_wholesale_cost BETWEEN 34 AND 54
GROUP BY
  i.i_item_desc,
  w.w_warehouse_name,
  dt.d_week_seq
ORDER BY
  total_cnt DESC,
  i.i_item_desc,
  w.w_warehouse_name,
  dt.d_week_seq
LIMIT 100