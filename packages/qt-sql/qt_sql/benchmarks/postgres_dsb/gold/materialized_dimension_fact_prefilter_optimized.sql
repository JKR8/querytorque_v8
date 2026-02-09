WITH filtered_date AS MATERIALIZED (
  SELECT d_date_sk, d_date, d_week_seq
  FROM date_dim
  WHERE d_year = 1998
),
filtered_item AS MATERIALIZED (
  SELECT i_item_sk, i_item_desc
  FROM item
  WHERE i_category IN ('Home', 'Men', 'Music')
),
filtered_cd AS MATERIALIZED (
  SELECT cd_demo_sk
  FROM customer_demographics
  WHERE cd_marital_status = 'M'
    AND cd_dep_count BETWEEN 9 AND 11
),
filtered_hd AS MATERIALIZED (
  SELECT hd_demo_sk
  FROM household_demographics
  WHERE hd_buy_potential = '501-1000'
),
cs_filtered AS MATERIALIZED (
  SELECT cs_item_sk, cs_bill_cdemo_sk, cs_bill_hdemo_sk, cs_sold_date_sk,
         cs_ship_date_sk, cs_promo_sk, cs_quantity, cs_wholesale_cost,
         cs_order_number
  FROM catalog_sales
  WHERE cs_wholesale_cost BETWEEN 34 AND 54
)
SELECT i.i_item_desc,
       w.w_warehouse_name,
       d1.d_week_seq,
       SUM(CASE WHEN p.p_promo_sk IS NULL THEN 1 ELSE 0 END) AS no_promo,
       SUM(CASE WHEN p.p_promo_sk IS NOT NULL THEN 1 ELSE 0 END) AS promo,
       COUNT(*) AS total_cnt
FROM cs_filtered cs
JOIN inventory inv ON cs.cs_item_sk = inv.inv_item_sk
JOIN warehouse w ON w.w_warehouse_sk = inv.inv_warehouse_sk
JOIN filtered_item i ON i.i_item_sk = cs.cs_item_sk
JOIN filtered_cd cd ON cs.cs_bill_cdemo_sk = cd.cd_demo_sk
JOIN filtered_hd hd ON cs.cs_bill_hdemo_sk = hd.hd_demo_sk
JOIN filtered_date d1 ON cs.cs_sold_date_sk = d1.d_date_sk
JOIN date_dim d2 ON inv.inv_date_sk = d2.d_date_sk
JOIN date_dim d3 ON cs.cs_ship_date_sk = d3.d_date_sk
LEFT OUTER JOIN promotion p ON cs.cs_promo_sk = p.p_promo_sk
LEFT OUTER JOIN catalog_returns cr ON cr.cr_item_sk = cs.cs_item_sk 
  AND cr.cr_order_number = cs.cs_order_number
WHERE d1.d_week_seq = d2.d_week_seq
  AND inv.inv_quantity_on_hand < cs.cs_quantity
  AND d3.d_date > d1.d_date + INTERVAL '3 day'
GROUP BY i.i_item_desc, w.w_warehouse_name, d1.d_week_seq
ORDER BY total_cnt DESC, i.i_item_desc, w.w_warehouse_name, d1.d_week_seq
LIMIT 100;