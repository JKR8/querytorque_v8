-- start query 72 in stream 0 using template query72.tpl
WITH filtered_dates AS (
    SELECT d_date_sk, d_week_seq, d_date, d_year
    FROM date_dim 
    WHERE d_year = 2002
),
filtered_inventory AS (
    SELECT inv_item_sk, inv_warehouse_sk, inv_date_sk, inv_quantity_on_hand
    FROM inventory
    WHERE inv_item_sk <= 203999  -- Push down filter from row estimates
),
filtered_customer_demo AS (
    SELECT cd_demo_sk
    FROM customer_demographics
    WHERE cd_marital_status = 'W'
),
filtered_household_demo AS (
    SELECT hd_demo_sk
    FROM household_demographics
    WHERE hd_buy_potential = '501-1000'
)
SELECT i_item_desc
      ,w_warehouse_name
      ,d1.d_week_seq
      ,SUM(CASE WHEN p_promo_sk IS NULL THEN 1 ELSE 0 END) no_promo
      ,SUM(CASE WHEN p_promo_sk IS NOT NULL THEN 1 ELSE 0 END) promo
      ,COUNT(*) total_cnt
FROM catalog_sales cs
JOIN filtered_inventory inv ON (cs.cs_item_sk = inv.inv_item_sk)
JOIN warehouse w ON (w.w_warehouse_sk = inv.inv_warehouse_sk)
JOIN item i ON (i.i_item_sk = cs.cs_item_sk AND i.i_item_sk <= 203999)
JOIN filtered_customer_demo cd ON (cs.cs_bill_cdemo_sk = cd.cd_demo_sk)
JOIN filtered_household_demo hd ON (cs.cs_bill_hdemo_sk = hd.hd_demo_sk)
JOIN filtered_dates d1 ON (cs.cs_sold_date_sk = d1.d_date_sk)
JOIN date_dim d2 ON (inv.inv_date_sk = d2.d_date_sk AND d2.d_week_seq = d1.d_week_seq)
JOIN date_dim d3 ON (cs.cs_ship_date_sk = d3.d_date_sk AND d3.d_date > d1.d_date + 5)
LEFT JOIN promotion p ON (cs.cs_promo_sk = p.p_promo_sk)
LEFT JOIN catalog_returns cr ON (cr.cr_item_sk = cs.cs_item_sk AND cr.cr_order_number = cs.cs_order_number)
WHERE inv.inv_quantity_on_hand < cs.cs_quantity
GROUP BY i_item_desc, w_warehouse_name, d1.d_week_seq
ORDER BY total_cnt DESC, i_item_desc, w_warehouse_name, d1.d_week_seq
LIMIT 100;

-- end query 72 in stream 0 using template query72.tpl