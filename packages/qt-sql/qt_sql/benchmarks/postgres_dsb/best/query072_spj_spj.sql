WITH filtered_item AS (
    SELECT i_item_sk
    FROM item
    WHERE i_category IN ('Home', 'Men', 'Music')
),
filtered_cd AS (
    SELECT cd_demo_sk
    FROM customer_demographics
    WHERE cd_marital_status = 'M'
      AND cd_dep_count BETWEEN 9 AND 11
),
filtered_hd AS (
    SELECT hd_demo_sk
    FROM household_demographics
    WHERE hd_buy_potential = '501-1000'
),
filtered_d1 AS (
    SELECT d_date_sk, d_week_seq, d_date
    FROM date_dim
    WHERE d_year = 1998
),
filtered_d2 AS (
    SELECT d2.d_date_sk, d2.d_week_seq
    FROM date_dim d2
    INNER JOIN filtered_d1 ON d2.d_week_seq = filtered_d1.d_week_seq
)
SELECT
  MIN(i_item_sk),
  MIN(w_warehouse_name),
  MIN(d1.d_week_seq),
  MIN(cs_item_sk),
  MIN(cs_order_number),
  MIN(inv_item_sk)
FROM catalog_sales
JOIN inventory ON cs_item_sk = inv_item_sk
JOIN warehouse ON w_warehouse_sk = inv_warehouse_sk
JOIN filtered_item ON i_item_sk = cs_item_sk
JOIN filtered_cd ON cs_bill_cdemo_sk = cd_demo_sk
JOIN filtered_hd ON cs_bill_hdemo_sk = hd_demo_sk
JOIN filtered_d1 d1 ON cs_sold_date_sk = d1.d_date_sk
JOIN filtered_d2 d2 ON inv_date_sk = d2.d_date_sk
JOIN date_dim d3 ON cs_ship_date_sk = d3.d_date_sk
LEFT OUTER JOIN promotion ON cs_promo_sk = p_promo_sk
LEFT OUTER JOIN catalog_returns ON cr_item_sk = cs_item_sk AND cr_order_number = cs_order_number
WHERE inv_quantity_on_hand < cs_quantity
  AND d3.d_date > d1.d_date + INTERVAL '3 DAY'
  AND cs_wholesale_cost BETWEEN 34 AND 54
