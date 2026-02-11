WITH
-- Isolate d1 with its filter
filtered_d1 AS (
    SELECT d_date_sk, d_week_seq, d_date
    FROM date_dim
    WHERE d_year = 2002
),
-- Isolate d2 (no independent filter, but we'll join later with d1 on week_seq)
filtered_d2 AS (
    SELECT d_date_sk, d_week_seq
    FROM date_dim
),
-- Isolate d3 (no independent filter, but we'll apply date comparison later)
filtered_d3 AS (
    SELECT d_date_sk, d_date
    FROM date_dim
),
-- Pre-filter all dimension tables
filtered_cd AS (
    SELECT cd_demo_sk
    FROM customer_demographics
    WHERE cd_marital_status = 'W'
),
filtered_hd AS (
    SELECT hd_demo_sk
    FROM household_demographics
    WHERE hd_buy_potential = '501-1000'
),
-- Join fact tables with pre-filtered dimensions early
cs_with_dates AS (
    SELECT
        cs.cs_item_sk,
        cs.cs_order_number,
        cs.cs_promo_sk,
        cs.cs_bill_cdemo_sk,
        cs.cs_bill_hdemo_sk,
        cs.cs_quantity,
        cs.cs_ship_date_sk,
        d1.d_week_seq,
        d1.d_date
    FROM catalog_sales cs
    JOIN filtered_d1 d1 ON cs.cs_sold_date_sk = d1.d_date_sk
    JOIN filtered_cd cd ON cs.cs_bill_cdemo_sk = cd.cd_demo_sk
    JOIN filtered_hd hd ON cs.cs_bill_hdemo_sk = hd.hd_demo_sk
),
inv_with_dates AS (
    SELECT
        inv.inv_item_sk,
        inv.inv_warehouse_sk,
        inv.inv_quantity_on_hand,
        inv.inv_date_sk,
        d2.d_week_seq
    FROM inventory inv
    JOIN filtered_d2 d2 ON inv.inv_date_sk = d2.d_date_sk
),
-- Main join combining filtered fact tables
base_data AS (
    SELECT
        cs.cs_item_sk,
        cs.cs_order_number,
        cs.cs_promo_sk,
        cs.cs_quantity,
        cs.d_week_seq,
        cs.d_date,
        inv.inv_warehouse_sk,
        inv.inv_quantity_on_hand,
        inv.d_week_seq AS inv_week_seq
    FROM cs_with_dates cs
    JOIN inv_with_dates inv ON cs.cs_item_sk = inv.inv_item_sk
    WHERE cs.d_week_seq = inv.d_week_seq
      AND inv.inv_quantity_on_hand < cs.cs_quantity
)
-- Final aggregation with remaining joins
SELECT
    i.i_item_desc,
    w.w_warehouse_name,
    base.d_week_seq,
    SUM(CASE WHEN p.p_promo_sk IS NULL THEN 1 ELSE 0 END) AS no_promo,
    SUM(CASE WHEN NOT p.p_promo_sk IS NULL THEN 1 ELSE 0 END) AS promo,
    COUNT(*) AS total_cnt
FROM base_data base
JOIN filtered_d3 d3 ON base.cs_ship_date_sk = d3.d_date_sk
JOIN item i ON base.cs_item_sk = i.i_item_sk
JOIN warehouse w ON base.inv_warehouse_sk = w.w_warehouse_sk
LEFT OUTER JOIN promotion p ON base.cs_promo_sk = p.p_promo_sk
LEFT OUTER JOIN catalog_returns cr ON 
    base.cs_item_sk = cr.cr_item_sk 
    AND base.cs_order_number = cr.cr_order_number
WHERE d3.d_date > base.d_date + 5
GROUP BY
    i.i_item_desc,
    w.w_warehouse_name,
    base.d_week_seq
ORDER BY
    total_cnt DESC,
    i.i_item_desc,
    w.w_warehouse_name,
    base.d_week_seq
LIMIT 100