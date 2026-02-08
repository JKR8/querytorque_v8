WITH filtered_d1 AS (
    SELECT d_date_sk, d_week_seq, d_date
    FROM date_dim
    WHERE d_year = 2002
),
filtered_hd AS (
    SELECT hd_demo_sk
    FROM household_demographics
    WHERE hd_buy_potential = '501-1000'
),
filtered_cd AS (
    SELECT cd_demo_sk
    FROM customer_demographics
    WHERE cd_marital_status = 'W'
),
pre_joined AS (
    SELECT
        cs.cs_item_sk,
        cs.cs_order_number,
        cs.cs_quantity,
        cs.cs_promo_sk,
        cs.cs_ship_date_sk,
        inv.inv_warehouse_sk,
        inv.inv_quantity_on_hand,
        inv.inv_date_sk,
        d1.d_week_seq,
        d1.d_date
    FROM catalog_sales cs
    JOIN inventory inv ON cs.cs_item_sk = inv.inv_item_sk
    JOIN filtered_d1 d1 ON cs.cs_sold_date_sk = d1.d_date_sk
    JOIN filtered_cd cd ON cs.cs_bill_cdemo_sk = cd.cd_demo_sk
    JOIN filtered_hd hd ON cs.cs_bill_hdemo_sk = hd.hd_demo_sk
    WHERE inv.inv_quantity_on_hand < cs.cs_quantity
)
SELECT
    i.i_item_desc,
    w.w_warehouse_name,
    d1.d_week_seq,
    SUM(CASE WHEN p.p_promo_sk IS NULL THEN 1 ELSE 0 END) AS no_promo,
    SUM(CASE WHEN NOT p.p_promo_sk IS NULL THEN 1 ELSE 0 END) AS promo,
    COUNT(*) AS total_cnt
FROM pre_joined pj
JOIN warehouse w ON pj.inv_warehouse_sk = w.w_warehouse_sk
JOIN item i ON pj.cs_item_sk = i.i_item_sk
JOIN date_dim d2 ON pj.inv_date_sk = d2.d_date_sk
JOIN date_dim d3 ON pj.cs_ship_date_sk = d3.d_date_sk
LEFT OUTER JOIN promotion p ON pj.cs_promo_sk = p.p_promo_sk
LEFT OUTER JOIN catalog_returns cr ON (
    pj.cs_item_sk = cr.cr_item_sk 
    AND pj.cs_order_number = cr.cr_order_number
)
WHERE d1.d_week_seq = d2.d_week_seq
    AND d3.d_date > d1.d_date + 5
GROUP BY
    i.i_item_desc,
    w.w_warehouse_name,
    d1.d_week_seq
ORDER BY
    total_cnt DESC,
    i.i_item_desc,
    w.w_warehouse_name,
    d1.d_week_seq
LIMIT 100