WITH filtered_date AS (
    SELECT d_date_sk, d_moy
    FROM date_dim
    WHERE d_year = 2002
      AND d_moy IN (10, 11)
),
filtered_item AS (
    SELECT i_item_sk
    FROM item
    WHERE i_category IN ('Home', 'Men')
      AND i_manager_id BETWEEN 25 AND 44
),
filtered_warehouse AS (
    SELECT w_warehouse_sk, w_warehouse_name
    FROM warehouse
),
inv_base AS (
    SELECT
        w.w_warehouse_name,
        w.w_warehouse_sk,
        i.i_item_sk,
        d.d_moy,
        STDDEV_SAMP(inv.inv_quantity_on_hand) AS stdev,
        AVG(inv.inv_quantity_on_hand) AS mean
    FROM inventory inv
    JOIN filtered_date d ON inv.inv_date_sk = d.d_date_sk
    JOIN filtered_item i ON inv.inv_item_sk = i.i_item_sk
    JOIN filtered_warehouse w ON inv.inv_warehouse_sk = w.w_warehouse_sk
    WHERE inv.inv_quantity_on_hand BETWEEN 140 AND 340
    GROUP BY
        w.w_warehouse_name,
        w.w_warehouse_sk,
        i.i_item_sk,
        d.d_moy
),
inv AS (
    SELECT
        w_warehouse_name,
        w_warehouse_sk,
        i_item_sk,
        d_moy,
        stdev,
        mean,
        CASE mean WHEN 0 THEN NULL ELSE stdev / mean END AS cov
    FROM inv_base
    WHERE CASE mean WHEN 0 THEN 0 ELSE stdev / mean END > 1
)
SELECT
    inv1.w_warehouse_sk,
    inv1.i_item_sk,
    inv1.d_moy,
    inv1.mean,
    inv1.cov,
    inv2.w_warehouse_sk,
    inv2.i_item_sk,
    inv2.d_moy,
    inv2.mean,
    inv2.cov
FROM inv AS inv1
JOIN inv AS inv2 ON inv1.i_item_sk = inv2.i_item_sk
    AND inv1.w_warehouse_sk = inv2.w_warehouse_sk
WHERE inv1.d_moy = 10
    AND inv2.d_moy = 11
ORDER BY
    inv1.w_warehouse_sk,
    inv1.i_item_sk,
    inv1.d_moy,
    inv1.mean,
    inv1.cov,
    inv2.d_moy,
    inv2.mean,
    inv2.cov