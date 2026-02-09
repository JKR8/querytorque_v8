WITH filtered_date AS (
  SELECT d_date_sk, d_moy
  FROM date_dim
  WHERE d_year = 1998
    AND d_moy IN (1, 2)
),
inv_raw AS (
  SELECT
    w.w_warehouse_sk,
    i.i_item_sk,
    fd.d_moy,
    STDDEV_SAMP(inv.inv_quantity_on_hand) AS stdev,
    AVG(inv.inv_quantity_on_hand) AS mean
  FROM inventory AS inv
  JOIN item AS i ON inv.inv_item_sk = i.i_item_sk
  JOIN warehouse AS w ON inv.inv_warehouse_sk = w.w_warehouse_sk
  JOIN filtered_date AS fd ON inv.inv_date_sk = fd.d_date_sk
  GROUP BY w.w_warehouse_sk, i.i_item_sk, fd.d_moy
),
inv AS (
  SELECT
    w_warehouse_sk,
    i_item_sk,
    d_moy,
    mean,
    CASE mean WHEN 0 THEN NULL ELSE stdev / mean END AS cov
  FROM inv_raw
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
JOIN inv AS inv2
  ON inv1.i_item_sk = inv2.i_item_sk
 AND inv1.w_warehouse_sk = inv2.w_warehouse_sk
WHERE inv1.d_moy = 1
  AND inv2.d_moy = 2
ORDER BY
  inv1.w_warehouse_sk,
  inv1.i_item_sk,
  inv1.d_moy,
  inv1.mean,
  inv1.cov,
  inv2.d_moy,
  inv2.mean,
  inv2.cov