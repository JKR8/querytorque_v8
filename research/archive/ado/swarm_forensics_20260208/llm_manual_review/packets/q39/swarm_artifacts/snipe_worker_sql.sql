WITH inv_m1 AS (
  SELECT
    w_warehouse_sk,
    i_item_sk,
    d_moy,
    mean,
    CASE mean WHEN 0 THEN NULL ELSE stdev / mean END AS cov
  FROM (
    SELECT
      w_warehouse_sk,
      i_item_sk,
      d_moy,
      AVG(inv_quantity_on_hand) AS mean,
      STDDEV_SAMP(inv_quantity_on_hand) AS stdev
    FROM inventory
    JOIN item ON inv_item_sk = i_item_sk
    JOIN warehouse ON inv_warehouse_sk = w_warehouse_sk
    JOIN date_dim ON inv_date_sk = d_date_sk
    WHERE d_year = 1998
      AND d_moy = 1
    GROUP BY
      w_warehouse_sk,
      i_item_sk,
      d_moy
  ) AS foo
  WHERE CASE mean WHEN 0 THEN 0 ELSE stdev / mean END > 1
),
inv_m2 AS (
  SELECT
    w_warehouse_sk,
    i_item_sk,
    d_moy,
    mean,
    CASE mean WHEN 0 THEN NULL ELSE stdev / mean END AS cov
  FROM (
    SELECT
      w_warehouse_sk,
      i_item_sk,
      d_moy,
      AVG(inv_quantity_on_hand) AS mean,
      STDDEV_SAMP(inv_quantity_on_hand) AS stdev
    FROM inventory
    JOIN item ON inv_item_sk = i_item_sk
    JOIN warehouse ON inv_warehouse_sk = w_warehouse_sk
    JOIN date_dim ON inv_date_sk = d_date_sk
    WHERE d_year = 1998
      AND d_moy = 2
    GROUP BY
      w_warehouse_sk,
      i_item_sk,
      d_moy
  ) AS foo
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
FROM inv_m1 AS inv1
JOIN inv_m2 AS inv2 ON inv1.i_item_sk = inv2.i_item_sk
  AND inv1.w_warehouse_sk = inv2.w_warehouse_sk
ORDER BY
  inv1.w_warehouse_sk,
  inv1.i_item_sk,
  inv1.d_moy,
  inv1.mean,
  inv1.cov,
  inv2.d_moy,
  inv2.mean,
  inv2.cov