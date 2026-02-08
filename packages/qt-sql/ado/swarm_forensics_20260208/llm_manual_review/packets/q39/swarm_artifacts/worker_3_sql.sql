WITH filtered_dates AS (
  SELECT d_date_sk, d_moy
  FROM date_dim
  WHERE d_year = 1998
    AND d_moy IN (1, 2)
),

inventory_joined AS (
  SELECT
    w.w_warehouse_sk,
    w.w_warehouse_name,
    i.i_item_sk,
    fd.d_moy,
    inv.inv_quantity_on_hand
  FROM inventory inv
  JOIN filtered_dates fd ON inv.inv_date_sk = fd.d_date_sk
  JOIN item i ON inv.inv_item_sk = i.i_item_sk
  JOIN warehouse w ON inv.inv_warehouse_sk = w.w_warehouse_sk
),

monthly_stats AS (
  SELECT
    w_warehouse_sk,
    w_warehouse_name,
    i_item_sk,
    d_moy,
    AVG(inv_quantity_on_hand) AS mean,
    STDDEV_SAMP(inv_quantity_on_hand) AS stdev
  FROM inventory_joined
  GROUP BY w_warehouse_sk, w_warehouse_name, i_item_sk, d_moy
),

filtered_stats AS (
  SELECT
    w_warehouse_sk,
    w_warehouse_name,
    i_item_sk,
    d_moy,
    mean,
    stdev,
    CASE mean WHEN 0 THEN NULL ELSE stdev / mean END AS cov
  FROM monthly_stats
  WHERE CASE mean WHEN 0 THEN 0 ELSE stdev / mean END > 1
),

pivoted AS (
  SELECT
    fs1.w_warehouse_sk AS w_warehouse_sk_1,
    fs1.i_item_sk AS i_item_sk_1,
    fs1.d_moy AS d_moy_1,
    fs1.mean AS mean_1,
    fs1.cov AS cov_1,
    fs2.w_warehouse_sk AS w_warehouse_sk_2,
    fs2.i_item_sk AS i_item_sk_2,
    fs2.d_moy AS d_moy_2,
    fs2.mean AS mean_2,
    fs2.cov AS cov_2
  FROM filtered_stats fs1
  JOIN filtered_stats fs2
    ON fs1.i_item_sk = fs2.i_item_sk
    AND fs1.w_warehouse_sk = fs2.w_warehouse_sk
    AND fs1.d_moy = 1
    AND fs2.d_moy = 2
)

SELECT
  w_warehouse_sk_1 AS w_warehouse_sk,
  i_item_sk_1 AS i_item_sk,
  d_moy_1 AS d_moy,
  mean_1 AS mean,
  cov_1 AS cov,
  w_warehouse_sk_2 AS w_warehouse_sk,
  i_item_sk_2 AS i_item_sk,
  d_moy_2 AS d_moy,
  mean_2 AS mean,
  cov_2 AS cov
FROM pivoted
ORDER BY
  w_warehouse_sk_1,
  i_item_sk_1,
  d_moy_1,
  mean_1,
  cov_1,
  d_moy_2,
  mean_2,
  cov_2