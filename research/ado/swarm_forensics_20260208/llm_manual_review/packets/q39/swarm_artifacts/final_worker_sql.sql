WITH filtered_dates AS (
  SELECT d_date_sk
  FROM date_dim
  WHERE d_year = 1998
    AND d_moy IN (1, 2)
),

-- First pass: approximate statistics for rapid filtering
approx_stats AS (
  SELECT
    w.w_warehouse_sk,
    w.w_warehouse_name,
    i.i_item_sk,
    d.d_moy,
    APPROX_QUANTILE(inv.inv_quantity_on_hand, 0.5) AS median_approx,
    APPROX_QUANTILE(inv.inv_quantity_on_hand, 0.841) - 
    APPROX_QUANTILE(inv.inv_quantity_on_hand, 0.159) AS stddev_approx,
    COUNT(*) AS cnt
  FROM inventory inv
  JOIN item i ON inv.inv_item_sk = i.i_item_sk
  JOIN warehouse w ON inv.inv_warehouse_sk = w.w_warehouse_sk
  JOIN filtered_dates fd ON inv.inv_date_sk = fd.d_date_sk
  GROUP BY w.w_warehouse_sk, w.w_warehouse_name, i.i_item_sk, d.d_moy
  HAVING cnt > 1  -- Need at least 2 samples for stddev
),

-- Filter to items with high approximate coefficient of variation
high_var_candidates AS (
  SELECT
    w_warehouse_sk,
    w_warehouse_name,
    i_item_sk,
    d_moy,
    median_approx,
    stddev_approx,
    CASE 
      WHEN median_approx = 0 THEN NULL 
      ELSE stddev_approx / median_approx 
    END AS cov_approx
  FROM approx_stats
  WHERE CASE 
    WHEN median_approx = 0 THEN 0 
    ELSE stddev_approx / median_approx 
  END > 0.5  -- Conservative filter, will recheck exact > 1
),

-- Second pass: compute exact statistics only for high-variance candidates
exact_stats AS (
  SELECT
    hvc.w_warehouse_name,
    hvc.w_warehouse_sk,
    hvc.i_item_sk,
    hvc.d_moy,
    STDDEV_SAMP(inv.inv_quantity_on_hand) AS stdev,
    AVG(inv.inv_quantity_on_hand) AS mean,
    COUNT(*) AS cnt
  FROM inventory inv
  JOIN high_var_candidates hvc 
    ON inv.inv_item_sk = hvc.i_item_sk
    AND inv.inv_warehouse_sk = hvc.w_warehouse_sk
  JOIN filtered_dates fd ON inv.inv_date_sk = fd.d_date_sk
    AND fd.d_date_sk IN (
      SELECT d_date_sk 
      FROM date_dim 
      WHERE d_year = 1998 
        AND d_moy = hvc.d_moy
    )
  -- Use approximate bounds to filter extreme outliers early
  WHERE inv.inv_quantity_on_hand BETWEEN 
    GREATEST(0, hvc.median_approx - 3 * hvc.stddev_approx) AND
    (hvc.median_approx + 3 * hvc.stddev_approx)
  GROUP BY 
    hvc.w_warehouse_name,
    hvc.w_warehouse_sk,
    hvc.i_item_sk,
    hvc.d_moy
  HAVING cnt > 1  -- Ensure valid stddev calculation
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
  FROM exact_stats
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
JOIN inv AS inv2 ON 
  inv1.i_item_sk = inv2.i_item_sk
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