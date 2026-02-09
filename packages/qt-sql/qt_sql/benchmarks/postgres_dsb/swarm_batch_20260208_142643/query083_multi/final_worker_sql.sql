WITH target_months AS (
  SELECT d_date_sk, d_date
  FROM date_dim
  WHERE d_month_seq IN (
    SELECT d_month_seq
    FROM date_dim
    WHERE d_date IN ('2002-02-01', '2002-04-11', '2002-07-17', '2002-10-09')
  )
),
filtered_items AS (
  SELECT i_item_sk, i_item_id
  FROM item
  WHERE i_category IN ('Jewelry', 'Music')
    AND i_manager_id BETWEEN 16 AND 25
),
sr_agg AS (
  SELECT
    i.i_item_id AS item_id,
    SUM(sr_return_quantity) AS sr_item_qty
  FROM store_returns sr
  JOIN filtered_items i ON sr.sr_item_sk = i.i_item_sk
  JOIN target_months d ON sr.sr_returned_date_sk = d.d_date_sk
  WHERE sr.sr_reason_sk IN (26, 32, 40, 66, 73)
    AND sr.sr_return_amt BETWEEN 184 * sr.sr_return_quantity AND 213 * sr.sr_return_quantity
  GROUP BY i.i_item_id
),
cr_agg AS (
  SELECT
    i.i_item_id AS item_id,
    SUM(cr_return_quantity) AS cr_item_qty
  FROM catalog_returns cr
  JOIN filtered_items i ON cr.cr_item_sk = i.i_item_sk
  JOIN target_months d ON cr.cr_returned_date_sk = d.d_date_sk
  WHERE cr.cr_reason_sk IN (26, 32, 40, 66, 73)
    AND cr.cr_return_amount BETWEEN 184 * cr.cr_return_quantity AND 213 * cr.cr_return_quantity
  GROUP BY i.i_item_id
),
wr_agg AS (
  SELECT
    i.i_item_id AS item_id,
    SUM(wr_return_quantity) AS wr_item_qty
  FROM web_returns wr
  JOIN filtered_items i ON wr.wr_item_sk = i.i_item_sk
  JOIN target_months d ON wr.wr_returned_date_sk = d.d_date_sk
  WHERE wr.wr_reason_sk IN (26, 32, 40, 66, 73)
    AND wr.wr_return_amt BETWEEN 184 * wr.wr_return_quantity AND 213 * wr.wr_return_quantity
  GROUP BY i.i_item_id
)
SELECT
  COALESCE(sr.item_id, cr.item_id, wr.item_id) AS item_id,
  COALESCE(sr.sr_item_qty, 0) AS sr_item_qty,
  CASE 
    WHEN COALESCE(sr.sr_item_qty, 0) + COALESCE(cr.cr_item_qty, 0) + COALESCE(wr.wr_item_qty, 0) = 0 THEN 0
    ELSE COALESCE(sr.sr_item_qty, 0) / (COALESCE(sr.sr_item_qty, 0) + COALESCE(cr.cr_item_qty, 0) + COALESCE(wr.wr_item_qty, 0)) / 3.0 * 100
  END AS sr_dev,
  COALESCE(cr.cr_item_qty, 0) AS cr_item_qty,
  CASE 
    WHEN COALESCE(sr.sr_item_qty, 0) + COALESCE(cr.cr_item_qty, 0) + COALESCE(wr.wr_item_qty, 0) = 0 THEN 0
    ELSE COALESCE(cr.cr_item_qty, 0) / (COALESCE(sr.sr_item_qty, 0) + COALESCE(cr.cr_item_qty, 0) + COALESCE(wr.wr_item_qty, 0)) / 3.0 * 100
  END AS cr_dev,
  COALESCE(wr.wr_item_qty, 0) AS wr_item_qty,
  CASE 
    WHEN COALESCE(sr.sr_item_qty, 0) + COALESCE(cr.cr_item_qty, 0) + COALESCE(wr.wr_item_qty, 0) = 0 THEN 0
    ELSE COALESCE(wr.wr_item_qty, 0) / (COALESCE(sr.sr_item_qty, 0) + COALESCE(cr.cr_item_qty, 0) + COALESCE(wr.wr_item_qty, 0)) / 3.0 * 100
  END AS wr_dev,
  (COALESCE(sr.sr_item_qty, 0) + COALESCE(cr.cr_item_qty, 0) + COALESCE(wr.wr_item_qty, 0)) / 3.0 AS average
FROM sr_agg sr
FULL OUTER JOIN cr_agg cr ON sr.item_id = cr.item_id
FULL OUTER JOIN wr_agg wr ON COALESCE(sr.item_id, cr.item_id) = wr.item_id
WHERE COALESCE(sr.sr_item_qty, 0) + COALESCE(cr.cr_item_qty, 0) + COALESCE(wr.wr_item_qty, 0) > 0
ORDER BY
  COALESCE(sr.item_id, cr.item_id, wr.item_id),
  COALESCE(sr.sr_item_qty, 0)
LIMIT 100;