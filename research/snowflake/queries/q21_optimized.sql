-- TPC-DS Query 21 (Optimized - date CTE isolation + explicit JOINs)
-- Transform: COMMA_JOIN_DATE_PRUNING_FAILURE fix (P1 pathology)
-- Key change: Date filter isolated in CTE enables runtime partition pruning
-- Previous result on X-Small: 0.7s (from TIMEOUT >300s)
WITH date_filter AS (
    SELECT d_date_sk, d_date
    FROM date_dim
    WHERE d_date BETWEEN DATEADD(DAY, -30, '2002-02-27'::DATE)
                     AND DATEADD(DAY, 30, '2002-02-27'::DATE)
)
SELECT * FROM (
    SELECT w_warehouse_name,
           i_item_id,
           SUM(CASE WHEN d_date < '2002-02-27'::DATE
                    THEN inv_quantity_on_hand ELSE 0 END) AS inv_before,
           SUM(CASE WHEN d_date >= '2002-02-27'::DATE
                    THEN inv_quantity_on_hand ELSE 0 END) AS inv_after
    FROM inventory
        JOIN date_filter ON inv_date_sk = d_date_sk
        JOIN warehouse ON inv_warehouse_sk = w_warehouse_sk
        JOIN item ON i_item_sk = inv_item_sk
    WHERE i_current_price BETWEEN 0.99 AND 1.49
    GROUP BY w_warehouse_name, i_item_id
) x
WHERE (CASE WHEN inv_before > 0 THEN inv_after / inv_before ELSE NULL END)
      BETWEEN 2.0/3.0 AND 3.0/2.0
ORDER BY w_warehouse_name, i_item_id
LIMIT 100;
