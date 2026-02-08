WITH filtered_item AS (
    SELECT i_item_sk, i_item_id
    FROM item
    WHERE i_current_price BETWEEN 0.99 AND 1.49
),
filtered_date AS (
    SELECT d_date_sk, d_date
    FROM date_dim
    WHERE d_date BETWEEN (
        CAST('2002-02-27' AS DATE) - INTERVAL '30' DAY
    ) AND (
        CAST('2002-02-27' AS DATE) + INTERVAL '30' DAY
    )
),
aggregated AS (
    SELECT
        w.w_warehouse_name,
        i.i_item_id,
        SUM(CASE WHEN d.d_date < CAST('2002-02-27' AS DATE) 
                THEN inv.inv_quantity_on_hand ELSE 0 END) AS inv_before,
        SUM(CASE WHEN d.d_date >= CAST('2002-02-27' AS DATE) 
                THEN inv.inv_quantity_on_hand ELSE 0 END) AS inv_after
    FROM inventory inv
    JOIN warehouse w ON inv.inv_warehouse_sk = w.w_warehouse_sk
    JOIN filtered_item i ON inv.inv_item_sk = i.i_item_sk
    JOIN filtered_date d ON inv.inv_date_sk = d.d_date_sk
    GROUP BY w.w_warehouse_name, i.i_item_id
)
SELECT *
FROM aggregated x
WHERE (
    CASE WHEN inv_before > 0 THEN inv_after / inv_before ELSE NULL END
) BETWEEN 2.0 / 3.0 AND 3.0 / 2.0
ORDER BY w_warehouse_name, i_item_id
LIMIT 100