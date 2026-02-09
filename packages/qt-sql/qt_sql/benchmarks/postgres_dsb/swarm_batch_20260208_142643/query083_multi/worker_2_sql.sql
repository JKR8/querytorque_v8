WITH date_filter AS (
    SELECT d_date_sk
    FROM date_dim
    WHERE d_month_seq IN (
        SELECT d_month_seq
        FROM date_dim
        WHERE d_date IN ('2002-02-01', '2002-04-11', '2002-07-17', '2002-10-09')
    )
),
filtered_item AS (
    SELECT i_item_sk, i_item_id
    FROM item
    WHERE i_category IN ('Jewelry', 'Music')
      AND i_manager_id BETWEEN 16 AND 25
),
sr_items AS (
    SELECT
        i.i_item_id AS item_id,
        SUM(sr_return_quantity) AS sr_item_qty
    FROM store_returns
    JOIN date_filter d ON sr_returned_date_sk = d.d_date_sk
    JOIN filtered_item i ON sr_item_sk = i.i_item_sk
    WHERE sr_return_amt / sr_return_quantity BETWEEN 184 AND 213
      AND sr_reason_sk IN (26, 32, 40, 66, 73)
    GROUP BY i.i_item_id
),
cr_items AS (
    SELECT
        i.i_item_id AS item_id,
        SUM(cr_return_quantity) AS cr_item_qty
    FROM catalog_returns
    JOIN date_filter d ON cr_returned_date_sk = d.d_date_sk
    JOIN filtered_item i ON cr_item_sk = i.i_item_sk
    WHERE cr_return_amount / cr_return_quantity BETWEEN 184 AND 213
      AND cr_reason_sk IN (26, 32, 40, 66, 73)
    GROUP BY i.i_item_id
),
wr_items AS (
    SELECT
        i.i_item_id AS item_id,
        SUM(wr_return_quantity) AS wr_item_qty
    FROM web_returns
    JOIN date_filter d ON wr_returned_date_sk = d.d_date_sk
    JOIN filtered_item i ON wr_item_sk = i.i_item_sk
    WHERE wr_return_amt / wr_return_quantity BETWEEN 184 AND 213
      AND wr_reason_sk IN (26, 32, 40, 66, 73)
    GROUP BY i.i_item_id
)
SELECT
    sr_items.item_id,
    sr_item_qty,
    sr_item_qty / (sr_item_qty + cr_item_qty + wr_item_qty) / 3.0 * 100 AS sr_dev,
    cr_item_qty,
    cr_item_qty / (sr_item_qty + cr_item_qty + wr_item_qty) / 3.0 * 100 AS cr_dev,
    wr_item_qty,
    wr_item_qty / (sr_item_qty + cr_item_qty + wr_item_qty) / 3.0 * 100 AS wr_dev,
    (sr_item_qty + cr_item_qty + wr_item_qty) / 3.0 AS average
FROM sr_items
JOIN cr_items ON sr_items.item_id = cr_items.item_id
JOIN wr_items ON sr_items.item_id = wr_items.item_id
ORDER BY
    sr_items.item_id,
    sr_item_qty
LIMIT 100