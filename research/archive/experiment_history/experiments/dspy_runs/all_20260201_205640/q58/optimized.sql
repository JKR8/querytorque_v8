WITH date_range AS (
    SELECT d_date_sk
    FROM date_dim
    WHERE d_week_seq = (SELECT d_week_seq 
                        FROM date_dim 
                        WHERE d_date = '2001-03-24')
),
ss_items AS (
    SELECT i_item_id AS item_id,
           SUM(ss_ext_sales_price) AS ss_item_rev
    FROM store_sales
    JOIN item ON ss_item_sk = i_item_sk
    JOIN date_range dr ON ss_sold_date_sk = dr.d_date_sk
    GROUP BY i_item_id
),
cs_items AS (
    SELECT i_item_id AS item_id,
           SUM(cs_ext_sales_price) AS cs_item_rev
    FROM catalog_sales
    JOIN item ON cs_item_sk = i_item_sk
    JOIN date_range dr ON cs_sold_date_sk = dr.d_date_sk
    GROUP BY i_item_id
),
ws_items AS (
    SELECT i_item_id AS item_id,
           SUM(ws_ext_sales_price) AS ws_item_rev
    FROM web_sales
    JOIN item ON ws_item_sk = i_item_sk
    JOIN date_range dr ON ws_sold_date_sk = dr.d_date_sk
    GROUP BY i_item_id
)
SELECT ss.item_id,
       ss.ss_item_rev,
       ss.ss_item_rev / ((ss.ss_item_rev + cs.cs_item_rev + ws.ws_item_rev) / 3) * 100 AS ss_dev,
       cs.cs_item_rev,
       cs.cs_item_rev / ((ss.ss_item_rev + cs.cs_item_rev + ws.ws_item_rev) / 3) * 100 AS cs_dev,
       ws.ws_item_rev,
       ws.ws_item_rev / ((ss.ss_item_rev + cs.cs_item_rev + ws.ws_item_rev) / 3) * 100 AS ws_dev,
       (ss.ss_item_rev + cs.cs_item_rev + ws.ws_item_rev) / 3 AS average
FROM ss_items ss
JOIN cs_items cs ON ss.item_id = cs.item_id
JOIN ws_items ws ON ss.item_id = ws.item_id
WHERE ss.ss_item_rev BETWEEN 0.9 * cs.cs_item_rev AND 1.1 * cs.cs_item_rev
  AND ss.ss_item_rev BETWEEN 0.9 * ws.ws_item_rev AND 1.1 * ws.ws_item_rev
  AND cs.cs_item_rev BETWEEN 0.9 * ws.ws_item_rev AND 1.1 * ws.ws_item_rev
ORDER BY ss.item_id, ss.ss_item_rev
LIMIT 100;