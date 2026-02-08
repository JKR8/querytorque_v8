WITH target_dates AS (
    SELECT d_date_sk, d_date
    FROM date_dim
    WHERE d_month_seq = (
        SELECT d_month_seq
        FROM date_dim
        WHERE d_date = '1999-05-02'
    )
),
filtered_item AS (
    SELECT i_item_sk, i_item_id
    FROM item
    WHERE i_manager_id BETWEEN 25 AND 54
),
filtered_customer AS (
    SELECT c_customer_sk, c_birth_year
    FROM customer
    WHERE c_birth_year BETWEEN 1961 AND 1967
),
ss_items AS (
    SELECT
        i_item_id AS item_id,
        c_birth_year AS birth_year,
        SUM(ss_ext_sales_price) AS ss_item_rev
    FROM store_sales
    JOIN filtered_item ON ss_item_sk = i_item_sk
    JOIN target_dates ON ss_sold_date_sk = d_date_sk
    JOIN filtered_customer ON ss_customer_sk = c_customer_sk
    WHERE ss_list_price BETWEEN 217 AND 246
    GROUP BY i_item_id, c_birth_year
),
cs_items AS (
    SELECT
        i_item_id AS item_id,
        c_birth_year AS birth_year,
        SUM(cs_ext_sales_price) AS cs_item_rev
    FROM catalog_sales
    JOIN filtered_item ON cs_item_sk = i_item_sk
    JOIN target_dates ON cs_sold_date_sk = d_date_sk
    JOIN filtered_customer ON cs_bill_customer_sk = c_customer_sk
    WHERE cs_list_price BETWEEN 217 AND 246
    GROUP BY i_item_id, c_birth_year
),
ws_items AS (
    SELECT
        i_item_id AS item_id,
        c_birth_year AS birth_year,
        SUM(ws_ext_sales_price) AS ws_item_rev
    FROM web_sales
    JOIN filtered_item ON ws_item_sk = i_item_sk
    JOIN target_dates ON ws_sold_date_sk = d_date_sk
    JOIN filtered_customer ON ws_bill_customer_sk = c_customer_sk
    WHERE ws_list_price BETWEEN 217 AND 246
    GROUP BY i_item_id, c_birth_year
)
SELECT
    ss_items.item_id,
    ss_items.birth_year,
    ss_item_rev,
    ss_item_rev / ((ss_item_rev + cs_item_rev + ws_item_rev) / 3) * 100 AS ss_dev,
    cs_item_rev,
    cs_item_rev / ((ss_item_rev + cs_item_rev + ws_item_rev) / 3) * 100 AS cs_dev,
    ws_item_rev,
    ws_item_rev / ((ss_item_rev + cs_item_rev + ws_item_rev) / 3) * 100 AS ws_dev,
    (ss_item_rev + cs_item_rev + ws_item_rev) / 3 AS average
FROM ss_items
JOIN cs_items ON ss_items.item_id = cs_items.item_id AND ss_items.birth_year = cs_items.birth_year
JOIN ws_items ON ss_items.item_id = ws_items.item_id AND ss_items.birth_year = ws_items.birth_year
WHERE ss_item_rev BETWEEN 0.9 * cs_item_rev AND 1.1 * cs_item_rev
    AND ss_item_rev BETWEEN 0.9 * ws_item_rev AND 1.1 * ws_item_rev
    AND cs_item_rev BETWEEN 0.9 * ss_item_rev AND 1.1 * ss_item_rev
    AND cs_item_rev BETWEEN 0.9 * ws_item_rev AND 1.1 * ws_item_rev
    AND ws_item_rev BETWEEN 0.9 * ss_item_rev AND 1.1 * ss_item_rev
    AND ws_item_rev BETWEEN 0.9 * cs_item_rev AND 1.1 * cs_item_rev
ORDER BY item_id, birth_year, ss_item_rev
LIMIT 100