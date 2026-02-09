WITH date_range AS (
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
combined_sales AS (
    SELECT
        i.i_item_id AS item_id,
        c.c_birth_year AS birth_year,
        'store' AS channel,
        SUM(ss_ext_sales_price) AS revenue
    FROM store_sales ss
    JOIN date_range d ON ss.ss_sold_date_sk = d.d_date_sk
    JOIN filtered_item i ON ss.ss_item_sk = i.i_item_sk
    JOIN filtered_customer c ON ss.ss_customer_sk = c.c_customer_sk
    WHERE ss.ss_list_price BETWEEN 217 AND 246
    GROUP BY i.i_item_id, c.c_birth_year
    
    UNION ALL
    
    SELECT
        i.i_item_id AS item_id,
        c.c_birth_year AS birth_year,
        'catalog' AS channel,
        SUM(cs_ext_sales_price) AS revenue
    FROM catalog_sales cs
    JOIN date_range d ON cs.cs_sold_date_sk = d.d_date_sk
    JOIN filtered_item i ON cs.cs_item_sk = i.i_item_sk
    JOIN filtered_customer c ON cs.cs_bill_customer_sk = c.c_customer_sk
    WHERE cs.cs_list_price BETWEEN 217 AND 246
    GROUP BY i.i_item_id, c.c_birth_year
    
    UNION ALL
    
    SELECT
        i.i_item_id AS item_id,
        c.c_birth_year AS birth_year,
        'web' AS channel,
        SUM(ws_ext_sales_price) AS revenue
    FROM web_sales ws
    JOIN date_range d ON ws.ws_sold_date_sk = d.d_date_sk
    JOIN filtered_item i ON ws.ws_item_sk = i.i_item_sk
    JOIN filtered_customer c ON ws.ws_bill_customer_sk = c.c_customer_sk
    WHERE ws.ws_list_price BETWEEN 217 AND 246
    GROUP BY i.i_item_id, c.c_birth_year
),
pivoted AS (
    SELECT
        item_id,
        birth_year,
        MAX(CASE WHEN channel = 'store' THEN revenue END) AS ss_item_rev,
        MAX(CASE WHEN channel = 'catalog' THEN revenue END) AS cs_item_rev,
        MAX(CASE WHEN channel = 'web' THEN revenue END) AS ws_item_rev
    FROM combined_sales
    GROUP BY item_id, birth_year
    HAVING COUNT(*) = 3
)
SELECT
    item_id,
    birth_year,
    ss_item_rev,
    ss_item_rev / ((ss_item_rev + cs_item_rev + ws_item_rev) / 3) * 100 AS ss_dev,
    cs_item_rev,
    cs_item_rev / ((ss_item_rev + cs_item_rev + ws_item_rev) / 3) * 100 AS cs_dev,
    ws_item_rev,
    ws_item_rev / ((ss_item_rev + cs_item_rev + ws_item_rev) / 3) * 100 AS ws_dev,
    (ss_item_rev + cs_item_rev + ws_item_rev) / 3 AS average
FROM pivoted
WHERE
    ss_item_rev BETWEEN 0.9 * cs_item_rev AND 1.1 * cs_item_rev
    AND ss_item_rev BETWEEN 0.9 * ws_item_rev AND 1.1 * ws_item_rev
    AND cs_item_rev BETWEEN 0.9 * ss_item_rev AND 1.1 * ss_item_rev
    AND cs_item_rev BETWEEN 0.9 * ws_item_rev AND 1.1 * ws_item_rev
    AND ws_item_rev BETWEEN 0.9 * ss_item_rev AND 1.1 * ss_item_rev
    AND ws_item_rev BETWEEN 0.9 * cs_item_rev AND 1.1 * cs_item_rev
ORDER BY
    item_id,
    birth_year,
    ss_item_rev
LIMIT 100;