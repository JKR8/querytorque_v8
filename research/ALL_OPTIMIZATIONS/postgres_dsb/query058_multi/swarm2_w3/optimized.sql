WITH date_filter AS (
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
all_sales AS (
  -- Store sales
  SELECT
    fi.i_item_id AS item_id,
    fc.c_birth_year AS birth_year,
    'ss' AS channel,
    ss_ext_sales_price AS sales_price
  FROM store_sales ss
  JOIN filtered_item fi ON ss.ss_item_sk = fi.i_item_sk
  JOIN date_filter df ON ss.ss_sold_date_sk = df.d_date_sk
  JOIN filtered_customer fc ON ss.ss_customer_sk = fc.c_customer_sk
  WHERE ss.ss_list_price BETWEEN 217 AND 246
  
  UNION ALL
  
  -- Catalog sales
  SELECT
    fi.i_item_id AS item_id,
    fc.c_birth_year AS birth_year,
    'cs' AS channel,
    cs_ext_sales_price AS sales_price
  FROM catalog_sales cs
  JOIN filtered_item fi ON cs.cs_item_sk = fi.i_item_sk
  JOIN date_filter df ON cs.cs_sold_date_sk = df.d_date_sk
  JOIN filtered_customer fc ON cs.cs_bill_customer_sk = fc.c_customer_sk
  WHERE cs.cs_list_price BETWEEN 217 AND 246
  
  UNION ALL
  
  -- Web sales
  SELECT
    fi.i_item_id AS item_id,
    fc.c_birth_year AS birth_year,
    'ws' AS channel,
    ws_ext_sales_price AS sales_price
  FROM web_sales ws
  JOIN filtered_item fi ON ws.ws_item_sk = fi.i_item_sk
  JOIN date_filter df ON ws.ws_sold_date_sk = df.d_date_sk
  JOIN filtered_customer fc ON ws.ws_bill_customer_sk = fc.c_customer_sk
  WHERE ws.ws_list_price BETWEEN 217 AND 246
),
aggregated AS (
  SELECT
    item_id,
    birth_year,
    SUM(CASE WHEN channel = 'ss' THEN sales_price ELSE 0 END) AS ss_item_rev,
    SUM(CASE WHEN channel = 'cs' THEN sales_price ELSE 0 END) AS cs_item_rev,
    SUM(CASE WHEN channel = 'ws' THEN sales_price ELSE 0 END) AS ws_item_rev
  FROM all_sales
  GROUP BY item_id, birth_year
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
FROM aggregated
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