WITH ss_items AS (
  SELECT
    i_item_id AS item_id,
    c_birth_year AS birth_year,
    SUM(ss_ext_sales_price) AS ss_item_rev
  FROM store_sales, item, date_dim, customer
  WHERE
    ss_item_sk = i_item_sk
    AND d_date IN (
      SELECT
        d_date
      FROM date_dim
      WHERE
        d_month_seq = (
          SELECT
            d_month_seq
          FROM date_dim
          WHERE
            d_date = '1998-05-19'
        )
    )
    AND ss_sold_date_sk = d_date_sk
    AND ss_list_price BETWEEN 50 AND 80
    AND i_manager_id BETWEEN 71 AND 100
    AND ss_customer_sk = c_customer_sk
    AND c_birth_year BETWEEN 1945 AND 1951
  GROUP BY
    i_item_id,
    c_birth_year
), cs_items AS (
  SELECT
    i_item_id AS item_id,
    c_birth_year AS birth_year,
    SUM(cs_ext_sales_price) AS cs_item_rev
  FROM catalog_sales, item, date_dim, customer
  WHERE
    cs_item_sk = i_item_sk
    AND d_date IN (
      SELECT
        d_date
      FROM date_dim
      WHERE
        d_month_seq = (
          SELECT
            d_month_seq
          FROM date_dim
          WHERE
            d_date = '1998-05-19'
        )
    )
    AND cs_sold_date_sk = d_date_sk
    AND cs_list_price BETWEEN 50 AND 80
    AND i_manager_id BETWEEN 71 AND 100
    AND cs_bill_customer_sk = c_customer_sk
    AND c_birth_year BETWEEN 1945 AND 1951
  GROUP BY
    i_item_id,
    c_birth_year
), ws_items AS (
  SELECT
    i_item_id AS item_id,
    c_birth_year AS birth_year,
    SUM(ws_ext_sales_price) AS ws_item_rev
  FROM web_sales, item, date_dim, customer
  WHERE
    ws_item_sk = i_item_sk
    AND d_date IN (
      SELECT
        d_date
      FROM date_dim
      WHERE
        d_month_seq = (
          SELECT
            d_month_seq
          FROM date_dim
          WHERE
            d_date = '1998-05-19'
        )
    )
    AND ws_sold_date_sk = d_date_sk
    AND ws_list_price BETWEEN 50 AND 80
    AND i_manager_id BETWEEN 71 AND 100
    AND ws_bill_customer_sk = c_customer_sk
    AND c_birth_year BETWEEN 1945 AND 1951
  GROUP BY
    i_item_id,
    c_birth_year
)
SELECT
  ss_items.item_id,
  ss_items.birth_year,
  ss_item_rev,
  ss_item_rev / (
    (
      ss_item_rev + cs_item_rev + ws_item_rev
    ) / 3
  ) * 100 AS ss_dev,
  cs_item_rev,
  cs_item_rev / (
    (
      ss_item_rev + cs_item_rev + ws_item_rev
    ) / 3
  ) * 100 AS cs_dev,
  ws_item_rev,
  ws_item_rev / (
    (
      ss_item_rev + cs_item_rev + ws_item_rev
    ) / 3
  ) * 100 AS ws_dev,
  (
    ss_item_rev + cs_item_rev + ws_item_rev
  ) / 3 AS average
FROM ss_items, cs_items, ws_items
WHERE
  ss_items.item_id = cs_items.item_id
  AND ss_items.item_id = ws_items.item_id
  AND ss_items.birth_year = cs_items.birth_year
  AND ss_items.birth_year = ws_items.birth_year
  AND TRUE
  AND TRUE
  AND TRUE
  AND TRUE
  AND TRUE
  AND TRUE
ORDER BY
  item_id,
  birth_year,
  ss_item_rev
LIMIT 100;
