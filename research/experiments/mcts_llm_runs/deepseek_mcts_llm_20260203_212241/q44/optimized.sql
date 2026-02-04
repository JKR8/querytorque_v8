WITH store_avg_profit AS (
  SELECT
    AVG(ss_net_profit) AS rank_col
  FROM store_sales
  WHERE
    ss_store_sk = 146 AND ss_addr_sk IS NULL
  GROUP BY
    ss_store_sk
), qualified_items AS (
  SELECT
    ss_item_sk AS item_sk,
    AVG(ss_net_profit) AS rank_col
  FROM store_sales AS ss1
  WHERE
    ss_store_sk = 146
  GROUP BY
    ss_item_sk
  HAVING
    AVG(ss_net_profit) > 0.9 * (
      SELECT
        rank_col
      FROM store_avg_profit
    )
)
SELECT
  asceding.rnk,
  i1.i_product_name AS best_performing,
  i2.i_product_name AS worst_performing
FROM (
  SELECT
    *
  FROM (
    SELECT
      item_sk,
      RANK() OVER (ORDER BY rank_col ASC) AS rnk
    FROM qualified_items
  ) AS V11
  WHERE
    rnk < 11
) AS asceding, (
  SELECT
    *
  FROM (
    SELECT
      item_sk,
      RANK() OVER (ORDER BY rank_col DESC) AS rnk
    FROM qualified_items
  ) AS V21
  WHERE
    rnk < 11
) AS descending, item AS i1, item AS i2
WHERE
  asceding.rnk = descending.rnk
  AND i1.i_item_sk = asceding.item_sk
  AND i2.i_item_sk = descending.item_sk
ORDER BY
  asceding.rnk
LIMIT 100