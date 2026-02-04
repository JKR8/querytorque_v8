WITH filtered_dates_d1 AS (
  SELECT
    d_date_sk
  FROM date_dim
  WHERE
    d_moy = 4 AND d_year = 2000
), filtered_dates_d2_d3 AS (
  SELECT
    d_date_sk
  FROM date_dim
  WHERE
    d_moy BETWEEN 4 AND 10 AND d_year = 2000
)
SELECT
  i_item_id,
  i_item_desc,
  s_store_id,
  s_store_name,
  SUM(ss_net_profit) AS store_sales_profit,
  SUM(sr_net_loss) AS store_returns_loss,
  SUM(cs_net_profit) AS catalog_sales_profit
FROM store_sales
JOIN filtered_dates_d1 AS d1 ON d1.d_date_sk = ss_sold_date_sk
JOIN item ON i_item_sk = ss_item_sk
JOIN store ON s_store_sk = ss_store_sk
JOIN store_returns ON ss_customer_sk = sr_customer_sk
  AND ss_item_sk = sr_item_sk
  AND ss_ticket_number = sr_ticket_number
JOIN filtered_dates_d2_d3 AS d2 ON sr_returned_date_sk = d2.d_date_sk
JOIN catalog_sales ON sr_customer_sk = cs_bill_customer_sk
  AND sr_item_sk = cs_item_sk
JOIN filtered_dates_d2_d3 AS d3 ON cs_sold_date_sk = d3.d_date_sk
GROUP BY
  i_item_id,
  i_item_desc,
  s_store_id,
  s_store_name
ORDER BY
  i_item_id,
  i_item_desc,
  s_store_id,
  s_store_name
LIMIT 100