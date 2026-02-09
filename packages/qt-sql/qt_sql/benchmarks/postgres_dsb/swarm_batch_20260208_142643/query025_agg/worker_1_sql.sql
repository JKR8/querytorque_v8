WITH filtered_d1 AS (
    SELECT d_date_sk
    FROM date_dim
    WHERE d_moy = 5
      AND d_year = 1999
),
filtered_d2 AS (
    SELECT d_date_sk
    FROM date_dim
    WHERE d_moy BETWEEN 5 AND 7
      AND d_year = 1999
),
filtered_d3 AS (
    SELECT d_date_sk
    FROM date_dim
    WHERE d_moy BETWEEN 5 AND 7
      AND d_year = 1999
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
JOIN filtered_d1 ON store_sales.ss_sold_date_sk = filtered_d1.d_date_sk
JOIN item ON store_sales.ss_item_sk = item.i_item_sk
JOIN store ON store_sales.ss_store_sk = store.s_store_sk
JOIN store_returns ON store_sales.ss_customer_sk = store_returns.sr_customer_sk
                  AND store_sales.ss_item_sk = store_returns.sr_item_sk
                  AND store_sales.ss_ticket_number = store_returns.sr_ticket_number
JOIN filtered_d2 ON store_returns.sr_returned_date_sk = filtered_d2.d_date_sk
JOIN catalog_sales ON store_returns.sr_customer_sk = catalog_sales.cs_bill_customer_sk
                  AND store_returns.sr_item_sk = catalog_sales.cs_item_sk
JOIN filtered_d3 ON catalog_sales.cs_sold_date_sk = filtered_d3.d_date_sk
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