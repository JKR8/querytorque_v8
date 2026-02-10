WITH filtered_d1 AS (
    SELECT d_date_sk
    FROM date_dim
    WHERE d_moy = 4
      AND d_year = 2000
),
filtered_d2 AS (
    SELECT d_date_sk
    FROM date_dim
    WHERE d_moy BETWEEN 4 AND 10
      AND d_year = 2000
),
filtered_d3 AS (
    SELECT d_date_sk
    FROM date_dim
    WHERE d_moy BETWEEN 4 AND 10
      AND d_year = 2000
),
store_sales_filtered AS (
    SELECT ss_item_sk,
           ss_store_sk,
           ss_customer_sk,
           ss_ticket_number,
           ss_net_profit
    FROM store_sales
    JOIN filtered_d1 ON ss_sold_date_sk = d_date_sk
),
store_returns_filtered AS (
    SELECT sr_item_sk,
           sr_customer_sk,
           sr_ticket_number,
           sr_net_loss
    FROM store_returns
    JOIN filtered_d2 ON sr_returned_date_sk = d_date_sk
),
catalog_sales_filtered AS (
    SELECT cs_item_sk,
           cs_bill_customer_sk,
           cs_net_profit
    FROM catalog_sales
    JOIN filtered_d3 ON cs_sold_date_sk = d_date_sk
)
SELECT i_item_id,
       i_item_desc,
       s_store_id,
       s_store_name,
       SUM(ss_net_profit) AS store_sales_profit,
       SUM(sr_net_loss) AS store_returns_loss,
       SUM(cs_net_profit) AS catalog_sales_profit
FROM store_sales_filtered
JOIN store_returns_filtered ON store_sales_filtered.ss_customer_sk = store_returns_filtered.sr_customer_sk
                           AND store_sales_filtered.ss_item_sk = store_returns_filtered.sr_item_sk
                           AND store_sales_filtered.ss_ticket_number = store_returns_filtered.sr_ticket_number
JOIN catalog_sales_filtered ON store_returns_filtered.sr_customer_sk = catalog_sales_filtered.cs_bill_customer_sk
                           AND store_returns_filtered.sr_item_sk = catalog_sales_filtered.cs_item_sk
JOIN item ON store_sales_filtered.ss_item_sk = item.i_item_sk
JOIN store ON store_sales_filtered.ss_store_sk = store.s_store_sk
GROUP BY i_item_id,
         i_item_desc,
         s_store_id,
         s_store_name
ORDER BY i_item_id,
         i_item_desc,
         s_store_id,
         s_store_name
LIMIT 100;