WITH filtered_d1 AS (
    SELECT d_date_sk
    FROM date_dim
    WHERE d_moy = 6
      AND d_year = 2002
),
filtered_d2 AS (
    SELECT d_date_sk
    FROM date_dim
    WHERE d_moy BETWEEN 6 AND 8
      AND d_year = 2002
),
filtered_d3 AS (
    SELECT d_date_sk
    FROM date_dim
    WHERE d_moy BETWEEN 6 AND 8
      AND d_year = 2002
)
SELECT
    MIN(i_item_id),
    MIN(i_item_desc),
    MIN(s_store_id),
    MIN(s_store_name),
    MIN(ss_net_profit),
    MIN(sr_net_loss),
    MIN(cs_net_profit),
    MIN(ss_item_sk),
    MIN(sr_ticket_number),
    MIN(cs_order_number)
FROM store_sales
JOIN filtered_d1 ON d_date_sk = ss_sold_date_sk
JOIN item ON i_item_sk = ss_item_sk
JOIN store ON s_store_sk = ss_store_sk
JOIN store_returns ON ss_customer_sk = sr_customer_sk
                  AND ss_item_sk = sr_item_sk
                  AND ss_ticket_number = sr_ticket_number
JOIN filtered_d2 ON d_date_sk = sr_returned_date_sk
JOIN catalog_sales ON sr_customer_sk = cs_bill_customer_sk
                  AND sr_item_sk = cs_item_sk
JOIN filtered_d3 ON d_date_sk = cs_sold_date_sk;