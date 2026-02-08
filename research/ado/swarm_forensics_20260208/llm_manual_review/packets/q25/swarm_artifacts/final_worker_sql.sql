WITH filtered_dates AS (
    SELECT d_date_sk
    FROM date_dim
    WHERE d_year = 2000 AND d_moy = 4
),
filtered_dates_apr_oct AS (
    SELECT d_date_sk
    FROM date_dim
    WHERE d_year = 2000 AND d_moy BETWEEN 4 AND 10
),
store_sales_agg AS (
    SELECT 
        ss_item_sk AS item_sk,
        ss_store_sk AS store_sk,
        SUM(ss_net_profit) AS store_sales_profit
    FROM store_sales
    JOIN filtered_dates ON d_date_sk = ss_sold_date_sk
    GROUP BY 1, 2
),
store_returns_agg AS (
    SELECT
        sr_item_sk AS item_sk,
        (SELECT ss_store_sk 
         FROM store_sales 
         WHERE ss_customer_sk = sr_customer_sk 
           AND ss_item_sk = sr_item_sk 
           AND ss_ticket_number = sr_ticket_number
         LIMIT 1) AS store_sk,
        SUM(sr_net_loss) AS store_returns_loss
    FROM store_returns
    JOIN filtered_dates_apr_oct ON d_date_sk = sr_returned_date_sk
    GROUP BY 1, 2
    HAVING store_sk IS NOT NULL
),
catalog_sales_agg AS (
    SELECT
        cs_item_sk AS item_sk,
        (SELECT ss_store_sk 
         FROM store_sales 
         JOIN store_returns ON sr_customer_sk = ss_customer_sk 
                           AND sr_item_sk = ss_item_sk 
                           AND sr_ticket_number = ss_ticket_number
         WHERE sr_customer_sk = cs_bill_customer_sk 
           AND sr_item_sk = cs_item_sk
         LIMIT 1) AS store_sk,
        SUM(cs_net_profit) AS catalog_sales_profit
    FROM catalog_sales
    JOIN filtered_dates_apr_oct ON d_date_sk = cs_sold_date_sk
    GROUP BY 1, 2
    HAVING store_sk IS NOT NULL
),
combined AS (
    SELECT
        COALESCE(ss.item_sk, sr.item_sk, cs.item_sk) AS item_sk,
        COALESCE(ss.store_sk, sr.store_sk, cs.store_sk) AS store_sk,
        COALESCE(ss.store_sales_profit, 0) AS store_sales_profit,
        COALESCE(sr.store_returns_loss, 0) AS store_returns_loss,
        COALESCE(cs.catalog_sales_profit, 0) AS catalog_sales_profit
    FROM store_sales_agg ss
    FULL OUTER JOIN store_returns_agg sr 
        ON ss.item_sk = sr.item_sk AND ss.store_sk = sr.store_sk
    FULL OUTER JOIN catalog_sales_agg cs 
        ON COALESCE(ss.item_sk, sr.item_sk) = cs.item_sk 
       AND COALESCE(ss.store_sk, sr.store_sk) = cs.store_sk
)
SELECT
    i.i_item_id,
    i.i_item_desc,
    s.s_store_id,
    s.s_store_name,
    c.store_sales_profit,
    c.store_returns_loss,
    c.catalog_sales_profit
FROM combined c
JOIN item i ON i.i_item_sk = c.item_sk
JOIN store s ON s.s_store_sk = c.store_sk
ORDER BY
    i.i_item_id,
    i.i_item_desc,
    s.s_store_id,
    s.s_store_name
LIMIT 100;