-- start query 49 in stream 0 using template query49.tpl
WITH date_filter AS (
    SELECT d_date_sk
    FROM date_dim
    WHERE d_year = 1999 AND d_moy = 12
),
web_data AS (
    SELECT 
        ws.ws_item_sk AS item,
        CAST(SUM(COALESCE(wr.wr_return_quantity, 0)) AS DECIMAL(15,4)) /
        CAST(SUM(COALESCE(ws.ws_quantity, 0)) AS DECIMAL(15,4)) AS return_ratio,
        CAST(SUM(COALESCE(wr.wr_return_amt, 0)) AS DECIMAL(15,4)) /
        CAST(SUM(COALESCE(ws.ws_net_paid, 0)) AS DECIMAL(15,4)) AS currency_ratio
    FROM web_sales ws
    JOIN date_filter df ON ws.ws_sold_date_sk = df.d_date_sk
    LEFT JOIN web_returns wr 
        ON ws.ws_order_number = wr.wr_order_number 
        AND ws.ws_item_sk = wr.wr_item_sk
        AND wr.wr_return_amt > 10000
    WHERE ws.ws_net_profit > 1
        AND ws.ws_net_paid > 0
        AND ws.ws_quantity > 0
    GROUP BY ws.ws_item_sk
),
catalog_data AS (
    SELECT 
        cs.cs_item_sk AS item,
        CAST(SUM(COALESCE(cr.cr_return_quantity, 0)) AS DECIMAL(15,4)) /
        CAST(SUM(COALESCE(cs.cs_quantity, 0)) AS DECIMAL(15,4)) AS return_ratio,
        CAST(SUM(COALESCE(cr.cr_return_amount, 0)) AS DECIMAL(15,4)) /
        CAST(SUM(COALESCE(cs.cs_net_paid, 0)) AS DECIMAL(15,4)) AS currency_ratio
    FROM catalog_sales cs
    JOIN date_filter df ON cs.cs_sold_date_sk = df.d_date_sk
    LEFT JOIN catalog_returns cr 
        ON cs.cs_order_number = cr.cr_order_number 
        AND cs.cs_item_sk = cr.cr_item_sk
        AND cr.cr_return_amount > 10000
    WHERE cs.cs_net_profit > 1
        AND cs.cs_net_paid > 0
        AND cs.cs_quantity > 0
    GROUP BY cs.cs_item_sk
),
store_data AS (
    SELECT 
        sts.ss_item_sk AS item,
        CAST(SUM(COALESCE(sr.sr_return_quantity, 0)) AS DECIMAL(15,4)) /
        CAST(SUM(COALESCE(sts.ss_quantity, 0)) AS DECIMAL(15,4)) AS return_ratio,
        CAST(SUM(COALESCE(sr.sr_return_amt, 0)) AS DECIMAL(15,4)) /
        CAST(SUM(COALESCE(sts.ss_net_paid, 0)) AS DECIMAL(15,4)) AS currency_ratio
    FROM store_sales sts
    JOIN date_filter df ON sts.ss_sold_date_sk = df.d_date_sk
    LEFT JOIN store_returns sr 
        ON sts.ss_ticket_number = sr.sr_ticket_number 
        AND sts.ss_item_sk = sr.sr_item_sk
        AND sr.sr_return_amt > 10000
    WHERE sts.ss_net_profit > 1
        AND sts.ss_net_paid > 0
        AND sts.ss_quantity > 0
    GROUP BY sts.ss_item_sk
),
web_ranked AS (
    SELECT 
        'web' AS channel,
        item,
        return_ratio,
        RANK() OVER (ORDER BY return_ratio) AS return_rank,
        RANK() OVER (ORDER BY currency_ratio) AS currency_rank
    FROM web_data
),
catalog_ranked AS (
    SELECT 
        'catalog' AS channel,
        item,
        return_ratio,
        RANK() OVER (ORDER BY return_ratio) AS return_rank,
        RANK() OVER (ORDER BY currency_ratio) AS currency_rank
    FROM catalog_data
),
store_ranked AS (
    SELECT 
        'store' AS channel,
        item,
        return_ratio,
        RANK() OVER (ORDER BY return_ratio) AS return_rank,
        RANK() OVER (ORDER BY currency_ratio) AS currency_rank
    FROM store_data
),
combined AS (
    SELECT channel, item, return_ratio, return_rank, currency_rank
    FROM web_ranked
    WHERE return_rank <= 10 OR currency_rank <= 10
    UNION ALL
    SELECT channel, item, return_ratio, return_rank, currency_rank
    FROM catalog_ranked
    WHERE return_rank <= 10 OR currency_rank <= 10
    UNION ALL
    SELECT channel, item, return_ratio, return_rank, currency_rank
    FROM store_ranked
    WHERE return_rank <= 10 OR currency_rank <= 10
)
SELECT channel, item, return_ratio, return_rank, currency_rank
FROM combined
ORDER BY channel, return_rank, currency_rank, item
LIMIT 100;

-- end query 49 in stream 0 using template query49.tpl