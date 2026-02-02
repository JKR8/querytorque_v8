-- start query 80 in stream 0 using template query80.tpl
WITH date_range AS (
    SELECT d_date_sk
    FROM date_dim
    WHERE d_date BETWEEN DATE '1998-08-28' 
                    AND DATE '1998-08-28' + INTERVAL '30' DAY
),
filtered_item AS (
    SELECT i_item_sk
    FROM item
    WHERE i_current_price > 50
),
filtered_promotion AS (
    SELECT p_promo_sk
    FROM promotion
    WHERE p_channel_tv = 'N'
),
ssr AS (
    SELECT s_store_id AS store_id,
           SUM(ss_ext_sales_price) AS sales,
           SUM(COALESCE(sr_return_amt, 0)) AS "returns",
           SUM(ss_net_profit - COALESCE(sr_net_loss, 0)) AS profit
    FROM store_sales
    LEFT JOIN store_returns ON ss_item_sk = sr_item_sk 
                            AND ss_ticket_number = sr_ticket_number
    JOIN date_range ON ss_sold_date_sk = date_range.d_date_sk
    JOIN store ON ss_store_sk = s_store_sk
    JOIN filtered_item ON ss_item_sk = filtered_item.i_item_sk
    JOIN filtered_promotion ON ss_promo_sk = filtered_promotion.p_promo_sk
    GROUP BY s_store_id
),
csr AS (
    SELECT cp_catalog_page_id AS catalog_page_id,
           SUM(cs_ext_sales_price) AS sales,
           SUM(COALESCE(cr_return_amount, 0)) AS "returns",
           SUM(cs_net_profit - COALESCE(cr_net_loss, 0)) AS profit
    FROM catalog_sales
    LEFT JOIN catalog_returns ON cs_item_sk = cr_item_sk 
                              AND cs_order_number = cr_order_number
    JOIN date_range ON cs_sold_date_sk = date_range.d_date_sk
    JOIN catalog_page ON cs_catalog_page_sk = cp_catalog_page_sk
    JOIN filtered_item ON cs_item_sk = filtered_item.i_item_sk
    JOIN filtered_promotion ON cs_promo_sk = filtered_promotion.p_promo_sk
    GROUP BY cp_catalog_page_id
),
wsr AS (
    SELECT web_site_id,
           SUM(ws_ext_sales_price) AS sales,
           SUM(COALESCE(wr_return_amt, 0)) AS "returns",
           SUM(ws_net_profit - COALESCE(wr_net_loss, 0)) AS profit
    FROM web_sales
    LEFT JOIN web_returns ON ws_item_sk = wr_item_sk 
                          AND ws_order_number = wr_order_number
    JOIN date_range ON ws_sold_date_sk = date_range.d_date_sk
    JOIN web_site ON ws_web_site_sk = web_site_sk
    JOIN filtered_item ON ws_item_sk = filtered_item.i_item_sk
    JOIN filtered_promotion ON ws_promo_sk = filtered_promotion.p_promo_sk
    GROUP BY web_site_id
)
SELECT channel,
       id,
       SUM(sales) AS sales,
       SUM("returns") AS "returns",
       SUM(profit) AS profit
FROM (
    SELECT 'store channel' AS channel,
           'store' || store_id AS id,
           sales,
           "returns",
           profit
    FROM ssr
    UNION ALL
    SELECT 'catalog channel' AS channel,
           'catalog_page' || catalog_page_id AS id,
           sales,
           "returns",
           profit
    FROM csr
    UNION ALL
    SELECT 'web channel' AS channel,
           'web_site' || web_site_id AS id,
           sales,
           "returns",
           profit
    FROM wsr
) x
GROUP BY ROLLUP (channel, id)
ORDER BY channel, id
LIMIT 100;

-- end query 80 in stream 0 using template query80.tpl