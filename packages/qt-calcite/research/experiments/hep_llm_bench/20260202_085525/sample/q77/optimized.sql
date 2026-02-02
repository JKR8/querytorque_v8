SELECT CHANNEL, ID, SUM(SALES) AS SALES, SUM(RETURNS_) AS RETURNS_, SUM(PROFIT) AS PROFIT
FROM (SELECT *
FROM (SELECT 'store channel' AS CHANNEL, t2.S_STORE_SK AS ID, t2.SALES, CASE WHEN t6.RETURNS_ IS NOT NULL THEN CAST(t6.RETURNS_ AS DECIMAL(19, 0)) ELSE 0 END AS RETURNS_, t2.PROFIT - CASE WHEN t6.PROFIT_LOSS IS NOT NULL THEN CAST(t6.PROFIT_LOSS AS DECIMAL(19, 0)) ELSE 0 END AS PROFIT
FROM (SELECT store.s_store_sk AS S_STORE_SK, SUM(store_sales.ss_ext_sales_price) AS SALES, SUM(store_sales.ss_net_profit) AS PROFIT
FROM store_sales
INNER JOIN (SELECT *
FROM date_dim
WHERE d_date >= DATE '2000-08-23' AND d_date <= DATE '2000-09-22') AS t ON store_sales.ss_sold_date_sk = t.d_date_sk
INNER JOIN store ON store_sales.ss_store_sk = store.s_store_sk
GROUP BY store.s_store_sk) AS t2
LEFT JOIN (SELECT store0.s_store_sk AS S_STORE_SK, SUM(store_returns.sr_return_amt) AS RETURNS_, SUM(store_returns.sr_net_loss) AS PROFIT_LOSS
FROM store_returns
INNER JOIN (SELECT *
FROM date_dim
WHERE d_date >= DATE '2000-08-23' AND d_date <= DATE '2000-09-22') AS t3 ON store_returns.sr_returned_date_sk = t3.d_date_sk
INNER JOIN store AS store0 ON store_returns.sr_store_sk = store0.s_store_sk
GROUP BY store0.s_store_sk) AS t6 ON t2.S_STORE_SK = t6.S_STORE_SK
UNION ALL
SELECT 'catalog channel' AS CHANNEL, t11.CS_CALL_CENTER_SK AS ID, t11.SALES, t15.RETURNS_, t11.PROFIT - t15.PROFIT_LOSS AS PROFIT
FROM (SELECT catalog_sales.cs_call_center_sk AS CS_CALL_CENTER_SK, SUM(catalog_sales.cs_ext_sales_price) AS SALES, SUM(catalog_sales.cs_net_profit) AS PROFIT
FROM catalog_sales
INNER JOIN (SELECT *
FROM date_dim
WHERE d_date >= DATE '2000-08-23' AND d_date <= DATE '2000-09-22') AS t8 ON catalog_sales.cs_sold_date_sk = t8.d_date_sk
GROUP BY catalog_sales.cs_call_center_sk) AS t11,
(SELECT catalog_returns.cr_call_center_sk AS CR_CALL_CENTER_SK, SUM(catalog_returns.cr_return_amount) AS RETURNS_, SUM(catalog_returns.cr_net_loss) AS PROFIT_LOSS
FROM catalog_returns
INNER JOIN (SELECT *
FROM date_dim
WHERE d_date >= DATE '2000-08-23' AND d_date <= DATE '2000-09-22') AS t12 ON catalog_returns.cr_returned_date_sk = t12.d_date_sk
GROUP BY catalog_returns.cr_call_center_sk) AS t15)
UNION ALL
SELECT 'web channel' AS CHANNEL, t21.WP_WEB_PAGE_SK AS ID, t21.SALES, CASE WHEN t25.RETURNS_ IS NOT NULL THEN CAST(t25.RETURNS_ AS DECIMAL(19, 0)) ELSE 0 END AS RETURNS_, t21.PROFIT - CASE WHEN t25.PROFIT_LOSS IS NOT NULL THEN CAST(t25.PROFIT_LOSS AS DECIMAL(19, 0)) ELSE 0 END AS PROFIT
FROM (SELECT web_page.wp_web_page_sk AS WP_WEB_PAGE_SK, SUM(web_sales.ws_ext_sales_price) AS SALES, SUM(web_sales.ws_net_profit) AS PROFIT
FROM web_sales
INNER JOIN (SELECT *
FROM date_dim
WHERE d_date >= DATE '2000-08-23' AND d_date <= DATE '2000-09-22') AS t18 ON web_sales.ws_sold_date_sk = t18.d_date_sk
INNER JOIN web_page ON web_sales.ws_web_page_sk = web_page.wp_web_page_sk
GROUP BY web_page.wp_web_page_sk) AS t21
LEFT JOIN (SELECT web_page0.wp_web_page_sk AS WP_WEB_PAGE_SK, SUM(web_returns.wr_return_amt) AS RETURNS_, SUM(web_returns.wr_net_loss) AS PROFIT_LOSS
FROM web_returns
INNER JOIN (SELECT *
FROM date_dim
WHERE d_date >= DATE '2000-08-23' AND d_date <= DATE '2000-09-22') AS t22 ON web_returns.wr_returned_date_sk = t22.d_date_sk
INNER JOIN web_page AS web_page0 ON web_returns.wr_web_page_sk = web_page0.wp_web_page_sk
GROUP BY web_page0.wp_web_page_sk) AS t25 ON t21.WP_WEB_PAGE_SK = t25.WP_WEB_PAGE_SK) AS t27
GROUP BY ROLLUP(CHANNEL, ID)
ORDER BY CHANNEL NULLS FIRST, ID NULLS FIRST, 4 DESC
FETCH NEXT 100 ROWS ONLY