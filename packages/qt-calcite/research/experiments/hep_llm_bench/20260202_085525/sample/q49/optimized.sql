SELECT *
FROM (SELECT *
FROM (SELECT 'web' AS CHANNEL, ITEM, RETURN_RATIO, RETURN_RANK, CURRENCY_RANK
FROM (SELECT t3.ws_item_sk AS ITEM, CAST(t3.$f1 AS DECIMAL(15, 4)) / CAST(t3.$f2 AS DECIMAL(15, 4)) AS RETURN_RATIO, CAST(t3.$f3 AS DECIMAL(15, 4)) / CAST(t3.$f4 AS DECIMAL(15, 4)) AS CURRENCY_RATIO, RANK() OVER (ORDER BY CAST(t3.$f1 AS DECIMAL(15, 4)) / CAST(t3.$f2 AS DECIMAL(15, 4))) AS RETURN_RANK, RANK() OVER (ORDER BY CAST(t3.$f3 AS DECIMAL(15, 4)) / CAST(t3.$f4 AS DECIMAL(15, 4))) AS CURRENCY_RANK
FROM (SELECT t.ws_item_sk, SUM(CASE WHEN t0.wr_return_quantity IS NOT NULL THEN CAST(t0.wr_return_quantity AS BIGINT) ELSE 0 END) AS $f1, SUM(CASE WHEN t.ws_quantity IS NOT NULL THEN CAST(t.ws_quantity AS BIGINT) ELSE 0 END) AS $f2, SUM(CASE WHEN t0.wr_return_amt IS NOT NULL THEN CAST(t0.wr_return_amt AS DECIMAL(19, 0)) ELSE 0 END) AS $f3, SUM(CASE WHEN t.ws_net_paid IS NOT NULL THEN CAST(t.ws_net_paid AS DECIMAL(19, 0)) ELSE 0 END) AS $f4
FROM (SELECT *
FROM web_sales
WHERE ws_net_profit > 1 AND ws_net_paid > 0 AND ws_quantity > 0) AS t
INNER JOIN (SELECT *
FROM web_returns
WHERE wr_return_amt > 10000) AS t0 ON t.ws_order_number = t0.wr_order_number AND t.ws_item_sk = t0.wr_item_sk
INNER JOIN (SELECT *
FROM date_dim
WHERE d_year = 2001 AND d_moy = 12) AS t1 ON t.ws_sold_date_sk = t1.d_date_sk
GROUP BY t.ws_item_sk) AS t3) AS t4
WHERE RETURN_RANK <= 10 OR CURRENCY_RANK <= 10
UNION
SELECT 'catalog' AS CHANNEL, ITEM, RETURN_RATIO, RETURN_RANK, CURRENCY_RANK
FROM (SELECT t11.cs_item_sk AS ITEM, CAST(t11.$f1 AS DECIMAL(15, 4)) / CAST(t11.$f2 AS DECIMAL(15, 4)) AS RETURN_RATIO, CAST(t11.$f3 AS DECIMAL(15, 4)) / CAST(t11.$f4 AS DECIMAL(15, 4)) AS CURRENCY_RATIO, RANK() OVER (ORDER BY CAST(t11.$f1 AS DECIMAL(15, 4)) / CAST(t11.$f2 AS DECIMAL(15, 4))) AS RETURN_RANK, RANK() OVER (ORDER BY CAST(t11.$f3 AS DECIMAL(15, 4)) / CAST(t11.$f4 AS DECIMAL(15, 4))) AS CURRENCY_RANK
FROM (SELECT t7.cs_item_sk, SUM(CASE WHEN t8.cr_return_quantity IS NOT NULL THEN CAST(t8.cr_return_quantity AS BIGINT) ELSE 0 END) AS $f1, SUM(CASE WHEN t7.cs_quantity IS NOT NULL THEN CAST(t7.cs_quantity AS BIGINT) ELSE 0 END) AS $f2, SUM(CASE WHEN t8.cr_return_amount IS NOT NULL THEN CAST(t8.cr_return_amount AS DECIMAL(19, 0)) ELSE 0 END) AS $f3, SUM(CASE WHEN t7.cs_net_paid IS NOT NULL THEN CAST(t7.cs_net_paid AS DECIMAL(19, 0)) ELSE 0 END) AS $f4
FROM (SELECT *
FROM catalog_sales
WHERE cs_net_profit > 1 AND cs_net_paid > 0 AND cs_quantity > 0) AS t7
INNER JOIN (SELECT *
FROM catalog_returns
WHERE cr_return_amount > 10000) AS t8 ON t7.cs_order_number = t8.cr_order_number AND t7.cs_item_sk = t8.cr_item_sk
INNER JOIN (SELECT *
FROM date_dim
WHERE d_year = 2001 AND d_moy = 12) AS t9 ON t7.cs_sold_date_sk = t9.d_date_sk
GROUP BY t7.cs_item_sk) AS t11) AS t12
WHERE RETURN_RANK <= 10 OR CURRENCY_RANK <= 10)
UNION
SELECT 'store' AS CHANNEL, ITEM, RETURN_RATIO, RETURN_RANK, CURRENCY_RANK
FROM (SELECT t20.ss_item_sk AS ITEM, CAST(t20.$f1 AS DECIMAL(15, 4)) / CAST(t20.$f2 AS DECIMAL(15, 4)) AS RETURN_RATIO, CAST(t20.$f3 AS DECIMAL(15, 4)) / CAST(t20.$f4 AS DECIMAL(15, 4)) AS CURRENCY_RATIO, RANK() OVER (ORDER BY CAST(t20.$f1 AS DECIMAL(15, 4)) / CAST(t20.$f2 AS DECIMAL(15, 4))) AS RETURN_RANK, RANK() OVER (ORDER BY CAST(t20.$f3 AS DECIMAL(15, 4)) / CAST(t20.$f4 AS DECIMAL(15, 4))) AS CURRENCY_RANK
FROM (SELECT t16.ss_item_sk, SUM(CASE WHEN t17.sr_return_quantity IS NOT NULL THEN CAST(t17.sr_return_quantity AS BIGINT) ELSE 0 END) AS $f1, SUM(CASE WHEN t16.ss_quantity IS NOT NULL THEN CAST(t16.ss_quantity AS BIGINT) ELSE 0 END) AS $f2, SUM(CASE WHEN t17.sr_return_amt IS NOT NULL THEN CAST(t17.sr_return_amt AS DECIMAL(19, 0)) ELSE 0 END) AS $f3, SUM(CASE WHEN t16.ss_net_paid IS NOT NULL THEN CAST(t16.ss_net_paid AS DECIMAL(19, 0)) ELSE 0 END) AS $f4
FROM (SELECT *
FROM store_sales
WHERE ss_net_profit > 1 AND ss_net_paid > 0 AND ss_quantity > 0) AS t16
INNER JOIN (SELECT *
FROM store_returns
WHERE sr_return_amt > 10000) AS t17 ON t16.ss_ticket_number = t17.sr_ticket_number AND t16.ss_item_sk = t17.sr_item_sk
INNER JOIN (SELECT *
FROM date_dim
WHERE d_year = 2001 AND d_moy = 12) AS t18 ON t16.ss_sold_date_sk = t18.d_date_sk
GROUP BY t16.ss_item_sk) AS t20) AS t21
WHERE RETURN_RANK <= 10 OR CURRENCY_RANK <= 10)
ORDER BY CHANNEL NULLS FIRST, RETURN_RANK NULLS FIRST, CURRENCY_RANK NULLS FIRST, ITEM NULLS FIRST
FETCH NEXT 100 ROWS ONLY