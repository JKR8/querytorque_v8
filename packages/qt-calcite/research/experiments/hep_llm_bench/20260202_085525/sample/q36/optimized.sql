SELECT GROSS_MARGIN, I_CATEGORY, I_CLASS, LOCHIERARCHY, RANK() OVER (PARTITION BY LOCHIERARCHY, CASE WHEN T_CLASS = 0 THEN I_CATEGORY ELSE NULL END ORDER BY GROSS_MARGIN) AS RANK_WITHIN_PARENT, CASE WHEN LOCHIERARCHY = 0 THEN I_CATEGORY ELSE NULL END
FROM (SELECT *
FROM (SELECT CAST(SUM(store_sales.ss_net_profit) AS DECIMAL(19, 4)) / SUM(store_sales.ss_ext_sales_price) AS GROSS_MARGIN, item.i_category AS I_CATEGORY, item.i_class AS I_CLASS, 0 AS T_CATEGORY, 0 AS T_CLASS, 0 AS LOCHIERARCHY
FROM store_sales
INNER JOIN (SELECT *
FROM date_dim
WHERE d_year = 2001) AS t ON store_sales.ss_sold_date_sk = t.d_date_sk
INNER JOIN item ON store_sales.ss_item_sk = item.i_item_sk
INNER JOIN (SELECT *
FROM store
WHERE s_state = 'TN') AS t0 ON store_sales.ss_store_sk = t0.s_store_sk
GROUP BY item.i_category, item.i_class
UNION
SELECT CAST(SUM(t8.SS_NET_PROFIT) AS DECIMAL(19, 4)) / SUM(t8.SS_EXT_SALES_PRICE) AS GROSS_MARGIN, t8.I_CATEGORY, NULL AS I_CLASS, 0 AS T_CATEGORY, 1 AS T_CLASS, 1 AS LOCHIERARCHY
FROM (SELECT item0.i_category AS I_CATEGORY, SUM(store_sales0.ss_net_profit) AS SS_NET_PROFIT, SUM(store_sales0.ss_ext_sales_price) AS SS_EXT_SALES_PRICE
FROM store_sales AS store_sales0
INNER JOIN (SELECT *
FROM date_dim
WHERE d_year = 2001) AS t4 ON store_sales0.ss_sold_date_sk = t4.d_date_sk
INNER JOIN item AS item0 ON store_sales0.ss_item_sk = item0.i_item_sk
INNER JOIN (SELECT *
FROM store
WHERE s_state = 'TN') AS t5 ON store_sales0.ss_store_sk = t5.s_store_sk
GROUP BY item0.i_category, item0.i_class) AS t8
GROUP BY t8.I_CATEGORY)
UNION
SELECT CAST(SUM(t16.SS_NET_PROFIT) AS DECIMAL(19, 4)) / SUM(t16.SS_EXT_SALES_PRICE) AS GROSS_MARGIN, NULL AS I_CATEGORY, NULL AS I_CLASS, 1 AS T_CATEGORY, 1 AS T_CLASS, 2 AS LOCHIERARCHY
FROM (SELECT SUM(store_sales1.ss_net_profit) AS SS_NET_PROFIT, SUM(store_sales1.ss_ext_sales_price) AS SS_EXT_SALES_PRICE
FROM store_sales AS store_sales1
INNER JOIN (SELECT *
FROM date_dim
WHERE d_year = 2001) AS t12 ON store_sales1.ss_sold_date_sk = t12.d_date_sk
INNER JOIN item AS item1 ON store_sales1.ss_item_sk = item1.i_item_sk
INNER JOIN (SELECT *
FROM store
WHERE s_state = 'TN') AS t13 ON store_sales1.ss_store_sk = t13.s_store_sk
GROUP BY item1.i_category, item1.i_class) AS t16) AS t19
ORDER BY LOCHIERARCHY DESC, 6 NULLS FIRST, 5 NULLS FIRST
FETCH NEXT 100 ROWS ONLY