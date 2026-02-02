SELECT t6.ITEM_ID, t6.SS_ITEM_REV, t6.SS_ITEM_REV / ((t6.SS_ITEM_REV + t14.CS_ITEM_REV + t22.WS_ITEM_REV) / 3) * 100 AS SS_DEV, t14.CS_ITEM_REV, t14.CS_ITEM_REV / ((t6.SS_ITEM_REV + t14.CS_ITEM_REV + t22.WS_ITEM_REV) / 3) * 100 AS CS_DEV, t22.WS_ITEM_REV, t22.WS_ITEM_REV / ((t6.SS_ITEM_REV + t14.CS_ITEM_REV + t22.WS_ITEM_REV) / 3) * 100 AS WS_DEV, (t6.SS_ITEM_REV + t14.CS_ITEM_REV + t22.WS_ITEM_REV) / 3 AS AVERAGE
FROM (SELECT item.i_item_id AS ITEM_ID, SUM(store_sales.ss_ext_sales_price) AS SS_ITEM_REV
FROM store_sales
INNER JOIN item ON store_sales.ss_item_sk = item.i_item_sk
INNER JOIN date_dim ON store_sales.ss_sold_date_sk = date_dim.d_date_sk
INNER JOIN (SELECT date_dim0.d_date AS D_DATE
FROM date_dim AS date_dim0
INNER JOIN (SELECT SINGLE_VALUE(d_week_seq) AS $f0
FROM date_dim
WHERE d_date = '2000-01-03') AS t1 ON date_dim0.d_week_seq = t1.$f0
GROUP BY date_dim0.d_date) AS t3 ON date_dim.d_date = t3.D_DATE
GROUP BY item.i_item_id) AS t6
INNER JOIN (SELECT item0.i_item_id AS ITEM_ID, SUM(catalog_sales.cs_ext_sales_price) AS CS_ITEM_REV
FROM catalog_sales
INNER JOIN item AS item0 ON catalog_sales.cs_item_sk = item0.i_item_sk
INNER JOIN date_dim AS date_dim2 ON catalog_sales.cs_sold_date_sk = date_dim2.d_date_sk
INNER JOIN (SELECT date_dim3.d_date AS D_DATE
FROM date_dim AS date_dim3
INNER JOIN (SELECT SINGLE_VALUE(d_week_seq) AS $f0
FROM date_dim
WHERE d_date = '2000-01-03') AS t9 ON date_dim3.d_week_seq = t9.$f0
GROUP BY date_dim3.d_date) AS t11 ON date_dim2.d_date = t11.D_DATE
GROUP BY item0.i_item_id) AS t14 ON t6.ITEM_ID = t14.ITEM_ID AND t6.SS_ITEM_REV >= 0.9 * t14.CS_ITEM_REV AND t6.SS_ITEM_REV <= 1.1 * t14.CS_ITEM_REV AND t14.CS_ITEM_REV >= 0.9 * t6.SS_ITEM_REV AND t14.CS_ITEM_REV <= 1.1 * t6.SS_ITEM_REV
INNER JOIN (SELECT item1.i_item_id AS ITEM_ID, SUM(web_sales.ws_ext_sales_price) AS WS_ITEM_REV
FROM web_sales
INNER JOIN item AS item1 ON web_sales.ws_item_sk = item1.i_item_sk
INNER JOIN date_dim AS date_dim5 ON web_sales.ws_sold_date_sk = date_dim5.d_date_sk
INNER JOIN (SELECT date_dim6.d_date AS D_DATE
FROM date_dim AS date_dim6
INNER JOIN (SELECT SINGLE_VALUE(d_week_seq) AS $f0
FROM date_dim
WHERE d_date = '2000-01-03') AS t17 ON date_dim6.d_week_seq = t17.$f0
GROUP BY date_dim6.d_date) AS t19 ON date_dim5.d_date = t19.D_DATE
GROUP BY item1.i_item_id) AS t22 ON t6.ITEM_ID = t22.ITEM_ID AND t6.SS_ITEM_REV >= 0.9 * t22.WS_ITEM_REV AND (t6.SS_ITEM_REV <= 1.1 * t22.WS_ITEM_REV AND t14.CS_ITEM_REV >= 0.9 * t22.WS_ITEM_REV) AND (t14.CS_ITEM_REV <= 1.1 * t22.WS_ITEM_REV AND t22.WS_ITEM_REV >= 0.9 * t6.SS_ITEM_REV AND (t22.WS_ITEM_REV <= 1.1 * t6.SS_ITEM_REV AND (t22.WS_ITEM_REV >= 0.9 * t14.CS_ITEM_REV AND t22.WS_ITEM_REV <= 1.1 * t14.CS_ITEM_REV)))
ORDER BY t6.ITEM_ID NULLS FIRST, t6.SS_ITEM_REV NULLS FIRST
FETCH NEXT 100 ROWS ONLY