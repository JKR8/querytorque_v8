SELECT t6.ITEM_ID, t6.SR_ITEM_QTY, CAST(t6.SR_ITEM_QTY AS DECIMAL(19, 4)) / (t6.SR_ITEM_QTY + t14.CR_ITEM_QTY + t22.WR_ITEM_QTY) / 3.0000 * 100 AS SR_DEV, t14.CR_ITEM_QTY, CAST(t14.CR_ITEM_QTY AS DECIMAL(19, 4)) / (t6.SR_ITEM_QTY + t14.CR_ITEM_QTY + t22.WR_ITEM_QTY) / 3.0000 * 100 AS CR_DEV, t22.WR_ITEM_QTY, CAST(t22.WR_ITEM_QTY AS DECIMAL(19, 4)) / (t6.SR_ITEM_QTY + t14.CR_ITEM_QTY + t22.WR_ITEM_QTY) / 3.0000 * 100 AS WR_DEV, (t6.SR_ITEM_QTY + t14.CR_ITEM_QTY + t22.WR_ITEM_QTY) / 3.0 AS AVERAGE
FROM (SELECT item.i_item_id AS ITEM_ID, SUM(store_returns.sr_return_quantity) AS SR_ITEM_QTY
FROM store_returns
INNER JOIN item ON store_returns.sr_item_sk = item.i_item_sk
INNER JOIN (SELECT *
FROM date_dim
WHERE d_date IN (SELECT d_date AS D_DATE
FROM date_dim
WHERE d_week_seq IN (SELECT d_week_seq AS D_WEEK_SEQ
FROM date_dim
WHERE d_date = '2000-06-30' OR d_date = '2000-09-27' OR d_date = '2000-11-17'))) AS t3 ON store_returns.sr_returned_date_sk = t3.d_date_sk
GROUP BY item.i_item_id) AS t6
INNER JOIN (SELECT item0.i_item_id AS ITEM_ID, SUM(catalog_returns.cr_return_quantity) AS CR_ITEM_QTY
FROM catalog_returns
INNER JOIN item AS item0 ON catalog_returns.cr_item_sk = item0.i_item_sk
INNER JOIN (SELECT *
FROM date_dim
WHERE d_date IN (SELECT d_date AS D_DATE
FROM date_dim
WHERE d_week_seq IN (SELECT d_week_seq AS D_WEEK_SEQ
FROM date_dim
WHERE d_date = '2000-06-30' OR d_date = '2000-09-27' OR d_date = '2000-11-17'))) AS t11 ON catalog_returns.cr_returned_date_sk = t11.d_date_sk
GROUP BY item0.i_item_id) AS t14 ON t6.ITEM_ID = t14.ITEM_ID
INNER JOIN (SELECT item1.i_item_id AS ITEM_ID, SUM(web_returns.wr_return_quantity) AS WR_ITEM_QTY
FROM web_returns
INNER JOIN item AS item1 ON web_returns.wr_item_sk = item1.i_item_sk
INNER JOIN (SELECT *
FROM date_dim
WHERE d_date IN (SELECT d_date AS D_DATE
FROM date_dim
WHERE d_week_seq IN (SELECT d_week_seq AS D_WEEK_SEQ
FROM date_dim
WHERE d_date = '2000-06-30' OR d_date = '2000-09-27' OR d_date = '2000-11-17'))) AS t19 ON web_returns.wr_returned_date_sk = t19.d_date_sk
GROUP BY item1.i_item_id) AS t22 ON t6.ITEM_ID = t22.ITEM_ID
ORDER BY t6.ITEM_ID NULLS FIRST, t6.SR_ITEM_QTY NULLS FIRST
FETCH NEXT 100 ROWS ONLY