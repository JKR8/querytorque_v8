SELECT t9.RNK, item.i_product_name AS BEST_PERFORMING, item0.i_product_name AS WORST_PERFORMING
FROM (SELECT *
FROM (SELECT ss_item_sk AS ITEM_SK, RANK() OVER (ORDER BY RANK_COL) AS RNK
FROM (SELECT ss_item_sk, AVG(ss_net_profit) AS RANK_COL
FROM store_sales
WHERE ss_store_sk = 4
GROUP BY ss_item_sk
HAVING AVG(ss_net_profit) > 0.9 * (((SELECT AVG(ss_net_profit) AS RANK_COL
FROM store_sales
WHERE ss_store_sk = 4 AND ss_addr_sk IS NULL
GROUP BY ss_store_sk)))) AS t6) AS t7
WHERE RNK < 11) AS t9
INNER JOIN (SELECT *
FROM (SELECT ss_item_sk AS ITEM_SK, RANK() OVER (ORDER BY RANK_COL DESC) AS RNK
FROM (SELECT ss_item_sk, AVG(ss_net_profit) AS RANK_COL
FROM store_sales
WHERE ss_store_sk = 4
GROUP BY ss_item_sk
HAVING AVG(ss_net_profit) > 0.9 * (((SELECT AVG(ss_net_profit) AS RANK_COL
FROM store_sales
WHERE ss_store_sk = 4 AND ss_addr_sk IS NULL
GROUP BY ss_store_sk)))) AS t17) AS t18
WHERE RNK < 11) AS t20 ON t9.RNK = t20.RNK
INNER JOIN item ON t9.ITEM_SK = item.i_item_sk
INNER JOIN item AS item0 ON t20.ITEM_SK = item0.i_item_sk
ORDER BY t9.RNK
FETCH NEXT 100 ROWS ONLY