SELECT I_CATEGORY, I_CLASS, I_BRAND, I_PRODUCT_NAME, D_YEAR, D_QOY, D_MOY, S_STORE_ID, SUMSALES, RK
FROM (SELECT t1.i_category AS I_CATEGORY, t1.i_class AS I_CLASS, t1.i_brand AS I_BRAND, t1.i_product_name AS I_PRODUCT_NAME, t1.d_year AS D_YEAR, t1.d_qoy AS D_QOY, t1.d_moy AS D_MOY, t1.s_store_id AS S_STORE_ID, t1.SUMSALES, RANK() OVER (PARTITION BY t1.i_category ORDER BY t1.SUMSALES DESC) AS RK
FROM (SELECT item.i_category, item.i_class, item.i_brand, item.i_product_name, t.d_year, t.d_qoy, t.d_moy, store.s_store_id, SUM(CASE WHEN store_sales.ss_sales_price IS NOT NULL AND store_sales.ss_quantity IS NOT NULL THEN CAST(store_sales.ss_sales_price * store_sales.ss_quantity AS DECIMAL(19, 0)) ELSE 0 END) AS SUMSALES
FROM store_sales
INNER JOIN (SELECT *
FROM date_dim
WHERE d_month_seq >= 1200 AND d_month_seq <= 1200 + 11) AS t ON store_sales.ss_sold_date_sk = t.d_date_sk
INNER JOIN store ON store_sales.ss_store_sk = store.s_store_sk
INNER JOIN item ON store_sales.ss_item_sk = item.i_item_sk
GROUP BY ROLLUP(item.i_category, item.i_class, item.i_brand, item.i_product_name, t.d_year, t.d_qoy, t.d_moy, store.s_store_id)) AS t1) AS t2
WHERE RK <= 100
ORDER BY I_CATEGORY NULLS FIRST, I_CLASS NULLS FIRST, I_BRAND NULLS FIRST, I_PRODUCT_NAME NULLS FIRST, D_YEAR NULLS FIRST, D_QOY NULLS FIRST, D_MOY NULLS FIRST, S_STORE_ID NULLS FIRST, SUMSALES NULLS FIRST, RK NULLS FIRST
FETCH NEXT 100 ROWS ONLY