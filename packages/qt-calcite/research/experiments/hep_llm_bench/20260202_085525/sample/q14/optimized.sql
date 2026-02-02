SELECT CHANNEL, I_BRAND_ID, I_CLASS_ID, I_CATEGORY_ID, SUM(SALES) AS SUM_SALES, SUM(NUMBER_SALES) AS SUM_NUMBER_SALES
FROM (SELECT *
FROM (SELECT 'store' AS CHANNEL, t13.i_brand_id AS I_BRAND_ID, t13.i_class_id AS I_CLASS_ID, t13.i_category_id AS I_CATEGORY_ID, t13.SALES, t13.NUMBER_SALES
FROM (SELECT item3.i_brand_id, item3.i_class_id, item3.i_category_id, SUM(t10.ss_quantity * t10.ss_list_price) AS SALES, COUNT(*) AS NUMBER_SALES
FROM (SELECT store_sales.ss_sold_date_sk, store_sales.ss_sold_time_sk, store_sales.ss_item_sk, store_sales.ss_customer_sk, store_sales.ss_cdemo_sk, store_sales.ss_hdemo_sk, store_sales.ss_addr_sk, store_sales.ss_store_sk, store_sales.ss_promo_sk, store_sales.ss_ticket_number, store_sales.ss_quantity, store_sales.ss_wholesale_cost, store_sales.ss_list_price, store_sales.ss_sales_price, store_sales.ss_ext_discount_amt, store_sales.ss_ext_sales_price, store_sales.ss_ext_wholesale_cost, store_sales.ss_ext_list_price, store_sales.ss_ext_tax, store_sales.ss_coupon_amt, store_sales.ss_net_paid, store_sales.ss_net_paid_inc_tax, store_sales.ss_net_profit
FROM store_sales
INNER JOIN (SELECT item.i_item_sk AS SS_ITEM_SK
FROM item,
(SELECT *
FROM (SELECT item0.i_brand_id AS BRAND_ID, item0.i_class_id AS CLASS_ID, item0.i_category_id AS CATEGORY_ID
FROM store_sales AS store_sales0,
item AS item0,
date_dim
WHERE store_sales0.ss_item_sk = item0.i_item_sk AND store_sales0.ss_sold_date_sk = date_dim.d_date_sk AND date_dim.d_year >= 1999 AND date_dim.d_year <= 1999 + 2
INTERSECT
SELECT item1.i_brand_id AS I_BRAND_ID, item1.i_class_id AS I_CLASS_ID, item1.i_category_id AS I_CATEGORY_ID
FROM catalog_sales,
item AS item1,
date_dim AS date_dim0
WHERE catalog_sales.cs_item_sk = item1.i_item_sk AND catalog_sales.cs_sold_date_sk = date_dim0.d_date_sk AND date_dim0.d_year >= 1999 AND date_dim0.d_year <= 1999 + 2)
INTERSECT
SELECT item2.i_brand_id AS I_BRAND_ID, item2.i_class_id AS I_CLASS_ID, item2.i_category_id AS I_CATEGORY_ID
FROM web_sales,
item AS item2,
date_dim AS date_dim1
WHERE web_sales.ws_item_sk = item2.i_item_sk AND web_sales.ws_sold_date_sk = date_dim1.d_date_sk AND date_dim1.d_year >= 1999 AND date_dim1.d_year <= 1999 + 2) AS t6
WHERE item.i_brand_id = t6.BRAND_ID AND item.i_class_id = t6.CLASS_ID AND item.i_category_id = t6.CATEGORY_ID
GROUP BY item.i_item_sk) AS t9 ON store_sales.ss_item_sk = t9.SS_ITEM_SK) AS t10
INNER JOIN item AS item3 ON t10.ss_item_sk = item3.i_item_sk
INNER JOIN (SELECT *
FROM date_dim
WHERE d_year = CAST(1999 + 2 AS BIGINT) AND d_moy = 11) AS t11 ON t10.ss_sold_date_sk = t11.d_date_sk
GROUP BY item3.i_brand_id, item3.i_class_id, item3.i_category_id) AS t13
LEFT JOIN (SELECT AVG(QUANTITY * LIST_PRICE) AS AVERAGE_SALES
FROM (SELECT *
FROM (SELECT store_sales1.ss_quantity AS QUANTITY, store_sales1.ss_list_price AS LIST_PRICE
FROM store_sales AS store_sales1,
date_dim AS date_dim3
WHERE store_sales1.ss_sold_date_sk = date_dim3.d_date_sk AND date_dim3.d_year >= 1999 AND date_dim3.d_year <= 1999 + 2
UNION ALL
SELECT catalog_sales0.cs_quantity AS QUANTITY, catalog_sales0.cs_list_price AS LIST_PRICE
FROM catalog_sales AS catalog_sales0,
date_dim AS date_dim4
WHERE catalog_sales0.cs_sold_date_sk = date_dim4.d_date_sk AND date_dim4.d_year >= 1999 AND date_dim4.d_year <= 1999 + 2)
UNION ALL
SELECT web_sales0.ws_quantity AS QUANTITY, web_sales0.ws_list_price AS LIST_PRICE
FROM web_sales AS web_sales0,
date_dim AS date_dim5
WHERE web_sales0.ws_sold_date_sk = date_dim5.d_date_sk AND date_dim5.d_year >= 1999 AND date_dim5.d_year <= 1999 + 2) AS t21) AS t24 ON TRUE
WHERE t13.SALES > t24.AVERAGE_SALES
UNION ALL
SELECT 'catalog' AS CHANNEL, t41.i_brand_id AS I_BRAND_ID, t41.i_class_id AS I_CLASS_ID, t41.i_category_id AS I_CATEGORY_ID, t41.SALES, t41.NUMBER_SALES
FROM (SELECT item8.i_brand_id, item8.i_class_id, item8.i_category_id, SUM(t38.cs_quantity * t38.cs_list_price) AS SALES, COUNT(*) AS NUMBER_SALES
FROM (SELECT catalog_sales1.cs_sold_date_sk, catalog_sales1.cs_sold_time_sk, catalog_sales1.cs_ship_date_sk, catalog_sales1.cs_bill_customer_sk, catalog_sales1.cs_bill_cdemo_sk, catalog_sales1.cs_bill_hdemo_sk, catalog_sales1.cs_bill_addr_sk, catalog_sales1.cs_ship_customer_sk, catalog_sales1.cs_ship_cdemo_sk, catalog_sales1.cs_ship_hdemo_sk, catalog_sales1.cs_ship_addr_sk, catalog_sales1.cs_call_center_sk, catalog_sales1.cs_catalog_page_sk, catalog_sales1.cs_ship_mode_sk, catalog_sales1.cs_warehouse_sk, catalog_sales1.cs_item_sk, catalog_sales1.cs_promo_sk, catalog_sales1.cs_order_number, catalog_sales1.cs_quantity, catalog_sales1.cs_wholesale_cost, catalog_sales1.cs_list_price, catalog_sales1.cs_sales_price, catalog_sales1.cs_ext_discount_amt, catalog_sales1.cs_ext_sales_price, catalog_sales1.cs_ext_wholesale_cost, catalog_sales1.cs_ext_list_price, catalog_sales1.cs_ext_tax, catalog_sales1.cs_coupon_amt, catalog_sales1.cs_ext_ship_cost, catalog_sales1.cs_net_paid, catalog_sales1.cs_net_paid_inc_tax, catalog_sales1.cs_net_paid_inc_ship, catalog_sales1.cs_net_paid_inc_ship_tax, catalog_sales1.cs_net_profit
FROM catalog_sales AS catalog_sales1
INNER JOIN (SELECT item4.i_item_sk AS SS_ITEM_SK
FROM item AS item4,
(SELECT *
FROM (SELECT item5.i_brand_id AS BRAND_ID, item5.i_class_id AS CLASS_ID, item5.i_category_id AS CATEGORY_ID
FROM store_sales AS store_sales2,
item AS item5,
date_dim AS date_dim6
WHERE store_sales2.ss_item_sk = item5.i_item_sk AND store_sales2.ss_sold_date_sk = date_dim6.d_date_sk AND date_dim6.d_year >= 1999 AND date_dim6.d_year <= 1999 + 2
INTERSECT
SELECT item6.i_brand_id AS I_BRAND_ID, item6.i_class_id AS I_CLASS_ID, item6.i_category_id AS I_CATEGORY_ID
FROM catalog_sales AS catalog_sales2,
item AS item6,
date_dim AS date_dim7
WHERE catalog_sales2.cs_item_sk = item6.i_item_sk AND catalog_sales2.cs_sold_date_sk = date_dim7.d_date_sk AND date_dim7.d_year >= 1999 AND date_dim7.d_year <= 1999 + 2)
INTERSECT
SELECT item7.i_brand_id AS I_BRAND_ID, item7.i_class_id AS I_CLASS_ID, item7.i_category_id AS I_CATEGORY_ID
FROM web_sales AS web_sales1,
item AS item7,
date_dim AS date_dim8
WHERE web_sales1.ws_item_sk = item7.i_item_sk AND web_sales1.ws_sold_date_sk = date_dim8.d_date_sk AND date_dim8.d_year >= 1999 AND date_dim8.d_year <= 1999 + 2) AS t34
WHERE item4.i_brand_id = t34.BRAND_ID AND item4.i_class_id = t34.CLASS_ID AND item4.i_category_id = t34.CATEGORY_ID
GROUP BY item4.i_item_sk) AS t37 ON catalog_sales1.cs_item_sk = t37.SS_ITEM_SK) AS t38
INNER JOIN item AS item8 ON t38.cs_item_sk = item8.i_item_sk
INNER JOIN (SELECT *
FROM date_dim
WHERE d_year = CAST(1999 + 2 AS BIGINT) AND d_moy = 11) AS t39 ON t38.cs_sold_date_sk = t39.d_date_sk
GROUP BY item8.i_brand_id, item8.i_class_id, item8.i_category_id) AS t41
LEFT JOIN (SELECT AVG(QUANTITY * LIST_PRICE) AS AVERAGE_SALES
FROM (SELECT *
FROM (SELECT store_sales3.ss_quantity AS QUANTITY, store_sales3.ss_list_price AS LIST_PRICE
FROM store_sales AS store_sales3,
date_dim AS date_dim10
WHERE store_sales3.ss_sold_date_sk = date_dim10.d_date_sk AND date_dim10.d_year >= 1999 AND date_dim10.d_year <= 1999 + 2
UNION ALL
SELECT catalog_sales3.cs_quantity AS QUANTITY, catalog_sales3.cs_list_price AS LIST_PRICE
FROM catalog_sales AS catalog_sales3,
date_dim AS date_dim11
WHERE catalog_sales3.cs_sold_date_sk = date_dim11.d_date_sk AND date_dim11.d_year >= 1999 AND date_dim11.d_year <= 1999 + 2)
UNION ALL
SELECT web_sales2.ws_quantity AS QUANTITY, web_sales2.ws_list_price AS LIST_PRICE
FROM web_sales AS web_sales2,
date_dim AS date_dim12
WHERE web_sales2.ws_sold_date_sk = date_dim12.d_date_sk AND date_dim12.d_year >= 1999 AND date_dim12.d_year <= 1999 + 2) AS t49) AS t52 ON TRUE
WHERE t41.SALES > t52.AVERAGE_SALES)
UNION ALL
SELECT 'web' AS CHANNEL, t70.i_brand_id AS I_BRAND_ID, t70.i_class_id AS I_CLASS_ID, t70.i_category_id AS I_CATEGORY_ID, t70.SALES, t70.NUMBER_SALES
FROM (SELECT item13.i_brand_id, item13.i_class_id, item13.i_category_id, SUM(t67.ws_quantity * t67.ws_list_price) AS SALES, COUNT(*) AS NUMBER_SALES
FROM (SELECT web_sales3.ws_sold_date_sk, web_sales3.ws_sold_time_sk, web_sales3.ws_ship_date_sk, web_sales3.ws_item_sk, web_sales3.ws_bill_customer_sk, web_sales3.ws_bill_cdemo_sk, web_sales3.ws_bill_hdemo_sk, web_sales3.ws_bill_addr_sk, web_sales3.ws_ship_customer_sk, web_sales3.ws_ship_cdemo_sk, web_sales3.ws_ship_hdemo_sk, web_sales3.ws_ship_addr_sk, web_sales3.ws_web_page_sk, web_sales3.ws_web_site_sk, web_sales3.ws_ship_mode_sk, web_sales3.ws_warehouse_sk, web_sales3.ws_promo_sk, web_sales3.ws_order_number, web_sales3.ws_quantity, web_sales3.ws_wholesale_cost, web_sales3.ws_list_price, web_sales3.ws_sales_price, web_sales3.ws_ext_discount_amt, web_sales3.ws_ext_sales_price, web_sales3.ws_ext_wholesale_cost, web_sales3.ws_ext_list_price, web_sales3.ws_ext_tax, web_sales3.ws_coupon_amt, web_sales3.ws_ext_ship_cost, web_sales3.ws_net_paid, web_sales3.ws_net_paid_inc_tax, web_sales3.ws_net_paid_inc_ship, web_sales3.ws_net_paid_inc_ship_tax, web_sales3.ws_net_profit
FROM web_sales AS web_sales3
INNER JOIN (SELECT item9.i_item_sk AS SS_ITEM_SK
FROM item AS item9,
(SELECT *
FROM (SELECT item10.i_brand_id AS BRAND_ID, item10.i_class_id AS CLASS_ID, item10.i_category_id AS CATEGORY_ID
FROM store_sales AS store_sales4,
item AS item10,
date_dim AS date_dim13
WHERE store_sales4.ss_item_sk = item10.i_item_sk AND store_sales4.ss_sold_date_sk = date_dim13.d_date_sk AND date_dim13.d_year >= 1999 AND date_dim13.d_year <= 1999 + 2
INTERSECT
SELECT item11.i_brand_id AS I_BRAND_ID, item11.i_class_id AS I_CLASS_ID, item11.i_category_id AS I_CATEGORY_ID
FROM catalog_sales AS catalog_sales4,
item AS item11,
date_dim AS date_dim14
WHERE catalog_sales4.cs_item_sk = item11.i_item_sk AND catalog_sales4.cs_sold_date_sk = date_dim14.d_date_sk AND date_dim14.d_year >= 1999 AND date_dim14.d_year <= 1999 + 2)
INTERSECT
SELECT item12.i_brand_id AS I_BRAND_ID, item12.i_class_id AS I_CLASS_ID, item12.i_category_id AS I_CATEGORY_ID
FROM web_sales AS web_sales4,
item AS item12,
date_dim AS date_dim15
WHERE web_sales4.ws_item_sk = item12.i_item_sk AND web_sales4.ws_sold_date_sk = date_dim15.d_date_sk AND date_dim15.d_year >= 1999 AND date_dim15.d_year <= 1999 + 2) AS t63
WHERE item9.i_brand_id = t63.BRAND_ID AND item9.i_class_id = t63.CLASS_ID AND item9.i_category_id = t63.CATEGORY_ID
GROUP BY item9.i_item_sk) AS t66 ON web_sales3.ws_item_sk = t66.SS_ITEM_SK) AS t67
INNER JOIN item AS item13 ON t67.ws_item_sk = item13.i_item_sk
INNER JOIN (SELECT *
FROM date_dim
WHERE d_year = CAST(1999 + 2 AS BIGINT) AND d_moy = 11) AS t68 ON t67.ws_sold_date_sk = t68.d_date_sk
GROUP BY item13.i_brand_id, item13.i_class_id, item13.i_category_id) AS t70
LEFT JOIN (SELECT AVG(QUANTITY * LIST_PRICE) AS AVERAGE_SALES
FROM (SELECT *
FROM (SELECT store_sales5.ss_quantity AS QUANTITY, store_sales5.ss_list_price AS LIST_PRICE
FROM store_sales AS store_sales5,
date_dim AS date_dim17
WHERE store_sales5.ss_sold_date_sk = date_dim17.d_date_sk AND date_dim17.d_year >= 1999 AND date_dim17.d_year <= 1999 + 2
UNION ALL
SELECT catalog_sales5.cs_quantity AS QUANTITY, catalog_sales5.cs_list_price AS LIST_PRICE
FROM catalog_sales AS catalog_sales5,
date_dim AS date_dim18
WHERE catalog_sales5.cs_sold_date_sk = date_dim18.d_date_sk AND date_dim18.d_year >= 1999 AND date_dim18.d_year <= 1999 + 2)
UNION ALL
SELECT web_sales5.ws_quantity AS QUANTITY, web_sales5.ws_list_price AS LIST_PRICE
FROM web_sales AS web_sales5,
date_dim AS date_dim19
WHERE web_sales5.ws_sold_date_sk = date_dim19.d_date_sk AND date_dim19.d_year >= 1999 AND date_dim19.d_year <= 1999 + 2) AS t78) AS t81 ON TRUE
WHERE t70.SALES > t81.AVERAGE_SALES) AS t84
GROUP BY ROLLUP(CHANNEL, I_BRAND_ID, I_CLASS_ID, I_CATEGORY_ID)
ORDER BY CHANNEL NULLS FIRST, I_BRAND_ID NULLS FIRST, I_CLASS_ID NULLS FIRST, I_CATEGORY_ID NULLS FIRST
FETCH NEXT 100 ROWS ONLY