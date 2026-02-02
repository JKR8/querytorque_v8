SELECT I_ITEM_ID, S_STATE, G_STATE, AGG1, AGG2, AGG3, AGG4
FROM (SELECT *
FROM (SELECT item.i_item_id AS I_ITEM_ID, t1.s_state AS S_STATE, 0 AS G_STATE, AVG(store_sales.ss_quantity) AS AGG1, AVG(store_sales.ss_list_price) AS AGG2, AVG(store_sales.ss_coupon_amt) AS AGG3, AVG(store_sales.ss_sales_price) AS AGG4
FROM store_sales
INNER JOIN (SELECT *
FROM customer_demographics
WHERE cd_gender = 'M' AND cd_marital_status = 'S' AND cd_education_status = 'College') AS t ON store_sales.ss_cdemo_sk = t.cd_demo_sk
INNER JOIN (SELECT *
FROM date_dim
WHERE d_year = 2002) AS t0 ON store_sales.ss_sold_date_sk = t0.d_date_sk
INNER JOIN (SELECT *
FROM store
WHERE s_state = 'TN') AS t1 ON store_sales.ss_store_sk = t1.s_store_sk
INNER JOIN item ON store_sales.ss_item_sk = item.i_item_sk
GROUP BY item.i_item_id, t1.s_state
UNION ALL
SELECT item0.i_item_id AS I_ITEM_ID, NULL AS S_STATE, 1 AS G_STATE, AVG(store_sales0.ss_quantity) AS AGG1, AVG(store_sales0.ss_list_price) AS AGG2, AVG(store_sales0.ss_coupon_amt) AS AGG3, AVG(store_sales0.ss_sales_price) AS AGG4
FROM store_sales AS store_sales0
INNER JOIN (SELECT *
FROM customer_demographics
WHERE cd_gender = 'M' AND cd_marital_status = 'S' AND cd_education_status = 'College') AS t5 ON store_sales0.ss_cdemo_sk = t5.cd_demo_sk
INNER JOIN (SELECT *
FROM date_dim
WHERE d_year = 2002) AS t6 ON store_sales0.ss_sold_date_sk = t6.d_date_sk
INNER JOIN (SELECT *
FROM store
WHERE s_state = 'TN') AS t7 ON store_sales0.ss_store_sk = t7.s_store_sk
INNER JOIN item AS item0 ON store_sales0.ss_item_sk = item0.i_item_sk
GROUP BY item0.i_item_id)
UNION ALL
SELECT NULL AS I_ITEM_ID, NULL AS S_STATE, 1 AS G_STATE, AVG(store_sales1.ss_quantity) AS AGG1, AVG(store_sales1.ss_list_price) AS AGG2, AVG(store_sales1.ss_coupon_amt) AS AGG3, AVG(store_sales1.ss_sales_price) AS AGG4
FROM store_sales AS store_sales1
INNER JOIN (SELECT *
FROM customer_demographics
WHERE cd_gender = 'M' AND cd_marital_status = 'S' AND cd_education_status = 'College') AS t12 ON store_sales1.ss_cdemo_sk = t12.cd_demo_sk
INNER JOIN (SELECT *
FROM date_dim
WHERE d_year = 2002) AS t13 ON store_sales1.ss_sold_date_sk = t13.d_date_sk
INNER JOIN (SELECT *
FROM store
WHERE s_state = 'TN') AS t14 ON store_sales1.ss_store_sk = t14.s_store_sk
INNER JOIN item AS item1 ON store_sales1.ss_item_sk = item1.i_item_sk)
ORDER BY I_ITEM_ID NULLS FIRST, S_STATE NULLS FIRST
FETCH NEXT 100 ROWS ONLY