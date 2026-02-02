SELECT item.i_item_id AS I_ITEM_ID, AVG(catalog_sales.cs_quantity) AS AGG1, AVG(catalog_sales.cs_list_price) AS AGG2, AVG(catalog_sales.cs_coupon_amt) AS AGG3, AVG(catalog_sales.cs_sales_price) AS AGG4
FROM catalog_sales
INNER JOIN (SELECT *
FROM customer_demographics
WHERE cd_gender = 'M' AND cd_marital_status = 'S' AND cd_education_status = 'College') AS t ON catalog_sales.cs_bill_cdemo_sk = t.cd_demo_sk
INNER JOIN (SELECT *
FROM date_dim
WHERE d_year = 2000) AS t0 ON catalog_sales.cs_sold_date_sk = t0.d_date_sk
INNER JOIN item ON catalog_sales.cs_item_sk = item.i_item_sk
INNER JOIN (SELECT *
FROM promotion
WHERE p_channel_email = 'N' OR p_channel_event = 'N') AS t1 ON catalog_sales.cs_promo_sk = t1.p_promo_sk
GROUP BY item.i_item_id
ORDER BY item.i_item_id
FETCH NEXT 100 ROWS ONLY