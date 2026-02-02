SELECT t2.i_product_name AS I_PRODUCT_NAME, t2.i_brand AS I_BRAND, t2.i_class AS I_CLASS, t2.i_category AS I_CATEGORY, t2.QOH
FROM (SELECT item.i_product_name, item.i_brand, item.i_class, item.i_category, AVG(inventory.inv_quantity_on_hand) AS QOH
FROM inventory
INNER JOIN (SELECT *
FROM date_dim
WHERE d_month_seq >= 1200 AND d_month_seq <= 1200 + 11) AS t ON inventory.inv_date_sk = t.d_date_sk
INNER JOIN item ON inventory.inv_item_sk = item.i_item_sk
GROUP BY ROLLUP(item.i_product_name, item.i_brand, item.i_class, item.i_category)
ORDER BY 5 NULLS FIRST, item.i_product_name NULLS FIRST, item.i_brand NULLS FIRST, item.i_class NULLS FIRST, item.i_category NULLS FIRST
FETCH NEXT 100 ROWS ONLY) AS t2