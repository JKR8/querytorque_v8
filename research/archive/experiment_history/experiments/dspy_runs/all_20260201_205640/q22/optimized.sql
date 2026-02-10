-- start query 22 in stream 0 using template query22.tpl
SELECT i_product_name
       ,i_brand
       ,i_class
       ,i_category
       ,AVG(inv_quantity_on_hand) AS qoh
FROM date_dim
JOIN inventory ON inv_date_sk = d_date_sk
JOIN item ON inv_item_sk = i_item_sk
WHERE d_month_seq BETWEEN 1188 AND 1188 + 11
GROUP BY ROLLUP(i_product_name, i_brand, i_class, i_category)
ORDER BY qoh, i_product_name, i_brand, i_class, i_category
LIMIT 100;
-- end query 22 in stream 0 using template query22.tpl