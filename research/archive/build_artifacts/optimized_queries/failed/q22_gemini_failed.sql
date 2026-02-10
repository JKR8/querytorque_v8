-- ERROR: Binder Error: Referenced column "inv_quantity_on_hand" not found in FROM clause!

WITH inv_counts AS (
SELECT inv_item_sk, SUM(inv_quantity_on_hand) AS inv_sum, COUNT(inv_quantity_on_hand) AS inv_count FROM inventory, date_dim WHERE inv_date_sk = d_date_sk AND d_month_seq BETWEEN 1188 AND 1199 GROUP BY inv_item_sk
)
SELECT i_product_name
             ,i_brand
             ,i_class
             ,i_category
             ,avg(inv_quantity_on_hand) qoh
FROM inv_counts, item
WHERE inv_item_sk = i_item_sk
group by rollup(i_product_name
                       ,i_brand
                       ,i_class
                       ,i_category)
order by qoh, i_product_name, i_brand, i_class, i_category
LIMIT 100;