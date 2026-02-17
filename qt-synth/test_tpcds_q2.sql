-- TPC-DS Q2 style: Catalog sales with date range and quantity filter
SELECT 
    i_item_id,
    i_item_desc,
    i_category,
    i_class,
    i_current_price,
    SUM(cs_ext_sales_price) as itemrevenue
FROM catalog_sales
JOIN item ON cs_item_sk = i_item_sk
JOIN date_dim ON cs_sold_date_sk = d_date_sk
WHERE d_date BETWEEN '2000-01-01' AND '2000-02-28'
  AND i_category IN ('Electronics', 'Sports')
  AND cs_quantity > 5
GROUP BY i_item_id, i_item_desc, i_category, i_class, i_current_price
ORDER BY itemrevenue DESC
LIMIT 100;
