-- TPC-DS Query 12 style - Web sales with date and category filters
SELECT 
    i_item_desc,
    i_category,
    i_class,
    i_current_price,
    SUM(ws_ext_sales_price) as itemrevenue,
    SUM(ws_ext_sales_price) * 100 / SUM(SUM(ws_ext_sales_price)) OVER (PARTITION BY i_class) as revenueratio
FROM web_sales
JOIN item ON ws_item_sk = i_item_sk
JOIN date_dim ON ws_sold_date_sk = d_date_sk
WHERE i_category IN ('Sports', 'Books', 'Home')
  AND d_date BETWEEN '2000-01-01' AND '2000-03-31'
GROUP BY i_item_desc, i_category, i_class, i_current_price
ORDER BY i_category, i_class, itemrevenue DESC
LIMIT 100;
