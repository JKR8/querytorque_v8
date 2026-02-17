SELECT 
    i_item_id,
    SUM(ss_sales_price) AS total_sales
FROM 
    store_sales
JOIN 
    item ON ss_item_sk = i_item_sk
JOIN 
    date_dim ON ss_sold_date_sk = d_date_sk
WHERE 
    i_category = 'Electronics'
    AND d_year = 2001
GROUP BY 
    i_item_id
ORDER BY 
    total_sales DESC
LIMIT 10;
