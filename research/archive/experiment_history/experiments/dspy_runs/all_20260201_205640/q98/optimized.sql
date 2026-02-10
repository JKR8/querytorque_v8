-- start query 98 in stream 0 using template query98.tpl
SELECT 
    i_item_id,
    i_item_desc,
    i_category,
    i_class,
    i_current_price,
    SUM(ss_ext_sales_price) AS itemrevenue,
    SUM(ss_ext_sales_price) * 100 / SUM(SUM(ss_ext_sales_price)) OVER (
        PARTITION BY i_class
    ) AS revenueratio
FROM 
    date_dim
JOIN 
    store_sales ON ss_sold_date_sk = d_date_sk
JOIN 
    item ON ss_item_sk = i_item_sk
WHERE 
    i_category IN ('Sports', 'Music', 'Shoes')
    AND d_date BETWEEN DATE '2002-05-20' AND (DATE '2002-05-20' + INTERVAL '30' DAY)
GROUP BY 
    i_item_id,
    i_item_desc,
    i_category,
    i_class,
    i_current_price
ORDER BY 
    i_category,
    i_class,
    i_item_id,
    i_item_desc,
    revenueratio;

-- end query 98 in stream 0 using template query98.tpl