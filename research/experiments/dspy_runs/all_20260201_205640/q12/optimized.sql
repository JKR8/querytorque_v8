-- start query 12 in stream 0 using template query12.tpl
SELECT i_item_id
      ,i_item_desc 
      ,i_category 
      ,i_class 
      ,i_current_price
      ,SUM(ws_ext_sales_price) AS itemrevenue 
      ,SUM(ws_ext_sales_price)*100/SUM(SUM(ws_ext_sales_price)) OVER
          (PARTITION BY i_class) AS revenueratio
FROM web_sales ws
JOIN item i ON ws.ws_item_sk = i.i_item_sk
JOIN date_dim d ON ws.ws_sold_date_sk = d.d_date_sk
WHERE i.i_category IN ('Books', 'Sports', 'Men')
  AND d.d_date BETWEEN CAST('1998-04-06' AS DATE) 
                   AND (CAST('1998-04-06' AS DATE) + INTERVAL 30 DAY)
GROUP BY i_item_id
        ,i_item_desc 
        ,i_category
        ,i_class
        ,i_current_price
ORDER BY i_category
        ,i_class
        ,i_item_id
        ,i_item_desc
        ,revenueratio
LIMIT 100;

-- end query 12 in stream 0 using template query12.tpl