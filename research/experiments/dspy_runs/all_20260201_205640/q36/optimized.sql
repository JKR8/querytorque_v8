-- start query 36 in stream 0 using template query36.tpl
SELECT 
    SUM(ss_net_profit)/SUM(ss_ext_sales_price) AS gross_margin,
    i_category,
    i_class,
    GROUPING(i_category)+GROUPING(i_class) AS lochierarchy,
    RANK() OVER (
        PARTITION BY GROUPING(i_category)+GROUPING(i_class),
        CASE WHEN GROUPING(i_class) = 0 THEN i_category END 
        ORDER BY SUM(ss_net_profit)/SUM(ss_ext_sales_price) ASC
    ) AS rank_within_parent
FROM store_sales
INNER JOIN date_dim d1 ON d1.d_date_sk = ss_sold_date_sk
INNER JOIN item ON i_item_sk = ss_item_sk
INNER JOIN store ON s_store_sk = ss_store_sk
WHERE d1.d_year = 2002 
    AND s_state IN ('SD','TN','GA','SC','MO','AL','MI','OH')
GROUP BY ROLLUP(i_category, i_class)
ORDER BY lochierarchy DESC,
    CASE WHEN lochierarchy = 0 THEN i_category END,
    rank_within_parent
LIMIT 100;

-- end query 36 in stream 0 using template query36.tpl