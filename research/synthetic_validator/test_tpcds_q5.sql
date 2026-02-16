-- TPC-DS Query 5 style - Multiple joins with date filtering
SELECT 
    s_store_name,
    i_item_desc,
    sc.revenue,
    i_current_price,
    i_wholesale_cost,
    i_brand
FROM store s
JOIN (
    SELECT 
        ss_store_sk,
        ss_item_sk,
        SUM(ss_sales_price) as revenue
    FROM store_sales
    JOIN date_dim ON ss_sold_date_sk = d_date_sk
    WHERE d_year = 2000
      AND d_moy = 1
    GROUP BY ss_store_sk, ss_item_sk
) sc ON s.s_store_sk = sc.ss_store_sk
JOIN item i ON sc.ss_item_sk = i.i_item_sk
WHERE s.s_store_name = 'ese'
ORDER BY revenue DESC
LIMIT 100;
