-- start query 67 in stream 0 using template query67.tpl
SELECT *
FROM (
    SELECT i_category
          ,i_class
          ,i_brand
          ,i_product_name
          ,d_year
          ,d_qoy
          ,d_moy
          ,s_store_id
          ,sumsales
          ,RANK() OVER (PARTITION BY i_category ORDER BY sumsales DESC) AS rk
    FROM (
        SELECT i.i_category
              ,i.i_class
              ,i.i_brand
              ,i.i_product_name
              ,d.d_year
              ,d.d_qoy
              ,d.d_moy
              ,s.s_store_id
              ,SUM(COALESCE(ss.ss_sales_price * ss.ss_quantity, 0)) AS sumsales
        FROM store_sales ss
        INNER JOIN date_dim d ON ss.ss_sold_date_sk = d.d_date_sk
        INNER JOIN store s ON ss.ss_store_sk = s.s_store_sk
        INNER JOIN item i ON ss.ss_item_sk = i.i_item_sk
        WHERE d.d_month_seq BETWEEN 1206 AND 1206 + 11
          AND s.s_store_sk <= 400
        GROUP BY ROLLUP(i.i_category, i.i_class, i.i_brand, i.i_product_name, d.d_year, d.d_qoy, d.d_moy, s.s_store_id)
    ) dw1
) dw2
WHERE rk <= 100
ORDER BY i_category
        ,i_class
        ,i_brand
        ,i_product_name
        ,d_year
        ,d_qoy
        ,d_moy
        ,s_store_id
        ,sumsales
        ,rk
LIMIT 100;

-- end query 67 in stream 0 using template query67.tpl