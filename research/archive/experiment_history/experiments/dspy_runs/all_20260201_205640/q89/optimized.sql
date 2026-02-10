-- start query 89 in stream 0 using template query89.tpl
SELECT *
FROM (
    SELECT 
        i.i_category,
        i.i_class,
        i.i_brand,
        s.s_store_name,
        s.s_company_name,
        d.d_moy,
        SUM(ss.ss_sales_price) AS sum_sales,
        AVG(SUM(ss.ss_sales_price)) OVER (
            PARTITION BY i.i_category, i.i_brand, s.s_store_name, s.s_company_name
        ) AS avg_monthly_sales
    FROM store_sales ss
    JOIN item i ON ss.ss_item_sk = i.i_item_sk
    JOIN date_dim d ON ss.ss_sold_date_sk = d.d_date_sk
    JOIN store s ON ss.ss_store_sk = s.s_store_sk
    WHERE d.d_year = 1999
      AND s.s_store_sk <= 400
      AND (
          (i.i_category IN ('Jewelry', 'Shoes', 'Electronics') 
           AND i.i_class IN ('semi-precious', 'athletic', 'portable'))
          OR 
          (i.i_category IN ('Men', 'Music', 'Women') 
           AND i.i_class IN ('accessories', 'rock', 'maternity'))
      )
    GROUP BY i.i_category, i.i_class, i.i_brand, s.s_store_name, s.s_company_name, d.d_moy
) tmp1
WHERE avg_monthly_sales <> 0 
  AND ABS(sum_sales - avg_monthly_sales) / avg_monthly_sales > 0.1
ORDER BY sum_sales - avg_monthly_sales, s_store_name
LIMIT 100;

-- end query 89 in stream 0 using template query89.tpl