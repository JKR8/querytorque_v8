WITH sales_aggregated AS (
    SELECT dt.d_year,
           item.i_category_id,
           item.i_category,
           SUM(ss_ext_sales_price) as total_sales
    FROM date_dim dt,
         store_sales,
         item
    WHERE dt.d_date_sk = store_sales.ss_sold_date_sk
      AND store_sales.ss_item_sk = item.i_item_sk
      AND item.i_manager_id = 1
      AND dt.d_moy = 11
      AND dt.d_year = 2002
    GROUP BY dt.d_year,
             item.i_category_id,
             item.i_category
)
SELECT d_year,
       i_category_id,
       i_category,
       total_sales
FROM sales_aggregated
ORDER BY total_sales DESC,
         d_year,
         i_category_id,
         i_category
LIMIT 100