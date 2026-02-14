-- TPC-DS Query 36 (Original - comma joins, full year scan)
-- Tables: store_sales, date_dim, item, store
-- Key: Full-year filter (d_year=2002) on STORE_SALES = massive scan
-- ROLLUP + RANK window function = memory-intensive aggregation
-- Expected: spills or times out on small warehouses
SELECT
    sum(ss_net_profit)/sum(ss_ext_sales_price) as gross_margin,
    i_category,
    i_class,
    grouping(i_category)+grouping(i_class) as lochierarchy,
    rank() over (
        partition by grouping(i_category)+grouping(i_class),
        case when grouping(i_class) = 0 then i_category end
        order by sum(ss_net_profit)/sum(ss_ext_sales_price) asc) as rank_within_parent
FROM
    store_sales,
    date_dim d1,
    item,
    store
WHERE
    d1.d_year = 2002
    AND d1.d_date_sk = ss_sold_date_sk
    AND i_item_sk = ss_item_sk
    AND s_store_sk = ss_store_sk
    AND s_state IN ('SD','TN','GA','SC','MO','AL','MI','OH')
GROUP BY ROLLUP(i_category, i_class)
ORDER BY
    lochierarchy DESC,
    case when lochierarchy = 0 then i_category end,
    rank_within_parent
LIMIT 100;
