WITH filtered_dims AS (
    SELECT d_date_sk, d_year, d_moy
    FROM date_dim
    WHERE d_year = 1998
),
filtered_time AS (
    SELECT t_time_sk
    FROM time_dim
    WHERE t_time BETWEEN 48821 AND 48821 + 28800
),
filtered_ship AS (
    SELECT sm_ship_mode_sk
    FROM ship_mode
    WHERE sm_carrier IN ('GREAT EASTERN', 'LATVIAN')
),
web_sales_data AS (
    SELECT 
        w_warehouse_name,
        w_warehouse_sq_ft,
        w_city,
        w_county,
        w_state,
        w_country,
        d_year as year,
        SUM(CASE WHEN d_moy = 1 THEN ws_ext_sales_price * ws_quantity ELSE 0 END) as jan_sales,
        SUM(CASE WHEN d_moy = 2 THEN ws_ext_sales_price * ws_quantity ELSE 0 END) as feb_sales,
        SUM(CASE WHEN d_moy = 3 THEN ws_ext_sales_price * ws_quantity ELSE 0 END) as mar_sales,
        SUM(CASE WHEN d_moy = 4 THEN ws_ext_sales_price * ws_quantity ELSE 0 END) as apr_sales,
        SUM(CASE WHEN d_moy = 5 THEN ws_ext_sales_price * ws_quantity ELSE 0 END) as may_sales,
        SUM(CASE WHEN d_moy = 6 THEN ws_ext_sales_price * ws_quantity ELSE 0 END) as jun_sales,
        SUM(CASE WHEN d_moy = 7 THEN ws_ext_sales_price * ws_quantity ELSE 0 END) as jul_sales,
        SUM(CASE WHEN d_moy = 8 THEN ws_ext_sales_price * ws_quantity ELSE 0 END) as aug_sales,
        SUM(CASE WHEN d_moy = 9 THEN ws_ext_sales_price * ws_quantity ELSE 0 END) as sep_sales,
        SUM(CASE WHEN d_moy = 10 THEN ws_ext_sales_price * ws_quantity ELSE 0 END) as oct_sales,
        SUM(CASE WHEN d_moy = 11 THEN ws_ext_sales_price * ws_quantity ELSE 0 END) as nov_sales,
        SUM(CASE WHEN d_moy = 12 THEN ws_ext_sales_price * ws_quantity ELSE 0 END) as dec_sales,
        SUM(CASE WHEN d_moy = 1 THEN ws_net_paid_inc_ship_tax * ws_quantity ELSE 0 END) as jan_net,
        SUM(CASE WHEN d_moy = 2 THEN ws_net_paid_inc_ship_tax * ws_quantity ELSE 0 END) as feb_net,
        SUM(CASE WHEN d_moy = 3 THEN ws_net_paid_inc_ship_tax * ws_quantity ELSE 0 END) as mar_net,
        SUM(CASE WHEN d_moy = 4 THEN ws_net_paid_inc_ship_tax * ws_quantity ELSE 0 END) as apr_net,
        SUM(CASE WHEN d_moy = 5 THEN ws_net_paid_inc_ship_tax * ws_quantity ELSE 0 END) as may_net,
        SUM(CASE WHEN d_moy = 6 THEN ws_net_paid_inc_ship_tax * ws_quantity ELSE 0 END) as jun_net,
        SUM(CASE WHEN d_moy = 7 THEN ws_net_paid_inc_ship_tax * ws_quantity ELSE 0 END) as jul_net,
        SUM(CASE WHEN d_moy = 8 THEN ws_net_paid_inc_ship_tax * ws_quantity ELSE 0 END) as aug_net,
        SUM(CASE WHEN d_moy = 9 THEN ws_net_paid_inc_ship_tax * ws_quantity ELSE 0 END) as sep_net,
        SUM(CASE WHEN d_moy = 10 THEN ws_net_paid_inc_ship_tax * ws_quantity ELSE 0 END) as oct_net,
        SUM(CASE WHEN d_moy = 11 THEN ws_net_paid_inc_ship_tax * ws_quantity ELSE 0 END) as nov_net,
        SUM(CASE WHEN d_moy = 12 THEN ws_net_paid_inc_ship_tax * ws_quantity ELSE 0 END) as dec_net
    FROM web_sales
    JOIN warehouse ON ws_warehouse_sk = w_warehouse_sk
    JOIN filtered_dims ON ws_sold_date_sk = d_date_sk
    JOIN filtered_time ON ws_sold_time_sk = t_time_sk
    JOIN filtered_ship ON ws_ship_mode_sk = sm_ship_mode_sk
    GROUP BY 
        w_warehouse_name,
        w_warehouse_sq_ft,
        w_city,
        w_county,
        w_state,
        w_country,
        d_year
),
catalog_sales_data AS (
    SELECT 
        w_warehouse_name,
        w_warehouse_sq_ft,
        w_city,
        w_county,
        w_state,
        w_country,
        d_year as year,
        SUM(CASE WHEN d_moy = 1 THEN cs_ext_list_price * cs_quantity ELSE 0 END) as jan_sales,
        SUM(CASE WHEN d_moy = 2 THEN cs_ext_list_price * cs_quantity ELSE 0 END) as feb_sales,
        SUM(CASE WHEN d_moy = 3 THEN cs_ext_list_price * cs_quantity ELSE 0 END) as mar_sales,
        SUM(CASE WHEN d_moy = 4 THEN cs_ext_list_price * cs_quantity ELSE 0 END) as apr_sales,
        SUM(CASE WHEN d_moy = 5 THEN cs_ext_list_price * cs_quantity ELSE 0 END) as may_sales,
        SUM(CASE WHEN d_moy = 6 THEN cs_ext_list_price * cs_quantity ELSE 0 END) as jun_sales,
        SUM(CASE WHEN d_moy = 7 THEN cs_ext_list_price * cs_quantity ELSE 0 END) as jul_sales,
        SUM(CASE WHEN d_moy = 8 THEN cs_ext_list_price * cs_quantity ELSE 0 END) as aug_sales,
        SUM(CASE WHEN d_moy = 9 THEN cs_ext_list_price * cs_quantity ELSE 0 END) as sep_sales,
        SUM(CASE WHEN d_moy = 10 THEN cs_ext_list_price * cs_quantity ELSE 0 END) as oct_sales,
        SUM(CASE WHEN d_moy = 11 THEN cs_ext_list_price * cs_quantity ELSE 0 END) as nov_sales,
        SUM(CASE WHEN d_moy = 12 THEN cs_ext_list_price * cs_quantity ELSE 0 END) as dec_sales,
        SUM(CASE WHEN d_moy = 1 THEN cs_net_paid_inc_ship_tax * cs_quantity ELSE 0 END) as jan_net,
        SUM(CASE WHEN d_moy = 2 THEN cs_net_paid_inc_ship_tax * cs_quantity ELSE 0 END) as feb_net,
        SUM(CASE WHEN d_moy = 3 THEN cs_net_paid_inc_ship_tax * cs_quantity ELSE 0 END) as mar_net,
        SUM(CASE WHEN d_moy = 4 THEN cs_net_paid_inc_ship_tax * cs_quantity ELSE 0 END) as apr_net,
        SUM(CASE WHEN d_moy = 5 THEN cs_net_paid_inc_ship_tax * cs_quantity ELSE 0 END) as may_net,
        SUM(CASE WHEN d_moy = 6 THEN cs_net_paid_inc_ship_tax * cs_quantity ELSE 0 END) as jun_net,
        SUM(CASE WHEN d_moy = 7 THEN cs_net_paid_inc_ship_tax * cs_quantity ELSE 0 END) as jul_net,
        SUM(CASE WHEN d_moy = 8 THEN cs_net_paid_inc_ship_tax * cs_quantity ELSE 0 END) as aug_net,
        SUM(CASE WHEN d_moy = 9 THEN cs_net_paid_inc_ship_tax * cs_quantity ELSE 0 END) as sep_net,
        SUM(CASE WHEN d_moy = 10 THEN cs_net_paid_inc_ship_tax * cs_quantity ELSE 0 END) as oct_net,
        SUM(CASE WHEN d_moy = 11 THEN cs_net_paid_inc_ship_tax * cs_quantity ELSE 0 END) as nov_net,
        SUM(CASE WHEN d_moy = 12 THEN cs_net_paid_inc_ship_tax * cs_quantity ELSE 0 END) as dec_net
    FROM catalog_sales
    JOIN warehouse ON cs_warehouse_sk = w_warehouse_sk
    JOIN filtered_dims ON cs_sold_date_sk = d_date_sk
    JOIN filtered_time ON cs_sold_time_sk = t_time_sk
    JOIN filtered_ship ON cs_ship_mode_sk = sm_ship_mode_sk
    GROUP BY 
        w_warehouse_name,
        w_warehouse_sq_ft,
        w_city,
        w_county,
        w_state,
        w_country,
        d_year
)
SELECT 
    w_warehouse_name,
    w_warehouse_sq_ft,
    w_city,
    w_county,
    w_state,
    w_country,
    'GREAT EASTERN,LATVIAN' as ship_carriers,
    year,
    SUM(jan_sales) as jan_sales,
    SUM(feb_sales) as feb_sales,
    SUM(mar_sales) as mar_sales,
    SUM(apr_sales) as apr_sales,
    SUM(may_sales) as may_sales,
    SUM(jun_sales) as jun_sales,
    SUM(jul_sales) as jul_sales,
    SUM(aug_sales) as aug_sales,
    SUM(sep_sales) as sep_sales,
    SUM(oct_sales) as oct_sales,
    SUM(nov_sales) as nov_sales,
    SUM(dec_sales) as dec_sales,
    SUM(jan_sales / w_warehouse_sq_ft) as jan_sales_per_sq_foot,
    SUM(feb_sales / w_warehouse_sq_ft) as feb_sales_per_sq_foot,
    SUM(mar_sales / w_warehouse_sq_ft) as mar_sales_per_sq_foot,
    SUM(apr_sales / w_warehouse_sq_ft) as apr_sales_per_sq_foot,
    SUM(may_sales / w_warehouse_sq_ft) as may_sales_per_sq_foot,
    SUM(jun_sales / w_warehouse_sq_ft) as jun_sales_per_sq_foot,
    SUM(jul_sales / w_warehouse_sq_ft) as jul_sales_per_sq_foot,
    SUM(aug_sales / w_warehouse_sq_ft) as aug_sales_per_sq_foot,
    SUM(sep_sales / w_warehouse_sq_ft) as sep_sales_per_sq_foot,
    SUM(oct_sales / w_warehouse_sq_ft) as oct_sales_per_sq_foot,
    SUM(nov_sales / w_warehouse_sq_ft) as nov_sales_per_sq_foot,
    SUM(dec_sales / w_warehouse_sq_ft) as dec_sales_per_sq_foot,
    SUM(jan_net) as jan_net,
    SUM(feb_net) as feb_net,
    SUM(mar_net) as mar_net,
    SUM(apr_net) as apr_net,
    SUM(may_net) as may_net,
    SUM(jun_net) as jun_net,
    SUM(jul_net) as jul_net,
    SUM(aug_net) as aug_net,
    SUM(sep_net) as sep_net,
    SUM(oct_net) as oct_net,
    SUM(nov_net) as nov_net,
    SUM(dec_net) as dec_net
FROM (
    SELECT * FROM web_sales_data
    UNION ALL
    SELECT * FROM catalog_sales_data
) combined_data
GROUP BY 
    w_warehouse_name,
    w_warehouse_sq_ft,
    w_city,
    w_county,
    w_state,
    w_country,
    ship_carriers,
    year
ORDER BY w_warehouse_name
LIMIT 100;