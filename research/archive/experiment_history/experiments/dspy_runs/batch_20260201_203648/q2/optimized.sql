-- start query 2 in stream 0 using template query2.tpl
with wscs as (
    select ws_sold_date_sk as sold_date_sk,
           ws_ext_sales_price as sales_price,
           'web' as source
    from web_sales
    union all
    select cs_sold_date_sk as sold_date_sk,
           cs_ext_sales_price as sales_price,
           'catalog' as source
    from catalog_sales
),
wswscs as (
    select d.d_week_seq,
           d.d_year,
           sum(case when d.d_day_name = 'Sunday' then w.sales_price else null end) as sun_sales,
           sum(case when d.d_day_name = 'Monday' then w.sales_price else null end) as mon_sales,
           sum(case when d.d_day_name = 'Tuesday' then w.sales_price else null end) as tue_sales,
           sum(case when d.d_day_name = 'Wednesday' then w.sales_price else null end) as wed_sales,
           sum(case when d.d_day_name = 'Thursday' then w.sales_price else null end) as thu_sales,
           sum(case when d.d_day_name = 'Friday' then w.sales_price else null end) as fri_sales,
           sum(case when d.d_day_name = 'Saturday' then w.sales_price else null end) as sat_sales
    from wscs w
    join date_dim d on d.d_date_sk = w.sold_date_sk
    where d.d_year in (1998, 1999)
    group by d.d_week_seq, d.d_year
)
select curr.d_week_seq as d_week_seq1,
       round(curr.sun_sales / next.sun_sales, 2) as sun_sales_ratio,
       round(curr.mon_sales / next.mon_sales, 2) as mon_sales_ratio,
       round(curr.tue_sales / next.tue_sales, 2) as tue_sales_ratio,
       round(curr.wed_sales / next.wed_sales, 2) as wed_sales_ratio,
       round(curr.thu_sales / next.thu_sales, 2) as thu_sales_ratio,
       round(curr.fri_sales / next.fri_sales, 2) as fri_sales_ratio,
       round(curr.sat_sales / next.sat_sales, 2) as sat_sales_ratio
from wswscs curr
join wswscs next on curr.d_week_seq = next.d_week_seq - 53
where curr.d_year = 1998
  and next.d_year = 1999
order by curr.d_week_seq;

-- end query 2 in stream 0 using template query2.tpl