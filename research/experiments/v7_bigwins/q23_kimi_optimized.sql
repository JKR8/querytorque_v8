with 
date_filtered as (
  select d_date_sk, d_date
  from date_dim
  where d_year in (2000, 2001, 2002, 2003)
),
ss_filtered as (
  select ss.ss_item_sk, ss.ss_customer_sk, ss.ss_quantity, ss.ss_sales_price, d.d_date
  from store_sales ss
  join date_filtered d on ss.ss_sold_date_sk = d.d_date_sk
),
frequent_ss_items as (
  select substr(i.i_item_desc,1,30) itemdesc, 
         i.i_item_sk item_sk, 
         ss.d_date solddate, 
         count(*) cnt
  from ss_filtered ss
  join item i on ss.ss_item_sk = i.i_item_sk
  group by substr(i.i_item_desc,1,30), i.i_item_sk, ss.d_date
  having count(*) > 4
),
customer_totals as (
  select c.c_customer_sk, sum(ss.ss_quantity*ss.ss_sales_price) as csales
  from ss_filtered ss
  join customer c on ss.ss_customer_sk = c.c_customer_sk
  group by c.c_customer_sk
),
max_store_sales as (
  select max(csales) as tpcds_cmax
  from customer_totals
),
best_ss_customer as (
  select c_customer_sk
  from customer_totals
  where csales > (95/100.0) * (select tpcds_cmax from max_store_sales)
)
select sum(sales)
from (
  select cs.cs_quantity*cs.cs_list_price sales
  from catalog_sales cs
  join date_dim d on cs.cs_sold_date_sk = d.d_date_sk
  where d.d_year = 2000 
    and d.d_moy = 5
    and cs.cs_item_sk in (select item_sk from frequent_ss_items)
    and cs.cs_bill_customer_sk in (select c_customer_sk from best_ss_customer)
  union all
  select ws.ws_quantity*ws.ws_list_price sales
  from web_sales ws
  join date_dim d on ws.ws_sold_date_sk = d.d_date_sk
  where d.d_year = 2000 
    and d.d_moy = 5
    and ws.ws_item_sk in (select item_sk from frequent_ss_items)
    and ws.ws_bill_customer_sk in (select c_customer_sk from best_ss_customer)
) combined
limit 100;