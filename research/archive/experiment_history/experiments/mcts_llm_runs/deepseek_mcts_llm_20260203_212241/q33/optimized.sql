with item_filter as (
    select i_manufact_id
    from item
    where i_category in ('Home')
),
ss_branch1 as (
    select i_manufact_id, sum(ss_ext_sales_price) total_sales
    from store_sales, date_dim, customer_address, item
    where ss_item_sk = i_item_sk
      and ss_sold_date_sk = d_date_sk
      and d_year = 2002
      and d_moy = 1
      and ss_addr_sk = ca_address_sk
      and ca_gmt_offset = -5
      and i_manufact_id in (select i_manufact_id from item_filter)
    group by i_manufact_id
),
cs_branch1 as (
    select i_manufact_id, sum(cs_ext_sales_price) total_sales
    from catalog_sales, date_dim, customer_address, item
    where cs_item_sk = i_item_sk
      and cs_sold_date_sk = d_date_sk
      and d_year = 2002
      and d_moy = 1
      and cs_bill_addr_sk = ca_address_sk
      and ca_gmt_offset = -5
      and i_manufact_id in (select i_manufact_id from item_filter)
    group by i_manufact_id
),
ws_branch1 as (
    select i_manufact_id, sum(ws_ext_sales_price) total_sales
    from web_sales, date_dim, customer_address, item
    where ws_item_sk = i_item_sk
      and ws_sold_date_sk = d_date_sk
      and d_year = 2002
      and d_moy = 1
      and ws_bill_addr_sk = ca_address_sk
      and ca_gmt_offset = -5
      and i_manufact_id in (select i_manufact_id from item_filter)
    group by i_manufact_id
)
select i_manufact_id, sum(total_sales) total_sales
from (
    select * from ss_branch1
    union all
    select * from cs_branch1
    union all
    select * from ws_branch1
) tmp1
group by i_manufact_id
order by total_sales
LIMIT 100