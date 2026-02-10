-- DeepSeek with reasoning mode enabled
-- Uses IN subquery for predicate pushdown
with customer_total_return as (
    select
        sr_customer_sk as ctr_customer_sk,
        sr_store_sk as ctr_store_sk,
        sum(SR_FEE) as ctr_total_return
    from store_returns
    inner join date_dim on sr_returned_date_sk = d_date_sk
    where d_year = 2000
      and sr_store_sk in (  -- Filter stores early
          select s_store_sk
          from store
          where s_state = 'SD'
      )
    group by sr_customer_sk, sr_store_sk
),
store_averages as (
    select
        ctr_store_sk,
        avg(ctr_total_return) * 1.2 as threshold
    from customer_total_return
    group by ctr_store_sk
)
select
    c_customer_id
from customer_total_return ctr1
inner join store_averages sa on ctr1.ctr_store_sk = sa.ctr_store_sk
inner join customer on ctr1.ctr_customer_sk = c_customer_sk
where ctr1.ctr_total_return > sa.threshold
order by c_customer_id
LIMIT 100;
