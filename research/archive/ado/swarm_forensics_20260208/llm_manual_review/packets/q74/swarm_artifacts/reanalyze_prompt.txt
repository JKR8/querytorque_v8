You are analyzing 5 failed optimization attempts to design a refined approach that reaches 2.0x speedup.

Your job: understand WHY each attempt fell short, identify unexplored optimization angles, and synthesize a NEW strategy that combines the best insights while avoiding repeated mistakes.

## Query: query_74
## Target: 2.0x speedup
## Dialect: duckdb

```sql
-- start query 74 in stream 0 using template query74.tpl
with year_total as (
 select c_customer_id customer_id
       ,c_first_name customer_first_name
       ,c_last_name customer_last_name
       ,d_year as year
       ,stddev_samp(ss_net_paid) year_total
       ,'s' sale_type
 from customer
     ,store_sales
     ,date_dim
 where c_customer_sk = ss_customer_sk
   and ss_sold_date_sk = d_date_sk
   and d_year in (1999,1999+1)
 group by c_customer_id
         ,c_first_name
         ,c_last_name
         ,d_year
 union all
 select c_customer_id customer_id
       ,c_first_name customer_first_name
       ,c_last_name customer_last_name
       ,d_year as year
       ,stddev_samp(ws_net_paid) year_total
       ,'w' sale_type
 from customer
     ,web_sales
     ,date_dim
 where c_customer_sk = ws_bill_customer_sk
   and ws_sold_date_sk = d_date_sk
   and d_year in (1999,1999+1)
 group by c_customer_id
         ,c_first_name
         ,c_last_name
         ,d_year
         )
  select
        t_s_secyear.customer_id, t_s_secyear.customer_first_name, t_s_secyear.customer_last_name
 from year_total t_s_firstyear
     ,year_total t_s_secyear
     ,year_total t_w_firstyear
     ,year_total t_w_secyear
 where t_s_secyear.customer_id = t_s_firstyear.customer_id
         and t_s_firstyear.customer_id = t_w_secyear.customer_id
         and t_s_firstyear.customer_id = t_w_firstyear.customer_id
         and t_s_firstyear.sale_type = 's'
         and t_w_firstyear.sale_type = 'w'
         and t_s_secyear.sale_type = 's'
         and t_w_secyear.sale_type = 'w'
         and t_s_firstyear.year = 1999
         and t_s_secyear.year = 1999+1
         and t_w_firstyear.year = 1999
         and t_w_secyear.year = 1999+1
         and t_s_firstyear.year_total > 0
         and t_w_firstyear.year_total > 0
         and case when t_w_firstyear.year_total > 0 then t_w_secyear.year_total / t_w_firstyear.year_total else null end
           > case when t_s_firstyear.year_total > 0 then t_s_secyear.year_total / t_s_firstyear.year_total else null end
 order by 2,1,3
 LIMIT 100;

-- end query 74 in stream 0 using template query74.tpl
```

## Previous Attempts

### Worker 1: conservative_pushdown_materialization
- **Status**: error (0.00x)
- **Error**: Binder Error: Referenced column "ss_ss_net_paid" not found in FROM clause!
Candidate bindings: "ss_net_paid", "ss_sold_date_sk", "ss_sales_price", "ss_ext_sales_price", "ss_ext_discount_amt"

LINE 7:         STDDEV_SAMP(ss_ss_net_paid) AS year_total
                            ^
- **Transforms**: none
- **Examples used**: pushdown, early_filter, materialize_cte
- **Strategy hint**: Push filters into CTEs to reduce data early, pre-filter dimension tables, and materialize the CTE to avoid repeated computation.

```sql
WITH store_sales_agg AS (
    SELECT
        c.c_customer_id AS customer_id,
        c.c_first_name AS customer_first_name,
        c.c_last_name AS customer_last_name,
        d.d_year AS year,
        STDDEV_SAMP(ss_ss_net_paid) AS year_total
    FROM customer c
    INNER JOIN store_sales ss ON c.c_customer_sk = ss.ss_customer_sk
    INNER JOIN date_dim d ON ss.ss_sold_date_sk = d.d_date_sk
    WHERE d.d_year IN (1999, 2000)
    GROUP BY
        c.c_customer_id,
        c.c_first_name,
        c.c_last_name,
        d.d_year
),
web_sales_agg AS (
    SELECT
        c.c_customer_id AS customer_id,
        c.c_first_name AS customer_first_name,
        c.c_last_name AS customer_last_name,
        d.d_year AS year,
        STDDEV_SAMP(ws.ws_net_paid) AS year_total
    FROM customer c
    INNER JOIN web_sales ws ON c.c_customer_sk = ws.ws_bill_customer_sk
    INNER JOIN date_dim d ON ws.ws_sold_date_sk = d.d_date_sk
    WHERE d.d_year IN (1999, 2000)
    GROUP BY
        c.c_customer_id,
-- ... (truncated)
```

### Worker 2: moderate_dimension_date_isolation
- **Status**: pass (1.21x)
- **Transforms**: none
- **Examples used**: date_cte_isolate, dimension_cte_isolate, union_cte_split
- **Strategy hint**: Isolate date and dimension filtering into separate CTEs, then split the UNION ALL CTE by channel to enable targeted optimizations.

```sql
WITH filtered_dates AS (
    SELECT d_date_sk
    FROM date_dim
    WHERE d_year IN (1999, 2000)
),
store_sales_agg AS (
    SELECT
        c.c_customer_id AS customer_id,
        c.c_first_name AS customer_first_name,
        c.c_last_name AS customer_last_name,
        d.d_year AS year,
        STDDEV_SAMP(ss.ss_net_paid) AS year_total,
        's' AS sale_type
    FROM customer c
    JOIN store_sales ss ON c.c_customer_sk = ss.ss_customer_sk
    JOIN date_dim d ON ss.ss_sold_date_sk = d.d_date_sk
    WHERE d.d_year IN (1999, 2000)
    GROUP BY
        c.c_customer_id,
        c.c_first_name,
        c.c_last_name,
        d.d_year
),
web_sales_agg AS (
    SELECT
        c.c_customer_id AS customer_id,
        c.c_first_name AS customer_first_name,
        c.c_last_name AS customer_last_name,
        d.d_year AS year,
        STDDEV_SAMP(ws.ws_net_paid) AS year_total,
-- ... (truncated)
```

### Worker 3: aggressive_multi_channel_prefetch
- **Status**: error (0.00x)
- **Error**: Binder Error: Referenced column "d_year" not found in FROM clause!
Candidate bindings: "c_email_address", "d_date_sk", "ss_addr_sk", "ss_ext_tax", "ss_net_paid"

LINE 21:     d_year
             ^
- **Transforms**: none
- **Examples used**: shared_dimension_multi_channel, multi_date_range_cte, prefetch_fact_join
- **Strategy hint**: Pre-filter shared dimensions for multiple sales channels, create separate CTEs for each date range, and prefetch fact joins to minimize intermediate data.

```sql
WITH filtered_dates AS (
  SELECT d_date_sk
  FROM date_dim
  WHERE d_year IN (1999, 2000)
),
store_sales_filtered AS (
  SELECT
    c_customer_id,
    c_first_name,
    c_last_name,
    d_year,
    STDDEV_SAMP(ss_net_paid) AS year_total,
    's' AS sale_type
  FROM customer
  JOIN store_sales ON c_customer_sk = ss_customer_sk
  JOIN filtered_dates ON ss_sold_date_sk = d_date_sk
  GROUP BY
    c_customer_id,
    c_first_name,
    c_last_name,
    d_year
),
web_sales_filtered AS (
  SELECT
    c_customer_id,
    c_first_name,
    c_last_name,
    d_year,
    STDDEV_SAMP(ws_net_paid) AS year_total,
    'w' AS sale_type
-- ... (truncated)
```

### Worker 4: novel_structural_transform
- **Status**: pass (1.22x)
- **Transforms**: none
- **Examples used**: or_to_union, intersect_to_exists, decorrelate
- **Strategy hint**: Transform OR conditions to UNION ALL, convert intersect patterns to EXISTS, and decorrelate subqueries to improve join planning and reduce dependencies.

```sql
WITH store_aggregates AS (
  SELECT
    c_customer_id AS customer_id,
    c_first_name AS customer_first_name,
    c_last_name AS customer_last_name,
    d_year AS year,
    STDDEV_SAMP(ss_net_paid) AS year_total
  FROM customer
  JOIN store_sales ON c_customer_sk = ss_customer_sk
  JOIN date_dim ON ss_sold_date_sk = d_date_sk
  WHERE d_year IN (1999, 2000)
  GROUP BY
    c_customer_id,
    c_first_name,
    c_last_name,
    d_year
),
web_aggregates AS (
  SELECT
    c_customer_id AS customer_id,
    c_first_name AS customer_first_name,
    c_last_name AS customer_last_name,
    d_year AS year,
    STDDEV_SAMP(ws_net_paid) AS year_total
  FROM customer
  JOIN web_sales ON c_customer_sk = ws_bill_customer_sk
  JOIN date_dim ON ws_sold_date_sk = d_date_sk
  WHERE d_year IN (1999, 2000)
  GROUP BY
    c_customer_id,
-- ... (truncated)
```

### Worker 5: refined_snipe
- **Status**: pass (1.23x)
- **Transforms**: none
- **Examples used**: 
- **Strategy hint**: Snipe from iter 1

```sql
WITH store_sales_agg AS (
  SELECT
    c_customer_id AS customer_id,
    c_first_name AS customer_first_name,
    c_last_name AS customer_last_name,
    d_year AS year,
    STDDEV_SAMP(ss_net_paid) AS year_total
  FROM customer
  JOIN store_sales ON c_customer_sk = ss_customer_sk
  JOIN date_dim ON ss_sold_date_sk = d_date_sk
  WHERE d_year IN (1999, 2000)
  GROUP BY c_customer_id, c_first_name, c_last_name, d_year
),
web_sales_agg AS (
  SELECT
    c_customer_id AS customer_id,
    c_first_name AS customer_first_name,
    c_last_name AS customer_last_name,
    d_year AS year,
    STDDEV_SAMP(ws_net_paid) AS year_total
  FROM customer
  JOIN web_sales ON c_customer_sk = ws_bill_customer_sk
  JOIN date_dim ON ws_sold_date_sk = d_date_sk
  WHERE d_year IN (1999, 2000)
  GROUP BY c_customer_id, c_first_name, c_last_name, d_year
)
SELECT
  ss2000.customer_id,
  ss2000.customer_first_name,
  ss2000.customer_last_name
-- ... (truncated)
```

## DAG Structure & Bottlenecks

| Node | Role | Cost % |
|------|------|-------:|
| year_total |  | 0.0% |
| main_query |  | 0.0% |

## Available Examples (Full Catalog)

- **composite_decorrelate_union** (2.42xx) — Decorrelate multiple correlated EXISTS subqueries into pre-materialized DISTINCT
- **date_cte_isolate** (4.00xx) — Extract date filtering into a separate CTE to enable predicate pushdown and redu
- **decorrelate** (2.92xx) — Convert correlated subquery to separate CTE with GROUP BY, then JOIN
- **deferred_window_aggregation** (1.36xx) — When multiple CTEs each perform GROUP BY + WINDOW (cumulative sum), then are joi
- **dimension_cte_isolate** (1.93xx) — Pre-filter ALL dimension tables into CTEs before joining with fact table, not ju
- **early_filter** (4.00xx) — Filter dimension tables FIRST, then join to fact tables to reduce expensive join
- **intersect_to_exists** (1.83xx) — Convert INTERSECT subquery pattern to multiple EXISTS clauses for better join pl
- **materialize_cte** (1.37xx) — Extract repeated subquery patterns into a CTE to avoid recomputation
- **multi_date_range_cte** (2.35xx) — When query uses multiple date_dim aliases with different filters (d1, d2, d3), c
- **multi_dimension_prefetch** (2.71xx) — Pre-filter multiple dimension tables (date + store) into separate CTEs before jo
- **or_to_union** (3.17xx) — Split OR conditions on different columns into UNION ALL branches for better inde
- **prefetch_fact_join** (3.77xx) — Pre-filter dimension table into CTE, then pre-join with fact table in second CTE
- **pushdown** (2.11xx) — Push filters from outer query into CTEs/subqueries to reduce intermediate result
- **shared_dimension_multi_channel** (1.30xx) — Extract shared dimension filters (date, item, promotion) into CTEs when multiple
- **single_pass_aggregation** (4.47xx) — Consolidate multiple subqueries scanning the same table into a single CTE with c
- **union_cte_split** (1.36xx) — Split a generic UNION ALL CTE into specialized CTEs when the main query filters 

## Your Task

Analyze the failed attempts and design a refined approach:

1. **Failure Analysis**: Why did all attempts fall short? Be specific about mechanisms.
2. **Common Patterns**: What did multiple workers try unsuccessfully?
3. **Unexplored Space**: What optimization angles were missed entirely?
4. **Refined Strategy**: Synthesize a NEW approach combining best insights.

### Output Format (follow EXACTLY)

```
FAILURE_ANALYSIS:
<Why all workers fell short — be specific about mechanisms>

UNEXPLORED_OPPORTUNITIES:
<What optimization approaches haven't been tried>

REFINED_STRATEGY:
<Concrete optimization approach for next attempt>

EXAMPLES: <ex1>, <ex2>, <ex3>
HINT: <specific guidance for the refined attempt>
```