You are coordinating a swarm of 4 optimization specialists. Each specialist will attempt to optimize the same query using a DIFFERENT strategy and set of examples.

Your job: analyze the query structure, identify 4 diverse optimization angles, and assign each specialist a unique strategy with 3 relevant examples. Maximize diversity to cover the optimization space.

## Query: query_88
## Dialect: duckdb

```sql
-- start query 88 in stream 0 using template query88.tpl
select  *
from
 (select count(*) h8_30_to_9
 from store_sales, household_demographics , time_dim, store
 where ss_sold_time_sk = time_dim.t_time_sk   
     and ss_hdemo_sk = household_demographics.hd_demo_sk 
     and ss_store_sk = s_store_sk
     and time_dim.t_hour = 8
     and time_dim.t_minute >= 30
     and ((household_demographics.hd_dep_count = -1 and household_demographics.hd_vehicle_count<=-1+2) or
          (household_demographics.hd_dep_count = 4 and household_demographics.hd_vehicle_count<=4+2) or
          (household_demographics.hd_dep_count = 3 and household_demographics.hd_vehicle_count<=3+2)) 
     and store.s_store_name = 'ese') s1,
 (select count(*) h9_to_9_30 
 from store_sales, household_demographics , time_dim, store
 where ss_sold_time_sk = time_dim.t_time_sk
     and ss_hdemo_sk = household_demographics.hd_demo_sk
     and ss_store_sk = s_store_sk 
     and time_dim.t_hour = 9 
     and time_dim.t_minute < 30
     and ((household_demographics.hd_dep_count = -1 and household_demographics.hd_vehicle_count<=-1+2) or
          (household_demographics.hd_dep_count = 4 and household_demographics.hd_vehicle_count<=4+2) or
          (household_demographics.hd_dep_count = 3 and household_demographics.hd_vehicle_count<=3+2))
     and store.s_store_name = 'ese') s2,
 (select count(*) h9_30_to_10 
 from store_sales, household_demographics , time_dim, store
 where ss_sold_time_sk = time_dim.t_time_sk
     and ss_hdemo_sk = household_demographics.hd_demo_sk
     and ss_store_sk = s_store_sk
     and time_dim.t_hour = 9
     and time_dim.t_minute >= 30
     and ((household_demographics.hd_dep_count = -1 and household_demographics.hd_vehicle_count<=-1+2) or
          (household_demographics.hd_dep_count = 4 and household_demographics.hd_vehicle_count<=4+2) or
          (household_demographics.hd_dep_count = 3 and household_demographics.hd_vehicle_count<=3+2))
     and store.s_store_name = 'ese') s3,
 (select count(*) h10_to_10_30
 from store_sales, household_demographics , time_dim, store
 where ss_sold_time_sk = time_dim.t_time_sk
     and ss_hdemo_sk = household_demographics.hd_demo_sk
     and ss_store_sk = s_store_sk
     and time_dim.t_hour = 10 
     and time_dim.t_minute < 30
     and ((household_demographics.hd_dep_count = -1 and household_demographics.hd_vehicle_count<=-1+2) or
          (household_demographics.hd_dep_count = 4 and household_demographics.hd_vehicle_count<=4+2) or
          (household_demographics.hd_dep_count = 3 and household_demographics.hd_vehicle_count<=3+2))
     and store.s_store_name = 'ese') s4,
 (select count(*) h10_30_to_11
 from store_sales, household_demographics , time_dim, store
 where ss_sold_time_sk = time_dim.t_time_sk
     and ss_hdemo_sk = household_demographics.hd_demo_sk
     and ss_store_sk = s_store_sk
     and time_dim.t_hour = 10 
     and time_dim.t_minute >= 30
     and ((household_demographics.hd_dep_count = -1 and household_demographics.hd_vehicle_count<=-1+2) or
          (household_demographics.hd_dep_count = 4 and household_demographics.hd_vehicle_count<=4+2) or
          (household_demographics.hd_dep_count = 3 and household_demographics.hd_vehicle_count<=3+2))
     and store.s_store_name = 'ese') s5,
 (select count(*) h11_to_11_30
 from store_sales, household_demographics , time_dim, store
 where ss_sold_time_sk = time_dim.t_time_sk
     and ss_hdemo_sk = household_demographics.hd_demo_sk
     and ss_store_sk = s_store_sk 
     and time_dim.t_hour = 11
     and time_dim.t_minute < 30
     and ((household_demographics.hd_dep_count = -1 and household_demographics.hd_vehicle_count<=-1+2) or
          (household_demographics.hd_dep_count = 4 and household_demographics.hd_vehicle_count<=4+2) or
          (household_demographics.hd_dep_count = 3 and household_demographics.hd_vehicle_count<=3+2))
     and store.s_store_name = 'ese') s6,
 (select count(*) h11_30_to_12
 from store_sales, household_demographics , time_dim, store
 where ss_sold_time_sk = time_dim.t_time_sk
     and ss_hdemo_sk = household_demographics.hd_demo_sk
     and ss_store_sk = s_store_sk
     and time_dim.t_hour = 11
     and time_dim.t_minute >= 30
     and ((household_demographics.hd_dep_count = -1 and household_demographics.hd_vehicle_count<=-1+2) or
          (household_demographics.hd_dep_count = 4 and household_demographics.hd_vehicle_count<=4+2) or
          (household_demographics.hd_dep_count = 3 and household_demographics.hd_vehicle_count<=3+2))
     and store.s_store_name = 'ese') s7,
 (select count(*) h12_to_12_30
 from store_sales, household_demographics , time_dim, store
 where ss_sold_time_sk = time_dim.t_time_sk
     and ss_hdemo_sk = household_demographics.hd_demo_sk
     and ss_store_sk = s_store_sk
     and time_dim.t_hour = 12
     and time_dim.t_minute < 30
     and ((household_demographics.hd_dep_count = -1 and household_demographics.hd_vehicle_count<=-1+2) or
          (household_demographics.hd_dep_count = 4 and household_demographics.hd_vehicle_count<=4+2) or
          (household_demographics.hd_dep_count = 3 and household_demographics.hd_vehicle_count<=3+2))
     and store.s_store_name = 'ese') s8
;

-- end query 88 in stream 0 using template query88.tpl
```

## DAG Structure & Bottlenecks

| Node | Role | Cost % | Key Operations |
|------|------|-------:|----------------|
| main_query |  | 0.0% | — |

## Top 0 Matched Examples (by structural similarity)


## All Available Examples (full catalog — can swap if needed)


## Your Task

Design 4 DIFFERENT optimization strategies exploring diverse approaches. You may keep the matched recommendations OR swap examples from the catalog.

**Constraints**:
- Each worker gets exactly 3 examples
- No duplicate examples across workers (12 total, 3 per worker)
- If fewer than 12 unique examples are available, reuse is allowed

**Diversity guidelines**:
- Worker 1: Conservative — proven patterns, low risk (e.g., pushdown, early filter)
- Worker 2: Moderate — date/dimension isolation, CTE restructuring
- Worker 3: Aggressive — multi-CTE restructuring, prefetch patterns
- Worker 4: Novel — OR-to-UNION, structural transforms, intersect-to-exists

For each worker, specify:
1. **Strategy name** (e.g., `aggressive_date_prefetch`)
2. **3 examples** to use (from matched picks or catalog)
3. **Strategy hint** (1-2 sentences guiding the optimization approach)

### Output Format (follow EXACTLY)

```
WORKER_1:
STRATEGY: <strategy_name>
EXAMPLES: <ex1>, <ex2>, <ex3>
HINT: <strategy guidance>

WORKER_2:
STRATEGY: <strategy_name>
EXAMPLES: <ex4>, <ex5>, <ex6>
HINT: <strategy guidance>

WORKER_3:
STRATEGY: <strategy_name>
EXAMPLES: <ex7>, <ex8>, <ex9>
HINT: <strategy guidance>

WORKER_4:
STRATEGY: <strategy_name>
EXAMPLES: <ex10>, <ex11>, <ex12>
HINT: <strategy guidance>
```