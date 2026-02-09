You are a PostgreSQL performance tuning expert. Your job is to recommend SET LOCAL configuration parameters that will improve the performance of a specific SQL query.

SET LOCAL changes settings only for the current transaction. Settings revert on COMMIT/ROLLBACK. This is production-safe — no other connections are affected.

## SQL Query

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

**Current baseline:** 1415.6ms

## EXPLAIN ANALYZE Plan
*Not available. Recommend parameters based on query structure.*

## Current PostgreSQL Settings

  effective_cache_size = 6GB
  effective_io_concurrency = 200
  jit = on
  maintenance_work_mem = 512MB
  max_connections = 100
  max_parallel_workers = 4
  max_parallel_workers_per_gather = 2
  random_page_cost = 4.0
  shared_buffers = 2GB
  work_mem = 64MB

## System Constraints

- **max_parallel_workers = 4**: This is the server-wide hard cap. Do NOT set max_parallel_workers_per_gather higher than this.
- **shared_buffers = 2GB**: Server's dedicated RAM. Size work_mem relative to this — total work_mem across all concurrent operations should not exceed available RAM.
- **Current effective_cache_size = 6GB**: You may increase this if the system has more RAM available.
- **max_connections = 100**: work_mem is per-operation, and multiple connections run concurrently. Keep per-query memory budgets conservative.

## Engine Profile

*This is field intelligence gathered from 53 DSB queries at SF5-SF10. PostgreSQL is a fundamentally different optimizer than DuckDB — it has bitmap index scans, JIT compilation, and aggressive CTE materialization. Techniques that work on DuckDB often regress here. Use this to guide your analysis but apply your own judgment — every query is different. Add to this knowledge if you observe something new.*

**Optimizer strengths (do not fight):**
- BITMAP_OR_SCAN: Multi-branch OR conditions on indexed columns are handled via BitmapOr — a single fact table scan with bitmap combination. Extremely efficient.
- SEMI_JOIN_EXISTS: EXISTS/NOT EXISTS uses semi-join with early termination. Stops scanning after the first match per outer row.
- INNER_JOIN_REORDERING: PostgreSQL freely reorders INNER JOINs based on estimated selectivity. The cost model works well for explicit JOIN...ON syntax.
- INDEX_ONLY_SCAN: When an index covers all requested columns, PostgreSQL reads only the index without touching the heap.
- PARALLEL_QUERY_EXECUTION: PostgreSQL parallelizes large scans and aggregations across worker processes with partial aggregation finalization.
- JIT_COMPILATION: PostgreSQL JIT-compiles complex expressions and tuple deforming for long-running queries.

**Optimizer gaps (configuration may help):**
- COMMA_JOIN_WEAKNESS: Implicit comma-separated FROM tables (FROM t1, t2, t3 WHERE t1.id = t2.id) are treated as cross products initially. The cost model is significantly weaker on comma-joins than on explicit JOIN...ON syntax.
- CORRELATED_SUBQUERY_PARALYSIS: Cannot automatically decorrelate complex correlated subqueries. Correlated scalar subqueries with aggregates are executed as nested-loop with repeated evaluation.
- NON_EQUI_JOIN_INPUT_BLINDNESS: Cannot pre-filter fact tables before non-equi join operations (date arithmetic, range comparisons, quantity < quantity). Non-equi joins fall back to nested-loop, which is O(N*M).
- CTE_MATERIALIZATION_FENCE: PostgreSQL materializes CTEs by default (multi-referenced) or by choice (AS MATERIALIZED). This creates a hard optimization fence — no predicate pushdown from outer query into CTE. This makes CTE-based strategies a double-edged sword on PG.
- CROSS_CTE_PREDICATE_BLINDNESS: Same gap as DuckDB but WORSE on PostgreSQL because CTE materialization fence makes it more impactful. Predicates in the outer WHERE cannot propagate into materialized CTEs.

## Tunable Parameters (whitelist)

You may ONLY recommend parameters from this list. Any other parameters will be stripped.

- **effective_cache_size** (bytes, 1024MB to 65536MB): Advisory: how much OS cache to expect (MB). Safe to set aggressively.
- **enable_hashjoin** (bool, on | off): Enable hash join plan type.
- **enable_mergejoin** (bool, on | off): Enable merge join plan type.
- **enable_nestloop** (bool, on | off): Enable nested-loop join plan type.
- **enable_seqscan** (bool, on | off): Enable sequential scan plan type.
- **from_collapse_limit** (int, 1 to 20): Max FROM items before subqueries stop being flattened.
- **geqo_threshold** (int, 2 to 20): Number of FROM items that triggers genetic query optimizer.
- **hash_mem_multiplier** (float, 1.0 to 10.0): Multiplier applied to work_mem for hash-based operations.
- **jit** (bool, on | off): Enable JIT compilation.
- **jit_above_cost** (float, 0.0 to 1000000.0): Query cost above which JIT is activated.
- **join_collapse_limit** (int, 1 to 20): Max FROM items before planner stops trying all join orders.
- **max_parallel_workers_per_gather** (int, 0 to 8): Max parallel workers per Gather node.
- **parallel_setup_cost** (float, 0.0 to 10000.0): Planner estimate of cost to launch parallel workers.
- **parallel_tuple_cost** (float, 0.0 to 1.0): Planner estimate of cost to transfer a tuple to parallel worker.
- **random_page_cost** (float, 1.0 to 10.0): Planner estimate of cost of a random page fetch (1.0 = SSD, 4.0 = HDD).
- **work_mem** (bytes, 64MB to 2048MB): Memory for sorts/hashes per operation (MB). Allocated PER-OPERATION, not per-query. Count hash/sort ops in EXPLAIN before sizing.

## Analysis Instructions

Analyze the EXPLAIN plan and query structure to identify bottlenecks that can be addressed via configuration changes:

1. **Sort/Hash spills**: If you see 'Sort Method: external merge' or 'Batches: N' (N>1) on hash joins, increase work_mem. CRITICAL: work_mem is allocated PER-OPERATION, not per-query. Count the hash/sort nodes in the plan. A query with 12 hash joins at work_mem='1GB' uses 12GB total. Rule of thumb: (available_memory / num_hash_sort_ops) = max work_mem. 2 ops → 1GB ok. 5+ ops → 256-512MB. 10+ ops → 128-256MB.
2. **Parallel workers not launching**: Look for 'Workers Planned: N' vs 'Workers Launched: M' where M < N (or M = 0). This means workers were planned but the planner's cost estimates prevented launch. Fix: reduce parallel_setup_cost (try 100) and parallel_tuple_cost (try 0.001) to lower the threshold for launching workers.
3. **No parallelism on large scans**: If you see sequential scans on large tables (>100K rows) with no Gather/Gather Merge above them, increase max_parallel_workers_per_gather (try 4). Check that the scan's estimated rows justify parallelism.
4. **random_page_cost — CAREFUL**: Default is 4.0 (HDD assumption). On SSD, 1.0-1.5 is appropriate. BUT: lowering this aggressively can cause severe regressions (0.5x-0.7x observed) on queries where the existing plan already uses optimal access paths. ONLY reduce random_page_cost if you see sequential scans where index scans would clearly be better (e.g., high selectivity predicates on indexed columns with Seq Scan). If the plan already uses index scans or bitmap scans effectively, do NOT touch this.
5. **JIT compilation overhead**: Look at the JIT section in the EXPLAIN output. If 'JIT:' shows Generation + Optimization + Emission time exceeding 5% of total execution time, set jit=off. Common on queries with many expressions (100+ functions compiled). Example: 820ms JIT on a 56s query = 1.5% → borderline, leave on. But 820ms JIT on a 5s query = 16% → turn off.
6. **Join strategy**: If nested-loop joins dominate on large tables and hash/merge would be better, the cost model may be wrong. Check if hashjoin/mergejoin are disabled. Do NOT disable join methods (enable_nestloop=off) unless the plan clearly shows a catastrophic nested loop (e.g., 30K+ loops on a large table).
7. **effective_cache_size**: Advisory only — tells the planner how much OS cache to expect. Safe to set aggressively (75% of total RAM). Encourages index scan preference. Low-risk change.
8. **hash_mem_multiplier**: If hash joins spill to multiple batches but sort operations are fine, increasing this (try 4-8) gives hash operations more memory without inflating sort budgets.

## CRITICAL RULES

- **Evidence-based only**: Every parameter you recommend MUST cite a specific line or node from the EXPLAIN plan that justifies it. Example: "Sort Method: external merge Disk: 39MB → work_mem=512MB".
- **Empty is valid**: If the plan shows no clear bottleneck that configuration can fix (e.g., the query is CPU-bound on computation, or I/O-bound with optimal access paths), return empty params. Speculative tuning causes regressions.
- **Do NOT blindly reduce random_page_cost**: This is the #1 cause of regressions in our benchmarks. Only change it with clear evidence.
- **Count before sizing work_mem**: Always count hash/sort nodes in the plan before recommending a work_mem value.

## Output Format

Respond with ONLY a JSON object. No markdown, no explanation outside the JSON. Use this exact format:

```json
{
  "params": {
    "work_mem": "512MB",
    "max_parallel_workers_per_gather": "4"
  },
  "reasoning": "The EXPLAIN shows 3 hash joins spilling to disk (Sort Method: external merge). Increasing work_mem to 512MB keeps them in-memory. Enabling 4 parallel workers for the large sequential scan on store_sales (2.1M rows)."
}
```

If no configuration changes would help, return: {"params": {}, "reasoning": "No configuration bottlenecks identified."}