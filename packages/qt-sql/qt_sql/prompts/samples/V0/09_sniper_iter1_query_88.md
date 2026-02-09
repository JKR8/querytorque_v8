You are a senior SQL optimization architect for DuckDB v1.4.3. You have FULL FREEDOM to design your own approach — you are NOT constrained to any specific DAG topology or CTE structure. The analyst's strategy guidance below is ADVISORY, not mandatory.

Preserve defensive guards: if the original uses CASE WHEN x > 0 THEN y/x END around a division, keep it — guards prevent silent breakage. Strip benchmark comments (-- start query, -- end query) from output.

## Target: >=2.0x speedup

Your target is >=2.0x speedup on this query. This is the bar. Anything below 2.0x is a miss.

## Previous Optimization Attempts
Target: **>=2.0x** | 4 workers tried | 4 reached target

| Worker | Strategy | Speedup | Status | Error |
|--------|----------|---------|--------|-------|
| W2 ★ | moderate_dimension_isolation | 6.2373882475101565x | pass |  |
| W4 | novel_structural_transform | 6.104619135142192x | pass |  |
| W3 | aggressive_single_pass_restructure | 5.853343016368592x | pass |  |
| W1 | conservative_early_reduction | 5.269233093530888x | pass |  |


## Best Foundation SQL

The best previous result. You may build on this or start fresh.

```sql
WITH filtered_store AS (
    SELECT s_store_sk
    FROM store
    WHERE s_store_name = 'ese'
),
filtered_hd AS (
    SELECT hd_demo_sk
    FROM household_demographics
    WHERE (
        (hd_dep_count = -1 AND hd_vehicle_count <= -1 + 2)
        OR (hd_dep_count = 4 AND hd_vehicle_count <= 4 + 2)
        OR (hd_dep_count = 3 AND hd_vehicle_count <= 3 + 2)
    )
),
time_ranges AS (
    SELECT 
        t_time_sk,
        CASE 
            WHEN t_hour = 8 AND t_minute >= 30 THEN 1
            WHEN t_hour = 9 AND t_minute < 30 THEN 2
            WHEN t_hour = 9 AND t_minute >= 30 THEN 3
            WHEN t_hour = 10 AND t_minute < 30 THEN 4
            WHEN t_hour = 10 AND t_minute >= 30 THEN 5
            WHEN t_hour = 11 AND t_minute < 30 THEN 6
            WHEN t_hour = 11 AND t_minute >= 30 THEN 7
            WHEN t_hour = 12 AND t_minute < 30 THEN 8
        END AS time_window
    FROM time_dim
    WHERE (
        (t_hour = 8 AND t_minute >= 30) OR
        (t_hour = 9 AND t_minute < 30) OR
        (t_hour = 9 AND t_minute >= 30) OR
        (t_hour = 10 AND t_minute < 30) OR
        (t_hour = 10 AND t_minute >= 30) OR
        (t_hour = 11 AND t_minute < 30) OR
        (t_hour = 11 AND t_minute >= 30) OR
        (t_hour = 12 AND t_minute < 30)
    )
),
sales_with_time AS (
    SELECT 
        tr.time_window,
        ss.ss_sold_time_sk
    FROM store_sales ss
    JOIN filtered_store fs ON ss.ss_store_sk = fs.s_store_sk
    JOIN filtered_hd fhd ON ss.ss_hdemo_sk = fhd.hd_demo_sk
    JOIN time_ranges tr ON ss.ss_sold_time_sk = tr.t_time_sk
)
SELECT 
    COUNT(CASE WHEN time_window = 1 THEN 1 END) AS h8_30_to_9,
    COUNT(CASE WHEN time_window = 2 THEN 1 END) AS h9_to_9_30,
    COUNT(CASE WHEN time_window = 3 THEN 1 END) AS h9_30_to_10,
    COUNT(CASE WHEN time_window = 4 THEN 1 END) AS h10_to_10_30,
    COUNT(CASE WHEN time_window = 5 THEN 1 END) AS h10_30_to_11,
    COUNT(CASE WHEN time_window = 6 THEN 1 END) AS h11_to_11_30,
    COUNT(CASE WHEN time_window = 7 THEN 1 END) AS h11_30_to_12,
    COUNT(CASE WHEN time_window = 8 THEN 1 END) AS h12_to_12_30
FROM sales_with_time;
```

## Failure Synthesis (from diagnostic analyst)

All 4 workers achieved strong wins (5.27x–6.24x) by consolidating 8 independent store_sales scans into a single pass. W2 (6.24x) was best because it classified time windows in the CTE itself using CASE, avoiding any post-join filtering. W1 (5.27x) was slowest because it kept 8 correlated subqueries against a single qualified_sales CTE — still 8 scans of the CTE. W3/W4 used similar single-pass strategies with minor structural differences.

## Unexplored Angles

1. Aggregate pushdown: compute counts directly in the sales_with_time CTE instead of materializing individual rows then aggregating
2. Hash join order optimization: join the smallest dimension first (store ~5 rows) to reduce intermediate cardinality earliest
3. Bit manipulation: encode time window as a bitmap for the 8 windows and use a single GROUP BY with bit operations

## Strategy Guidance (ADVISORY — not mandatory)

W2's approach is nearly optimal — 6.24x from a single-pass scan. The remaining headroom is in join ordering (smallest dim first) and potentially pushing the aggregation into a single SELECT without materializing sales_with_time as a separate CTE.

## Example Adaptation Notes

single_pass_aggregation: W2 already applied this. The sniper should focus on whether inlining the final aggregation into the CTE body (no separate final_counts step) improves performance.
dimension_cte_isolate: Already applied by all workers. Verify that join order matches dimension cardinality (store first, then hd, then time).

## Hazard Flags

- All workers passed — no semantic errors to avoid
- The 8 COUNT(CASE) pattern is proven correct
- Avoid re-introducing 8 separate scans (the original bottleneck)

## Engine Profile

### Optimizer Strengths (DO NOT fight these)
- **INTRA_SCAN_PREDICATE_PUSHDOWN**: Pushes WHERE filters directly into SEQ_SCAN. Single-table predicates are applied at scan time, zero overhead.
- **SAME_COLUMN_OR**: OR on the SAME column (e.g., t_hour BETWEEN 8 AND 11 OR t_hour BETWEEN 16 AND 17) is handled in a single scan with range checks.
- **HASH_JOIN_SELECTION**: Selects hash joins automatically. Join ordering is generally sound for 2-4 table joins.
- **CTE_INLINING**: CTEs referenced once are typically inlined (treated as subquery). Multi-referenced CTEs may be materialized.
- **COLUMNAR_PROJECTION**: Only reads columns actually referenced. Unused columns have zero I/O cost.
- **PARALLEL_AGGREGATION**: Scans and aggregations parallelized across threads. PERFECT_HASH_GROUP_BY is highly efficient.
- **EXISTS_SEMI_JOIN**: EXISTS/NOT EXISTS uses semi-join with early termination — stops after first match per outer row.

### Optimizer Gaps (opportunities)
- **CROSS_CTE_PREDICATE_BLINDNESS**: Cannot push predicates from the outer query backward into CTE definitions.
  Opportunity: Move selective predicates INTO the CTE definition. Pre-filter dimensions/facts before they get materialized.
    + Q6/Q11: 4.00x — date filter moved into CTE
    + Q63: 3.77x — pre-joined filtered dates with fact table before other dims
    + Q93: 2.97x — dimension filter applied before LEFT JOIN chain
- **REDUNDANT_SCAN_ELIMINATION**: Cannot detect when the same fact table is scanned N times with similar filters across subquery boundaries.
  Opportunity: Consolidate N subqueries on the same table into 1 scan with CASE WHEN / FILTER() inside aggregates.
    + Q88: 6.28x — 8 time-bucket subqueries consolidated into 1 scan with 8 CASE branches
    + Q9: 4.47x — 15 separate store_sales scans consolidated into 1 scan with 5 CASE buckets
- **CORRELATED_SUBQUERY_PARALYSIS**: Cannot automatically decorrelate correlated aggregate subqueries into GROUP BY + JOIN.
  Opportunity: Convert correlated WHERE to CTE with GROUP BY on the correlation column, then JOIN back.
    + Q1: 2.92x — correlated AVG with store_sk correlation converted to GROUP BY store_sk + JOIN
- **CROSS_COLUMN_OR_DECOMPOSITION**: Cannot decompose OR conditions that span DIFFERENT columns into independent targeted scans.
  Opportunity: Split cross-column ORs into UNION ALL branches, each with a targeted single-column filter.
    + Q88: 6.28x — 8 time-bucket subqueries with distinct hour ranges (distinct access paths)
    + Q15: 3.17x — (zip OR state OR price) split to 3 targeted branches
    + Q10: 1.49x, Q45: 1.35x, Q41: 1.89x
- **LEFT_JOIN_FILTER_ORDER_RIGIDITY**: Cannot reorder LEFT JOINs to apply selective dimension filters before expensive fact table joins.
  Opportunity: Pre-filter the selective dimension into a CTE, then use the filtered result as the JOIN partner.
    + Q93: 2.97x — filtered reason dimension FIRST, then LEFT JOIN to returns then fact
    + Q80: 1.40x — dimension isolation before fact join
- **UNION_CTE_SELF_JOIN_DECOMPOSITION**: When a UNION ALL CTE is self-joined N times with each join filtering to a different discriminator, the optimizer materializes the full UNION once and probes it N times, discarding most rows each time.
  Opportunity: Split the UNION ALL into N separate CTEs (one per discriminator value).
    + Q74: 1.36x — UNION of store/web sales split into separate year-partitioned CTEs

## Correctness Invariants (HARD STOPS — non-negotiable)

These 4 constraints are absolute. Even with full creative freedom, you may NEVER violate these:

- **COMPLETE_OUTPUT**: The rewritten query must output ALL columns from the original SELECT. Never drop, rename, or reorder output columns. Every column alias must be preserved exactly as in the original.
- **CTE_COLUMN_COMPLETENESS**: CRITICAL: When creating or modifying a CTE, its SELECT list MUST include ALL columns referenced by downstream queries. Check the Node Contracts section: every column in downstream_refs MUST appear in the CTE output. Also ensure: (1) JOIN columns used by consumers are included in SELECT, (2) every table referenced in WHERE is present in FROM/JOIN, (3) no ambiguous column names between the CTE and re-joined tables. Dropping a column that a downstream node needs will cause an execution error.
- **LITERAL_PRESERVATION**: CRITICAL: When rewriting SQL, you MUST copy ALL literal values (strings, numbers, dates) EXACTLY from the original query. Do NOT invent, substitute, or 'improve' any filter values. If the original says d_year = 2000, your rewrite MUST say d_year = 2000. If the original says ca_state = 'GA', your rewrite MUST say ca_state = 'GA'. Changing these values will produce WRONG RESULTS and the rewrite will be REJECTED.
- **SEMANTIC_EQUIVALENCE**: The rewritten query MUST return exactly the same rows, columns, and ordering as the original. This is the prime directive. Any rewrite that changes the result set — even by one row, one column, or a different sort order — is WRONG and will be REJECTED.

## Aggregation Semantics Check (HARD STOP)

- STDDEV_SAMP/VARIANCE are grouping-sensitive — changing group membership changes the result.
- AVG and STDDEV are NOT duplicate-safe.
- FILTER over a combined group != separate per-group computation.
- Verify aggregation equivalence for ANY proposed restructuring.

## Original SQL

```sql
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
```

## Rewrite Checklist (must pass before final SQL)

- Verify output schema matches the Column Completeness Contract (same columns, same names, same order).
- Keep all semantic invariants from `Correctness Invariants` (including join/null behavior).
- Verify aggregation equivalence: same rows participate in each group, same aggregate semantics.
- Preserve all literals exactly (numbers, strings, date values).
- Apply `Hazard Flags` as hard guards against known failure modes.

## Output Format

Return a JSON object with your rewrite as `rewrite_sets`. Each node is a CTE
or the final SELECT. You MUST declare the output columns for every node in
`node_contracts` — this forces you to reason about what flows between CTEs.

Only include nodes you **changed or added**. Unchanged nodes are auto-filled
from the original query.

### Column Completeness Contract

Your `main_query` node MUST produce **exactly** these output columns (same names, same order):

  1. `*`

Do NOT add, remove, or rename any output columns. The result set schema must be identical to the original query.

```json
{
  "rewrite_sets": [{
    "id": "rs_01",
    "transform": "<transform_name>",
    "nodes": {
      "<cte_name>": "<SQL for this CTE body>",
      "main_query": "<final SELECT>"
    },
    "node_contracts": {
      "<cte_name>": ["col1", "col2", "..."],
      "main_query": ["col1", "col2", "..."]
    },
    "set_local": ["SET LOCAL work_mem = '512MB'", "SET LOCAL jit = 'off'"],
    "data_flow": "<cte_a> -> <cte_b> -> main_query",
    "invariants_kept": ["same output columns", "same rows"],
    "expected_speedup": "2.0x",
    "risk": "low"
  }]
}
```

### Rules
- Every node in `nodes` MUST appear in `node_contracts` and vice versa
- `node_contracts`: list the output column names each node produces
- `data_flow`: show the CTE dependency chain (forces you to think about order)
- `main_query` = the final SELECT — its contract must match the Column Completeness Contract above
- New CTE structures are encouraged — design the best topology for the query

After the JSON, explain the mechanism:

```
Changes: <1-2 sentences: what structural change + the expected mechanism>
Expected speedup: <estimate>
```

Now output your rewrite as JSON: