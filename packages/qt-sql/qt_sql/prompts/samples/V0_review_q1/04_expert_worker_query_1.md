You are a SQL rewrite engine for DuckDB v1.4.3. Follow the Target DAG structure below. Your job is to write correct, executable SQL for each node — not to decide whether to restructure. Preserve exact semantic equivalence (same rows, same columns, same ordering). Preserve defensive guards: if the original uses CASE WHEN x > 0 THEN y/x END around a division, keep it — even when a WHERE clause makes the zero case unreachable. Guards prevent silent breakage if filters change upstream. Strip benchmark comments (-- start query, -- end query) from your output.

DuckDB specifics: columnar storage (SELECT only needed columns). CTEs referenced once are typically inlined; CTEs referenced multiple times may be materialized. FILTER clause is native (`COUNT(*) FILTER (WHERE cond)`). Predicate pushdown stops at UNION ALL boundaries and multi-level CTE references.

## Semantic Contract (MUST preserve)

This query counts store sales by 8 consecutive half-hour time windows (8:30-12:30) at store 'ese' for households matching 3 specific (hd_dep_count, hd_vehicle_count) conditions. Returns a single row with 8 COUNT columns. The 8 subqueries are independent — same fact table, same dim filters, different time_dim conditions. Cross-join semantics: each count is independent.

## Target DAG + Node Contracts

Build your rewrite following this CTE structure. Each node's OUTPUT list is exhaustive — your SQL must produce exactly those columns.

TARGET_DAG:
  filtered_store -> sales_with_time
  filtered_hd -> sales_with_time
  time_ranges -> sales_with_time -> final_counts

NODE_CONTRACTS:
  filtered_store:
    FROM: store
    WHERE: s_store_name = 'ese'
    OUTPUT: s_store_sk
    EXPECTED_ROWS: ~1-5
    CONSUMERS: sales_with_time
  filtered_hd:
    FROM: household_demographics
    WHERE: (hd_dep_count = -1 AND hd_vehicle_count <= 1)
       OR (hd_dep_count = 4 AND hd_vehicle_count <= 6)
       OR (hd_dep_count = 3 AND hd_vehicle_count <= 5)
    OUTPUT: hd_demo_sk
    EXPECTED_ROWS: ~1200
    CONSUMERS: sales_with_time
  time_ranges:
    FROM: time_dim
    WHERE: (t_hour BETWEEN 8 AND 12)
    SELECT: t_time_sk, CASE WHEN t_hour=8 AND t_minute>=30 THEN 1 WHEN t_hour=9 AND t_minute<30 THEN 2 ... WHEN t_hour=12 AND t_minute<30 THEN 8 END AS time_window
    OUTPUT: t_time_sk, time_window
    EXPECTED_ROWS: ~240 (8 half-hour windows x ~30 per window)
    CONSUMERS: sales_with_time
  sales_with_time:
    FROM: store_sales JOIN filtered_store JOIN filtered_hd JOIN time_ranges
    JOIN: ss_store_sk = s_store_sk, ss_hdemo_sk = hd_demo_sk, ss_sold_time_sk = t_time_sk
    OUTPUT: time_window
    EXPECTED_ROWS: ~10K-50K
    CONSUMERS: final_counts
  final_counts:
    FROM: sales_with_time
    AGGREGATE: COUNT(CASE WHEN time_window = N THEN 1 END) for N=1..8
    OUTPUT: h8_30_to_9, h9_to_9_30, h9_30_to_10, h10_to_10_30, h10_30_to_11, h11_to_11_30, h11_30_to_12, h12_to_12_30
    EXPECTED_ROWS: 1

## Hazard Flags (avoid these specific risks)

- The 8 subqueries use different OR conditions on household_demographics for different time windows. VERIFY: all 8 subqueries actually share the same hd filters — they do (all use the same 3 OR branches). The time window is the only differentiator.
- Do NOT use FILTER clause with COUNT — use COUNT(CASE WHEN ... THEN 1 END) for maximum DuckDB compatibility.
- Preserve exact literal values: hd_dep_count = -1, hd_dep_count = 4, hd_dep_count = 3, hd_vehicle_count <= -1+2, <= 4+2, <= 3+2.

## Regression Warnings (observed failures on similar queries)

None applicable — Q88's 8 independent subqueries have no cross-CTE dependencies that could cause the known regression patterns.

## Constraints (analyst-filtered for this query)

- LITERAL_PRESERVATION: hd_dep_count = -1, hd_dep_count = 4, hd_dep_count = 3 and corresponding hd_vehicle_count bounds must be preserved exactly.
- SEMANTIC_EQUIVALENCE: Each COUNT must produce identical results — no row duplication from joins.
- COMPLETE_OUTPUT: All 8 output columns with exact aliases.
- CTE_COLUMN_COMPLETENESS: Every CTE must pass through all columns needed by downstream consumers.

## Example Adaptation Notes

For each example: what to apply to your rewrite, and what to ignore.

dimension_cte_isolate:
  APPLY: Pre-filter household_demographics and store into CTEs. Q88 joins these 8 times each — pre-filtering from 7.2K/1K rows to ~1200/5 rows eliminates repeated hash probes.
  IGNORE: The date_dim isolation from the original example — Q88 uses time_dim, not date_dim.

date_cte_isolate:
  APPLY: Same pattern but for time_dim. Add CASE-based time window classification during the CTE so downstream only needs the window ID.
  IGNORE: The d_year filter pattern — Q88 uses t_hour/t_minute.

shared_dimension_multi_channel:
  APPLY: Shared dimension CTEs across all 8 time windows. Each window shares the same store + household filters.
  IGNORE: The multi-channel (store/web/catalog) structure — Q88 is single-channel (store_sales only).

## Original SQL

```sql
-- start query 1 in stream 0 using template query1.tpl
with customer_total_return as
(select sr_customer_sk as ctr_customer_sk
,sr_store_sk as ctr_store_sk
,sum(SR_FEE) as ctr_total_return
from store_returns
,date_dim
where sr_returned_date_sk = d_date_sk
and d_year =2000
group by sr_customer_sk
,sr_store_sk)
 select c_customer_id
from customer_total_return ctr1
,store
,customer
where ctr1.ctr_total_return > (select avg(ctr_total_return)*1.2
from customer_total_return ctr2
where ctr1.ctr_store_sk = ctr2.ctr_store_sk)
and s_store_sk = ctr1.ctr_store_sk
and s_state = 'SD'
and ctr1.ctr_customer_sk = c_customer_sk
order by c_customer_id
 LIMIT 100;

-- end query 1 in stream 0 using template query1.tpl
```

## Rewrite Checklist (must pass before final SQL)

- Follow every node in `TARGET_DAG` and produce each `NODE_CONTRACT` output column exactly.
- Keep all semantic invariants from `Semantic Contract` and `Constraints` (including join/null behavior).
- Preserve all literals and the exact final output schema/order.
- Apply `Hazard Flags` and `Regression Warnings` as hard guards against known failure modes.

## Output Format

Return a JSON object with your rewrite as `rewrite_sets`. Each node is a CTE
or the final SELECT. You MUST declare the output columns for every node in
`node_contracts` — this forces you to reason about what flows between CTEs.

Only include nodes you **changed or added**. Unchanged nodes are auto-filled
from the original query.

### Column Completeness Contract

Your `main_query` node MUST produce **exactly** these output columns (same names, same order):

  1. `c_customer_id`

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