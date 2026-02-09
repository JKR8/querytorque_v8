You are a senior query optimization architect. Your job is to deeply analyze a SQL query, determine the single best optimization strategy, and then produce the optimized SQL directly.

You have all the data: EXPLAIN plans, DAG costs, full constraint list, global knowledge, and the complete example catalog. Analyze thoroughly, then implement the best strategy as working SQL.

## Query: prepaid_service_daily
## Dialect: duckdb

```sql
1 | SELECT cs_bill_customer_sk AS customer_id, cs_item_sk AS service_id, 'Consumer' AS customer_type, 'Operator_A' AS service_provider, i_product_name AS service_name, 'Standard_Plan' AS plan_tier, d_date AS calendar_date FROM catalog_sales JOIN item ON cs_item_sk = i_item_sk JOIN date_dim ON cs_sold_date_sk = d_date_sk
```

## EXPLAIN Plan

*EXPLAIN plan not available for this query. Use DAG cost percentages as proxy for bottleneck identification.*

## Query Structure (DAG)

### 1. main_query
**Role**: Root / Output (Definition Order: 0)
**Stats**: 0% Cost | ~0 rows
**Outputs**: [customer_id, service_id, customer_type, service_provider, service_name, plan_tier, calendar_date]
**Dependencies**: catalog_sales, item (join), date_dim (join)
**Key Logic (SQL)**:
```sql
SELECT
  cs_bill_customer_sk AS customer_id,
  cs_item_sk AS service_id,
  'Consumer' AS customer_type,
  'Operator_A' AS service_provider,
  i_product_name AS service_name,
  'Standard_Plan' AS plan_tier,
  d_date AS calendar_date
FROM catalog_sales
JOIN item
  ON cs_item_sk = i_item_sk
JOIN date_dim
  ON cs_sold_date_sk = d_date_sk
```


## Aggregation Semantics Check

You MUST verify aggregation equivalence for any proposed restructuring:

- **STDDEV_SAMP(x)** requires >=2 non-NULL values per group. Returns NULL for 0-1 values. Changing group membership changes the result.
- `STDDEV_SAMP(x) FILTER (WHERE year=1999)` over a combined (1999,2000) group is NOT equivalent to `STDDEV_SAMP(x)` over only 1999 rows — FILTER still uses the combined group's membership for the stddev denominator.
- **AVG and STDDEV are NOT duplicate-safe**: if a join introduces row duplication, the aggregate result changes.
- When splitting a UNION ALL CTE with GROUP BY + aggregate, each split branch must preserve the exact GROUP BY columns and filter to the exact same row set as the original.
- **SAFE ALTERNATIVE**: If GROUP BY includes the discriminator column (e.g., d_year), each group is already partitioned. STDDEV_SAMP computed per-group is correct. You can then pivot using `MAX(CASE WHEN year = 1999 THEN year_total END) AS year_total_1999` because the GROUP BY guarantees exactly one row per (customer, year) — the MAX is just a row selector, not a real aggregation.

## Your Task

First, use a `<reasoning>` block for your internal analysis. This will be stripped before parsing. Work through these steps IN ORDER:

1. **CLASSIFY**: What structural archetype is this query?
   (channel-comparison self-join / correlated-aggregate filter / star-join with late dim filter / repeated fact scan / multi-channel UNION ALL / EXISTS-set operations / other)

2. **EXPLAIN PLAN ANALYSIS**: From the EXPLAIN ANALYZE output, identify:
   - Compute wall-clock ms per EXPLAIN node. Sum repeated operations (e.g., 2x store_sales joins = total cost). The EXPLAIN is ground truth, not the DAG cost percentages.
   - Which nodes consume >10% of runtime and WHY
   - Where row counts drop sharply (existing selectivity)
   - Where row counts DON'T drop (missed optimization opportunity)
   - Whether the optimizer already splits CTEs, pushes predicates, or performs transforms you might otherwise assign
   - Count scans per base table. If a fact table is scanned N times, a restructuring that reduces it to 1 scan saves (N-1)/N of that table's I/O cost. Prioritize transforms that reduce scan count on the largest tables.
   - Whether the CTE is materialized once and probed multiple times, or re-executed per reference

3. **GAP MATCHING**: Compare the EXPLAIN analysis to the Engine Profile gaps above. For each gap:
   - Does this query exhibit the gap? (e.g., is a predicate NOT pushed into a CTE? Is the same fact table scanned multiple times?)
   - Check the 'opportunity' — does this query's structure match?
   - Check 'what_didnt_work' and 'field_notes' — any disqualifiers for this query?
   - Also verify: is the optimizer ALREADY handling this well? (Check the Optimizer Strengths above — if the engine already does it, your transform adds overhead, not value.)

4. **AGGREGATION TRAP CHECK**: For every aggregate function in the query, verify: does my proposed restructuring change which rows participate in each group? STDDEV_SAMP, VARIANCE, PERCENTILE_CONT, CORR are grouping-sensitive. SUM, COUNT, MIN, MAX are grouping-insensitive (modulo duplicates). If the query uses FILTER clauses or conditional aggregation, verify equivalence explicitly.

5. **TRANSFORM SELECTION**: From the matched engine gaps, select the single best transform (or compound strategy) that maximizes expected value (rows affected × historical speedup from evidence) for THIS query.
   REJECT tag-matched examples whose primary technique requires a structural feature this query lacks. Tag matching is approximate — always verify structural applicability.

6. **DAG DESIGN**: Define the target DAG topology for your chosen strategy. Verify that every node contract has exhaustive output columns by checking downstream references.
   CTE materialization matters: a CTE referenced by 2+ consumers will likely be materialized. A CTE referenced once may be inlined.

7. **WRITE REWRITE**: Implement your strategy as a JSON rewrite_set. Each changed or added CTE is a node. Produce per-node SQL matching your DAG design from step 6. Declare output columns for every node in `node_contracts`. The rewrite must be semantically equivalent to the original.

Then produce the structured briefing in EXACTLY this format:

```
=== SHARED BRIEFING ===

SEMANTIC_CONTRACT: (80-150 tokens, cover ONLY:)
(a) One sentence of business intent (start from pre-computed intent if available).
(b) JOIN type semantics that constrain rewrites (INNER = intersection = all sides must match).
(c) Any aggregation function traps specific to THIS query.
(d) Any filter dependencies that a rewrite could break.
Do NOT repeat information already in ACTIVE_CONSTRAINTS or REGRESSION_WARNINGS.

BOTTLENECK_DIAGNOSIS:
[Which operation dominates cost and WHY (not just '50% cost').
Scan-bound vs join-bound vs aggregation-bound.
Cardinality flow (how many rows at each stage).
What the optimizer already handles well (don't re-optimize).
Whether DAG cost percentages are misleading.]

ACTIVE_CONSTRAINTS:
- [CORRECTNESS_CONSTRAINT_ID]: [Why it applies to this query, 1 line]
- [ENGINE_GAP_ID]: [Evidence from EXPLAIN that this gap is active]
(List all 4 correctness constraints + the 1-3 engine gaps that
are active for THIS query based on your EXPLAIN analysis.)

REGRESSION_WARNINGS:
1. [Pattern name] ([observed regression]):
   CAUSE: [What happened mechanistically]
   RULE: [Actionable avoidance rule for THIS query]
(If no regression warnings are relevant, write 'None applicable.')

=== REWRITE ===

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

Rules:
- Every node in `nodes` MUST appear in `node_contracts` and vice versa
- `node_contracts`: list the output column names each node produces
- `data_flow`: show the CTE dependency chain
- `main_query` = the final SELECT
- Only include nodes you changed or added; unchanged nodes auto-filled from original

After the JSON, explain the mechanism:

```
Changes: <1-2 sentences: what structural change + the expected mechanism>
Expected speedup: <estimate>
```
```

## Section Validation Checklist (MUST pass before final output)

Use this checklist to verify content quality, not just section presence:

### SHARED BRIEFING
- `SEMANTIC_CONTRACT`: 80-150 tokens and includes business intent, JOIN semantics, aggregation trap, and filter dependency.
- `BOTTLENECK_DIAGNOSIS`: states dominant mechanism, bound type (`scan-bound`/`join-bound`/`aggregation-bound`), cardinality flow, and what optimizer already handles well.
- `ACTIVE_CONSTRAINTS`: includes all 4 correctness IDs plus 1-3 active engine gaps with EXPLAIN evidence.
- `REGRESSION_WARNINGS`: either `None applicable.` or numbered entries with both `CAUSE:` and `RULE:`.

### REWRITE
- JSON `rewrite_sets` block is present with at least one rewrite set.
- `transform`: non-empty, names the optimization transform.
- `nodes`: every changed/added CTE has per-node SQL.
- `node_contracts`: every node in `nodes` has a matching contract with output column list.
- `data_flow`: shows the CTE dependency chain.
- `main_query` output columns match original query exactly (same names, same order).
- All literals preserved exactly (numbers, strings, date values).
- Semantically equivalent to the original query.

## Transform Catalog

Select the best transform (or compound strategy of 2-3 transforms) that maximizes expected speedup for THIS query.

### Predicate Movement
- **global_predicate_pushdown**: Trace selective predicates from late in the CTE chain back to the earliest scan via join equivalences. Biggest win when a dimension filter is applied after a large intermediate materialization.
  Maps to examples: pushdown, early_filter, date_cte_isolate
- **transitive_predicate_propagation**: Infer predicates through join equivalence chains (A.key = B.key AND B.key = 5 -> A.key = 5). Especially across CTE boundaries where optimizers stop propagating.
  Maps to examples: early_filter, dimension_cte_isolate
- **null_rejecting_join_simplification**: When downstream WHERE rejects NULLs from the outer side of a LEFT JOIN, convert to INNER. Enables reordering and predicate pushdown. CHECK: does the query actually have LEFT/OUTER joins before assigning this.
  Maps to examples: (no direct gold example — novel transform)

### Join Restructuring
- **self_join_elimination**: When a UNION ALL CTE is self-joined N times with each join filtering to a different discriminator, split into N pre-partitioned CTEs. Eliminates discriminator filtering and repeated hash probes on rows that don't match.
  Maps to examples: union_cte_split, shared_dimension_multi_channel
- **decorrelation**: Convert correlated EXISTS/IN/scalar subqueries to CTE + JOIN. CHECK: does the query actually have correlated subqueries before assigning this.
  Maps to examples: decorrelate, composite_decorrelate_union
- **aggregate_pushdown**: When GROUP BY follows a multi-table join but aggregation only uses columns from one side, push the GROUP BY below the join. CHECK: verify the join doesn't change row multiplicity for the aggregate (one-to-many breaks AVG/STDDEV).
  Maps to examples: (no direct gold example — novel transform)
- **late_attribute_binding**: When a dimension table is joined only to resolve display columns (names, descriptions) that aren't used in filters, aggregations, or join conditions, defer that join until after all filtering and aggregation is complete. Join on the surrogate key once against the final reduced result set. This eliminates N-1 dimension scans when the CTE references the dimension N times. CHECK: verify the deferred columns aren't used in WHERE, GROUP BY, or JOIN ON — only in the final SELECT.
  Maps to examples: dimension_cte_isolate (partial pattern), early_filter

### Scan Optimization
- **star_join_prefetch**: Pre-filter ALL dimension tables into CTEs, then probe fact table with the combined key intersection.
  Maps to examples: dimension_cte_isolate, multi_dimension_prefetch, prefetch_fact_join, date_cte_isolate
- **single_pass_aggregation**: Merge N subqueries on the same fact table into 1 scan with CASE/FILTER inside aggregates. CHECK: STDDEV_SAMP/VARIANCE are grouping-sensitive — FILTER over a combined group != separate per-group computation.
  Maps to examples: single_pass_aggregation, channel_bitmap_aggregation
- **scan_consolidation_pivot**: When a CTE is self-joined N times with each reference filtering to a different discriminator (e.g., year, channel), consolidate into fewer scans that GROUP BY the discriminator, then pivot rows to columns using MAX(CASE WHEN discriminator = X THEN agg_value END). This halves the fact scans and dimension joins. SAFE when GROUP BY includes the discriminator — each group is naturally partitioned, so aggregates like STDDEV_SAMP are computed correctly per-partition. The pivot MAX is just a row selector (one row per group), not a real aggregation.
  Maps to examples: single_pass_aggregation, union_cte_split

### Structural Transforms
- **union_consolidation**: Share dimension lookups across UNION ALL branches that scan different fact tables with the same dim joins.
  Maps to examples: shared_dimension_multi_channel
- **window_optimization**: Push filters before window functions when they don't affect the frame. Convert ROW_NUMBER + filter to LATERAL + LIMIT. Merge same-PARTITION windows into one sort pass.
  Maps to examples: deferred_window_aggregation
- **exists_restructuring**: Convert INTERSECT to EXISTS for semi-join short-circuit, or restructure complex EXISTS with shared CTEs. CHECK: does the query actually have INTERSECT or complex EXISTS.
  Maps to examples: intersect_to_exists, multi_intersect_exists_cte

## Strategy Selection Rules

1. **CHECK APPLICABILITY**: Each transform has a structural prerequisite (correlated subquery, UNION ALL CTE, LEFT JOIN, etc.). Verify the query actually has the prerequisite before assigning a transform. DO NOT assign decorrelation if there are no correlated subqueries.
2. **CHECK OPTIMIZER OVERLAP**: Read the EXPLAIN plan. If the optimizer already performs a transform (e.g., already splits a UNION CTE, already pushes a predicate), that transform will have marginal benefit. Note this in your reasoning and prefer transforms the optimizer is NOT already doing.
3. **MAXIMIZE EXPECTED VALUE**: Select the single strategy with the highest expected speedup, considering both the magnitude of the bottleneck it addresses and the historical success rate.
4. **ASSESS RISK PER-QUERY**: Risk is a function of (transform x query complexity), not an inherent property of the transform. Decorrelation is low-risk on a simple EXISTS and high-risk on nested correlation inside a CTE. Assess per-assignment.
5. **COMPOSITION IS ALLOWED AND ENCOURAGED**: A strategy can combine 2-3 transforms from different categories (e.g., star_join_prefetch + scan_consolidation_pivot, or date_cte_isolate + early_filter + decorrelate). The TARGET_DAG should reflect the combined structure. Compound strategies are often the source of the biggest wins.

Select 1-3 examples that genuinely match the strategy. Do NOT pad with irrelevant examples — an irrelevant example is worse than no example. Use example IDs from the catalog above.

For TARGET_DAG: Define the CTE structure you want produced. For NODE_CONTRACTS: Be exhaustive with OUTPUT columns — missing columns cause semantic breaks.
