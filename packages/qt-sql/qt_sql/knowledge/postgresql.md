**SQL Optimizer Swarm: PostgreSQL Focus**

## 1. ENGINE STRENGTHS
1. **BITMAP_OR_SCAN**: Handles multi-branch ORs on indexed columns via single scan with bitmap combination. **Do NOT** split OR conditions into UNION ALL branches.
2. **SEMI_JOIN_EXISTS**: EXISTS/NOT EXISTS uses semi-join with early termination. **Do NOT** convert EXISTS to IN/NOT IN or materializing CTEs.
3. **INNER_JOIN_REORDERING**: Freely reorders INNER JOINs based on selectivity. **Do NOT** manually restructure INNER JOIN orders.
4. **INDEX_ONLY_SCAN**: Reads only index when covering all requested columns. **Do NOT** pre-filter small dimensions into CTEs for index-only scans.
5. **PARALLEL_QUERY_EXECUTION**: Parallelizes large scans/aggregations across workers. **Do NOT** restructure into CTEs that block parallelism.
6. **JIT_COMPILATION**: JIT-compiles complex expressions for long queries. **Do NOT** simplify expressions for per-row overhead.

## 2. CORRECTNESS RULES
- Preserve exact row count — no filtering/duplication.
- Maintain NULL semantics in WHERE/ON conditions.
- Do not add/remove ORDER BY unless proven safe.
- Preserve LIMIT semantics — no result set expansion.

## 3. OPTIMIZER GAPS

### COMMA_JOIN_WEAKNESS
PostgreSQL's comma joins confuse cardinality estimation. Opportunity: Convert to explicit JOINs with pre-filtered CTEs.

**pg_dimension_prefetch_star** [W1] — HIGH reliability, 1 win, avg 3.32x
Pre-filter selective dimensions into CTEs; convert comma joins to explicit JOIN syntax.
- ✓ Q080: 3.32x — date, item, promotion CTEs + explicit joins

**pg_date_cte_explicit_join** [W1] — HIGH reliability, 1 win, avg 2.28x
Materialize selective dimension filters into CTEs AND convert comma joins to explicit JOIN.
- ✓ Q099: 2.28x — date_dim CTE + explicit join syntax

### CORRELATED_SUBQUERY_PARALYSIS
Correlated scalar subqueries re-execute per outer row. Opportunity: Decorate via MATERIALIZED CTEs.

**inline_decorrelate_materialized** [W3] — HIGH reliability, 1 win, avg 461.92x
Decompose correlated scalar subquery into 3 MATERIALIZED CTEs: dimension filter, fact filter, per-key aggregate.
- Guard: Use AS MATERIALIZED on CTEs to prevent inlining.

**early_filter_decorrelate** [W3] — LOW reliability, 1 win, avg 1.13x
Push dimension filters into CTE definitions; pre-compute thresholds in separate CTEs.
- Guard: Limited benefit; use only when early filtering is significant.

### CROSS_CTE_PREDICATE_BLINDNESS
Same fact+dimension scan appears multiple times. Opportunity: Materialize once and reuse.

**pg_self_join_decomposition** [W1] — HIGH reliability, 1 win, avg 3.93x
Materialize identical fact+dimension scan once as CTE; derive aggregates from single result.
- ✓ Q065: 3.93x — store_sales+date_dim scanned once, reused

### NON_EQUI_JOIN_INPUT_BLINDNESS
Expensive non-equi joins lack pre-filtering. Opportunity: Reduce both sides via MATERIALIZED CTEs.

**pg_materialized_dimension_fact_prefilter** [W2] — HIGH reliability, 1 win, avg 2.68x
Stage reduction: shrink BOTH dimension and fact tables via MATERIALIZED CTEs before non-equi join.
- ✓ Q072: 2.68x — fact CTE removed 70% rows, dimension CTEs tiny

## 4. STANDALONE TRANSFORMS
*(none)*

## 5. GLOBAL GUARD RAILS
1. Never split OR conditions into UNION ALL — caused 0.21x on Q085.
2. Never convert EXISTS to IN/NOT IN or materializing CTEs — caused 0.50x on Q069.
3. Never restructure INNER JOIN orders — optimizer handles reordering.
4. Avoid CTEs for small dimension lookups (<10K rows) — index-only scans are faster.
5. Avoid CTEs that block parallel execution — materialization is single-threaded.
6. Use AS MATERIALIZED when decorrelating — prevents optimizer inlining.
7. Skip transforms if baseline <100ms — overhead exceeds savings.
8. Preserve efficient existing CTEs — don't decompose working patterns.
9. Verify NULL semantics in NOT IN conversions — can block hash anti-joins.
10. Maintain ROLLUP/window pushdown — CTEs can prevent optimizations.