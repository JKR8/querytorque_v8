# PostgreSQL Query Rewrite Decision Tree

Distilled from 6 gold wins + 2 regressions across DSB SF10.
Cross-reference: `decisions.md` pathologies 1, 3, 8 for shared patterns with DuckDB.

## ENGINE STRENGTHS — do NOT rewrite these patterns

1. **BITMAP_OR_SCAN**: Multi-branch ORs on indexed columns handled via bitmap combination in one scan. Splitting ORs to UNION ALL is lethal (0.21x Q085).
2. **EXISTS semi-join**: Uses early termination. Converting to materializing CTEs caused 0.50x Q069. **Never materialize EXISTS.**
3. **INNER JOIN reordering**: Freely reorders INNER JOINs by selectivity estimates. Do NOT manually restructure INNER JOIN order.
4. **Index-only scan**: Reads only index when covering all requested columns. Small dimension lookups may not need CTEs.
5. **Parallel query execution**: Large scans and aggregations parallelized across workers. CTEs can block parallelism (materialization is single-threaded).
6. **JIT compilation**: JIT-compiles complex expressions for long-running queries.

## CORRECTNESS RULES

- Preserve exact row count — no filtering or duplication.
- Maintain NULL semantics in WHERE/ON conditions.
- Do not add/remove ORDER BY unless proven safe.
- Preserve LIMIT semantics — no result set expansion.
- NOT IN with NULLs blocks hash anti-joins — preserve EXISTS form.

## GLOBAL GUARDS (check always, before any rewrite)

1. OR conditions → never split to UNION ALL (0.21x Q085)
2. EXISTS/NOT EXISTS → never materialize into CTEs (0.50x Q069)
3. INNER JOIN order → never restructure (optimizer handles reordering)
4. Small dimensions (< 10K rows) → index-only scan may be faster than CTE
5. Baseline < 100ms → skip CTE-based rewrites (overhead exceeds savings)
6. CTEs block parallel execution — only use when benefit outweighs parallelism loss
7. Use AS MATERIALIZED when CTE must not be inlined (decorrelation, shared scans)
8. Preserve efficient existing CTEs — don't decompose working patterns
9. Verify NULL semantics in NOT IN conversions
10. ROLLUP/window in same query → CTE may prevent pushdown optimizations

## PATHOLOGY DETECTION (read explain plan, identify expensive nodes)

### P1: Comma join confusing cardinality estimation
  Explain signal: hash/nested-loop join with poor row estimates, large intermediate results
  SQL signal: FROM t1, t2, t3 WHERE t1.key = t2.key AND ... (comma joins, no explicit JOIN)
  Gap: COMMA_JOIN_WEAKNESS

  → DECISION: Convert to explicit JOIN syntax + pre-filter selective dimensions into MATERIALIZED CTEs
  → Gates: multiple tables in comma-separated FROM, selective dimension filters available
  → Expected: 2.3x–3.3x | Worst: no known regressions
  → Transforms: pg_dimension_prefetch_star, pg_date_cte_explicit_join
  → Workers get: Q080 (3.32x — date+item+promo CTEs + explicit joins),
    Q099 (2.28x — date_dim CTE + explicit join syntax)

### P2: Nested loop executing correlated subquery per outer row
  Explain signal: nested loop, inner side re-executes aggregate per outer row
  If EXPLAIN shows hash join on correlation key → optimizer already decorrelated → STOP
  SQL signal: WHERE col > (SELECT AGG(...) FROM ... WHERE outer.key = inner.key)
  Gap: CORRELATED_SUBQUERY_PARALYSIS

  → DECISION: Decompose into MATERIALIZED CTEs — dimension filter, fact filter, per-key aggregate
  → Gates: NOT EXISTS (semi-join destroyed), not already hash join in EXPLAIN,
    use AS MATERIALIZED to prevent optimizer inlining CTEs back
  → Expected: 1.1x–460x (timeout rescue) | Worst: no known regressions for this pattern
  → Transforms: inline_decorrelate_materialized, early_filter_decorrelate
  → Workers get: Q032 (461.92x — timeout rescue via 3 MATERIALIZED CTEs),
    Q001 (1.13x — early filter + decorrelate)

### P3: Same fact+dimension scan repeated across subquery boundaries
  Explain signal: identical scan subtrees appearing 2+ times in plan with similar costs
  SQL signal: same fact table joined to same dimensions in multiple subqueries or self-join
  Gap: CROSS_CTE_PREDICATE_BLINDNESS

  → DECISION: Materialize identical scan once as MATERIALIZED CTE, derive aggregates from single result
  → Gates: scans are truly identical (same tables, same joins, same filters),
    use AS MATERIALIZED to prevent inlining
  → Expected: 3.9x | Worst: no known regressions
  → Transforms: pg_self_join_decomposition
  → Workers get: Q065 (3.93x — store_sales+date_dim scanned once, reused for both aggregates)

### P4: Non-equi join without prefiltering
  Explain signal: expensive non-equi join (BETWEEN, <, >) with large inputs on both sides
  SQL signal: JOIN ... ON a.col BETWEEN b.low AND b.high, neither side pre-filtered
  Gap: NON_EQUI_JOIN_INPUT_BLINDNESS

  → DECISION: Shrink BOTH sides via MATERIALIZED CTEs before the non-equi join
  → Gates: dimension side filterable by WHERE, fact side reducible by pre-joining filtered dimensions
  → Expected: 2.7x | Worst: no known regressions
  → Transforms: pg_materialized_dimension_fact_prefilter
  → Workers get: Q072 (2.68x — fact CTE removed 70% rows, dimension CTEs tiny)

### NO MATCH
  Record: which pathologies checked, which gates failed
  Features present: structural features for future pattern discovery
  → Workers get: broad gold example set, analyst's manual reasoning

## SAFETY RANKING

| Rank | Pathology                | Regr. | Worst | Recommendation                |
|------|--------------------------|-------|-------|-------------------------------|
| 1    | P1: Comma join + CTE     | 0     | —     | Fix when comma joins present  |
| 2    | P2: Correlated subquery  | 0     | —     | Fix when nested loop visible  |
| 3    | P3: Repeated scan        | 0     | —     | Fix when identical subtrees   |
| 4    | P4: Non-equi join        | 0     | —     | Fix when BETWEEN join present |

All four PG pathologies have ZERO observed regressions. PG regressions come
from applying wrong patterns: OR→UNION (0.21x Q085) and EXISTS→CTE (0.50x
Q069). These are blocked by global guards 1 and 2.
