# PostgreSQL Rewrite Playbook
# 31 gold wins + 21 improved + 7 regressions | DSB SF10

## HOW TO USE THIS DOCUMENT

Work in phase order. Each phase changes the plan shape — re-evaluate later phases after each.

  Phase 1: Reduce scan volume (P1, P6, P7) — always first. Every optimization benefits from smaller input.
  Phase 2: Eliminate redundant work (P2, P3)
  Phase 3: Fix structural inefficiencies (P4, P5)

Before choosing any strategy, scan the explain plan for:
- Row count profile: monotonically decreasing = healthy. Flat then sharp drop = pushback opportunity.
- Join types: hash join = good. Nested loop on large table = decorrelation candidate.
- Repeated tables: same table N times = consolidation (P3).
- CTE materialization: large CTE + small post-filter = pushback. Use AS MATERIALIZED when needed.
- Bitmap OR scan: indexed OR already optimized — do NOT split to UNION.
- Parallel workers: active parallelism — avoid CTE fence that blocks parallel execution.
- Index-only scan on dimension: small dimension already efficient — CTE wrapper may hurt.
- EXISTS/NOT EXISTS: uses semi-join early termination — NEVER materialize.

## ENGINE STRENGTHS — do NOT rewrite these patterns

1. **BITMAP_OR_SCAN**: Multi-branch ORs on indexed columns handled via bitmap combination in one scan. Splitting ORs to UNION ALL is lethal (0.21x Q085 V1).
2. **EXISTS semi-join**: Uses early termination. Converting to materializing CTEs caused 0.50x Q069 V1, 0.75x Q069_i2. **Never materialize EXISTS.**
3. **INNER JOIN reordering**: Freely reorders INNER JOINs by selectivity estimates. Do NOT manually restructure INNER JOIN order.
4. **Index-only scan**: Reads only index when covering all requested columns. Small dimension lookups may not need CTEs.
5. **Parallel query execution**: Large scans and aggregations parallelized across workers. CTEs block parallelism (materialization is single-threaded).
6. **JIT compilation**: JIT-compiles complex expressions for long-running queries (>100ms).

## CORRECTNESS RULES

- Preserve exact row count — no filtering or duplication.
- Maintain NULL semantics in WHERE/ON conditions.
- Do not add/remove ORDER BY unless proven safe.
- Preserve LIMIT semantics — no result set expansion.
- NOT IN with NULLs blocks hash anti-joins — preserve EXISTS form.

## GLOBAL GUARDS (check always, before any rewrite)

1. OR conditions on indexed columns → never split to UNION ALL (0.21x Q085)
2. EXISTS/NOT EXISTS → never materialize into CTEs (0.50x Q069, 0.75x Q069_i2)
3. INNER JOIN order → never restructure (optimizer handles reordering)
4. Small dimensions (< 10K rows) → index-only scan may be faster than CTE
5. Baseline < 100ms → skip CTE-based rewrites (overhead exceeds savings)
6. CTEs block parallel execution — only use when benefit outweighs parallelism loss
7. Use AS MATERIALIZED when CTE must not be inlined (decorrelation, shared scans)
8. Preserve efficient existing CTEs — don't decompose working patterns
9. Verify NULL semantics in NOT IN conversions
10. ROLLUP/window in same query → CTE may prevent pushdown optimizations
11. Never inline a large UNION CTE — re-execution multiplied per reference (0.16x Q075)
12. Max 2 cascading fact-table CTE chains — deeper chains block parallelism

---

## PATHOLOGIES

### P1: Comma join confusing cardinality estimation [Phase 1 — LOW RISK]

  Gap: COMMA_JOIN_WEAKNESS — PostgreSQL's planner uses cross-product estimation
  for comma-separated joins in the FROM clause. Without explicit JOIN syntax, the
  planner lacks the join-key hint that enables hash-join probing with filtered
  dimension tables. This manifests as poor row estimates on intermediate joins,
  leading to nested-loop plans on large fact tables.

  The fix has two parts: (1) convert comma joins to explicit INNER JOIN syntax,
  and (2) pre-filter selective dimensions into MATERIALIZED CTEs to create tiny
  hash probe tables. Both are required — the CTE alone can hurt, but CTE +
  explicit JOINs together enable optimal hash join planning.

  Signal: hash/nested-loop join with poor row estimates in EXPLAIN, large
  intermediate results. SQL shows FROM t1, t2, t3 WHERE t1.key = t2.key
  AND ... (comma joins, no explicit JOIN).

  Decision gates:
  - Structural: multiple tables in comma-separated FROM with equi-join predicates
  - Selectivity: dimension filters available (date range, state, category)
  - Fact table: 1-2 fact tables only (3+ → join order lock)
  - CTE count: max 3-4 dimension CTEs (avoid over-materialization)
  - Stop: if all JOINs already explicit → skip to P6/P7

  Transform selection (lightest sufficient):
  - Date filter + star schema → pg_date_cte_explicit_join (4 wins, 2.1x avg)
  - Multiple dimension filters → pg_dimension_prefetch_star (3 wins, 2.8x avg)
  - Complex multi-join → explicit_join_materialized (2 wins, 5.9x avg)

  Ordering: apply first — reduces fact table scan before other optimizations.
  Composition: combines well with P2 (decorrelation) and P6 (date consolidation).
  After applying: re-evaluate P4 (non-equi inputs now smaller).

  Wins: Q083 8.56x, Q025 3.10x, Q099 2.50x, Q023 1.83x
  Improved: Q080 1.42x
  Regressions: Q058_i1 0.88x (explicit join overhead on simple query)

### P2: Correlated subquery executing per outer row [Phase 2 — HIGHEST IMPACT]

  Gap: CORRELATED_SUBQUERY_PARALYSIS — PostgreSQL cannot decorrelate correlated
  aggregate subqueries into GROUP BY + hash join. It falls back to nested-loop
  re-execution, scanning the inner relation once per outer row. For N outer rows
  and M inner rows, cost is O(N × M) instead of O(N + M).

  This is the single most impactful pathology on PostgreSQL. It accounts for 9
  of 31 wins including the three largest speedups (8044x, 1465x, 439x). The
  extreme wins occur when the correlated subquery causes a timeout — the original
  query never finishes, but the decorrelated version completes in milliseconds.

  The fix: extract the correlated aggregate into a MATERIALIZED CTE with GROUP BY
  on the correlation key, then JOIN back. Use AS MATERIALIZED to prevent the
  optimizer from inlining the CTE back into a correlated form.

  Signal: nested loop in EXPLAIN, inner side re-executes aggregate per outer row.
  If EXPLAIN shows hash join on correlation key → already decorrelated → STOP.
  SQL signal: WHERE col > (SELECT AGG(...) FROM ... WHERE outer.key = inner.key)

  Decision gates:
  - Structural: correlated scalar subquery with aggregate (AVG, SUM, COUNT)
  - EXPLAIN: nested loop with inner re-execution (NOT hash join)
  - NOT EXISTS: NEVER decorrelate EXISTS/NOT EXISTS (destroys semi-join, 0.50x Q069)
  - Shared scan: if inner and outer scan same table → extract common scan to shared CTE
  - CTE keyword: ALWAYS use AS MATERIALIZED (prevents optimizer re-correlating)
  - Multi-fact: 1-2 fact tables safe, 3+ → STOP (0.51x Q054)

  Transform selection (lightest sufficient):
  - Simple avg comparison → inline_decorrelate_materialized (3 wins, avg 500x)
  - Multiple correlation keys → decorrelate (8 wins, avg 3.2x)
  - Inner = outer table → shared scan + decorrelate (2 wins, avg 7000x)

  Ordering: apply after P1 (smaller inputs make decorrelation cheaper).
  Composition: almost always combined with P1 (comma join conversion).
  After applying: re-evaluate P3 (decorrelated CTEs may now be reusable).

  Wins: Q092 8044x, Q032 1465x, Q081 439x, Q001 27.80x, Q083 8.56x,
        Q001_i1 7.99x, Q065 2.05x, Q065_i1 1.90x, Q030 1.86x
  Improved: Q058 1.49x, Q014_i2 1.12x
  Regressions: Q054 0.51x (multi-fact join lock), Q069_i2 0.75x (EXISTS materialized)

### P3: Same fact+dimension scan repeated across subquery boundaries [Phase 2 — ZERO REGRESSIONS]

  Gap: CROSS_CTE_PREDICATE_BLINDNESS — PostgreSQL cannot detect that N subqueries
  all scan the same fact table with identical joins and filters. Each subquery is
  an independent plan unit with no Common Subexpression Elimination across query
  boundaries. This includes self-join patterns where the same aggregation is
  computed at different granularities.

  Two fix strategies: (1) Materialize identical scan once as CTE, derive
  aggregates from single result. (2) Consolidate multiple channel scans
  (store/catalog/web) into single UNION ALL scan with CASE-based pivoting.

  Signal: identical scan subtrees appearing 2+ times in EXPLAIN with similar
  costs. SQL signal: same fact table joined to same dimensions in multiple
  subqueries, or self-join with different GROUP BY granularity.

  Decision gates:
  - Structural: 2+ subqueries scanning same fact table with identical filters
  - Aggregation: COUNT/SUM/AVG/MIN/MAX only (not STDDEV/PERCENTILE)
  - Self-join: if query joins CTE to itself → consolidate into single CTE
  - Channel pattern: 3 channel scans (store/catalog/web) → single_pass_aggregation

  Transform selection:
  - Multi-channel INTERSECT-like → single_pass_aggregation (1 win, 1.98x)
  - Year-over-year self-join → self_join_pivot (1 win, 1.79x)

  Ordering: apply after P1/P2 — reduced inputs make consolidation cheaper.
  After applying: P4 benefits from smaller materialized inputs.

  Wins: Q014 1.98x, Q031 1.79x
  Regressions: none observed

### P4: Non-equi join without prefiltering [Phase 3 — ZERO REGRESSIONS]

  Gap: NON_EQUI_JOIN_INPUT_BLINDNESS — PostgreSQL handles non-equi joins
  (BETWEEN, <, >) via nested-loop or hash join with recheck, but cannot push
  dimension filters past the non-equi join boundary. Both sides of the non-equi
  join receive full unfiltered input, making the join O(N × M) on large tables.

  Fix: shrink BOTH sides via MATERIALIZED CTEs before the non-equi join.
  Pre-filter dimensions (date, demographics, household) into small CTEs, then
  join fact table with pre-filtered dimensions to reduce cardinality before the
  non-equi join.

  Signal: expensive non-equi join (BETWEEN, <, >) in EXPLAIN with large inputs.
  SQL signal: JOIN ... ON a.col BETWEEN b.low AND b.high, neither side filtered.

  Decision gates:
  - Structural: non-equi join predicate (BETWEEN, range comparison)
  - Cardinality: both join inputs > 10K rows
  - Dimension filters: at least one side has selective dimension filter available
  - Fact side: reducible by pre-joining with filtered dimensions

  Transform selection:
  - Multiple dimension filters → pg_materialized_dimension_fact_prefilter (1 win, 12.07x)

  Ordering: apply after P1 (explicit join syntax) and P6 (date CTE).
  After applying: non-equi join now operates on pre-filtered inputs.

  Wins: Q072 12.07x
  Regressions: none observed

### P5: Set operation materializing full result sets [Phase 3 — CAUTION]

  Gap: SET_OPERATION_MATERIALIZATION — INTERSECT and EXCEPT are implemented via
  full materialization + sort/hash comparison. For EXISTS (positive set test)
  this destroys semi-join early termination. For NOT EXISTS (negative set test)
  the planner uses hash-anti-join which is efficient, but correlated set
  operations re-execute per outer row.

  Two opposite fixes depending on direction:
  (a) INTERSECT → EXISTS: replace set materialization with semi-join early
      termination. PostgreSQL's EXISTS uses index + early termination.
  (b) Correlated EXISTS/NOT EXISTS on large sets → MATERIALIZED CTE + LEFT JOIN
      + IS NULL: pre-compute distinct customer sets once, then hash join for
      set difference. Only when 3+ channel checks (store, web, catalog).

  Signal: INTERSECT/EXCEPT between large result sets in EXPLAIN.
  SQL signal: EXISTS subquery correlated to outer with fact+date scan inside.

  Decision gates:
  - INTERSECT with 10K+ rows → convert to EXISTS (P5a)
  - Correlated NOT EXISTS on 3+ channels → materialize channel sets (P5b)
  - Simple EXISTS (single channel) → KEEP EXISTS (semi-join is optimal)
  - NOT EXISTS already using hash anti-join in EXPLAIN → STOP
  - CAUTION: materializing simple EXISTS destroys semi-join (0.75x Q069_i2)

  Transform selection:
  - INTERSECT → intersect_to_exists (1 win, 1.78x)
  - Multi-channel EXISTS/NOT EXISTS → set_operation_materialization (1 win, 17.48x)

  Ordering: apply after P1 (explicit joins for channel CTEs).
  Composition: P5a (INTERSECT→EXISTS) is standalone; P5b combines with P1/P6.
  After applying: check P3 if set operations were the only repeated-scan source.

  Wins: Q069 17.48x, Q038 1.78x
  Regressions: Q069_i2 0.75x (over-materialized date CTE in EXISTS path)

### P6: Multiple date_dim aliases with overlapping filters [Phase 1 — HIGHEST RELIABILITY]

  Gap: DATE_DIM_REDUNDANCY — PostgreSQL cannot detect that N references to
  date_dim with overlapping year/month filters select the same rows. Each alias
  is an independent scan. On star schemas with 3 date_dim instances (sold,
  returned, shipped), the optimizer scans date_dim 3 times with similar predicates.

  Fix: consolidate overlapping date filters into a single CTE (all_dates) that
  selects the union of needed date_sk values, then join each fact table reference
  to the shared CTE with specific MOY conditions.

  Signal: 2+ date_dim aliases in FROM with similar year/month_seq/moy predicates.
  SQL signal: d1.d_year = 1999 AND d2.d_year = 1999 AND d3.d_year = 1999.

  Decision gates:
  - Structural: 2+ date_dim instances with overlapping date predicates
  - Selectivity: date filter selects < 1% of date_dim (always true for year+month)
  - Combine with: explicit JOIN conversion when comma joins present

  Transform selection:
  - 2-3 date aliases, same year → date_consolidation (1 win, 3.10x)
  - Single date filter, star schema → date_cte_isolate (3 wins + 7 improved)

  Ordering: apply first (Phase 1) — date CTE is the smallest, most reliable transform.
  Composition: always combine with P1 (explicit join syntax).
  After applying: fact table scans reduced, all downstream pathologies benefit.

  Wins: Q025 3.10x, Q025_i1 2.23x, Q010 2.00x
  Improved: Q102 1.26x, Q080_i2 1.22x, Q091 1.18x, Q050_i2 1.10x,
            Q018 1.07x, Q072_i1 1.07x, Q094_i2 1.07x
  Regressions: none observed

### P7: Multi-dimension prefetch for star-schema aggregation [Phase 1 — CAUTION]

  Gap: DIMENSION_FILTER_PUSHDOWN_FAILURE — when multiple selective dimension
  filters exist (item category, store state, customer demographics), the planner
  may not apply them early enough. Pre-filtering dimensions into small CTEs and
  joining them to the fact table reduces cardinality before expensive aggregation.

  Fix: create MATERIALIZED CTEs for each selective dimension, then join fact table
  to all filtered dimensions using explicit INNER JOIN syntax.

  Signal: large fact table scan followed by late dimension filter in EXPLAIN.
  SQL signal: star schema with 3+ dimension filters in WHERE clause.

  Decision gates:
  - Structural: star schema with 3+ selective dimension filters
  - Dimension selectivity: each dimension filter selects < 10% of dimension table
  - Fact table: single fact table, NOT self-join or multi-fact
  - Stop: if query has self-join pattern → use P3 instead (0.25x Q031_i1)
  - Stop: if query has multi-fact join → dimension prefetch locks join order (0.51x Q054)

  Transform selection:
  - 2-3 dimensions → pg_dimension_prefetch_star (2 wins, 2.5x avg)
  - Mixed dimensions + date → multi_dimension_prefetch (1 win, 2.50x)
  - Fact + date + non-equi → combine with P4 (pg_materialized_dimension_fact_prefilter)

  Ordering: apply with P1/P6 (all Phase 1 optimizations together).
  CAUTION: do NOT apply to self-join or multi-fact queries.

  Wins: Q099 2.50x, Q064 2.12x, Q023 1.83x
  Improved: Q094 1.25x, Q084 1.10x, Q040 1.09x
  Regressions: Q031_i1 0.25x (self-join), Q054 0.51x (multi-fact)

### NO MATCH

  Record: which pathologies checked, which gates failed.
  Nearest miss: closest pathology + why it didn't qualify.
  Features present: structural features for future pattern discovery.
  → Workers get: broad gold example set, analyst's manual reasoning.

---

## SAFETY RANKING

| Rank | Pathology | Regr. | Worst | Action |
|------|-----------|-------|-------|--------|
| 1 | P6: Date CTE isolation | 0 | — | Always fix (zero regressions) |
| 2 | P3: Repeated scans | 0 | — | Always fix (verify agg type) |
| 3 | P4: Non-equi prefilter | 0 | — | Always fix |
| 4 | P1: Comma join + CTE | 1 | 0.88x | Fix when comma joins present |
| 5 | P5: Set operation | 1 | 0.75x | Check direction (INTERSECT vs EXISTS) |
| 6 | P2: Correlated subquery | 2 | 0.51x | Check EXPLAIN, never on EXISTS |
| 7 | P7: Multi-dim prefetch | 2 | 0.25x | Star schema only, not self-join |

## VERIFICATION CHECKLIST

Before finalizing any rewrite:
- [ ] AS MATERIALIZED used on all decorrelation CTEs (prevents re-correlation)
- [ ] EXISTS/NOT EXISTS still uses EXISTS (not materialized into CTE)
- [ ] OR conditions on indexed columns still intact (not split to UNION)
- [ ] Comma joins converted to explicit INNER JOIN
- [ ] Parallel execution not blocked by unnecessary CTE materialization
- [ ] No orphaned CTEs (every CTE referenced downstream)
- [ ] NULL semantics preserved in NOT IN conversions
- [ ] Row counts decrease monotonically through CTE chain
- [ ] Max 2 cascading fact-table CTE chains
- [ ] Rewrite doesn't match any REGRESSION REGISTRY pattern

## PRUNING GUIDE

Skip pathologies the plan rules out:

| Plan shows | Skip |
|---|---|
| No comma joins (all explicit JOINs) | P1 (comma join fix) |
| No nested loops on large tables | P2 (decorrelation) |
| Each table appears once | P3 (repeated scans) |
| No non-equi joins (BETWEEN, <, >) | P4 (non-equi prefilter) |
| No INTERSECT/EXCEPT and no correlated multi-channel EXISTS | P5 (set operation) |
| Single date_dim reference | P6 (date consolidation) |
| No GROUP BY or only 1 dimension filter | P7 (multi-dim prefetch) |
| Baseline < 100ms | ALL CTE-based transforms |
| Bitmap OR scan present | OR→UNION rewrites |
| Parallel workers active + query fast | CTE-heavy transforms |

## REGRESSION REGISTRY

| Severity | Query | Transform | Result | Root cause |
|----------|-------|-----------|--------|------------|
| CATASTROPHIC | Q075_i1 | cte_inlining | 0.16x | Inlined large UNION CTE → 6 fact scans re-executed 2x each |
| SEVERE | Q101_i1 | multi_dim_prefetch | 0.15x | CTEs blocked date-predicate pushdown on 90-day interval join |
| SEVERE | Q031_i1 | dimension_prefetch | 0.25x | Applied star-schema pattern to 6-way self-join → parallelism destroyed |
| MAJOR | Q075_i2 | cte_materialization | 0.30x | Multi-scan CTE overhead similar to Q075_i1 |
| MAJOR | Q054_i1 | early_fact_filtering | 0.51x | Disabled nestloop too aggressively + DISTINCT forced hash spill |
| MAJOR | Q069_i2 | date_cte_prefetch | 0.75x | Over-materialized date CTE in EXISTS path → destroyed semi-join |
| MODERATE | Q058_i1 | explicit_join | 0.88x | Explicit join conversion overhead exceeded benefit on simple query |
