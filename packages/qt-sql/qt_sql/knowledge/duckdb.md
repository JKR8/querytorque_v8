# DuckDB Rewrite Playbook
# TPC-DS SF1–SF10 field intelligence

## ENGINE STRENGTHS — do NOT rewrite

1. **Predicate pushdown**: filter inside scan node → leave it.
2. **Same-column OR**: handled natively in one scan. Splitting = lethal (0.23x observed).
3. **Hash join selection**: sound for 2–4 tables. Reduce inputs, not order.
4. **CTE inlining**: single-ref CTEs inlined automatically (zero overhead).
5. **Columnar projection**: only referenced columns read.
6. **Parallel aggregation**: scans and aggregations parallelized across threads.
7. **EXISTS semi-join**: early termination. **Never materialize** (0.14x observed).

## GLOBAL GUARDS

1. EXISTS/NOT EXISTS → never materialize (0.14x, 0.54x — semi-join destroyed)
2. Same-column OR → never split to UNION (0.23x, 0.59x — native OR handling)
3. Baseline < 100ms → skip CTE-based rewrites (overhead exceeds savings)
4. 3+ fact table joins → do not pre-materialize facts (locks join order)
5. Every CTE MUST have a WHERE clause (0.85x observed)
6. No orphaned CTEs — remove original after splitting (0.49x, 0.68x — double materialization)
7. No cross-joining 3+ dimension CTEs (0.0076x — Cartesian product)
8. Max 2 cascading fact-table CTE chains (0.78x observed)
9. Convert comma joins to explicit JOIN...ON
10. NOT EXISTS → NOT IN breaks with NULLs — preserve EXISTS form

---

## DOCUMENTED CASES

Cases ordered by safety (zero-regression cases first, then by decreasing risk).

**P0: Predicate Chain Pushback** (SMALLEST SET FIRST) — ~35% of wins

| Aspect | Detail |
|---|---|
| Detect | Row counts flat through CTE chain, sharp drop at late filter. 2+ stage CTE chain + late predicate with columns available earlier. |
| Gates | Filter ratio >5:1 strong, 2:1–5:1 moderate if baseline >200ms, <2:1 skip. 1 fact = safe, 2 = careful, 3+ = STOP (0.50x). ROLLUP/WINDOW downstream: CAUTION (0.85x). CTE already filtered on this predicate: skip (0.71x). |
| Treatments | date_cte_isolate (12 wins, 1.34x avg), prefetch_fact_join (4 wins, 1.89x avg), multi_dimension_prefetch (3 wins, 1.55x avg), multi_date_range_cte (3 wins, 1.42x avg), shared_dimension_multi_channel (1 win, 1.40x), self_join_decomposition (1 win, 4.76x) |
| Failures | 0.0076x (3 dim CTE cross-join → Cartesian), 0.50x (3-fact join lock), 0.85x (ROLLUP blocked), 0.71x (over-decomposed) |

**P1: Repeated Scans of Same Table** (DON'T REPEAT WORK) — ZERO REGRESSIONS

| Aspect | Detail |
|---|---|
| Detect | N separate SEQ_SCAN nodes on same table, identical joins, different bucket filters. |
| Gates | Identical join structure across all subqueries, max 8 branches, COUNT/SUM/AVG/MIN/MAX only (not STDDEV/VARIANCE/PERCENTILE). |
| Treatments | single_pass_aggregation (8 wins, 1.88x avg), channel_bitmap_aggregation (1 win, 6.24x) |
| Failures | None observed. |

**P3: Aggregation After Join** (MINIMIZE ROWS TOUCHED) — ZERO REGRESSIONS

| Aspect | Detail |
|---|---|
| Detect | GROUP BY input rows >> distinct keys, aggregate node sits after join. |
| Gates | GROUP BY keys ⊇ join keys (CORRECTNESS). Reconstruct AVG from SUM/COUNT when pre-aggregating for ROLLUP. |
| Treatments | aggregate_pushdown, star_join_prefetch. 3 wins (1.3x–42.9x, avg 15.3x). |
| Failures | None observed. |

**P5: LEFT JOIN + NULL-Eliminating WHERE** (ARM THE OPTIMIZER) — ZERO REGRESSIONS

| Aspect | Detail |
|---|---|
| Detect | LEFT JOIN + WHERE on right-table column (proves right non-null). |
| Gates | No CASE WHEN IS NULL / COALESCE on right-table column. |
| Treatments | inner_join_conversion. 2 wins (1.9x–3.4x, avg 2.7x). |
| Failures | None observed. |

**P6: INTERSECT Materializing Both Sides** (SETS OVER LOOPS) — ZERO REGRESSIONS

| Aspect | Detail |
|---|---|
| Detect | INTERSECT between 10K+ row result sets. |
| Gates | Both sides >1K rows. |
| Treatments | intersect_to_exists, multi_intersect_exists_cte. 1 win (2.7x). Related: semi_join_exists (1.67x). |
| Failures | None observed. |

**P8: Window Functions in CTEs Before Join** (MINIMIZE ROWS TOUCHED) — ZERO REGRESSIONS

| Aspect | Detail |
|---|---|
| Detect | N WINDOW nodes inside CTEs, same ORDER BY key, CTEs then joined. |
| Gates | Not LAG/LEAD (depends on pre-join row order), not ROWS BETWEEN with specific frame. SUM() OVER() naturally skips NULLs. |
| Treatments | deferred_window_aggregation. 1 win (1.4x). |
| Failures | None observed. |

**P7: Self-Joined CTE Materialized for All Values** (SMALLEST SET FIRST)

| Aspect | Detail |
|---|---|
| Detect | CTE joined to itself with different WHERE per arm (e.g., period=1 vs period=2). |
| Gates | 2–4 discriminator values, MUST remove original combined CTE after splitting. |
| Treatments | self_join_decomposition (1 win, 4.76x), union_cte_split (2 wins, 1.72x avg), rollup_to_union_windowing (1 win, 2.47x) |
| Failures | 0.49x (orphaned CTE → double materialization), 0.68x (orphaned variant) |

**P2: Correlated Subquery Nested Loop** (SETS OVER LOOPS)

| Aspect | Detail |
|---|---|
| Detect | Nested loop, inner re-executes aggregate per outer row. If EXPLAIN shows hash join on correlation key → already decorrelated → STOP. |
| Gates | NEVER decorrelate EXISTS (0.34x, 0.14x — semi-join destroyed). Preserve ALL WHERE filters. Check if Phase 1 reduced outer to <1000 rows (nested loop may be fast enough). |
| Treatments | decorrelate (3 wins, 2.45x avg), composite_decorrelate_union (1 win, 2.42x) |
| Failures | 0.34x (semi-join destroyed), 0.71x (already decorrelated) |

**P9: Shared Subexpression Executed Multiple Times** (DON'T REPEAT WORK)

| Aspect | Detail |
|---|---|
| Detect | Identical subtrees with identical costs scanning same tables. HARD STOP: EXISTS/NOT EXISTS → NEVER materialize (0.14x). |
| Gates | NOT EXISTS, subquery is expensive (joins/aggregates), CTE must have WHERE. |
| Treatments | materialize_cte. 1 win (1.4x). |
| Failures | 0.14x (EXISTS materialized → semi-join destroyed), 0.54x (correlated EXISTS pairs broken) |

**P4: Cross-Column OR Forcing Full Scan** (MINIMIZE ROWS TOUCHED) — HIGHEST VARIANCE

| Aspect | Detail |
|---|---|
| Detect | Single scan, OR across DIFFERENT columns, 70%+ rows discarded. CRITICAL: same column in all OR arms → STOP (engine handles natively). |
| Gates | Max 3 branches, cross-column only, no self-join, no nested OR (multiplicative expansion). |
| Treatments | or_to_union. 4 wins (1.4x–6.3x, avg 3.1x). |
| Failures | 0.23x (9 branches from nested OR), 0.41x (nested OR expansion), 0.59x (same-col split), 0.51x (self-join re-executed per branch) |

---

## PRUNING GUIDE

| Plan shows | Skip |
|---|---|
| No nested loops | P2 (decorrelation) |
| Each table appears once | P1 (repeated scans) |
| No LEFT JOIN | P5 (INNER conversion) |
| No OR predicates | P4 (OR decomposition) |
| No GROUP BY | P3 (aggregate pushdown) |
| No WINDOW/OVER | P8 (deferred window) |
| No INTERSECT/EXCEPT | P6 (set rewrite) |
| Baseline < 50ms | ALL CTE-based transforms |
| Row counts monotonically decreasing | P0 (predicate pushback) |

## REGRESSION REGISTRY

| Severity | Transform | Result | Root cause |
|----------|-----------|--------|------------|
| CATASTROPHIC | dimension_cte_isolate | 0.0076x | Cross-joined 3 dim CTEs: Cartesian product |
| CATASTROPHIC | materialize_cte | 0.14x | Materialized EXISTS → semi-join destroyed |
| SEVERE | or_to_union | 0.23x | 9 UNION branches from nested OR |
| SEVERE | decorrelate | 0.34x | LEFT JOIN was already semi-join |
| MAJOR | union_cte_split | 0.49x | Original CTE kept → double materialization |
| MAJOR | date_cte_isolate | 0.50x | 3-way fact join locked optimizer order |
| MAJOR | or_to_union | 0.51x | Self-join re-executed per branch |
| MAJOR | semantic_rewrite | 0.54x | Correlated EXISTS pairs broken |
| MODERATE | or_to_union | 0.59x | Split same-column OR |
| MODERATE | union_cte_split | 0.68x | Original CTE kept alongside split |
| MODERATE | decorrelate | 0.71x | Pre-aggregated ALL stores when only subset needed |
| MODERATE | prefetch_fact_join | 0.78x | 3rd cascading CTE chain |
| MINOR | multi_dimension_prefetch | 0.77x | Forced suboptimal join order |
| MINOR | date_cte_isolate | 0.85x | CTE blocked ROLLUP pushdown |
