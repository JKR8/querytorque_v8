# Duckdb Rewrite Playbook
# duckdb_tpcds field intelligence

<!-- Auto-generated from blackboard (101 queries, 92 wins, 1.87x avg). Review and polish before use. -->

## ENGINE STRENGTHS — do NOT rewrite

1. **Intra Scan Predicate Pushdown**: Pushes WHERE filters directly into SEQ_SCAN. Single-table predicates are applied at scan time, zero overhead. If EXPLAIN shows the filter inside the scan node, do not create a CTE to push it.
2. **Same Column Or**: OR on the SAME column handled in a single scan with range checks. Never split same-column ORs into UNION ALL. 0.59x and 0.23x observed.
3. **Hash Join Selection**: Selects hash joins automatically. Join ordering sound for 2-4 tables. Focus on reducing join inputs, not reordering joins.
4. **Cte Inlining**: Single-reference CTEs inlined automatically. Multi-referenced CTEs may be materialized. Single-ref CTEs are free — use for clarity. CTE-based strategies are low-cost on DuckDB.
5. **Columnar Projection**: Only referenced columns read. Unused columns have zero I/O cost. When creating pre-filter CTEs, only SELECT columns downstream needs.
6. **Parallel Aggregation**: Scans and aggregations parallelized across threads. PERFECT_HASH_GROUP_BY efficient. Restructuring simple aggregation queries rarely helps unless reducing input rows.
7. **Exists Semi Join**: EXISTS/NOT EXISTS uses semi-join with early termination. NEVER materialize EXISTS into CTEs. 0.14x and 0.54x from this mistake.

## GLOBAL GUARDS

1. If EXPLAIN shows the filter inside the scan node, do not create a CTE to push it.
2. Never split same-column ORs into UNION ALL. 0.59x and 0.23x observed.
3. NEVER materialize EXISTS into CTEs. 0.14x and 0.54x from this mistake.
4. decorrelate caused 0.01x regression — review gates before applying
5. or_to_union caused 0.02x regression — review gates before applying
6. intersect_to_exists caused 0.02x regression — review gates before applying
7. dimension_cte_isolate caused 0.02x regression — review gates before applying
8. date_cte_isolate caused 0.02x regression — review gates before applying
9. prefetch_fact_join caused 0.02x regression — review gates before applying
10. multi_dimension_prefetch caused 0.02x regression — review gates before applying
11. single_pass_aggregation caused 0.02x regression — review gates before applying
12. multi_date_range_cte caused 0.07x regression — review gates before applying
13. pushdown caused 0.09x regression — review gates before applying
14. materialize_cte caused 0.09x regression — review gates before applying
15. early_filter caused 0.09x regression — review gates before applying
16. union_cte_split caused 0.35x regression — review gates before applying
17. Baseline < 100ms → skip CTE-based rewrites (overhead exceeds savings)
18. Convert comma joins to explicit JOIN...ON
19. Every CTE MUST have a WHERE clause

---

## DOCUMENTED CASES

Cases ordered by safety (zero-regression cases first, then by decreasing risk).

**P0: Predicate Chain Pushback** (SMALLEST SET FIRST) — ~327 wins

| Aspect | Detail |
|---|---|
| Detect | Row counts flat through CTE chain, sharp drop at late filter. 2+ stage CTE chain + late predicate with columns available earlier. |
| Gates | Filter ratio >5:1 strong, 2:1-5:1 moderate if baseline >200ms. 1 fact = safe, 2 = careful, 3+ = STOP. ROLLUP/WINDOW downstream: CAUTION. CTE already filtered: skip. |
| Treatments | date_cte_isolate (125 wins, 1.80x avg), pushdown (42 wins, 1.72x avg), prefetch_fact_join (41 wins, 1.73x avg), dimension_cte_isolate (38 wins, 1.52x avg), early_filter (36 wins, 1.49x avg), multi_dimension_prefetch (35 wins, 1.59x avg), multi_date_range_cte (10 wins, 1.83x avg) |
| Failures | 0.02x (date_cte_isolate on q61), 0.09x (pushdown on q4), 0.02x (prefetch_fact_join on q37), 0.02x (dimension_cte_isolate on q61), 0.09x (early_filter on q94), 0.02x (multi_dimension_prefetch on q91), 0.06x (multi_date_range_cte on q25) |

**P1: Correlated Subquery Nested Loop** (SETS OVER LOOPS) — ~45 wins

| Aspect | Detail |
|---|---|
| Detect | Nested loop, inner re-executes aggregate per outer row. If hash join on correlation key → already decorrelated → STOP. |
| Gates | NEVER decorrelate EXISTS (0.34x, 0.14x — semi-join destroyed). Preserve ALL WHERE filters. Check if outer <1000 rows after Phase 1. |
| Treatments | decorrelate (45 wins, 1.50x avg) |
| Failures | 0.01x (decorrelate on q16) |

**P2: Cross-Column OR Forcing Full Scan** (MINIMIZE ROWS TOUCHED) — ~39 wins

| Aspect | Detail |
|---|---|
| Detect | Single scan, OR across DIFFERENT columns, 70%+ rows discarded. CRITICAL: same column in all OR arms → STOP. |
| Gates | Max 3 branches, cross-column only, no self-join, no nested OR (multiplicative expansion). |
| Treatments | or_to_union (39 wins, 2.14x avg) |
| Failures | 0.02x (or_to_union on q16) |

**P3: Repeated Scans of Same Table** (DON'T REPEAT WORK) — ~21 wins

| Aspect | Detail |
|---|---|
| Detect | N separate SEQ_SCAN nodes on same table, identical joins, different bucket filters. |
| Gates | Identical join structure across all subqueries, max 8 branches, COUNT/SUM/AVG/MIN/MAX only (not STDDEV/VARIANCE/PERCENTILE). |
| Treatments | single_pass_aggregation (21 wins, 2.10x avg) |
| Failures | 0.02x (single_pass_aggregation on q91) |

**P4: Self-Joined CTE Materialized for All Values** (SMALLEST SET FIRST) — ~7 wins

| Aspect | Detail |
|---|---|
| Detect | CTE joined to itself with different WHERE per arm (e.g., period=1 vs period=2). |
| Gates | 2-4 discriminator values, MUST remove original combined CTE after splitting. |
| Treatments | union_cte_split (7 wins, 1.99x avg) |
| Failures | 0.35x (union_cte_split on q41) |

---

## PRUNING GUIDE

| Plan shows | Skip |
|---|---|
| Row counts monotonically decreasing | P0 (predicate pushback) |
| No nested loops | P1 (decorrelation) |
| No OR predicates | P2 (OR decomposition) |
| Each table appears once | P3 (repeated scans) |
| No self-joined CTEs | P4 (self-join decomp) |
| Baseline < 50ms | ALL CTE-based transforms |

## REGRESSION REGISTRY

| Severity | Transform | Result | Query | Strategy |
|----------|-----------|--------|-------|----------|
| CATASTROPHIC | decorrelate | 0.01x | q16 |  |
| CATASTROPHIC | or_to_union | 0.02x | q16 | novel_structural_transform |
| CATASTROPHIC | intersect_to_exists | 0.02x | q16 | novel_structural_transform |
| CATASTROPHIC | dimension_cte_isolate | 0.02x | q61 | moderate_dimension_isolation |
| CATASTROPHIC | date_cte_isolate | 0.02x | q61 | moderate_dimension_isolation |
| CATASTROPHIC | date_cte_isolate | 0.02x | q37 |  |
| CATASTROPHIC | prefetch_fact_join | 0.02x | q37 |  |
| CATASTROPHIC | prefetch_fact_join | 0.02x | q91 | aggressive_prefetch_restructuring |
| CATASTROPHIC | multi_dimension_prefetch | 0.02x | q91 | aggressive_prefetch_restructuring |
| CATASTROPHIC | single_pass_aggregation | 0.02x | q91 | aggressive_prefetch_restructuring |
| CATASTROPHIC | or_to_union | 0.04x | q73 |  |
| CATASTROPHIC | decorrelate | 0.04x | q16 |  |
| CATASTROPHIC | date_cte_isolate | 0.05x | q3 |  |
| CATASTROPHIC | dimension_cte_isolate | 0.05x | q3 |  |
| CATASTROPHIC | decorrelate | 0.06x | q63 |  |
| CATASTROPHIC | date_cte_isolate | 0.06x | q63 |  |
| CATASTROPHIC | multi_dimension_prefetch | 0.06x | q63 |  |
| CATASTROPHIC | date_cte_isolate | 0.06x | q4 |  |
| CATASTROPHIC | prefetch_fact_join | 0.07x | q90 | aggressive_prefetch_restructure |
| CATASTROPHIC | single_pass_aggregation | 0.07x | q90 | aggressive_prefetch_restructure |
| CATASTROPHIC | multi_date_range_cte | 0.07x | q90 | aggressive_prefetch_restructure |
| CATASTROPHIC | decorrelate | 0.08x | q94 |  |
| CATASTROPHIC | date_cte_isolate | 0.09x | q4 |  |
| CATASTROPHIC | pushdown | 0.09x | q4 |  |
| CATASTROPHIC | single_pass_aggregation | 0.09x | q11 |  |
| CATASTROPHIC | date_cte_isolate | 0.09x | q11 |  |
| CATASTROPHIC | prefetch_fact_join | 0.09x | q11 |  |
| CATASTROPHIC | decorrelate | 0.09x | q94 |  |
| CATASTROPHIC | date_cte_isolate | 0.09x | q94 |  |
| CATASTROPHIC | materialize_cte | 0.09x | q94 |  |
| CATASTROPHIC | early_filter | 0.09x | q94 |  |
| CATASTROPHIC | decorrelate | 0.11x | q90 |  |
| CATASTROPHIC | single_pass_aggregation | 0.11x | q90 |  |
| CATASTROPHIC | dimension_cte_isolate | 0.11x | q90 |  |
| CATASTROPHIC | date_cte_isolate | 0.12x | q7 |  |
| CATASTROPHIC | pushdown | 0.12x | q7 |  |
| CATASTROPHIC | or_to_union | 0.12x | q7 |  |
| CATASTROPHIC | materialize_cte | 0.17x | q73 |  |
| CATASTROPHIC | multi_dimension_prefetch | 0.18x | q96 |  |
| CATASTROPHIC | decorrelate | 0.18x | q47 |  |
| CATASTROPHIC | dimension_cte_isolate | 0.18x | q47 |  |
| CATASTROPHIC | prefetch_fact_join | 0.18x | q47 |  |
| CATASTROPHIC | materialize_cte | 0.19x | q94 |  |
| SEVERE | materialize_cte | 0.23x | q31 |  |
| SEVERE | pushdown | 0.23x | q31 |  |
| SEVERE | or_to_union | 0.26x | q48 | novel_or_restructuring |
| SEVERE | intersect_to_exists | 0.26x | q48 | novel_or_restructuring |
| SEVERE | decorrelate | 0.27x | q32 |  |
| SEVERE | pushdown | 0.27x | q32 |  |
| SEVERE | early_filter | 0.27x | q32 |  |
| SEVERE | or_to_union | 0.27x | q32 |  |
| SEVERE | pushdown | 0.27x | q25 | conservative_filter_pushdown |
| SEVERE | early_filter | 0.27x | q25 | conservative_filter_pushdown |
| SEVERE | materialize_cte | 0.27x | q25 | conservative_filter_pushdown |
| SEVERE | prefetch_fact_join | 0.27x | q9 | aggressive_fact_prefetch |
| SEVERE | multi_dimension_prefetch | 0.27x | q9 | aggressive_fact_prefetch |
| SEVERE | multi_date_range_cte | 0.27x | q9 | aggressive_fact_prefetch |
| SEVERE | multi_date_range_cte | 0.28x | q25 | moderate_date_dimension_isolation |
| SEVERE | dimension_cte_isolate | 0.28x | q25 | moderate_date_dimension_isolation |
| SEVERE | pushdown | 0.28x | q85 |  |
| SEVERE | or_to_union | 0.29x | q34 |  |
| SEVERE | prefetch_fact_join | 0.30x | q29 |  |
| SEVERE | date_cte_isolate | 0.31x | q48 |  |
| SEVERE | or_to_union | 0.31x | q48 |  |
| SEVERE | materialize_cte | 0.31x | q27 |  |
| SEVERE | dimension_cte_isolate | 0.31x | q27 |  |
| SEVERE | pushdown | 0.32x | q25 |  |
| SEVERE | multi_date_range_cte | 0.32x | q25 |  |
| SEVERE | early_filter | 0.33x | q34 |  |
| SEVERE | or_to_union | 0.33x | q34 |  |
| SEVERE | date_cte_isolate | 0.33x | q54 |  |
| SEVERE | decorrelate | 0.33x | q54 |  |
| SEVERE | early_filter | 0.33x | q54 |  |
| SEVERE | date_cte_isolate | 0.34x | q85 |  |
| SEVERE | or_to_union | 0.34x | q85 |  |
| SEVERE | or_to_union | 0.34x | q26 |  |
| SEVERE | dimension_cte_isolate | 0.35x | q7 |  |
| SEVERE | prefetch_fact_join | 0.35x | q7 |  |
| SEVERE | or_to_union | 0.35x | q41 | novel_structural_transform |
| SEVERE | intersect_to_exists | 0.35x | q41 | novel_structural_transform |
| SEVERE | union_cte_split | 0.35x | q41 | novel_structural_transform |
| SEVERE | decorrelate | 0.37x | q94 | novel_structural_transforms |
| SEVERE | intersect_to_exists | 0.37x | q94 | novel_structural_transforms |
| SEVERE | or_to_union | 0.37x | q12 | novel_structural_transforms |
| SEVERE | intersect_to_exists | 0.37x | q12 | novel_structural_transforms |
| SEVERE | intersect_to_exists | 0.37x | q64 | novel_structural_transforms |
| SEVERE | or_to_union | 0.37x | q64 | novel_structural_transforms |
| SEVERE | decorrelate | 0.37x | q64 | novel_structural_transforms |
| SEVERE | dimension_cte_isolate | 0.39x | q9 | moderate_dimension_prefilter |
| SEVERE | early_filter | 0.39x | q9 | moderate_dimension_prefilter |
| SEVERE | materialize_cte | 0.39x | q9 |  |
| SEVERE | or_to_union | 0.39x | q9 | novel_structural_transform |
| SEVERE | intersect_to_exists | 0.39x | q9 | novel_structural_transform |
| SEVERE | or_to_union | 0.39x | q78 |  |
| SEVERE | single_pass_aggregation | 0.39x | q78 |  |
| SEVERE | date_cte_isolate | 0.39x | q78 |  |
| SEVERE | prefetch_fact_join | 0.39x | q78 |  |
| MAJOR | or_to_union | 0.40x | q61 |  |
| MAJOR | date_cte_isolate | 0.41x | q26 |  |
| MAJOR | multi_dimension_prefetch | 0.41x | q26 |  |
| MAJOR | decorrelate | 0.41x | q9 |  |
| MAJOR | materialize_cte | 0.41x | q9 |  |
| MAJOR | date_cte_isolate | 0.41x | q40 |  |
| MAJOR | prefetch_fact_join | 0.41x | q72 | aggressive_prefetch_restructuring |
| MAJOR | multi_dimension_prefetch | 0.41x | q72 | aggressive_prefetch_restructuring |
| MAJOR | decorrelate | 0.42x | q9 |  |
| MAJOR | pushdown | 0.42x | q9 | conservative_single_pass |
| MAJOR | single_pass_aggregation | 0.42x | q9 | conservative_single_pass |
| MAJOR | materialize_cte | 0.42x | q9 | conservative_single_pass |
| MAJOR | or_to_union | 0.43x | q36 | novel_structural_transform |
| MAJOR | intersect_to_exists | 0.43x | q36 | novel_structural_transform |
| MAJOR | union_cte_split | 0.43x | q36 | novel_structural_transform |
| MAJOR | multi_dimension_prefetch | 0.43x | q25 |  |
| MAJOR | prefetch_fact_join | 0.44x | q32 | aggressive_prefetch_restructuring |
| MAJOR | multi_dimension_prefetch | 0.44x | q32 | aggressive_prefetch_restructuring |
| MAJOR | single_pass_aggregation | 0.44x | q32 | aggressive_prefetch_restructuring |
| MAJOR | or_to_union | 0.45x | q82 | structural_query_transform |
| MAJOR | intersect_to_exists | 0.45x | q82 | structural_query_transform |
| MAJOR | union_cte_split | 0.45x | q11 | novel_structural_transform |
| MAJOR | intersect_to_exists | 0.45x | q11 | novel_structural_transform |
| MAJOR | or_to_union | 0.45x | q11 | novel_structural_transform |
| MAJOR | pushdown | 0.46x | q31 |  |
| MAJOR | or_to_union | 0.46x | q10 |  |
| MAJOR | or_to_union | 0.47x | q34 |  |
| MAJOR | or_to_union | 0.47x | q78 | novel_structural_transform |
| MAJOR | intersect_to_exists | 0.47x | q78 | novel_structural_transform |
| MAJOR | decorrelate | 0.47x | q78 | novel_structural_transform |
| MAJOR | or_to_union | 0.48x | q46 |  |
| MAJOR | pushdown | 0.48x | q31 |  |
| MAJOR | date_cte_isolate | 0.49x | q16 |  |
| MAJOR | multi_dimension_prefetch | 0.49x | q16 |  |
| MAJOR | date_cte_isolate | 0.49x | q10 |  |
| MAJOR | or_to_union | 0.49x | q10 |  |
| MAJOR | materialize_cte | 0.49x | q9 |  |
| MAJOR | or_to_union | 0.51x | q63 | novel_structural_transformation |
| MAJOR | intersect_to_exists | 0.51x | q63 | novel_structural_transformation |
| MAJOR | or_to_union | 0.51x | q53 |  |
| MAJOR | or_to_union | 0.51x | q26 | novel_structural_transform |
| MAJOR | intersect_to_exists | 0.51x | q26 | novel_structural_transform |
| MAJOR | pushdown | 0.52x | q7 |  |
| MAJOR | early_filter | 0.52x | q7 |  |
| MAJOR | or_to_union | 0.52x | q7 |  |
| MAJOR | date_cte_isolate | 0.52x | q64 |  |
| MAJOR | pushdown | 0.52x | q64 |  |
| MAJOR | date_cte_isolate | 0.53x | q33 |  |
| MAJOR | date_cte_isolate | 0.54x | q48 |  |
| MAJOR | prefetch_fact_join | 0.54x | q48 |  |
| MAJOR | date_cte_isolate | 0.54x | q29 |  |
| MAJOR | pushdown | 0.54x | q61 | conservative_pushdown_filtering |
| MAJOR | early_filter | 0.54x | q61 | conservative_pushdown_filtering |
| MAJOR | materialize_cte | 0.54x | q61 | conservative_pushdown_filtering |
| MAJOR | or_to_union | 0.55x | q26 |  |
| MAJOR | prefetch_fact_join | 0.55x | q12 |  |
| MAJOR | date_cte_isolate | 0.56x | q26 |  |
| MAJOR | or_to_union | 0.56x | q26 |  |
| MAJOR | pushdown | 0.57x | q75 |  |
| MAJOR | or_to_union | 0.57x | q97 | novel_structural_transform |
| MAJOR | intersect_to_exists | 0.57x | q97 | novel_structural_transform |
| MAJOR | multi_date_range_cte | 0.57x | q25 |  |
| MAJOR | date_cte_isolate | 0.57x | q25 |  |
| MAJOR | materialize_cte | 0.57x | q25 |  |
| MAJOR | or_to_union | 0.57x | q19 | novel_structural_transform |
| MAJOR | intersect_to_exists | 0.57x | q19 | novel_structural_transform |
| MAJOR | single_pass_aggregation | 0.57x | q28 | aggressive_single_pass |
| MAJOR | prefetch_fact_join | 0.57x | q28 | aggressive_single_pass |
| MAJOR | decorrelate | 0.58x | q92 |  |
| MAJOR | date_cte_isolate | 0.58x | q1 |  |
| MAJOR | prefetch_fact_join | 0.58x | q1 |  |
| MAJOR | early_filter | 0.59x | q3 |  |
| MAJOR | or_to_union | 0.59x | q61 | novel_structural_transform |
| MAJOR | intersect_to_exists | 0.59x | q61 | novel_structural_transform |
| MAJOR | early_filter | 0.59x | q47 |  |
| MAJOR | or_to_union | 0.59x | q47 |  |
| MAJOR | decorrelate | 0.60x | q28 |  |
| MAJOR | single_pass_aggregation | 0.60x | q28 |  |
| MAJOR | materialize_cte | 0.60x | q28 |  |
| MAJOR | or_to_union | 0.60x | q91 |  |
| MODERATE | pushdown | 0.60x | q89 |  |
| MODERATE | or_to_union | 0.60x | q89 |  |
| MODERATE | date_cte_isolate | 0.60x | q97 |  |
| MODERATE | date_cte_isolate | 0.60x | q73 |  |
| MODERATE | pushdown | 0.60x | q73 |  |
| MODERATE | early_filter | 0.60x | q73 |  |
| MODERATE | or_to_union | 0.60x | q73 |  |
| MODERATE | dimension_cte_isolate | 0.61x | q32 | moderate_dimension_isolation |
| MODERATE | date_cte_isolate | 0.61x | q32 | moderate_dimension_isolation |
| MODERATE | or_to_union | 0.61x | q45 |  |
| MODERATE | date_cte_isolate | 0.62x | q91 |  |
| MODERATE | multi_dimension_prefetch | 0.62x | q91 |  |
| MODERATE | date_cte_isolate | 0.62x | q21 |  |
| MODERATE | early_filter | 0.62x | q21 |  |
| MODERATE | date_cte_isolate | 0.62x | q13 |  |
| MODERATE | pushdown | 0.62x | q13 |  |
| MODERATE | or_to_union | 0.62x | q13 |  |
| MODERATE | date_cte_isolate | 0.64x | q47 |  |
| MODERATE | or_to_union | 0.64x | q47 |  |
| MODERATE | date_cte_isolate | 0.64x | q40 |  |
| MODERATE | date_cte_isolate | 0.64x | q91 |  |
| MODERATE | or_to_union | 0.64x | q91 |  |
| MODERATE | date_cte_isolate | 0.65x | q43 |  |
| MODERATE | multi_dimension_prefetch | 0.65x | q69 |  |
| MODERATE | date_cte_isolate | 0.65x | q69 |  |
| MODERATE | materialize_cte | 0.65x | q69 |  |
| MODERATE | early_filter | 0.65x | q43 |  |
| MODERATE | date_cte_isolate | 0.65x | q25 |  |
| MODERATE | date_cte_isolate | 0.66x | q78 |  |
| MODERATE | date_cte_isolate | 0.66x | q72 |  |
| MODERATE | or_to_union | 0.66x | q91 |  |
| MODERATE | decorrelate | 0.66x | q28 |  |
| MODERATE | single_pass_aggregation | 0.66x | q28 |  |
| MODERATE | single_pass_aggregation | 0.67x | q31 |  |
| MODERATE | prefetch_fact_join | 0.67x | q31 |  |
| MODERATE | early_filter | 0.67x | q32 | conservative_predicate_pushdown |
| MODERATE | pushdown | 0.67x | q32 | conservative_predicate_pushdown |
| MODERATE | materialize_cte | 0.67x | q32 | conservative_predicate_pushdown |
| MODERATE | or_to_union | 0.67x | q68 | novel_structural_transform |
| MODERATE | decorrelate | 0.67x | q68 | novel_structural_transform |
| MODERATE | or_to_union | 0.68x | q53 | novel_structural_transform |
| MODERATE | intersect_to_exists | 0.68x | q53 | novel_structural_transform |
| MODERATE | or_to_union | 0.68x | q45 |  |
| MODERATE | decorrelate | 0.68x | q44 |  |
| MODERATE | dimension_cte_isolate | 0.68x | q44 |  |
| MODERATE | date_cte_isolate | 0.69x | q21 |  |
| MODERATE | date_cte_isolate | 0.69x | q42 |  |
| MODERATE | date_cte_isolate | 0.70x | q80 |  |
| MODERATE | prefetch_fact_join | 0.70x | q94 |  |
| MODERATE | date_cte_isolate | 0.71x | q20 |  |
| MODERATE | dimension_cte_isolate | 0.71x | q20 |  |
| MODERATE | or_to_union | 0.71x | q91 |  |
| MODERATE | dimension_cte_isolate | 0.71x | q69 |  |
| MODERATE | prefetch_fact_join | 0.71x | q69 |  |
| MODERATE | or_to_union | 0.71x | q46 |  |
| MODERATE | or_to_union | 0.72x | q57 | novel_structural_transform |
| MODERATE | intersect_to_exists | 0.72x | q57 | novel_structural_transform |
| MODERATE | single_pass_aggregation | 0.72x | q57 | novel_structural_transform |
| MODERATE | date_cte_isolate | 0.72x | q38 |  |
| MODERATE | decorrelate | 0.73x | q76 |  |
| MODERATE | date_cte_isolate | 0.73x | q76 |  |
| MODERATE | dimension_cte_isolate | 0.73x | q76 |  |
| MODERATE | early_filter | 0.73x | q42 |  |
| MODERATE | intersect_to_exists | 0.74x | q95 | novel_structural_transform |
| MODERATE | or_to_union | 0.74x | q95 | novel_structural_transform |
| MODERATE | or_to_union | 0.75x | q47 | novel_structural_transform |
| MODERATE | intersect_to_exists | 0.75x | q47 | novel_structural_transform |
| MODERATE | date_cte_isolate | 0.75x | q82 |  |
| MODERATE | unknown | 0.75x | q70 |  |
| MODERATE | date_cte_isolate | 0.75x | q79 |  |
| MODERATE | early_filter | 0.76x | q31 | conservative_predicate_pushdown |
| MODERATE | pushdown | 0.76x | q31 | conservative_predicate_pushdown |
| MODERATE | materialize_cte | 0.76x | q31 | conservative_predicate_pushdown |
| MODERATE | materialize_cte | 0.76x | q88 |  |
| MODERATE | prefetch_fact_join | 0.76x | q45 | aggressive_prefetch_restructuring |
| MODERATE | multi_dimension_prefetch | 0.76x | q45 | aggressive_prefetch_restructuring |
| MODERATE | single_pass_aggregation | 0.76x | q45 | aggressive_prefetch_restructuring |
| MODERATE | date_cte_isolate | 0.76x | q20 |  |
| MODERATE | dimension_cte_isolate | 0.77x | q91 |  |
| MODERATE | prefetch_fact_join | 0.77x | q91 |  |
| MODERATE | date_cte_isolate | 0.77x | q52 |  |
| MODERATE | prefetch_fact_join | 0.77x | q93 | aggressive_prefetch_restructure |
| MODERATE | multi_dimension_prefetch | 0.77x | q93 | aggressive_prefetch_restructure |
| MODERATE | single_pass_aggregation | 0.77x | q93 | aggressive_prefetch_restructure |
| MODERATE | multi_dimension_prefetch | 0.77x | q31 | aggressive_prefetch_restructuring |
| MODERATE | prefetch_fact_join | 0.77x | q31 | aggressive_prefetch_restructuring |
| MODERATE | single_pass_aggregation | 0.77x | q31 | aggressive_prefetch_restructuring |
| MODERATE | date_cte_isolate | 0.77x | q31 | moderate_dimension_isolation |
| MODERATE | dimension_cte_isolate | 0.77x | q31 | moderate_dimension_isolation |
| MODERATE | prefetch_fact_join | 0.77x | q51 |  |
| MODERATE | date_cte_isolate | 0.77x | q71 |  |
| MODERATE | pushdown | 0.77x | q71 |  |
| MODERATE | or_to_union | 0.77x | q71 |  |
| MODERATE | date_cte_isolate | 0.78x | q20 |  |
| MODERATE | pushdown | 0.78x | q39 |  |
| MODERATE | date_cte_isolate | 0.78x | q53 |  |
| MODERATE | or_to_union | 0.78x | q53 |  |
| MODERATE | date_cte_isolate | 0.78x | q82 |  |
| MODERATE | or_to_union | 0.78x | q28 | novel_or_to_union_transform |
| MODERATE | intersect_to_exists | 0.78x | q28 | novel_or_to_union_transform |
| MODERATE | prefetch_fact_join | 0.78x | q74 |  |
| MODERATE | pushdown | 0.79x | q85 |  |
| MODERATE | early_filter | 0.79x | q85 |  |
| MODERATE | or_to_union | 0.79x | q85 |  |
| MODERATE | intersect_to_exists | 0.79x | q87 | novel_set_transform |
| MODERATE | or_to_union | 0.79x | q87 | novel_set_transform |
| MODERATE | prefetch_fact_join | 0.80x | q8 | aggressive_multi_cte_prefetch |
| MODERATE | multi_dimension_prefetch | 0.80x | q8 | aggressive_multi_cte_prefetch |
| MODERATE | single_pass_aggregation | 0.80x | q8 | aggressive_multi_cte_prefetch |
| MODERATE | or_to_union | 0.80x | q93 | novel_structural_transform |
| MODERATE | intersect_to_exists | 0.80x | q93 | novel_structural_transform |
| MINOR | or_to_union | 0.80x | q68 |  |
| MINOR | early_filter | 0.80x | q93 |  |
| MINOR | date_cte_isolate | 0.80x | q20 |  |
| MINOR | date_cte_isolate | 0.80x | q25 |  |
| MINOR | prefetch_fact_join | 0.81x | q31 |  |
| MINOR | early_filter | 0.81x | q93 | conservative_early_filter_restructure |
| MINOR | pushdown | 0.81x | q93 | conservative_early_filter_restructure |
| MINOR | materialize_cte | 0.81x | q93 | conservative_early_filter_restructure |
| MINOR | single_pass_aggregation | 0.81x | q92 | novel_structural_transform |
| MINOR | or_to_union | 0.81x | q92 | novel_structural_transform |
| MINOR | materialize_cte | 0.81x | q33 |  |
| MINOR | early_filter | 0.82x | q47 |  |
| MINOR | or_to_union | 0.82x | q47 |  |
| MINOR | dimension_cte_isolate | 0.82x | q91 | moderate_dimension_isolation |
| MINOR | date_cte_isolate | 0.82x | q91 | moderate_dimension_isolation |
| MINOR | early_filter | 0.82x | q68 |  |
| MINOR | date_cte_isolate | 0.82x | q82 |  |
| MINOR | early_filter | 0.82x | q82 |  |
| MINOR | or_to_union | 0.82x | q18 | novel_structural_transforms |
| MINOR | intersect_to_exists | 0.82x | q18 | novel_structural_transforms |
| MINOR | date_cte_isolate | 0.82x | q99 |  |
| MINOR | date_cte_isolate | 0.82x | q29 |  |
| MINOR | date_cte_isolate | 0.82x | q45 |  |
| MINOR | or_to_union | 0.82x | q45 |  |
| MINOR | date_cte_isolate | 0.83x | q8 |  |
| MINOR | or_to_union | 0.83x | q13 | novel_structural_transform |
| MINOR | intersect_to_exists | 0.83x | q13 | novel_structural_transform |
| MINOR | single_pass_aggregation | 0.83x | q13 | novel_structural_transform |
| MINOR | early_filter | 0.83x | q91 | conservative_early_filtering |
| MINOR | pushdown | 0.83x | q91 | conservative_early_filtering |
| MINOR | materialize_cte | 0.83x | q91 | conservative_early_filtering |
| MINOR | or_to_union | 0.84x | q45 | novel_or_transform_decorrelate |
| MINOR | decorrelate | 0.84x | q45 | novel_or_transform_decorrelate |
| MINOR | intersect_to_exists | 0.84x | q45 | novel_or_transform_decorrelate |
| MINOR | early_filter | 0.84x | q37 |  |
| MINOR | or_to_union | 0.84x | q68 |  |
| MINOR | date_cte_isolate | 0.84x | q87 | moderate_date_shared_dimension_cte |
| MINOR | materialize_cte | 0.84x | q87 | moderate_date_shared_dimension_cte |
| MINOR | early_filter | 0.84x | q57 |  |
| MINOR | or_to_union | 0.84x | q57 |  |
| MINOR | date_cte_isolate | 0.84x | q20 |  |
| MINOR | or_to_union | 0.84x | q48 |  |
| MINOR | prefetch_fact_join | 0.85x | q13 | aggressive_multi_cte_restructuring |
| MINOR | multi_date_range_cte | 0.85x | q13 | aggressive_multi_cte_restructuring |
| MINOR | date_cte_isolate | 0.85x | q79 |  |
| MINOR | pushdown | 0.85x | q79 |  |
| MINOR | or_to_union | 0.85x | q79 |  |
| MINOR | date_cte_isolate | 0.86x | q33 |  |
| MINOR | materialize_cte | 0.86x | q33 |  |
| MINOR | date_cte_isolate | 0.86x | q38 |  |
| MINOR | pushdown | 0.86x | q57 |  |
| MINOR | early_filter | 0.86x | q57 |  |
| MINOR | or_to_union | 0.86x | q57 |  |
| MINOR | early_filter | 0.86x | q87 |  |
| MINOR | prefetch_fact_join | 0.87x | q29 | aggressive_multi_stage_prefetch |
| MINOR | multi_dimension_prefetch | 0.87x | q29 | aggressive_multi_stage_prefetch |
| MINOR | or_to_union | 0.87x | q40 | Novel Structural Transformation |
| MINOR | intersect_to_exists | 0.87x | q40 | Novel Structural Transformation |
| MINOR | early_filter | 0.87x | q24 |  |
| MINOR | multi_date_range_cte | 0.87x | q92 | aggressive_prefetch_restructure |
| MINOR | prefetch_fact_join | 0.87x | q92 | aggressive_prefetch_restructure |
| MINOR | date_cte_isolate | 0.87x | q10 |  |
| MINOR | multi_dimension_prefetch | 0.87x | q10 |  |
| MINOR | intersect_to_exists | 0.87x | q8 | novel_structural_transform |
| MINOR | or_to_union | 0.87x | q8 | novel_structural_transform |
| MINOR | date_cte_isolate | 0.88x | q49 |  |
| MINOR | date_cte_isolate | 0.89x | q8 |  |
| MINOR | prefetch_fact_join | 0.89x | q47 | aggressive_prefetch_restructure |
| MINOR | multi_dimension_prefetch | 0.89x | q47 | aggressive_prefetch_restructure |
| MINOR | date_cte_isolate | 0.89x | q87 |  |
| MINOR | prefetch_fact_join | 0.89x | q87 |  |
| MINOR | prefetch_fact_join | 0.89x | q23b | aggressive_prefetch_consolidation |
| MINOR | multi_dimension_prefetch | 0.89x | q23b | aggressive_prefetch_consolidation |
| MINOR | single_pass_aggregation | 0.89x | q23b | aggressive_prefetch_consolidation |
| MINOR | date_cte_isolate | 0.90x | q97 |  |
| MINOR | decorrelate | 0.90x | q71 |  |
| MINOR | date_cte_isolate | 0.90x | q71 |  |
| MINOR | multi_dimension_prefetch | 0.90x | q71 |  |
