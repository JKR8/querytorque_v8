# DuckDB Rewrite Playbook
# 22 gold wins + 10 regressions | TPC-DS SF1–SF10

## HOW TO USE THIS DOCUMENT

Work in phase order. Each phase changes the plan shape — re-evaluate later phases after each.

  Phase 1: Reduce scan volume (P0) — always first. Every other optimization benefits from smaller input.
  Phase 2: Eliminate redundant work (P1, P3)
  Phase 3: Fix structural inefficiencies (P2, P4–P9)

Before choosing any strategy, scan the explain plan for:
- Row count profile: monotonically decreasing = healthy. Flat then sharp drop = pushback opportunity.
- Join types: hash join = good. Nested loop = decorrelation candidate.
- Repeated tables: same table N times = consolidation.
- CTE sizes: large materialization + small post-filter = pushback.
- Aggregation inputs: GROUP BY over millions with thousands of distinct keys = pushdown.
- LEFT JOIN + WHERE on right table = INNER conversion.
- INTERSECT/EXCEPT = EXISTS conversion.

## ENGINE STRENGTHS — do NOT rewrite

1. **Predicate pushdown**: filter inside scan node → leave it.
2. **Same-column OR**: handled natively in one scan. Splitting = lethal (0.23x Q13).
3. **Hash join selection**: sound for 2–4 tables. Reduce inputs, not order.
4. **CTE inlining**: single-ref CTEs inlined automatically (zero overhead).
5. **Columnar projection**: only referenced columns read.
6. **Parallel aggregation**: scans and aggregations parallelized across threads.
7. **EXISTS semi-join**: early termination. **Never materialize** (0.14x Q16).

## CORRECTNESS RULES

- Identical rows, columns, ordering as original.
- Copy ALL literals exactly (strings, numbers, dates).
- Every CTE must SELECT all columns referenced downstream.
- Never drop, rename, or reorder output columns.

## GLOBAL GUARDS

1. EXISTS/NOT EXISTS → never materialize (0.14x Q16, 0.54x Q95)
2. Same-column OR → never split to UNION (0.23x Q13, 0.59x Q90)
3. Baseline < 100ms → skip CTE-based rewrites (overhead exceeds savings)
4. 3+ fact table joins → do not pre-materialize facts (locks join order)
5. Every CTE MUST have a WHERE clause (0.85x Q67)
6. No orphaned CTEs — remove original after splitting (0.49x Q31, 0.68x Q74)
7. No cross-joining 3+ dimension CTEs (0.0076x Q80 — Cartesian product)
8. Max 2 cascading fact-table CTE chains (0.78x Q4)
9. Convert comma joins to explicit JOIN...ON
10. NOT EXISTS → NOT IN breaks with NULLs — preserve EXISTS form

---

## PATHOLOGIES

### P0: Predicate chain pushback [Phase 1 — always first, ~35% of wins]

  Gap: CROSS_CTE_PREDICATE_BLINDNESS — DuckDB plans each CTE independently.
  Predicates in the outer query or later CTEs cannot propagate backward into
  earlier CTE definitions. The CTE materializes blind to how its output will
  be consumed.

  This is the general case. date_cte_isolate, early_filter, prefetch_fact_join,
  multi_dimension_prefetch are all specific instances where the pushed predicate
  is a dimension filter. The principle applies to ANY selective predicate:
  dimension filters, HAVING thresholds, JOIN conditions, subquery results.
  The rule: find the most selective predicate, find the earliest CTE where it
  CAN apply, put it there.

  Signal: row counts stay flat through CTE chain stages then drop sharply at a
  late filter. Target state: monotonically decreasing rows through the chain.

  Decision gates:
  - Structural: 2+ stage CTE chain + late predicate with columns available earlier
  - Cardinality: filter ratio >5:1 strong, 2:1–5:1 moderate if baseline >200ms, <2:1 skip
  - Multi-fact: 1 fact = safe, 2 = careful, 3+ = STOP (0.50x Q25)
  - ROLLUP/WINDOW downstream: CAUTION (0.85x Q67)
  - CTE already filtered on this predicate: skip (0.71x Q1)

  Transform selection (lightest sufficient):
  - Single dim, ≤2 stages → date_cte_isolate (12 wins, 1.34x avg)
  - Single dim, ≥3 stages → prefetch_fact_join (4 wins, 1.89x avg)
  - Multiple dims → multi_dimension_prefetch (3 wins, 1.55x avg)
  - date_dim 3+ aliases → multi_date_range_cte (3 wins, 1.42x avg)
  - Multi-channel shared dims → shared_dimension_multi_channel (1 win, 1.40x)
  - CTE self-join with literal discriminators → self_join_decomposition (1 win, 4.76x)

  Ordering: push most selective predicate first. Selectivity compounds —
  once the first filter reduces 7M to 50K, everything downstream operates on 50K.
  Composition: often combines with aggregate_pushdown (P3) or decorrelation (P2).
  After applying: re-evaluate P1 (scans may now be small enough to skip),
  P2 (outer set may be small enough that nested loop is fine),
  P3 (pre-agg on smaller set may now be more valuable).

  Wins: Q6 4.00x, Q11 4.00x, Q39 4.76x, Q63 3.77x, Q93 2.97x, Q43 2.71x, Q29 2.35x, Q26 1.93x
  Regressions: Q80 0.0076x (dim cross-join), Q25 0.50x (3-fact), Q67 0.85x (ROLLUP), Q1 0.71x (over-decomposed)

### P1: Repeated scans of same table [Phase 2 — ZERO REGRESSIONS]

  Gap: REDUNDANT_SCAN_ELIMINATION — the optimizer cannot detect that N subqueries
  all scan the same table with the same joins. Each subquery is an independent plan
  unit with no Common Subexpression Elimination across boundaries.

  Signal: N separate SEQ_SCAN nodes on same table, identical joins, different bucket filters.
  Decision: consolidate to single scan with CASE WHEN / FILTER (WHERE ...).
  Gates: identical join structure across all subqueries, max 8 branches,
  COUNT/SUM/AVG/MIN/MAX only (not STDDEV/VARIANCE/PERCENTILE).

  Transforms: single_pass_aggregation (8 wins, 1.88x avg), channel_bitmap_aggregation (1 win, 6.24x)
  Wins: Q88 6.24x, Q9 4.47x, Q61 2.27x, Q32 1.61x, Q4 1.53x, Q90 1.47x

### P2: Correlated subquery nested loop [Phase 3]

  Gap: CORRELATED_SUBQUERY_PARALYSIS — the optimizer cannot decorrelate correlated
  aggregate subqueries into GROUP BY + hash join. It falls back to nested-loop
  re-execution instead of recognizing the equivalence.

  Signal: nested loop, inner re-executes aggregate per outer row.
  If EXPLAIN shows hash join on correlation key → already decorrelated → STOP.
  Decision: extract correlated aggregate into CTE with GROUP BY on correlation key, JOIN back.
  Gates: NEVER decorrelate EXISTS (0.34x Q93, 0.14x Q16), preserve ALL WHERE filters,
  check if Phase 1 reduced outer to <1000 rows (nested loop may be fast enough).

  Transforms: decorrelate (3 wins, 2.45x avg), composite_decorrelate_union (1 win, 2.42x)
  Wins: Q1 2.92x, Q35 2.42x
  Regressions: Q93 0.34x (semi-join destroyed), Q1 variant 0.71x (already decorrelated)

### P3: Aggregation after join — fan-out before GROUP BY [Phase 2 — ZERO REGRESSIONS]

  Gap: AGGREGATE_BELOW_JOIN_BLINDNESS — the optimizer cannot push GROUP BY below
  joins even when aggregation keys align with join keys. It always joins first
  (producing M rows), then aggregates (reducing to K groups, K << M).

  Signal: GROUP BY input rows >> distinct keys, aggregate node sits after join.
  Decision: pre-aggregate fact by join key BEFORE dimension join.
  Gates: GROUP BY keys ⊇ join keys (CORRECTNESS — wrong results if violated),
  reconstruct AVG from SUM/COUNT when pre-aggregating for ROLLUP.

  Transforms: aggregate_pushdown, star_join_prefetch
  Wins: Q22 42.90x (biggest win), Q65 1.80x, Q72 1.27x

### P4: Cross-column OR forcing full scan [Phase 3 — HIGHEST VARIANCE]

  Gap: CROSS_COLUMN_OR_DECOMPOSITION — the optimizer handles same-column OR
  efficiently (single scan range) but OR across different columns forces a full
  scan evaluating all conditions for every row.

  Signal: single scan, OR across DIFFERENT columns, 70%+ rows discarded.
  CRITICAL: same column in all OR arms → STOP (engine handles natively).
  Decision: split into UNION ALL branches + shared dim CTE.
  Gates: max 3 branches, cross-column only, no self-join, no nested OR (multiplicative expansion).

  Transforms: or_to_union
  Wins: Q15 3.17x, Q88 6.28x, Q10 1.49x, Q45 1.35x
  Regressions: Q13 0.23x (9 branches), Q48 0.41x (nested OR), Q90 0.59x (same-col), Q23 0.51x (self-join)

### P5: LEFT JOIN + NULL-eliminating WHERE [Phase 3 — ZERO REGRESSIONS]

  Gap: LEFT_JOIN_FILTER_ORDER_RIGIDITY — the optimizer cannot infer that WHERE on
  a right-table column makes LEFT JOIN semantically equivalent to INNER. LEFT JOIN
  also blocks join reordering (not commutative).

  Signal: LEFT JOIN + WHERE on right-table column (proves right non-null).
  Decision: convert to INNER JOIN, optionally pre-filter right table into CTE.
  Gate: no CASE WHEN IS NULL / COALESCE on right-table column.

  Transforms: inner_join_conversion
  Wins: Q93 3.44x, Q80 1.89x

### P6: INTERSECT materializing both sides [Phase 3 — ZERO REGRESSIONS]

  Gap: INTERSECT is implemented as set materialization + comparison. The optimizer
  doesn't recognize that EXISTS semi-join is algebraically equivalent and can
  short-circuit at first match per row.

  Signal: INTERSECT between 10K+ row result sets.
  Decision: replace with EXISTS semi-join.
  Gate: both sides >1K rows.
  Related: semi_join_exists — replace full JOIN with EXISTS when joined columns not in output (1.67x).

  Transforms: intersect_to_exists, multi_intersect_exists_cte
  Wins: Q14 2.72x

### P7: Self-joined CTE materialized for all values [Phase 3]

  Gap: UNION_CTE_SELF_JOIN_DECOMPOSITION + CROSS_CTE_PREDICATE_BLINDNESS — the
  optimizer materializes the CTE once for all values. Self-join discriminator
  filters cannot propagate backward into the CTE definition. Each arm post-filters
  the full materialized result instead of computing only its needed partition.

  Signal: CTE joined to itself with different WHERE per arm (e.g., period=1 vs period=2).
  Decision: split into per-partition CTEs, each embedding its discriminator.
  Gates: 2–4 discriminator values, MUST remove original combined CTE after splitting.

  Transforms: self_join_decomposition (1 win, 4.76x), union_cte_split (2 wins, 1.72x avg),
  rollup_to_union_windowing (1 win, 2.47x)
  Wins: Q39 4.76x, Q36 2.47x, Q74 1.57x
  Regressions: Q31 0.49x (orphaned CTE), Q74 0.68x (orphaned variant)

### P8: Window functions in CTEs before join [Phase 3 — ZERO REGRESSIONS]

  Gap: the optimizer cannot defer window computation past a join when
  partition/ordering is preserved. It computes the window in the CTE because
  that's where the SQL places it.

  Signal: N WINDOW nodes inside CTEs, same ORDER BY key, CTEs then joined.
  Decision: remove windows from CTEs, compute once on joined result.
  Gates: not LAG/LEAD (depends on pre-join row order), not ROWS BETWEEN with specific frame.
  Note: SUM() OVER() naturally skips NULLs — handles FULL OUTER JOIN gaps.

  Transforms: deferred_window_aggregation
  Wins: Q51 1.36x

### P9: Shared subexpression executed multiple times [Phase 3]

  Gap: the optimizer may not CSE identical subqueries across different query branches.
  When it doesn't, cost is N× what single execution would be.
  HARD STOP: EXISTS/NOT EXISTS → NEVER materialize (0.14x Q16). Semi-join
  short-circuit is destroyed by CTE materialization.

  Signal: identical subtrees with identical costs scanning same tables.
  Decision: extract shared subexpression into CTE.
  Gates: NOT EXISTS, subquery is expensive (joins/aggregates), CTE must have WHERE.

  Transforms: materialize_cte
  Wins: Q95 1.43x
  Regressions: Q16 0.14x (EXISTS materialized), Q95 0.54x (cardinality severed)

### NO MATCH

  Record: which pathologies checked, which gates failed.
  Nearest miss: closest pathology + why it didn't qualify.
  Features present: structural features for future pattern discovery.

---

## SAFETY RANKING

| Rank | Pathology | Regr. | Worst | Action |
|------|-----------|-------|-------|--------|
| 1 | P1: Repeated scans | 0 | — | Always fix |
| 2 | P3: Agg after join | 0 | — | Always fix (verify keys) |
| 3 | P5: LEFT→INNER | 0 | — | Always fix |
| 4 | P6: INTERSECT | 0 | — | Always fix |
| 5 | P8: Pre-join windows | 0 | — | Always fix |
| 6 | P7: Self-join CTE | 1 | 0.49x | Check orphan CTE |
| 7 | P0: Predicate pushback | 4 | 0.0076x | All gates must pass |
| 8 | P2: Correlated loop | 2 | 0.34x | Check EXPLAIN first |
| 9 | P9: Shared expr | 3 | 0.14x | Never on EXISTS |
| 10 | P4: Cross-col OR | 4 | 0.23x | Max 3, cross-column only |

## VERIFICATION CHECKLIST

Before finalizing any rewrite:
- [ ] Row counts decrease monotonically through CTE chain
- [ ] No orphaned CTEs (every CTE referenced downstream)
- [ ] No unfiltered CTEs (every CTE has WHERE)
- [ ] No cross-joined dimension CTEs (each dim joins to fact)
- [ ] EXISTS still uses EXISTS (not materialized)
- [ ] Same-column ORs still intact (not split)
- [ ] All original WHERE filters preserved in CTEs
- [ ] Max 2 cascading fact-table CTE chains
- [ ] Comma joins converted to explicit JOIN...ON
- [ ] Rewrite doesn't match any known regression pattern

## PRUNING GUIDE

Skip pathologies the plan rules out:

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

| Severity | Query | Transform | Result | Root cause |
|----------|-------|-----------|--------|------------|
| CATASTROPHIC | Q80 | dimension_cte_isolate | 0.0076x | Cross-joined 3 dim CTEs: Cartesian product |
| CATASTROPHIC | Q16 | materialize_cte | 0.14x | Materialized EXISTS → semi-join destroyed |
| SEVERE | Q13 | or_to_union | 0.23x | 9 UNION branches from nested OR |
| SEVERE | Q93 | decorrelate | 0.34x | LEFT JOIN was already semi-join |
| MAJOR | Q31 | union_cte_split | 0.49x | Original CTE kept → double materialization |
| MAJOR | Q25 | date_cte_isolate | 0.50x | 3-way fact join locked optimizer order |
| MAJOR | Q23 | or_to_union | 0.51x | Self-join re-executed per branch |
| MAJOR | Q95 | semantic_rewrite | 0.54x | Correlated EXISTS pairs broken |
| MODERATE | Q90 | or_to_union | 0.59x | Split same-column OR |
| MODERATE | Q74 | union_cte_split | 0.68x | Original CTE kept alongside split |
| MODERATE | Q1 | decorrelate | 0.71x | Pre-aggregated ALL stores when only SD needed |
| MODERATE | Q4 | prefetch_fact_join | 0.78x | 3rd cascading CTE chain |
| MINOR | Q72 | multi_dimension_prefetch | 0.77x | Forced suboptimal join order |
| MINOR | Q67 | date_cte_isolate | 0.85x | CTE blocked ROLLUP pushdown |
