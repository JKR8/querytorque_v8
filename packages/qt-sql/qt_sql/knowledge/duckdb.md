# DuckDB Rewrite Playbook
# TPC-DS SF1–SF10 field intelligence

## HOW TO USE THIS DOCUMENT

Work in phase order. Each phase changes the plan shape — re-evaluate later phases after each.

  Phase 1: Reduce scan volume (P0) — always first. Every other optimization benefits from smaller input.
  Phase 2: Eliminate redundant work (P1, P3)
  Phase 3: Fix structural inefficiencies (P2, P4–P9)

## EXPLAIN ANALYSIS PROCEDURE

Before choosing any strategy, execute this procedure on the EXPLAIN plan:

1. IDENTIFY THE COST SPINE — which sequence of nodes accounts for >70% of total cost?
   The spine is your optimization target. Everything else is noise.
2. CLASSIFY EACH SPINE NODE:
   - SEQ_SCAN: how many rows? Is there a filter? Is the filter selective (>5:1)?
   - HASH_JOIN: what's the build side cardinality? Is it the smaller table?
   - AGGREGATE: input rows vs output rows ratio? >10:1 = pushdown candidate.
   - NESTED_LOOP: ALWAYS suspicious — check if decorrelation is possible.
   - WINDOW: is it computed before or after a join? Could it be deferred?
3. TRACE DATA FLOW: row counts should decrease monotonically through the plan.
   Where do they stay flat or increase? That transition point is the bottleneck.
4. CHECK THE SYMPTOM ROUTING TABLE: match your observations to primary hypotheses.
5. FORM BOTTLENECK HYPOTHESIS: "The optimizer is doing X, but Y would be better
   because Z." This hypothesis drives both pathology matching AND novel reasoning.

## SYMPTOM ROUTING — from EXPLAIN to hypothesis

Two routing paths exist and should agree. If they disagree, trust Q-Error (quantitative).

### Path A: Q-Error routing (quantitative — from §2b-i when available)

Q-Error = max(estimated/actual, actual/estimated) per operator.
The operator with the highest Q-Error is where the planner's worst decision lives.

| Q-Error Locus | Direction  | Primary hypothesis     | Why                                      |
|---------------|------------|------------------------|------------------------------------------|
| JOIN          | UNDER_EST  | P2 (decorrelate)       | Planner thinks join is cheap, it's not    |
| JOIN          | ZERO_EST   | P0, P2                 | Planner has no join estimate at all       |
| JOIN          | OVER_EST   | P5 (LEFT→INNER)        | Planner over-provisions for NULLs         |
| SCAN          | OVER_EST   | P1, P4                 | Redundant scans or missed pruning         |
| SCAN          | ZERO_EST   | P2                     | DELIM_SCAN = correlated subquery          |
| AGGREGATE     | OVER_EST   | P3 (agg below join)    | Fan-out before GROUP BY                   |
| CTE           | ZERO_EST   | P0, P7                 | Planner blind to CTE statistics           |
| CTE           | UNDER_EST  | P2, P0                 | CTE output larger than expected           |
| PROJECTION    | OVER_EST   | P7, P0, P4             | Redundant computation                     |
| PROJECTION    | UNDER_EST  | P6, P5, P0             | Set operation or join underestimate       |
| FILTER        | OVER_EST   | P9, P0                 | Shared expression or missed pushdown      |

Structural flags (free, no execution needed):
- DELIM_SCAN → P2 (correlated subquery the optimizer couldn't decorrelate)
- EST_ZERO → P0/P7 (planner gave up — CTE boundary blocks stats)
- EST_ONE_NONLEAF → P2/P0 (planner guessing on non-leaf node)
- REPEATED_TABLE → P1 (single-pass consolidation opportunity)

### Path B: Structural routing (qualitative — from EXPLAIN tree inspection)

| EXPLAIN symptom                          | Primary hypothesis   | Verify           |
|------------------------------------------|---------------------|------------------|
| Row counts flat through CTEs, late drop  | P0 (predicate push) | Filter ratio, chain depth |
| Same table scanned N times               | P1 (repeated scans) | Join structure identical? |
| Nested loop with inner aggregate         | P2 (correlated sub)  | Already hash join? |
| Aggregate input >> output after join     | P3 (agg below join)  | Key alignment    |
| Full scan, OR across DIFFERENT columns   | P4 (cross-col OR)    | Same column? → STOP |
| LEFT JOIN + WHERE on right column        | P5 (LEFT→INNER)      | COALESCE check   |
| INTERSECT node, large inputs             | P6 (INTERSECT)       | Row count >1K?   |
| CTE self-joined with discriminators      | P7 (self-join CTE)   | 2-4 values?      |
| Window in CTE before join                | P8 (deferred window) | LAG/LEAD check   |
| Identical subtrees in different branches | P9 (shared expr)     | EXISTS check     |
| None of the above                        | FIRST-PRINCIPLES     | See NO MATCH     |

## ENGINE STRENGTHS — do NOT rewrite

1. **Predicate pushdown**: filter inside scan node → leave it.
2. **Same-column OR**: handled natively in one scan. Splitting = lethal (0.23x observed).
3. **Hash join selection**: sound for 2–4 tables. Reduce inputs, not order.
4. **CTE inlining**: single-ref CTEs inlined automatically (zero overhead).
5. **Columnar projection**: only referenced columns read.
6. **Parallel aggregation**: scans and aggregations parallelized across threads.
7. **EXISTS semi-join**: early termination. **Never materialize** (0.14x observed).

## CORRECTNESS RULES

- Identical rows, columns, ordering as original.
- Copy ALL literals exactly (strings, numbers, dates).
- Every CTE must SELECT all columns referenced downstream.
- Never drop, rename, or reorder output columns.

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
  - Multi-fact: 1 fact = safe, 2 = careful, 3+ = STOP (0.50x on 3-fact query)
  - ROLLUP/WINDOW downstream: CAUTION (0.85x observed)
  - CTE already filtered on this predicate: skip (0.71x observed)

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

  Wins: 8 validated (1.9x–4.8x, avg 3.3x)
  Regressions: 0.0076x (dim cross-join), 0.50x (3-fact join lock), 0.85x (ROLLUP blocked), 0.71x (over-decomposed)

### P1: Repeated scans of same table [Phase 2 — ZERO REGRESSIONS]

  Gap: REDUNDANT_SCAN_ELIMINATION — the optimizer cannot detect that N subqueries
  all scan the same table with the same joins. Each subquery is an independent plan
  unit with no Common Subexpression Elimination across boundaries.

  Signal: N separate SEQ_SCAN nodes on same table, identical joins, different bucket filters.
  Decision: consolidate to single scan with CASE WHEN / FILTER (WHERE ...).
  Gates: identical join structure across all subqueries, max 8 branches,
  COUNT/SUM/AVG/MIN/MAX only (not STDDEV/VARIANCE/PERCENTILE).

  Transforms: single_pass_aggregation (8 wins, 1.88x avg), channel_bitmap_aggregation (1 win, 6.24x)
  Wins: 6 validated (1.5x–6.2x, avg 2.9x)

### P2: Correlated subquery nested loop [Phase 3]

  Gap: CORRELATED_SUBQUERY_PARALYSIS — the optimizer cannot decorrelate correlated
  aggregate subqueries into GROUP BY + hash join. It falls back to nested-loop
  re-execution instead of recognizing the equivalence.

  Signal: nested loop, inner re-executes aggregate per outer row.
  If EXPLAIN shows hash join on correlation key → already decorrelated → STOP.
  Decision: extract correlated aggregate into CTE with GROUP BY on correlation key, JOIN back.
  Gates: NEVER decorrelate EXISTS (0.34x, 0.14x — semi-join destroyed), preserve ALL WHERE filters,
  check if Phase 1 reduced outer to <1000 rows (nested loop may be fast enough).

  Transforms: decorrelate (3 wins, 2.45x avg), composite_decorrelate_union (1 win, 2.42x)
  Wins: 2 validated (2.4x–2.9x, avg 2.7x)
  Regressions: 0.34x (semi-join destroyed), 0.71x (already decorrelated)

### P3: Aggregation after join — fan-out before GROUP BY [Phase 2 — ZERO REGRESSIONS]

  Gap: AGGREGATE_BELOW_JOIN_BLINDNESS — the optimizer cannot push GROUP BY below
  joins even when aggregation keys align with join keys. It always joins first
  (producing M rows), then aggregates (reducing to K groups, K << M).

  Signal: GROUP BY input rows >> distinct keys, aggregate node sits after join.
  Decision: pre-aggregate fact by join key BEFORE dimension join.
  Gates: GROUP BY keys ⊇ join keys (CORRECTNESS — wrong results if violated),
  reconstruct AVG from SUM/COUNT when pre-aggregating for ROLLUP.

  Transforms: aggregate_pushdown, star_join_prefetch
  Wins: 3 validated (1.3x–42.9x, avg 15.3x)

### P4: Cross-column OR forcing full scan [Phase 3 — HIGHEST VARIANCE]

  Gap: CROSS_COLUMN_OR_DECOMPOSITION — the optimizer handles same-column OR
  efficiently (single scan range) but OR across different columns forces a full
  scan evaluating all conditions for every row.

  Signal: single scan, OR across DIFFERENT columns, 70%+ rows discarded.
  CRITICAL: same column in all OR arms → STOP (engine handles natively).
  Decision: split into UNION ALL branches + shared dim CTE.
  Gates: max 3 branches, cross-column only, no self-join, no nested OR (multiplicative expansion).

  Transforms: or_to_union
  Wins: 4 validated (1.4x–6.3x, avg 3.1x)
  Regressions: 0.23x (9 branches from nested OR), 0.41x (nested OR expansion), 0.59x (same-col split), 0.51x (self-join re-executed per branch)

### P5: LEFT JOIN + NULL-eliminating WHERE [Phase 3 — ZERO REGRESSIONS]

  Gap: LEFT_JOIN_FILTER_ORDER_RIGIDITY — the optimizer cannot infer that WHERE on
  a right-table column makes LEFT JOIN semantically equivalent to INNER. LEFT JOIN
  also blocks join reordering (not commutative).

  Signal: LEFT JOIN + WHERE on right-table column (proves right non-null).
  Decision: convert to INNER JOIN, optionally pre-filter right table into CTE.
  Gate: no CASE WHEN IS NULL / COALESCE on right-table column.

  Transforms: inner_join_conversion
  Wins: 2 validated (1.9x–3.4x, avg 2.7x)

### P6: INTERSECT materializing both sides [Phase 3 — ZERO REGRESSIONS]

  Gap: INTERSECT is implemented as set materialization + comparison. The optimizer
  doesn't recognize that EXISTS semi-join is algebraically equivalent and can
  short-circuit at first match per row.

  Signal: INTERSECT between 10K+ row result sets.
  Decision: replace with EXISTS semi-join.
  Gate: both sides >1K rows.
  Related: semi_join_exists — replace full JOIN with EXISTS when joined columns not in output (1.67x).

  Transforms: intersect_to_exists, multi_intersect_exists_cte
  Wins: 1 validated (2.7x)

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
  Wins: 3 validated (1.6x–4.8x, avg 2.9x)
  Regressions: 0.49x (orphaned CTE — double materialization), 0.68x (orphaned variant)

### P8: Window functions in CTEs before join [Phase 3 — ZERO REGRESSIONS]

  Gap: the optimizer cannot defer window computation past a join when
  partition/ordering is preserved. It computes the window in the CTE because
  that's where the SQL places it.

  Signal: N WINDOW nodes inside CTEs, same ORDER BY key, CTEs then joined.
  Decision: remove windows from CTEs, compute once on joined result.
  Gates: not LAG/LEAD (depends on pre-join row order), not ROWS BETWEEN with specific frame.
  Note: SUM() OVER() naturally skips NULLs — handles FULL OUTER JOIN gaps.

  Transforms: deferred_window_aggregation
  Wins: 1 validated (1.4x)

### P9: Shared subexpression executed multiple times [Phase 3]

  Gap: the optimizer may not CSE identical subqueries across different query branches.
  When it doesn't, cost is N× what single execution would be.
  HARD STOP: EXISTS/NOT EXISTS → NEVER materialize (0.14x observed). Semi-join
  short-circuit is destroyed by CTE materialization.

  Signal: identical subtrees with identical costs scanning same tables.
  Decision: extract shared subexpression into CTE.
  Gates: NOT EXISTS, subquery is expensive (joins/aggregates), CTE must have WHERE.

  Transforms: materialize_cte
  Wins: 1 validated (1.4x)
  Regressions: 0.14x (EXISTS materialized — semi-join destroyed), 0.54x (correlated EXISTS pairs broken)

### NO MATCH — First-Principles Reasoning

If no pathology matches this query, do NOT stop.

1. **Check §2b-i Q-Error routing first.** Even when no pathology gate passes,
   the Q-Error direction+locus still points to where the planner is wrong.
   Use it as a starting hypothesis for novel intervention design.
2. Identify the single largest cost node. What operation dominates? Can it be restructured?
3. Count scans per base table. Repeated scans are always a consolidation opportunity.
4. Trace row counts through the plan. Where do they stay flat or increase?
5. Look for operations the optimizer DIDN'T optimize that it could have:
   - Subqueries not flattened
   - Predicates not pushed through CTE boundaries
   - CTEs re-executed instead of materialized
6. Use the transform catalog (§5a) as a menu. For each transform, check: does the
   EXPLAIN show the optimizer already handles this? If not → candidate.

Record: which pathologies checked, which gates failed, nearest miss, structural
features present. This data seeds pathology discovery for future updates.

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
