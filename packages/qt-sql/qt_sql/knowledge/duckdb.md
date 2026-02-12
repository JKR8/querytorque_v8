# DuckDB Query Rewrite Decision Tree

Distilled from 22 gold wins + 10 regressions across TPC-DS SF1–SF10.
Cross-reference: `decisions.md` for detailed pathology cards with full risk calibration tables.

## ENGINE STRENGTHS — do NOT rewrite these patterns

1. **Predicate pushdown**: Single-table WHERE pushed into scan. If EXPLAIN shows filter inside scan node → leave it.
2. **Same-column OR**: Handled natively in one scan. Splitting to UNION is lethal (0.23x Q13, 0.59x Q90).
3. **Hash join selection**: Sound for 2–4 tables. Focus on reducing join *inputs*, not reordering.
4. **CTE inlining**: Single-ref CTEs inlined automatically (zero overhead). Multi-ref CTEs may materialize.
5. **Columnar projection**: Only referenced columns read. Fewer columns in intermediate CTEs = less materialization.
6. **Parallel aggregation**: Scans and aggregations parallelized across threads.
7. **EXISTS semi-join**: Uses early termination. **Never materialize EXISTS** (0.14x Q16, 0.54x Q95).

## CORRECTNESS RULES

- Identical rows, columns, ordering as original.
- Copy ALL literals exactly (strings, numbers, dates).
- Every CTE must SELECT all columns referenced downstream.
- Never drop, rename, or reorder output columns.

## GLOBAL GUARDS (check always, before any rewrite)

1. EXISTS/NOT EXISTS → never materialize (0.14x Q16, 0.54x Q95)
2. Same-column OR → never split to UNION (0.23x Q13, 0.59x Q90)
3. Baseline < 100ms → skip CTE-based rewrites (overhead exceeds savings)
4. 3+ fact table joins → do not pre-materialize facts (locks join order)
5. Every CTE MUST have a WHERE clause (0.85x Q67 — unfiltered = pure overhead)
6. No orphaned CTEs — remove original after splitting (0.49x Q31, 0.68x Q74)
7. No cross-joining 3+ dimension CTEs (0.0076x Q80 — Cartesian product)
8. Max 2 cascading fact-table CTE chains (0.78x Q4)
9. Convert comma joins to explicit JOIN...ON
10. NOT EXISTS → NOT IN breaks with NULLs — preserve EXISTS form

## PATHOLOGY DETECTION (read explain plan, identify expensive nodes)

### P1: Large scan feeding into join — filter not pushed down
  Explain signal: fact table SEQ_SCAN producing millions of rows, filter node ABOVE join discards 80%+
  SQL signal: dimension WHERE above fact join, CTE definitions lacking filters that outer query applies
  Gap: CROSS_CTE_PREDICATE_BLINDNESS

  → DECISION: Move selective filter INTO a CTE, join small CTE result to fact table
  → Gates: filter selectivity < 20%, baseline > 100ms, not 3+ fact joins, every CTE has WHERE
  → Expected: 1.3x–4.0x | Worst: 0.0076x (Q80 dim cross-join), 0.50x (Q25 low baseline)
  → Transforms: date_cte_isolate (12 wins, 1.34x avg), early_filter (6 wins, 1.67x avg),
    dimension_cte_isolate (5 wins, 1.48x avg), prefetch_fact_join (4 wins, 1.89x avg),
    multi_date_range_cte, multi_dimension_prefetch, shared_dimension_multi_channel
  → Workers get: Q6 (4.00x), Q93 (2.97x), Q63 (3.77x), Q43 (2.71x), Q29 (2.35x), Q26 (1.93x)
  → Regressions: Q80 (0.0076x cross-join dims), Q25 (0.50x 31ms baseline),
    Q67 (0.85x ROLLUP blocked), Q51 (0.87x window blocked)

### P2: Same fact table scanned N times with identical joins
  Explain signal: N separate SEQ_SCAN nodes on same table with similar row counts
  SQL signal: same fact table N times (N ≥ 3) in FROM, identical joins, different bucket filters
  Gap: REDUNDANT_SCAN_ELIMINATION

  → DECISION: Consolidate to single scan with CASE WHEN / FILTER (WHERE ...)
  → Gates: all scans share identical join structure, max 8 branches, COUNT/SUM/AVG/MIN/MAX only
  → Expected: 1.3x–6.2x | Worst: no known regressions
  → ZERO REGRESSIONS. Safest pathology to fix.
  → Transforms: single_pass_aggregation (8 wins, 1.88x avg), channel_bitmap_aggregation (1 win, 6.24x)
  → Workers get: Q88 (6.24x), Q9 (4.47x), Q61 (2.27x), Q32 (1.61x), Q4 (1.53x), Q90 (1.47x)

### P3: Nested loop executing correlated subquery per outer row
  Explain signal: nested loop with inner side re-executing aggregate subquery per outer row
  If EXPLAIN shows hash join on correlation key → optimizer ALREADY decorrelated → STOP
  SQL signal: WHERE col > (SELECT AGG(...) FROM ... WHERE outer.key = inner.key)
  Gap: CORRELATED_SUBQUERY_PARALYSIS

  → DECISION: Extract correlated aggregate into CTE with GROUP BY on correlation key, then JOIN
  → Gates: NOT EXISTS (semi-join destroyed 0.34x Q93), check EXPLAIN first (hash join = already done),
    preserve ALL WHERE filters from original subquery in CTE
  → Expected: 1.5x–2.9x | Worst: 0.34x (Q93 semi-join destroyed)
  → Transforms: decorrelate (3 wins, 2.45x avg), composite_decorrelate_union (1 win, 2.42x)
  → Workers get: Q1 (2.92x), Q35 (2.42x)
  → Regressions: Q93 (0.34x EXISTS→CTE destroyed semi-join), Q1 variant (0.71x already decorrelated)

### P4: Aggregation after join — fact table fan-out before GROUP BY
  Explain signal: GROUP BY input rows >> distinct key count, aggregate node sits after join
  SQL signal: GROUP BY on fact columns that are also join keys, dims only in SELECT (not WHERE)
  Gap: AGGREGATE_BELOW_JOIN_BLINDNESS

  → DECISION: Pre-aggregate fact table by join key BEFORE dimension join
  → Gates: GROUP BY keys ⊇ join keys (CORRECTNESS — wrong results if violated),
    reconstruct AVG from SUM/COUNT when pre-aggregating for ROLLUP
  → Expected: 5x–43x | Worst: no known regressions
  → ZERO REGRESSIONS. Key alignment is the safety gate.
  → Transforms: aggregate_pushdown, star_join_prefetch (compound: isolate selective dim + pre-aggregate)
  → Workers get: Q22 (42.90x — biggest single win in entire benchmark), Q65 (1.80x), Q72 (1.27x)

### P5: Cross-column OR evaluated as single scan with row-by-row filter
  Explain signal: sequential scan with OR filter discarding 70%+ rows, no index usage
  SQL signal: OR conditions on DIFFERENT columns, max 3 top-level branches
  CRITICAL: same column in all OR arms → STOP (engine handles natively)
  Gap: CROSS_COLUMN_OR_DECOMPOSITION

  → DECISION: Split into UNION ALL branches, one per OR condition (+ shared dim CTE)
  → Gates: max 3 branches, cross-column only, no self-join, no nested OR (multiplicative expansion)
  → Expected: 1.3x–3.2x | Worst: 0.23x (Q13 — 9 branches from nested OR)
  → HIGHEST VARIANCE. Our biggest wins AND worst regressions.
  → Transforms: or_to_union
  → Workers get: Q15 (3.17x), Q88 (6.28x time-range), Q10 (1.49x), Q45 (1.35x)
  → Regressions: Q13 (0.23x 9 branches), Q48 (0.41x nested OR),
    Q90 (0.59x same-col), Q23 (0.51x self-join)

### P6: LEFT JOIN preserving NULL rows that WHERE immediately discards
  Explain signal: LEFT JOIN output ≈ INNER JOIN output (filter above discards NULLs)
  SQL signal: LEFT JOIN followed by WHERE on right-table column, no IS NULL / COALESCE logic
  Gap: LEFT_JOIN_FILTER_ORDER_RIGIDITY

  → DECISION: Convert LEFT JOIN to INNER JOIN (+ optional early filter CTE)
  → Gates: no CASE WHEN IS NULL on right-table column, WHERE proves right side non-null
  → Expected: 1.5x–3.4x | Worst: no known regressions
  → ZERO REGRESSIONS. Safe pathology.
  → Transforms: inner_join_conversion
  → Workers get: Q93 (3.44x), Q80 (1.89x)

### P7: INTERSECT materializing both sides before comparison
  Explain signal: two large materialization nodes feeding INTERSECT operator
  SQL signal: INTERSECT between queries producing 10K+ rows each

  → DECISION: Replace INTERSECT with EXISTS semi-join
  → Gates: both sides produce large results (> 1K rows)
  → Expected: 1.8x–2.7x | Worst: no known regressions
  → ZERO REGRESSIONS. Safe pathology.
  → Transforms: intersect_to_exists, multi_intersect_exists_cte
  → Related: semi_join_exists — replace full JOIN with EXISTS when joined columns not in output (1.67x)
  → Workers get: Q14 (2.72x)

### P8: Self-joined CTE materialized for all values, post-filtered per arm
  Explain signal: CTE materialization with 2+ joins on same CTE, each filtering different values
  SQL signal: WITH cte AS (...) ... FROM cte a JOIN cte b WHERE a.period = 1 AND b.period = 2
  Gap: UNION_CTE_SELF_JOIN_DECOMPOSITION + CROSS_CTE_PREDICATE_BLINDNESS

  → DECISION: Split CTE into per-partition CTEs, each embedding its discriminator
  → Gates: 2–4 discriminator values, MUST remove original combined CTE after splitting
  → Expected: 1.4x–4.8x | Worst: 0.49x (Q31 orphaned CTE — double materialization)
  → Transforms: self_join_decomposition (1 win, 4.76x), union_cte_split (2 wins, 1.72x avg),
    rollup_to_union_windowing (1 win, 2.47x)
  → Workers get: Q39 (4.76x), Q36 (2.47x), Q74 (1.57x)
  → Regressions: Q31 (0.49x original CTE kept), Q74 (0.68x orphaned variant)

### P9: Window functions computed in CTEs before join
  Explain signal: N separate WINDOW nodes inside CTE scans, all sharing same ORDER BY key
  SQL signal: SUM(...) OVER (ORDER BY ...) inside CTE definition, CTE then joined to another

  → DECISION: Remove windows from CTEs, compute once on joined result
  → Gates: not LAG/LEAD (depends on pre-join row order), not ROWS BETWEEN with specific frame
  → Expected: 1.3x–1.4x | Worst: no known regressions
  → ZERO REGRESSIONS. Safe pathology.
  → Transforms: deferred_window_aggregation
  → Workers get: Q51 (1.36x)

### P10: Shared subexpression executed multiple times
  Explain signal: two identical subtrees with identical costs scanning same tables
  SQL signal: same subquery text appears 2+ times
  HARD STOP: EXISTS/NOT EXISTS in shared expression → NEVER materialize (0.14x Q16)

  → DECISION: Extract shared subexpression into CTE
  → Gates: NOT EXISTS (destroyed semi-join), subquery is expensive (joins/aggregates),
    CTE must have a WHERE clause
  → Expected: 1.3x–1.4x | Worst: 0.14x (Q16 EXISTS materialized)
  → Transforms: materialize_cte
  → Workers get: Q95 (1.43x)
  → Regressions: Q16 (0.14x EXISTS materialized), Q95 (0.54x cardinality severed)

### NO MATCH
  Record: which pathologies were checked, which gates failed
  Nearest miss: closest pathology + why it didn't qualify
  Features present: structural features for future pattern discovery
  → Workers get: broad gold example set, analyst's manual reasoning

## SAFETY RANKING

| Rank | Pathology              | Regr. | Worst   | Recommendation           |
|------|------------------------|-------|---------|--------------------------|
| 1    | P2: Repeated scans     | 0     | —       | Always fix               |
| 2    | P4: Agg after join     | 0     | —       | Always fix (verify keys) |
| 3    | P6: LEFT→INNER         | 0     | —       | Always fix               |
| 4    | P7: INTERSECT          | 0     | —       | Always fix               |
| 5    | P9: Pre-join windows   | 0     | —       | Always fix               |
| 6    | P8: Self-join CTE      | 1     | 0.49x   | Check orphan CTE         |
| 7    | P1: Filter not pushed  | 4     | 0.0076x | All gates must pass      |
| 8    | P3: Correlated loop    | 2     | 0.34x   | Check EXPLAIN first      |
| 9    | P10: Shared expr       | 3     | 0.14x   | Never on EXISTS          |
| 10   | P5: Cross-col OR       | 4     | 0.23x   | Max 3, cross-column only |
