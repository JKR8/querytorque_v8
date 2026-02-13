# Snowflake Rewrite Playbook
# DISCOVERY MODE — building empirical evidence | TPC-DS SF10TCL

## HOW TO USE THIS DOCUMENT

Work in phase order. Each phase changes the plan shape — re-evaluate later phases after each.
  Phase 1: Reduce scan volume — always first.
  Phase 2: Eliminate redundant work
  Phase 3: Fix structural inefficiencies

Before choosing any strategy, scan the EXPLAIN plan / Query Profile for:
- Partitions scanned vs total: high ratio = pruning failure. Target: <20% scanned.
- Bytes spilled (local or remote): ANY spill = memory pressure. Red flag.
- Row counts through plan: monotonically decreasing = healthy. Flat then sharp drop = pushdown opportunity.
- Repeated TableScan on same table: consolidation candidate.
- WithClause/WithReference: CTE materialized once, probed many = good.
- JoinFilter nodes: bloom filter applied = optimizer already pruning. Don't fight it.
- Build vs probe side sizes: smaller table should be build side.
- Filter node position: before TableScan = good. After Join = missed pushdown.
- CartesianJoin: OK for tiny dim tables, PROBLEM for anything else.
- SortWithLimit vs full Sort: LIMIT should produce SortWithLimit.

## ENGINE STRENGTHS — do NOT rewrite these patterns

1. **Micro-partition pruning**: Snowflake's #1 optimization. Filters on clustered columns
   skip entire micro-partitions at scan level.
   DO NOT wrap filter columns in functions (kills pruning).

2. **Column pruning through CTEs**: Reads only columns referenced by final query.
   UNLESS final SELECT is *.
   DO NOT manually project through CTE chains for pruning — it's automatic.

3. **Predicate pushdown**: Filters pushed to storage layer, including through single-ref CTEs.
   Also does predicate MIRRORING across join sides.
   DO NOT manually duplicate filters to both sides of a join.

4. **Correlated subquery decorrelation**: Transforms correlated subqueries into hash joins.
   Benchmarked: equal or better than manual decorrelation on TPCH_SF100.
   DO NOT decorrelate unless EXPLAIN shows a nested loop.

5. **EXISTS/NOT EXISTS semi-join**: Early termination. SemiJoin node in plan.
   NEVER materialize EXISTS into CTEs.

6. **Join filtering (bloom filters)**: JoinFilter nodes push bloom filters from build side
   to probe-side TableScan. 77/99 TPC-DS queries show JoinFilter nodes.
   DO NOT restructure joins that already have JoinFilter.

7. **Cost-based join ordering**: Usually correct. Can fail with functions on join keys.
   DO NOT force join order unless you have evidence of a flipped join.

8. **Metadata-based scan elimination**: MIN/MAX/COUNT served directly from micro-partition
   metadata without scanning data.

9. **QUALIFY clause**: Native window-function filtering. More efficient than nested subquery.
   PREFER QUALIFY over subquery-based row filtering.

10. **Distributed aggregation**: Multi-level partial aggregation for parallel warehouse execution.

## CORRECTNESS RULES

- Identical rows, columns, ordering as original.
- Copy ALL literals exactly (strings, numbers, dates).
- Preserve NULL semantics — NOT IN with NULLs ≠ NOT EXISTS.
- Every CTE must SELECT all columns referenced downstream.
- Never drop, rename, or reorder output columns.
- Preserve LIMIT semantics — no result set expansion.
- QUALIFY semantics: window computed on full partition, then filtered.

## GLOBAL GUARDS

1. EXISTS/NOT EXISTS → never materialize into CTEs (kills SemiJoin early termination).
2. UNION ALL → limit to ≤3 branches (each = separate scan pipeline).
3. CTEs referenced once → inline. CTEs referenced 2+ times → keep.
4. QUALIFY is native — prefer over subquery-based row filtering.
5. Do NOT restructure joins that have JoinFilter.
6. Do NOT wrap filter columns in functions → prevents micro-partition pruning.
7. NOT IN → NOT EXISTS for NULL safety.
8. Baseline < 100ms → skip structural rewrites.
9. Range/inequality joins → can produce internal Cartesian products.
10. Do NOT pre-filter both sides of a join if JoinFilter already handles it.

## HYPOTHESIZED GAPS (unverified — first run)

### H1: CTE_PREDICATE_FENCE [HIGH confidence] — Phase 1
  Source: Snowflake docs, select.dev analysis. Cross-engine: identical to DuckDB P0 (~35% of wins).
  DETECT: CTE referenced 2+ times. Outer query has selective filter on columns inside CTE.
  HYPOTHESIS: Predicates don't propagate into multi-ref CTE definitions.
  TEST: Split CTE into consumer-specific versions or push most selective predicate into CTE WHERE.
  DECISION GATES (from DuckDB P0 — apply cautiously):
  - Filter ratio >5:1 = strong candidate. <2:1 = skip.
  - 3+ fact tables in CTE chain = CAUTION (DuckDB: 0.50x on Q25)
  - CTE already filtered on this predicate = skip (DuckDB: 0.71x on Q1)
  - ROLLUP/WINDOW downstream = CAUTION (DuckDB: 0.85x on Q67)
  EXPECTED IMPACT: 1.3x–4.0x

### H2: JOIN_ORDER_FLIPPING [HIGH confidence] — Phase 3
  Source: Documented production incidents (Fresha SEV-1, 2025). DIRECTED JOIN exists for this.
  DETECT: Build side has significantly MORE rows than probe side. Functions on join keys.
  HYPOTHESIS: Cardinality estimates wrong → larger table on build side → memory pressure.
  TEST: Pre-filter smaller side in CTE, or remove functions from join keys.
  DECISION GATES:
  - Confirm build side > probe side in Query Profile
  - Functions on join keys = high likelihood
  - No spill and acceptable runtime = skip
  EXPECTED IMPACT: 2x–10x in pathological cases

### H3: RANGE_JOIN_CARTESIAN [HIGH confidence] — Phase 3
  Source: Snowflake docs, Greybeam analysis. Non-equi joins produce Cartesian products.
  DETECT: CartesianJoin with range predicate. Join output >> inputs.
  HYPOTHESIS: Range join N×M rows produces full Cartesian before filter.
  TEST: Binning — bucket range values, equi-join on buckets, then precise range post-filter.
  DECISION GATES:
  - Both sides > 1K rows = candidate. One side < 100 rows = skip.
  EXPECTED IMPACT: Up to 300x in worst case

### H4: AGGREGATE_BELOW_JOIN [MEDIUM confidence] — Phase 2
  Source: Cross-engine from DuckDB P3 (Q22: 42.9x).
  DETECT: GROUP BY after join. Input rows >> output groups. Agg keys ⊇ join keys.
  HYPOTHESIS: Joins first (fan-out), then aggregates. Pre-aggregating reduces intermediate.
  TEST: Pre-aggregate fact table by join key in CTE before dimension join.
  DECISION GATES:
  - GROUP BY keys ⊇ join keys (CORRECTNESS)
  - Reconstruct AVG from SUM/COUNT
  - Fan-out ratio > 10:1 = strong candidate. < 3:1 = skip.
  EXPECTED IMPACT: 1.3x–40x

### H5: COLUMN_PRUNING_FAILURE_THROUGH_JOINS [MEDIUM confidence] — Phase 1
  Source: select.dev analysis (Paul Vernon).
  DETECT: Wide table (50+ cols) joined, final SELECT uses few columns.
  HYPOTHESIS: Column pruning stops at join boundary. All columns read.
  TEST: Pre-project wide table to only required columns in CTE before join.
  DECISION GATES:
  - Table < 10 columns = skip. Bytes scanned close to expected = already pruned, skip.
  EXPECTED IMPACT: Proportional to column reduction

### H6: FUNCTION_ON_FILTER_KILLS_PRUNING [HIGH confidence] — Phase 1
  Source: Snowflake documentation, optimization guides.
  DETECT: WHERE applies function to column (YEAR(), CAST(), etc.). 0% pruning on clustered col.
  HYPOTHESIS: Function transforms comparison → metadata can't prune → full scan.
  TEST: Rewrite as range predicate. WHERE YEAR(d)=2024 → WHERE d >= '2024-01-01' AND d < '2025-01-01'
  DECISION GATES:
  - Table < 1M rows = skip. Column has no clustering = skip.
  EXPECTED IMPACT: 2x–20x

### H7: SPILL_INDUCING_INTERMEDIATES [MEDIUM confidence] — Phase 2
  Source: Snowflake docs, Greybeam. Remote spill = red flag.
  DETECT: Bytes spilled > 0 in Query Profile (local or remote).
  HYPOTHESIS: Intermediate exceeds warehouse memory. Earlier filtering reduces below threshold.
  TEST: Apply H1, H6, H4 to reduce intermediate sizes. Often a COMPOUND fix.
  DECISION GATES:
  - Only local spill, small volume = may not justify rewrite
  - Remote spill = always investigate
  EXPECTED IMPACT: 2x–15x

### H8: MULTI_CTE_REFERENCE_OVERHEAD [LOW confidence] — Phase 3
  Source: select.dev CTE analysis.
  DETECT: Simple CTE (single scan + filter, no joins) referenced 2-3 times.
  HYPOTHESIS: Materialization overhead exceeds re-execution cost for simple CTEs.
  TEST: Replace CTE with repeated subqueries.
  DECISION GATES:
  - CTE has joins/aggregates = skip. CTE referenced 4+ times = skip.
  EXPECTED IMPACT: 1.1x–1.5x (marginal)

### H9: LEFT_JOIN_NON_SIMPLIFICATION [LOW confidence] — Phase 3
  Source: Cross-engine from DuckDB P5 (zero regressions).
  DETECT: LEFT JOIN followed by WHERE on right-table column (proves non-null).
  HYPOTHESIS: Optimizer doesn't auto-convert LEFT to INNER. LEFT blocks join reordering.
  TEST: Convert to INNER JOIN.
  DECISION GATES:
  - COALESCE/IS NULL on right column = DO NOT convert
  - JoinFilter present = may not help
  EXPECTED IMPACT: 1.2x–3.4x

## NO MATCH — First-Principles Reasoning

If no hypothesized gap applies:
1. Identify the single largest cost node. Can it be restructured?
2. Count scans per base table. Repeated scans = consolidation opportunity.
3. Check partition pruning ratios. >50% scanned with filter = pruning failure.
4. Check for spill. Any spill = intermediate too large = earlier filtering needed.
5. Look for operations the optimizer DIDN'T do:
   - Subqueries not flattened
   - Predicates not pushed to scan
   - CTE re-executed instead of materialized
6. Use transform catalog (§5a) as a menu. Check each: does EXPLAIN show optimizer handles it?
   If not → candidate.

Record: which hypotheses checked, which gates failed, nearest miss, structural features.

## VERIFICATION CHECKLIST

Before finalizing any rewrite:
 1. Every output column from original appears in rewrite (same name, order, type)
 2. Every literal value copied exactly from original
 3. Every CTE SELECTs all columns referenced by downstream consumers
 4. JOIN semantics preserved (INNER stays INNER, LEFT stays LEFT unless proven equivalent)
 5. NULL handling unchanged (NOT IN ≠ NOT EXISTS for nullable columns)
 6. Aggregation groups unchanged (no row duplication/elimination from join changes)
 7. LIMIT/ORDER BY preserved exactly
 8. No Cartesian products introduced by missing join conditions
 9. QUALIFY used instead of subquery WHERE on window results (Snowflake-native)
10. If rewrite introduces CTEs: each CTE referenced ≥2 times (otherwise inline)
11. Functions not introduced on filter columns (kills micro-partition pruning)
12. Rewrite doesn't restructure joins that had JoinFilter (would lose bloom filter)

## PRUNING GUIDE

Skip hypotheses the plan rules out:

| Plan shows                              | Skip                                |
|-----------------------------------------|-------------------------------------|
| Partitions scanned << total             | H6 (pruning already working)        |
| No spill (local or remote)             | H7 (no memory pressure)             |
| Each table appears once                 | (repeated scan consolidation)        |
| No CTE / each CTE referenced once      | H1 (predicate fence), H8 (CTE overhead) |
| No LEFT JOIN                            | H9 (INNER conversion)               |
| No range predicates in joins            | H3 (range join cartesian)           |
| No GROUP BY                             | H4 (aggregate pushdown)             |
| No functions on filter columns          | H6 (function on filter)             |
| Build side < probe side on all joins    | H2 (join flipping)                  |
| Baseline < 100ms                        | ALL structural rewrites              |

## SAFETY RANKING

| Rank | Hypothesis | Confidence | Risk | Action |
|------|------------|------------|------|--------|
| 1 | H6: Function on filter | HIGH | LOW | Always fix |
| 2 | H5: Column pruning failure | MEDIUM | LOW | Always fix |
| 3 | H9: LEFT→INNER | LOW | LOW | Fix if no COALESCE/IS NULL |
| 4 | H4: Agg below join | MEDIUM | LOW | Fix if keys align |
| 5 | H1: CTE predicate fence | HIGH | MEDIUM | All gates must pass |
| 6 | H7: Spill reduction | MEDIUM | MEDIUM | Usually compound fix |
| 7 | H2: Join order flipping | HIGH | MEDIUM | Confirm in profile first |
| 8 | H3: Range join cartesian | HIGH | MEDIUM | Binning adds complexity |
| 9 | H8: CTE overhead | LOW | LOW | Marginal gains |
