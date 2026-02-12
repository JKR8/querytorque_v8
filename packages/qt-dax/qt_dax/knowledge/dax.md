# DAX Rewrite Playbook
# 1 gold win (150x) + 5 unvalidated optimizations | ESG Carbon Intensity Model

## HOW TO USE THIS DOCUMENT

Work in phase order. Each phase eliminates a class of overhead — re-evaluate later phases after each.

  Phase 1: Reduce computational volume (P1, P2) — always first. Collapse measure trees and cache slicer state before touching iterators.
  Phase 2: Eliminate row-by-row overhead (P3, P4, P5) — convert FE iteration to SE-friendly patterns.
  Phase 3: Fix structural inefficiencies (P6, P7) — correct mathematical patterns, flatten nesting.

Before choosing any strategy, diagnose the measure:
- Measure chain depth: count [MeasureRef] hops. >3 levels = P1 candidate.
- Iterator scope: find SUMX/FILTER/AVERAGEX — what table? How many rows?
- Context transitions: measure references inside iterators = per-row CALCULATE.
- SELECTEDVALUE/ISINSCOPE: inside iterator body = P2 candidate.
- GROUPBY+SUMX: inside IF/SWITCH branches = P5 candidate.
- Division inside SUMX: sum-of-ratios = P6 candidate (correctness issue).
- CALCULATE nesting: count nested CALCULATE calls. >=4 = P7 candidate.

## ENGINE ARCHITECTURE — understand before rewriting

### Storage Engine (SE) — the fast path
- Multi-threaded columnar scan engine.
- Handles: SUM, COUNT, MIN, MAX, DISTINCTCOUNT, simple CALCULATE filters.
- Produces "datacaches" — pre-aggregated results the Formula Engine consumes.
- Goal: push as much work here as possible.

### Formula Engine (FE) — the slow path
- Single-threaded row-by-row iterator.
- Handles: SUMX, FILTER(Table), complex expressions, measure references in row context.
- Every row in an iterator = one FE evaluation cycle.
- CallbackDataID: when SE cannot evaluate an expression, it calls back to FE per row.
- Goal: minimize rows processed here; eliminate callbacks.

### Context Transitions
- Each measure reference [MeasureName] inside an iterator triggers a full CALCULATE.
- Cost: save filter context → apply row context → evaluate → restore. Per row.
- 1M rows × 1 measure ref = 1M context transitions.

## ENGINE STRENGTHS — do NOT rewrite these patterns

1. **CALCULATE filter arguments**: Pushed to SE as bitmap filters. Parallel, cached. Leave them.
2. **Simple aggregations** (SUM, COUNT, MIN, MAX): Native SE operations. Do not wrap in SUMX.
3. **VAR evaluation**: Evaluated once, result cached for RETURN scope. Already optimal.
4. **SUMMARIZECOLUMNS**: Optimized SE path for visuals. Do not replace with SUMMARIZE.
5. **TREATAS virtual relationships**: Efficient for cross-filter. Only replace if proven slow.
6. **EXISTS semi-join in CALCULATE**: Early termination. Do not materialize.

## CORRECTNESS RULES

- Preserve exact business logic — no silent filter changes.
- Maintain NULL/BLANK handling (DIVIDE vs /, COALESCE behavior).
- Respect filter context propagation (KEEPFILTERS vs REMOVEFILTERS).
- Preserve ALLEXCEPT expanded table semantics.
- Do not change aggregation grain without verifying row-level equivalence.
- Benchmark/Portfolio position type filtering must be consistent in numerator and denominator.
- Test with multiple slicer combinations, not just one.

## GLOBAL GUARDS (check always, before any rewrite)

1. Measure is in a Calculation Group → test with ALL calculation items, not just default
2. ALLEXCEPT present → do not change table references (expanded table semantics)
3. Bidirectional relationship → CALCULATETABLE may produce unexpected cross-filter
4. DirectQuery mode → iterator rewrites that increase SE queries may be WORSE
5. RLS (Row-Level Security) active → CALCULATETABLE + KEEPFILTERS may bypass RLS
6. Composite model → USERELATIONSHIP blocks aggregation table hits; document if changed
7. Simple measure (<100 chars, no iterator) → skip optimization (overhead exceeds savings)
8. Measure used as calculated column source → context transition behavior differs

---

## PATHOLOGIES

### P1: Measure forest causing repeated table scans [Phase 1 — HIGHEST IMPACT]

  Gap: MEASURE_CHAIN_REPEATED_SCANS — the DAX engine evaluates each measure
  reference as an independent query plan. A chain of N measures, each calling
  the next, produces N separate Storage Engine queries scanning the same tables.
  There is no Common Subexpression Elimination across measure boundaries.

  This is the single most impactful DAX pathology. The confirmed case study
  showed 150x speedup (60s → 0.4s) by collapsing a 38-measure deep chain
  into a single orchestrator measure with VARs.

  The fix has three parts: (1) trace the full dependency closure of the target
  measure, (2) identify which intermediate measures scan the same tables with
  similar filters, (3) collapse into a single measure using VARs to compute
  each intermediate result once.

  Signal: measure references [MeasureName] → more measure references → 3+ levels.
  Performance Analyzer shows multiple SE queries hitting the same tables.
  Slow measures (>5s) with simple-looking expressions that delegate to other measures.

  Decision gates:
  - Structural: measure chain depth >= 3 levels
  - Overlap: 2+ measures in the chain scan the same fact table
  - Stop: if each measure in the chain scans a DIFFERENT table → chain is efficient
  - Stop: if measure is intentionally decomposed for reuse across 5+ visuals → refactor risks breakage
  - Stop: if Calculation Group wraps the measures → test CG interaction before collapsing

  Transform: measure_forest_collapse
  Steps:
  1. Map full dependency closure (use MeasureDependencyAnalyzer)
  2. Identify shared table scans across chain
  3. Collapse into single VAR-based orchestrator
  4. Cache slicer selections (SELECTEDVALUE) into VARs at top
  5. Build intermediate tables at grain (ADDCOLUMNS + VALUES/SUMMARIZE)
  6. Compute final result from VARs

  Ordering: always apply first — reduces total SE queries from N to ~2-3.
  Composition: enables P2 (slicer caching), P5 (GROUPBY elimination), P6 (ratio fix).
  After applying: re-evaluate P3/P4 (iterators now visible in single measure).

  Wins: CR Intensity 150x (60s → 0.4s) — 38-measure chain → single orchestrator
  Related: Apportioned Carbon (14s original), WACI (25s original), CR Intensity parent (94s original)
  Regressions: none observed (but only 1 fully validated case)

### P2: Uncached slicer state evaluated per iterator row [Phase 1 — ZERO RISK]

  Gap: SELECTEDVALUE_IN_ITERATOR — SELECTEDVALUE, ISINSCOPE, and similar slicer-
  reading functions are re-evaluated per row when placed inside an iterator body.
  The engine cannot hoist them above the iteration scope because they are formally
  context-dependent expressions. In practice, slicer state does not change per row.

  Fix: move all SELECTEDVALUE/ISINSCOPE calls to VARs ABOVE the iterator. VARs
  are evaluated once in the outer filter context, then the cached scalar is used
  inside the iterator body.

  Signal: SELECTEDVALUE or ISINSCOPE inside SUMX/FILTER/ADDCOLUMNS body.
  Multiple IF/SWITCH branches inside iterators testing slicer state.

  Decision gates:
  - Structural: SELECTEDVALUE/ISINSCOPE found inside iterator scope
  - Always safe: VAR caching never changes semantics (filter context at VAR = filter context at SUMX)
  - Stop: if the expression is SELECTEDVALUE of a column IN the iterated table → this IS row-varying

  Transform: cache_slicer_state
  Steps:
  1. Identify all SELECTEDVALUE/ISINSCOPE calls
  2. Move each to a VAR declaration above the outermost iterator
  3. Replace all references inside iterator with the VAR name
  4. Keep RETURN unchanged

  Ordering: apply with P1 — trivial to do during measure collapse.
  Composition: always combined with P1.

  Wins: included in CR Intensity 150x (SELECTEDVALUE for Scope, Market Cap Mode)
  Regressions: none (semantics-preserving by construction)

### P3: FILTER(Table) forcing row-by-row Storage Engine bypass [Phase 2 — CRITICAL]

  Gap: FILTER_TABLE_FULL_SCAN — FILTER('Table', condition) forces the Formula
  Engine to iterate every row of the table, evaluating the condition per row.
  The Storage Engine cannot optimize this into a bitmap filter because FILTER
  returns a physical table object, not a filter predicate.

  With 1M rows, this is 1M FE evaluations vs a single SE bitmap scan.
  Combined with SUMX (P3+iterator = double iteration), impact is 100-1000x.

  Fix: replace FILTER(Table, condition) with CALCULATE filter arguments or
  CALCULATETABLE. This pushes the filter to the Storage Engine.

  Signal: FILTER('TableName', ...) where first argument is a table name, not
  ALL/VALUES/DISTINCT/CALCULATETABLE.

  Decision gates:
  - Structural: FILTER with table name as first argument
  - Exclude: FILTER(ALL(...)), FILTER(VALUES(...)), FILTER(DISTINCT(...)) — these are intentional patterns
  - Exclude: FILTER(CALCULATETABLE(...)) — already SE-optimized input
  - Stop: if condition uses RELATED across multiple hops → may need iterator for correctness

  Transform: filter_to_calculate
  - Single condition → CALCULATE([Measure], Table[Column] = Value)
  - Multi-condition → CALCULATE([Measure], Table[Col1] = V1, Table[Col2] = V2)
  - Needs table output → CALCULATETABLE(Table, condition)

  Performance impact: 10-100x on large tables (>100K rows)
  Confirmed: rule DAX001 (critical severity)
  Regressions: none documented

### P4: CallbackDataID from complex expressions in iterators [Phase 2 — HIGH IMPACT]

  Gap: CALLBACK_DATA_ID — when the Storage Engine encounters expressions it
  cannot evaluate natively (IF/SWITCH conditions, string operations, FORMAT,
  division, date arithmetic), it creates a "callback" to the Formula Engine
  for each row. This forces single-threaded FE evaluation and destroys SE
  parallelism and caching.

  The worst form is IF/SWITCH inside SUMX over a large table — every row
  triggers an FE callback to evaluate the branch condition.

  Signal: SUMX/AVERAGEX/FILTER body contains IF, SWITCH, FORMAT, CONCATENATE,
  or division operator. Performance Analyzer shows high FE time with SE callbacks.

  Decision gates:
  - Structural: IF/SWITCH/FORMAT/string ops inside iterator body
  - Scale: iterator table > 10K rows (small tables → overhead is negligible)
  - Stop: if condition genuinely varies per row AND cannot be pre-computed → accept cost
  - Stop: if replacing with calculated column would cause refresh-time issues

  Transform: eliminate_callback
  - Move branch condition to CALCULATE filter context
  - Pre-compute conditional values as calculated columns
  - Split into multiple CALCULATE paths instead of IF inside iterator
  - Use SWITCH(TRUE(), ...) at measure level, not inside SUMX

  Performance impact: 5-50x on large tables with complex branch logic
  Confirmed: rules DAX006, DAX008
  Related: P5 (GROUPBY+SUMX is a specific instance)

### P5: GROUPBY+SUMX in conditional branches [Phase 2 — CONFIRMED]

  Gap: GROUPBY_SUMX_CONDITIONAL — GROUPBY+SUMX inside IF/SWITCH branches
  creates heavy Formula Engine iteration that cannot be optimized by the SE.
  GROUPBY itself forces FE iteration (it's an iterator function), and SUMX
  inside CURRENTGROUP() adds a nested iteration layer. When placed inside
  a conditional branch, the overhead is multiplied by the number of branches
  the engine must evaluate.

  This pattern was confirmed in the CR Intensity case study — the Benchmark
  pathway used GROUPBY+SUMX to roll up Sub_Sector results.

  Fix: replace with grain-first approach using ADDCOLUMNS + SUMMARIZE + CALCULATE.
  Pre-aggregate at the desired grain, then iterate the small result.

  Signal: GROUPBY(..., SUMX(CURRENTGROUP(), ...)) inside IF/SWITCH.
  GROUPBY with SUMX where CURRENTGROUP() is referenced.

  Decision gates:
  - Structural: GROUPBY + SUMX(CURRENTGROUP(), ...) pattern
  - Context: inside IF/SWITCH branch (conditional evaluation)
  - Scale: grouping over > 1K distinct groups × > 10K source rows
  - Stop: if CURRENTGROUP() is essential for correctness (rare — usually CALCULATE suffices)

  Transform: grain_first_aggregate
  Steps:
  1. Replace GROUPBY with SUMMARIZE or ADDCOLUMNS(SUMMARIZE(...))
  2. Replace SUMX(CURRENTGROUP(), ...) with CALCULATE(SUM(...))
  3. Move the grain computation OUTSIDE the IF/SWITCH branch
  4. Use VAR to store the pre-aggregated table, then reference in branch

  Performance impact: 10-100x
  Confirmed: CR Intensity case (part of 150x composite improvement)
  Rule: DAX026
  Regressions: none documented

### P6: Sum-of-ratios producing incorrect weighted averages [Phase 3 — CORRECTNESS]

  Gap: SUM_OF_RATIOS_PATTERN — computing ratios per row inside SUMX produces
  mathematically incorrect results for intensity/weighted-average metrics.
  SUMX('Table', [Weight] * DIVIDE([Numerator], [Denominator])) is NOT the
  same as DIVIDE(SUMX([Weight]*[Numerator]), SUMX([Weight]*[Denominator])).

  This is both a correctness AND performance issue. The per-row division
  forces CallbackDataID overhead (P4), AND the result is wrong.

  Fix: compute ratio of sums, not sum of ratios. Accumulate weighted
  numerator and weighted denominator separately, then divide once.

  Signal: DIVIDE or / operator inside SUMX/AVERAGEX body.
  Intensity/WACI/weighted-average measures with per-asset computation.

  Decision gates:
  - Structural: division inside iterator body
  - Semantic: metric is a weighted average or intensity ratio
  - Stop: if per-row ratio IS the desired metric (e.g., per-asset return %)
  - Stop: if denominator is constant (ratio-of-sums = sum-of-ratios when denom is same)

  Transform: ratio_of_sums
  Steps:
  1. Identify numerator and denominator components
  2. Compute weighted numerator: SUMX(Table, Weight * Numerator)
  3. Compute weighted denominator: SUMX(Table, Weight * Denominator)
  4. Final result: DIVIDE(WeightedNumerator, WeightedDenominator)

  Mathematical proof:
  - Sum-of-ratios: Sum(C_i/R_i) — mixes different bases, incorrect
  - Ratio-of-sums: Sum(W*C) / Sum(W*R) — proper weighted intensity

  Confirmed: CR Intensity case (part of 150x composite improvement)
  Rule: DAX028
  Regressions: none documented (but verify per-row ratio intent)

### P7: Deep CALCULATE nesting causing exponential context transitions [Phase 3 — HIGH RISK]

  Gap: CALCULATE_NESTING_EXPLOSION — each nested CALCULATE triggers a context
  transition. With N nested CALCULATEs, context operations scale O(2^N) in
  the worst case. 4 nested = 16x overhead; 12 nested = 4096x overhead.

  This often occurs alongside P1 (measure chains) because each measure
  in the chain may have its own CALCULATE, producing implicit nesting.

  Fix: flatten nested CALCULATEs into a single CALCULATE with combined
  filter arguments. Use VAR for intermediate results.

  Signal: 4+ CALCULATE keywords in a single measure expression.
  Measure chain where each level adds a CALCULATE wrapper.

  Decision gates:
  - Structural: 4+ CALCULATE nesting levels (count by AST depth, not text count)
  - Context: filters are all independent (can be combined into one CALCULATE)
  - Stop: if CALCULATE levels have DIFFERENT filter directions (REMOVEFILTERS vs KEEPFILTERS)
  - Stop: if intermediate CALCULATE result is used by multiple downstream branches

  Transform: flatten_calculate
  Steps:
  1. List all filter arguments across the nesting chain
  2. Combine into single CALCULATE with all filters
  3. Use KEEPFILTERS where intersection semantics were intended
  4. Store base result in VAR if used multiple times

  Performance impact: exponential reduction in context transitions
  Rule: DAX003, DAX004
  Regressions: flattening may change semantics if filters interact (REMOVEFILTERS + KEEPFILTERS)

### NO MATCH

  Record: which pathologies checked, which gates failed.
  If no DAX pathology matches, check Model pathologies:
  - MDL001: Auto Date/Time (50-80% model size waste)
  - MDL002: Referential integrity violations
  - MDL003: High cardinality columns (DateTime with timestamps)
  - MDL004: Bidirectional relationships (7x degradation documented)
  → Model issues are fixed in Power BI Desktop settings or Power Query, not DAX.

---

## SAFETY RANKING

| Rank | Pathology | Regr. | Worst | Action |
|------|-----------|-------|-------|--------|
| 1 | P2: Slicer caching | 0 | — | Always fix (semantics-preserving) |
| 2 | P3: FILTER(Table) | 0 | — | Always fix (CALCULATE equivalent) |
| 3 | P6: Sum-of-ratios | 0 | — | Always fix (correctness + perf) |
| 4 | P5: GROUPBY+SUMX | 0 | — | Always fix (grain-first) |
| 5 | P1: Measure forest | 0 | — | Fix when chain >= 3 (verify reuse) |
| 6 | P4: CallbackDataID | 0 | — | Fix when table > 10K rows |
| 7 | P7: CALCULATE nesting | ? | ? | Check filter interaction before flattening |

## VERIFICATION CHECKLIST

Before finalizing any rewrite:
- [ ] Original and optimized measures return identical values for all slicer combinations
- [ ] NULL/BLANK handling preserved (DIVIDE alternate result, COALESCE, IF ISBLANK)
- [ ] ALLEXCEPT table references unchanged
- [ ] Position_Type filter (Benchmark/Portfolio) applied consistently in numerator and denominator
- [ ] No orphaned VARs (every VAR referenced in RETURN or downstream VARs)
- [ ] KEEPFILTERS used where intersection semantics were intended
- [ ] Measure naming follows model conventions
- [ ] Helper calculated columns (if added) use correct table and data types
- [ ] Tested with: single asset, multiple assets, no assets (empty filter), benchmark mode, portfolio mode
- [ ] Performance validated: warmup + 3 timed runs minimum (never single-run comparison)
- [ ] Rewrite doesn't match any REGRESSION REGISTRY pattern

## PRUNING GUIDE

Skip pathologies the measure rules out:

| Measure shows | Skip |
|---|---|
| No measure references (leaf measure) | P1 (measure forest) |
| No SELECTEDVALUE/ISINSCOPE | P2 (slicer caching) |
| No FILTER(Table) | P3 (FILTER replacement) |
| No IF/SWITCH/FORMAT inside iterator | P4 (CallbackDataID) |
| No GROUPBY | P5 (GROUPBY+SUMX) |
| No division inside SUMX/AVERAGEX | P6 (sum-of-ratios) |
| < 4 CALCULATE calls | P7 (nesting) |
| Simple measure (< 100 chars) | ALL pathologies |
| DirectQuery mode | P3, P5 (SE-push may increase remote queries) |

## REGRESSION REGISTRY

| Severity | Pattern | Result | Root cause |
|----------|---------|--------|------------|
| CAUTION | Collapsing measure used by 5+ visuals | untested | Shared measures may have different filter context per visual; single orchestrator may not handle all contexts |
| CAUTION | Adding calculated columns for Ownership_Factor | blocked | Columns not present in checked-in model; requires model schema change |
| CAUTION | CALCULATETABLE + KEEPFILTERS with bidirectional relationship | untested | May propagate filters in unexpected direction |
| CAUTION | Flattening CALCULATE with REMOVEFILTERS + KEEPFILTERS mixed | untested | Filter interaction semantics may change |

## MODEL PATHOLOGIES (separate from DAX measure optimization)

### MDL001: Auto Date/Time — 50-80% model size waste
- Detection: LocalDateTable count > 0 in VPAX
- Impact: ESG model had 53.3 MB wasted (74.6% of total)
- Fix: File → Options → Data Load → Uncheck Auto Date/Time. Create single shared Date table.

### MDL002: Referential Integrity — silent incorrect aggregations
- Detection: Relationship MissingKeys > 0
- Impact: incorrect results in visuals, silent data loss
- Fix: add "Unknown" member to dimension, or filter orphans in Power Query

### MDL003: High Cardinality — memory explosion
- Detection: Column cardinality > 100K (warning), > 1M (critical)
- Common culprit: DateTime with timestamps, GUIDs
- Fix: split date/time, round timestamps, consider aggregation tables

### MDL004: Bidirectional Relationships — 7x performance degradation
- Detection: CrossFilteringBehavior = Both
- When OK: intentional M:M bridge tables
- Fix: remove bidirectional unless required for M:M bridge pattern
