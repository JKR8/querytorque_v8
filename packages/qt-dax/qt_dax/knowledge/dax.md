# DAX Rewrite Playbook
# 1 validated composite win (150x) + 5 timed unvalidated | ESG Carbon Intensity Model
# Data flows UP: trials.jsonl → transforms.json → examples/ → this playbook

## HOW TO USE THIS DOCUMENT

Work in phase order. Each phase eliminates a class of overhead — re-evaluate later phases after each.

  Phase 1: Reduce computational volume (P1, P2) — always first. Collapse measure trees and cache slicer state.
  Phase 2: Fix iteration patterns (P3, P4) — reduce Formula Engine row-by-row overhead.
  Phase 3: Fix mathematical patterns (P5) — correctness + performance.

Before choosing any strategy, diagnose the measure:
- Measure chain depth: count [MeasureRef] hops. >3 levels = P1 candidate.
- SELECTEDVALUE/ISINSCOPE: inside iterator body = P2 candidate.
- GROUPBY+SUMX: inside IF/SWITCH branches = P3 candidate.
- Iterator scope: SUMX/ADDCOLUMNS over large table with context transitions = P4 candidate.
- Division inside SUMX: sum-of-ratios = P5 candidate (correctness issue).

## EVIDENCE BASE

All pathologies derive from 12 optimization trials on a single ESG Carbon
Intensity model (Asset Management, Portfolio Analytics). The validated 150x
win (esg_carbon_005) applied P1+P2+P3+P4+P5 as a composite — individual
contributions cannot be isolated. 5 additional measures (9s–94s baseline)
have optimized rewrites but lack validated timing.

| Trial | Measure | Baseline | Speedup | Status |
|---|---|---|---|---|
| esg_carbon_005 | CR Intensity Switch_BM (Portfolio) | 60s | 150x (0.4s) | CONFIRMED |
| esg_carbon_012 | CR Intensity Switch_BM (parent) | 94s | pending | OBSERVED |
| esg_carbon_010 | WACI Switch_BM | 25s | pending | OBSERVED |
| esg_carbon_011 | Relative Difference Apportioned Carbon | 25s | pending | OBSERVED |
| esg_carbon_001 | Apportioned Carbon Switch_BM (Portfolio) | 14s | pending | OBSERVED |
| esg_carbon_002 | Apportioned Carbon Switch_BM (Benchmark) | 9s | pending | OBSERVED |

Source: trials.jsonl (12 records), transforms.json (10 transforms), examples/ (5 gold)

## ENGINE ARCHITECTURE

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

1. **CALCULATE filter arguments**: Pushed to SE as bitmap filters. Parallel, cached.
2. **Simple aggregations** (SUM, COUNT, MIN, MAX): Native SE operations. Do not wrap in SUMX.
3. **VAR evaluation**: Evaluated once, result cached for RETURN scope. Already optimal.
4. **SUMMARIZECOLUMNS**: Optimized SE path for visuals. Do not replace with SUMMARIZE.
5. **TREATAS virtual relationships**: Efficient cross-filter. Only replace if proven slow.
6. **EXISTS semi-join in CALCULATE**: Early termination. Do not materialize.

## CORRECTNESS RULES

- Preserve exact business logic — no silent filter changes.
- Maintain NULL/BLANK handling (DIVIDE vs /, COALESCE behavior).
- Respect filter context propagation (KEEPFILTERS vs REMOVEFILTERS).
- Preserve ALLEXCEPT expanded table semantics.
- Do not change aggregation grain without verifying row-level equivalence.
- Test with multiple slicer combinations, not just one.

## GLOBAL GUARDS (check always, before any rewrite)

1. Measure is in a Calculation Group → test with ALL calculation items, not just default
2. ALLEXCEPT present → do not change table references (expanded table semantics)
3. Bidirectional relationship → CALCULATETABLE may produce unexpected cross-filter
4. DirectQuery mode → iterator rewrites that increase SE queries may be WORSE
5. RLS (Row-Level Security) active → CALCULATETABLE + KEEPFILTERS may bypass RLS
6. Composite model → USERELATIONSHIP blocks aggregation table hits
7. Simple measure (<100 chars, no iterator) → skip optimization
8. Measure used as calculated column source → context transition behavior differs

---

## PATHOLOGIES

### P1: Measure forest causing repeated table scans [Phase 1 — CONFIRMED, HIGHEST IMPACT]

  Trial evidence: esg_carbon_005 (150x composite), esg_carbon_012 (94s),
  esg_carbon_001 (14s), esg_carbon_002 (9s)
  Transform: measure_forest_collapse

  Gap: MEASURE_CHAIN_REPEATED_SCANS — the DAX engine evaluates each measure
  reference as an independent query plan. A chain of N measures produces N
  separate SE queries scanning the same tables. There is no Common
  Subexpression Elimination across measure boundaries.

  In trial: CR Intensity had a 38-measure deep dependency closure. GS Asset,
  Daily Position, ESG Trucost Climate each scanned independently by multiple
  measures. Collapsing to a single orchestrator with VARs reduced SE queries
  from ~38 to 3.

  Signal: measure references [MeasureName] → more measure references → 3+ levels.
  Performance Analyzer shows multiple SE queries hitting the same tables.
  Slow measures (>5s) with simple expressions that delegate to other measures.

  Decision gates:
  - Structural: measure chain depth >= 3 levels
  - Overlap: 2+ measures in the chain scan the same fact table
  - Stop: if each measure scans a DIFFERENT table → chain is efficient
  - Stop: if measure is decomposed for reuse across 5+ visuals → refactor risks breakage
  - Stop: if Calculation Group wraps the measures → test CG interaction first

  Steps:
  1. Map full dependency closure (MeasureDependencyAnalyzer)
  2. Identify shared table scans across chain
  3. Collapse into single VAR-based orchestrator
  4. Cache slicer selections into VARs at top (→ P2)
  5. Build intermediate tables at grain (ADDCOLUMNS + VALUES/SUMMARIZE)
  6. Compute final result from VARs

  Ordering: always apply first — reduces SE queries from N to ~2-3.
  Composition: enables P2 (slicer caching), P3 (GROUPBY elimination), P5 (ratio fix).

  Wins: esg_carbon_005 150x (60s → 0.4s) — 38-measure → single orchestrator
  Related: esg_carbon_001 (14s), esg_carbon_002 (9s), esg_carbon_010 (25s),
           esg_carbon_011 (25s), esg_carbon_012 (94s) — all same pattern family
  Regressions: none observed

### P2: Uncached slicer state per iterator row [Phase 1 — CONFIRMED, ZERO RISK]

  Trial evidence: esg_carbon_005 (part of 150x composite)
  Transform: cache_slicer_state

  Gap: SELECTEDVALUE_IN_ITERATOR — SELECTEDVALUE and ISINSCOPE are re-evaluated
  per row inside iterator bodies. The engine cannot hoist them because they are
  formally context-dependent. In practice, slicer state does not change per row.

  In trial: SELECTEDVALUE('Scope Emission Types'[Scope_Type_Code]) and
  SELECTEDVALUE('Market Cap Type'[Market_Cap_Code]) were evaluated inside
  nested SWITCH/IF branches within iterators. Caching as VARs at measure top
  eliminated redundant per-row evaluation.

  Signal: SELECTEDVALUE or ISINSCOPE inside SUMX/FILTER/ADDCOLUMNS body.

  Decision gates:
  - Structural: SELECTEDVALUE/ISINSCOPE found inside iterator scope
  - Always safe: VAR caching never changes semantics
  - Stop: if SELECTEDVALUE references column IN the iterated table → row-varying

  Ordering: apply with P1 — trivial during measure collapse.
  Composition: always combined with P1.

  Wins: included in esg_carbon_005 150x (Scope, Market Cap Mode)
  Regressions: none (semantics-preserving by construction)

### P3: GROUPBY+SUMX in conditional branches [Phase 2 — CONFIRMED]

  Trial evidence: esg_carbon_005 (part of 150x composite), esg_carbon_012 (94s)
  Transform: groupby_to_addcolumns

  Gap: GROUPBY_SUMX_CONDITIONAL — GROUPBY forces FE iteration; SUMX inside
  CURRENTGROUP() adds a nested iteration layer. Inside IF/SWITCH branches,
  overhead multiplies per branch.

  In trial: Benchmark pathway used SUMX(GROUPBY('Benchmark Portfolio Mapping'
  [Sub_Sector]), [Aggregate MV ...]). Parent (esg_carbon_012, 94s) was slower
  than Portfolio-only child (esg_carbon_005, 60s) — GROUPBY path adds ~34s.

  Fix: ADDCOLUMNS(SUMMARIZE(..., grain_col), '@Result', CALCULATE(SUM(...))).
  Move grain computation OUTSIDE IF/SWITCH branch using VAR.

  Signal: GROUPBY(..., SUMX(CURRENTGROUP(), ...)) inside IF/SWITCH.

  Decision gates:
  - Structural: GROUPBY + SUMX(CURRENTGROUP()) pattern
  - Context: inside IF/SWITCH branch
  - Stop: if CURRENTGROUP() essential for correctness (rare)

  Wins: part of esg_carbon_005 150x composite
  Regressions: none observed

### P4: Grain-first materialization for iterator cost [Phase 2 — CONFIRMED]

  Trial evidence: esg_carbon_005 (150x composite), esg_carbon_010 (25s),
  esg_carbon_011 (25s)
  Transforms: grain_first_materialize, hoist_global_constant, shared_intermediate_reuse

  Gap: MEASURE_CHAIN_REPEATED_SCANS — SUMX over large fact tables with
  context transitions per row. Each row triggers full recalculation when
  intermediate measures are referenced inside the iterator.

  Three sub-patterns observed in trials:

  (a) grain_first_materialize: Build compact per-grain tables before aggregation.
      Trial: esg_carbon_005 — original SUMX('GS Asset', [MV Ownership] *
      SUMX('ESG Trucost Climate', scope sums)) triggered per-asset context
      transitions. Fix: ADDCOLUMNS(VALUES('GS Asset'[ISIN]), '@Ownership',
      ..., '@Carbon', ...) then SUMX over compact joined result.
      Steps: (1) identify grain column (ISIN), (2) build per-grain table via
      ADDCOLUMNS + VALUES/SUMMARIZE, (3) compute ownership/carbon/revenue
      once per grain, (4) NATURALINNERJOIN for final compact table,
      (5) SUMX over joined result.

  (b) hoist_global_constant: Pre-compute totals (market value, benchmark
      weights) as VARs outside iterators.
      Trial: esg_carbon_010 (25s) — total market value denominator computed
      inside nested measures, recalculated per context transition. Fix:
      VAR _TotalMV = CALCULATE(SUM([MARKET VALUE BASE]), ALLEXCEPT(...)).
      Preserve ALLEXCEPT expanded table semantics when hoisting.

  (c) shared_intermediate_reuse: Compute shared values once for both
      Benchmark and Portfolio paths.
      Trial: esg_carbon_011 (25s) — calls [Apportioned Carbon Switch_BM]
      twice (Benchmark filter, Portfolio filter). Each triggers full 38-measure
      chain independently. Fix: carbon-per-ISIN is identical for both position
      types (only ownership weights differ) — compute once, join to two
      ownership tables.
      Gate: verify values are context-independent before sharing.

  Signal: SUMX over large table with measure references in body.
  Performance Analyzer shows high SE query count.

  Decision gates:
  - Structural: SUMX over >10K rows with context transitions
  - Grain: grain column exists (ISIN, Product, Customer)
  - shared_intermediate_reuse: verify values are context-independent
  - hoist_global_constant: preserve ALLEXCEPT semantics when hoisting

  Wins: part of esg_carbon_005 150x; patterns observed in 010 (25s), 011 (25s)
  Regressions: none observed

### P5: Sum-of-ratios producing incorrect weighted averages [Phase 3 — CONFIRMED]

  Trial evidence: esg_carbon_005 (part of 150x composite)
  Exception evidence: esg_carbon_010 (WACI — sum-of-ratios IS correct)
  Transform: ratio_of_sums

  Gap: SUM_OF_RATIOS_PATTERN — computing ratios per row inside SUMX produces
  mathematically incorrect results for intensity metrics. Both a correctness
  AND performance issue — per-row division forces CallbackDataID overhead.

  In trial: original chain computed apportioned carbon and revenue in separate
  chains then divided. Fix: DIVIDE(SUMX(Joined, own*carbon), SUMX(Joined,
  own*revenue)) — ratio of weighted sums.

  CRITICAL EXCEPTION: WACI (esg_carbon_010) uses weight * (carbon/revenue)
  per asset. This IS sum-of-ratios by definition. Check the metric's
  mathematical definition before applying.

  Signal: DIVIDE or / inside SUMX/AVERAGEX body.

  Decision gates:
  - Structural: division inside iterator body
  - Semantic: metric is a weighted average or intensity ratio
  - Stop: if per-row ratio IS the desired metric (WACI, per-asset return)
  - Stop: if denominator is constant (no benefit)

  Wins: part of esg_carbon_005 150x composite
  Regressions: WACI exception (esg_carbon_010) — applying would corrupt results

### NO MATCH

  Record: which pathologies checked, which gates failed.
  If no DAX pathology matches, check Model pathologies:
  - MDL001: Auto Date/Time (50-80% model size waste)
  - MDL002: Referential integrity violations
  - MDL003: High cardinality columns
  - MDL004: Bidirectional relationships
  → Model issues are fixed in Power BI settings or Power Query, not DAX.

---

## CATALOG TRANSFORMS (no trial evidence)

These transforms are in transforms.json but have no trial backing. They are
industry-accepted best practices from the DAX rule catalog (dax_rules.md).
They will be promoted to pathologies when trial evidence is obtained.

- **filter_to_calculate**: Replace FILTER('Table', condition) with CALCULATE
  filter arguments. Pushes predicate from FE to SE. Rules: DAX001, DAX002.
  Not observed in ESG trials (measures already used CALCULATE).

- **eliminate_callback**: Remove IF/SWITCH/FORMAT/division from iterator
  bodies to prevent CallbackDataID. Rules: DAX006, DAX008. Partially
  observed in esg_carbon_005 but addressed via P1+P2 rather than directly.

- **flatten_calculate**: Flatten nested CALCULATE chains into single CALCULATE.
  Rules: DAX003, DAX004. Indirectly observed in esg_carbon_005 but addressed
  via P1 rather than directly. CAUTION: mixed REMOVEFILTERS + KEEPFILTERS.

---

## SAFETY RANKING

| Rank | Pathology | Evidence | Regr. | Action |
|------|-----------|----------|-------|--------|
| 1 | P2: Slicer caching | CONFIRMED | 0 | Always fix (semantics-preserving) |
| 2 | P5: Sum-of-ratios | CONFIRMED | 0 | Always fix if intensity (check WACI) |
| 3 | P3: GROUPBY+SUMX | CONFIRMED | 0 | Always fix (grain-first) |
| 4 | P1: Measure forest | CONFIRMED | 0 | Fix when chain >= 3 |
| 5 | P4: Grain-first | CONFIRMED | 0 | Fix on large iterators |

## VERIFICATION CHECKLIST

Before finalizing any rewrite:
- [ ] Original and optimized return identical values for all slicer combinations
- [ ] NULL/BLANK handling preserved (DIVIDE alternate result, COALESCE, IF ISBLANK)
- [ ] ALLEXCEPT table references unchanged
- [ ] Position_Type filter (Benchmark/Portfolio) consistent in numerator and denominator
- [ ] No orphaned VARs (every VAR referenced in RETURN or downstream VARs)
- [ ] KEEPFILTERS used where intersection semantics were intended
- [ ] Helper calculated columns (if added) use correct table and data types
- [ ] Tested with: single asset, multiple assets, no assets, benchmark mode, portfolio mode
- [ ] Performance validated: warmup + 3 timed runs minimum
- [ ] Rewrite doesn't match any REGRESSION REGISTRY pattern

## PRUNING GUIDE

Skip pathologies the measure rules out:

| Measure shows | Skip |
|---|---|
| No measure references (leaf measure) | P1 (measure forest) |
| No SELECTEDVALUE/ISINSCOPE | P2 (slicer caching) |
| No GROUPBY | P3 (GROUPBY+SUMX) |
| No SUMX over large table | P4 (grain-first) |
| No division inside SUMX/AVERAGEX | P5 (sum-of-ratios) |
| Simple measure (< 100 chars) | ALL pathologies |
| DirectQuery mode | P4 (may increase remote queries) |

## REGRESSION REGISTRY

| Severity | Pattern | Result | Root cause | Trial |
|----------|---------|--------|------------|-------|
| CRITICAL | Applying ratio-of-sums to WACI | incorrect | WACI IS sum-of-ratios by definition; converting corrupts the metric | esg_carbon_010 |
| CAUTION | Collapsing measure used by 5+ visuals | untested | Different filter context per visual; orchestrator may not handle all | — |
| CAUTION | Adding calculated columns for Ownership_Factor | blocked | Requires model schema change; columns not in checked-in model | esg_carbon_005 |
| CAUTION | CALCULATETABLE + KEEPFILTERS + bidirectional | untested | May propagate filters in unexpected direction | — |

## MODEL PATHOLOGIES (from VPAX analysis, separate from DAX measure optimization)

### MDL001: Auto Date/Time — 50-80% model size waste [CONFIRMED]
- Detection: LocalDateTable count > 0 in VPAX
- Evidence: ESG model VPAX analysis — 53.3 MB wasted (74.6% of total)
- Fix: File → Options → Data Load → Uncheck Auto Date/Time

### MDL002: Referential Integrity — silent incorrect aggregations [CATALOG]
- Detection: Relationship MissingKeys > 0
- Fix: add "Unknown" member to dimension, or filter orphans in Power Query

### MDL003: High Cardinality — memory explosion [CATALOG]
- Detection: Column cardinality > 100K (warning), > 1M (critical)
- Fix: split date/time, round timestamps, consider aggregation tables

### MDL004: Bidirectional Relationships — 7x degradation [CATALOG]
- Detection: CrossFilteringBehavior = Both
- Fix: remove bidirectional unless required for M:M bridge pattern
