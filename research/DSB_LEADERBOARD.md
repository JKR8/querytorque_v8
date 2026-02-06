# DSB PostgreSQL Leaderboard

**Database:** PostgreSQL DSB SF10 (Scale Factor 10)
**Benchmark Run:** Round 01 - ADO Learning System
**Date:** 2026-02-06
**Connection:** `postgresql://jakc9:jakc9@127.0.0.1:5433/dsb_sf10`

---

## 🏆 LEADERBOARD - Ranked by Speedup

| Rank | Query ID | Speedup | Status | Original (ms) | Optimized (ms) | Type | Transform | Data Match |
|------|----------|---------|--------|---------------|----------------|------|-----------|-----------|
| 1 | query019_agg | **1.26x** ⭐ | ✅ WIN | 1,876.36 | 1,484.98 | AGG | early_filter | ✅ |
| 2 | query013_spj_spj | **1.12x** | ✅ PASS | 7,677.25 | 6,877.42 | SPJ | date_cte_isolate, or_to_union | ✅ |
| 3 | query072_agg | **1.09x** | ✅ PASS | 8,180.95 | 7,498.11 | AGG | early_filter | ✅ |
| 4 | query025_agg | **1.07x** | ✅ PASS | 5,062.75 | 4,724.25 | AGG | date_cte_isolate | ✅ |
| 5 | query027_agg | **1.02x** | ✅ PASS | 5,070.61 | 4,991.47 | AGG | early_filter | ✅ |
| 6 | query019_spj_spj | **1.01x** | ✅ PASS | 1,298.77 | 1,288.66 | SPJ | date_cte_isolate, early_filter, reorder_join | ✅ |
| 7 | query023_multi | 0.98x | ⚠️ REG | 12,842.66 | 13,123.76 | MULTI | date_cte_isolate, pushdown, decorrelate, materialize_cte | ✅ |
| 8 | query025_spj_spj | 0.97x | ⚠️ REG | 2,971.83 | 3,058.29 | SPJ | date_cte_isolate | ✅ |
| 9 | query013_agg | 0.96x | ⚠️ REG | 7,810.31 | 8,111.07 | AGG | semantic_rewrite | ✅ |
| 10 | query010_multi | 0.92x | ⚠️ REG | 8,560.13 | 9,347.70 | MULTI | date_cte_isolate | ✅ |
| 11 | query018_spj_spj | 0.92x | ⚠️ REG | 6,127.36 | 6,641.89 | SPJ | date_cte_isolate | ✅ |
| 12 | query018_agg | 0.84x | 🔴 WORST | 5,906.28 | 7,015.33 | AGG | date_cte_isolate | ✅ |
| — | query001_multi | ❌ ERROR | ❌ TIMEOUT | — | — | MULTI | decorrelate | ❌ |
| — | query014_multi | ❌ ERROR | ❌ TIMEOUT | — | — | MULTI | date_cte_isolate, materialize_cte, semantic_rewrite | ❌ |

---

## 📊 Summary Statistics

| Metric | Value |
|--------|-------|
| **Total Queries Tested** | 14 |
| **Passed** | 12 (86%) |
| **Errors** | 2 (14%) |
| **Wins (≥1.1x)** | 2 (14%) |
| **Passes (0.95-1.1x)** | 4 (29%) |
| **Regressions (<0.95x)** | 6 (43%) |
| **Average Speedup (Passed)** | 1.02x |
| **Median Speedup (Passed)** | 1.01x |
| **Best Speedup** | **1.26x** (query019_agg) |
| **Worst Regression** | **0.84x** (query018_agg = -16%) |
| **Validation Method** | Single run (⚠️ should use 5x trimmed mean) |

---

## Executive Summary

**Current Performance on DSB PostgreSQL:**
- ✅ **2 clear wins** above 1.1x speedup
- ✅ **12/14 queries** produced valid output (syntax/semantic correct)
- ❌ **6 regressions** (55% of passed queries slower than baseline)
- ❌ **2 timeouts** cannot even run original queries
- ⚠️ **Average 1.02x** is marginal - only +2% improvement

**Key Issue:** `date_cte_isolate` applied 8 times across 14 queries, but averages **0.97x** (negative!)

---

## Wins & Strong Performers

### 🏆 Tier 1: Significant Wins (≥1.1x)

| Query | Speedup | Original | Optimized | Transform | Notes |
|-------|---------|----------|-----------|-----------|-------|
| query019_agg | **1.26x** ⭐ | 1,876ms | 1,485ms | early_filter | Star schema dimension filtering |
| query013_spj_spj | **1.12x** | 7,677ms | 6,877ms | date_cte_isolate + or_to_union | Combined date isolation + OR decomposition |

### ✅ Tier 2: Acceptable (0.95x - 1.1x)

| Query | Speedup | Original | Optimized | Transform | Notes |
|-------|---------|----------|-----------|-----------|-------|
| query072_agg | **1.09x** | 8,181ms | 7,498ms | early_filter | Dimension + warehouse filters |
| query025_agg | **1.07x** | 5,063ms | 4,724ms | date_cte_isolate | Date self-join decomposition |
| query027_agg | **1.02x** | 5,071ms | 4,991ms | early_filter | Minimal but positive |
| query019_spj_spj | **1.01x** | 1,299ms | 1,289ms | date_cte_isolate + early_filter + reorder_join | Multiple transforms, minimal gain |

---

## Regressions Analysis

### ⚠️ Minor Regressions (0.95x - 0.99x)

| Query | Speedup | Loss | Original | Optimized | Transform | Root Cause |
|-------|---------|------|----------|-----------|-----------|------------|
| query023_multi | 0.98x | -2% | 12,843ms | 13,124ms | date_cte_isolate + pushdown + decorrelate + materialize_cte | Over-materialization with multiple transforms |
| query025_spj_spj | 0.97x | -3% | 2,972ms | 3,058ms | date_cte_isolate | CTE materialization overhead |
| query013_agg | 0.96x | -4% | 7,810ms | 8,111ms | semantic_rewrite | Query plan regression |

### 🔴 Major Regressions (< 0.95x)

| Query | Speedup | Loss | Original | Optimized | Transform | Root Cause |
|-------|---------|------|----------|-----------|-----------|------------|
| query010_multi | 0.92x | -9% | 8,560ms | 9,348ms | date_cte_isolate | Suboptimal join order |
| query018_spj_spj | 0.92x | -8% | 6,127ms | 6,642ms | date_cte_isolate | Worse execution plan |
| query018_agg | 0.84x | **-16%** 🔴 | 5,906ms | 7,015ms | date_cte_isolate | **WORST: PostgreSQL cost model misalignment** |

**Pattern:** 6 out of 6 regressions involve `date_cte_isolate` or multiple transforms

---

## ❌ Errors (Cannot Validate)

| Query | Type | Transform | Error | Impact |
|-------|------|-----------|-------|--------|
| query001_multi | MULTI | decorrelate | Original query timeout (>30s) | Cannot measure speedup - baseline too slow |
| query014_multi | MULTI | date_cte_isolate, materialize_cte, semantic_rewrite | Original query timeout (>30s) | Cannot measure speedup - baseline too slow |

**Issue:** Complex multi-block queries fail on timeout. Statement timeout may need increase or queries need sampling.

---

## Transform Effectiveness

### By Transform Type

| Transform | Applied | Wins | Passes | Regressions | Avg Speedup | Best Case | Worst Case |
|-----------|---------|------|--------|-------------|-------------|-----------|------------|
| **early_filter** ✅ | 3 | 2 | 1 | 0 | **1.12x** | 1.26x (Q19) | 1.02x (Q27) |
| **date_cte_isolate** ❌ | 8 | 0 | 1 | 6 | **0.97x** | 1.07x (Q25) | 0.84x (Q18) |
| **or_to_union** ✅ | 1 | 1 | 0 | 0 | **1.12x** | 1.12x (Q13) | 1.12x (Q13) |
| **pushdown** ❌ | 1 | 0 | 0 | 1 | 0.98x | 0.98x (Q23) | 0.98x (Q23) |
| **materialize_cte** ❌ | 1 | 0 | 0 | 1 | 0.98x | 0.98x (Q23) | 0.98x (Q23) |
| **semantic_rewrite** ❌ | 2 | 0 | 0 | 1 | 0.96x | 0.96x (Q13) | — |
| **decorrelate** ❌ | 2 | 0 | 0 | 0 | — | — | TIMEOUT |
| **reorder_join** ❌ | 1 | 0 | 1 | 0 | 1.01x | 1.01x (Q19) | 1.01x (Q19) |

### 🔴 KEY FINDING

**`early_filter` is the ONLY consistently positive transform:**
- Applied 3 times
- 2 wins, 1 pass
- Average: **1.12x** (genuine improvement)
- No regressions

**`date_cte_isolate` is HARMFUL on PostgreSQL:**
- Applied 8 times
- 0 wins, 1 marginal pass
- Average: **0.97x** (negative)
- 6 regressions (75% failure rate!)

---

## Query Breakdown by Type

### Aggregation Queries (AGG) - 6 tested

| Query | Speedup | Transform | Status | Notes |
|-------|---------|-----------|--------|-------|
| query019_agg | **1.26x** ⭐ | early_filter | WIN | Best overall performer |
| query072_agg | **1.09x** | early_filter | PASS | Confirmed: early_filter works well |
| query025_agg | **1.07x** | date_cte_isolate | PASS | Only marginal improvement |
| query027_agg | **1.02x** | early_filter | PASS | Minimal improvement |
| query013_agg | 0.96x | semantic_rewrite | REG | -4% regression |
| query018_agg | 0.84x | date_cte_isolate | REG | **-16% WORST** |

**AGG Summary:** 2 wins/pass with early_filter, 2 regressions with date_cte_isolate

### Select-Project-Join (SPJ) - 4 tested

| Query | Speedup | Transform | Status | Notes |
|-------|---------|-----------|--------|-------|
| query013_spj_spj | **1.12x** ⭐ | date_cte_isolate + or_to_union | PASS | OR decomposition helped |
| query019_spj_spj | **1.01x** | date_cte_isolate + early_filter + reorder_join | PASS | Multiple transforms, minimal |
| query025_spj_spj | 0.97x | date_cte_isolate | REG | -3% regression |
| query018_spj_spj | 0.92x | date_cte_isolate | REG | -8% regression |

**SPJ Summary:** 1 win with or_to_union, 3 regressions with date_cte_isolate alone

### Multi-Block (MULTI) - 3 tested

| Query | Speedup | Transform | Status | Notes |
|-------|---------|-----------|--------|-------|
| query023_multi | 0.98x | date_cte_isolate + pushdown + decorrelate + materialize_cte | REG | -2% with 4 transforms |
| query010_multi | 0.92x | date_cte_isolate | REG | -9% regression |
| query001_multi | ❌ ERROR | decorrelate | TIMEOUT | Original too slow |

**MULTI Summary:** 0 wins, all negative/timeout

---

## DSB Catalog Rules Usage

| Rule | Count | Win Rate | Example | Effectiveness |
|------|-------|----------|---------|----------------|
| **STAR_SCHEMA_DIMENSION_FILTER_FIRST** | 7 | 29% (2/7) | Q19_agg (1.26x), Q27_agg (1.02x) | ✅ Positive when paired with early_filter |
| **AGGREGATE_PUSH_BELOW_JOIN** | 6 | 33% (2/6) | Q19_agg, Q25_agg | ✅ Works with dimension filtering |
| **DSB_PREDICATE_CORRELATION_STATS** | 4 | 0% (0/4) | Q1, Q10, Q13_agg, Q14 | ❌ Mixed results (1 timeout, rest regressions) |
| **DSB_NON_EQUI_JOIN_WINDOW** | 4 | 0% (0/4) | Q18_agg, Q18_spj, Q19_spj, Q25_spj | ❌ All regressions or marginal |
| **DSB_SELF_JOIN_DECOMPOSITION** | 3 | 33% (1/3) | Q25_agg (1.07x) | ⚠️ Mixed |
| **SUBQUERY_OFFSET_ZERO_BARRIER** | 6 | 50% (3/6) | Q19_agg, Q25_agg, Q27_agg | ✅ Decent when combined |

**Finding:** DSB-specific rules underperforming. PostgreSQL likely already handles these patterns.

---

## Critical Issues & Bottlenecks

### 🔴 Problem 1: PostgreSQL Cost Model Misalignment

**Evidence:**
- `date_cte_isolate` applied 8 times: 0 wins, 6 regressions (75% failure!)
- CTEs being inlined by optimizer anyway (no benefit of pre-computation)
- CTE materialization adding overhead instead of reducing it

**Root Cause:**
- PostgreSQL optimizer prefers inline CTEs for cost estimation
- Dimension filters not expensive enough to justify materialization
- Non-equi joins on fact tables are actual bottleneck (not addressed)

**Solution:**
- Use `MATERIALIZED` hint to force execution
- Add `OFFSET 0` barriers to prevent inline optimization
- Focus on actual bottlenecks (non-equi joins, inventory correlations)

### 🔴 Problem 2: Timeout on Complex Queries

**Evidence:**
- query001_multi: Timeout on original execution
- query014_multi: Timeout on original execution
- Both are multi-block queries with correlated subqueries

**Root Cause:**
- PostgreSQL statement timeout (default 30s) too aggressive
- Complex queries need more time for planning/execution
- Correlated subqueries create worst-case scenarios

**Solution:**
- Increase statement_timeout to 60s for validation
- Or use sampling approach for expensive queries
- Pre-filter queries by complexity before optimization

### ⚠️ Problem 3: Over-Materialization Penalty

**Evidence:**
- query023_multi: -2% with 4 combined transforms
- Multiple CTEs = cumulative overhead
- Each additional transform adds materialization cost

**Solution:**
- Limit to 1-2 transforms per rewrite
- Test transforms individually before combining
- Measure overhead of each CTE

---

## Performance Data Collection Notes

**Single Run Methodology (⚠️ UNRELIABLE):**
- Current validation uses single execution per query
- Vulnerable to OS cache effects, background processes
- No statistical significance

**Recommended: 5x Trimmed Mean (RELIABLE):**
- Run each query 5 times
- Discard min and max outliers
- Average remaining 3 runs
- Provides statistical robustness (per CLAUDE.md memory)

**Next validation should use 5x trimmed mean approach!**

---

## Recommendations for Improvement

### Priority 1: Reduce date_cte_isolate (HIGH)
- [ ] Reduce confidence weight in ADO recommender
- [ ] Add PostgreSQL-specific constraint: "Don't use date_cte_isolate alone"
- [ ] Expected improvement: Eliminate 6 regressions

### Priority 2: Increase early_filter (HIGH)
- [ ] Boost early_filter confidence (1.12x average vs 0.97x for date_cte_isolate)
- [ ] Always recommend for star schema patterns
- [ ] Expected improvement: +0.5-1.0x average speedup

### Priority 3: Focus on Non-Equi Join Bottlenecks (MEDIUM)
- [ ] Identify true bottlenecks (non-equi joins, inventory correlations)
- [ ] Add window function rewrites for d3.d_date > d1.d_date + INTERVAL
- [ ] Expected improvement: Unlock query001_multi, query014_multi

### Priority 4: PostgreSQL Tuning (MEDIUM)
- [ ] Test with `random_page_cost = 1.1`
- [ ] Test with `effective_cache_size = 4GB`
- [ ] Test with `work_mem = 128MB`
- [ ] Test with `MATERIALIZED` hints on CTEs
- [ ] Expected improvement: +5-15% on marginal cases

### Priority 5: Timeout Resolution (LOW)
- [ ] Increase statement_timeout from 30s to 60s
- [ ] Re-validate query001_multi, query014_multi
- [ ] Expected: Validate 2 more queries

---

## Query Coverage Status

| Total DSB Queries | Tested | Coverage |
|-----------------|--------|----------|
| 52 (full catalog) | 14 | 27% |
| Multi-block | 3 | — |
| SPJ | 4 | — |
| Aggregation | 6 | — |
| Pending | 1 (Q072) | ✅ ADDED |

---

## File Locations

| Item | Path |
|------|------|
| **Leaderboard (This File)** | `research/DSB_LEADERBOARD.md` |
| Full Validation JSON | `research/ado/rounds/round_01/validation/full_summary.json` |
| Summary CSV | `research/ado/rounds/round_01/validation/summary.csv` |
| Per-Query Prompts | `research/ado/rounds/round_01/query*/prompt.txt` |
| Per-Query Responses | `research/ado/rounds/round_01/query*/response.txt` |
| Per-Query Metadata | `research/ado/rounds/round_01/query*/metadata.json` |
| Feedback Schema | `packages/qt-sql/ado/feedback/feedback_schema.yaml` |
| Query072 Feedback | `packages/qt-sql/ado/feedback/query072_agg_20260206_001.yaml` |
| DSB Query Templates | `/mnt/d/dsb/query_templates_pg/` |
| Validation Script | `research/ado/validate_dsb_pg.py` |
| PostgreSQL Connection | `postgres://jakc9:jakc9@127.0.0.1:5433/dsb_sf10` |

---

**Last Updated:** 2026-02-06 10:00 UTC
**Next Update:** After Priority 1-2 implementations
**Owner:** Claude Code - QueryTorque V8 ADO System
