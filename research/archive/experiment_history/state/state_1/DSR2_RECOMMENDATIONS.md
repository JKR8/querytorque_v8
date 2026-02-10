# DSR2 Recommendations Report

Generated for state_1 prompt generation.

## Executive Summary

### Current Portfolio (Best Across All Attempts)
- **WIN** (>=1.5x): 19 queries
- **IMPROVED** (1.1-1.5x): 33 queries
- **NEUTRAL** (0.95-1.1x): 47 queries
- **REGRESSION** (<0.95x): 0 queries

### DSR1 Round Results (DeepSeek Reasoner)
- WIN: 6
- IMPROVED: 24
- NEUTRAL: 30
- REGRESSION: 26
- ERROR: 13

### DSR2 Opportunity
- **Tier 1 (High Value)**: 63 queries - NEUTRAL/REGRESSION with room to improve
- **Tier 2 (Medium Value)**: 19 queries - IMPROVED, could push to WIN
- **Tier 3 (Already Winning)**: 17 queries - WIN or fast, lower priority

## Transform Effectiveness (Across All Attempts)

| Transform | Attempts | Win% | Avg Speedup | Regressions | Errors |
|-----------|----------|------|-------------|-------------|--------|
| intersect_to_exists | 1 | 100% | 1.40x | 0 | 0 |
| materialize_cte, date_cte_isolate | 1 | 100% | 1.60x | 0 | 0 |
| decorrelate, date_cte_isolate, pushdown | 1 | 100% | 1.20x | 0 | 0 |
| single_pass_aggregation | 1 | 100% | 3.32x | 0 | 0 |
| dimension_cte_isolate | 3 | 33% | 1.15x | 1 | 0 |
| early_filter | 9 | 33% | 1.33x | 1 | 2 |
| decorrelate | 13 | 31% | 1.23x | 1 | 1 |
| prefetch_fact_join | 20 | 25% | 0.99x | 4 | 2 |
| multi_dimension_prefetch | 25 | 24% | 1.02x | 5 | 4 |
| pushdown | 15 | 20% | 1.03x | 1 | 0 |
| materialize_cte | 12 | 17% | 1.12x | 1 | 1 |
| semantic_rewrite | 13 | 15% | 1.00x | 3 | 0 |
| date_cte_isolate | 59 | 8% | 0.99x | 8 | 2 |
| or_to_union | 18 | 6% | 1.10x | 0 | 0 |
| multi_date_range_cte | 2 | 0% | 0.90x | 1 | 1 |
| multi_push_predicate | 1 | 0% | 1.00x | 0 | 0 |
| reorder_join | 1 | 0% | 1.00x | 0 | 0 |

## What's New in DSR2 Prompts

### New Constraints (6 added, 8 total)
- **CTE_COLUMN_COMPLETENESS** [CRITICAL]: CTE SELECT must include all downstream columns
- **NO_MATERIALIZE_EXISTS** [CRITICAL]: Never convert EXISTS to materialized CTE
- **MIN_BASELINE_THRESHOLD** [HIGH]: Skip CTE transforms on <100ms queries
- **NO_UNFILTERED_DIMENSION_CTE** [HIGH]: Every CTE must have a filtering WHERE
- **NO_UNION_SAME_COLUMN_OR** [HIGH]: Don't split same-column OR into UNION
- **REMOVE_REPLACED_CTES** [HIGH]: Remove original CTEs after replacement

### New Gold Examples (2 added, 15 total)
- **composite_decorrelate_union** (Q35, 2.42x): Decorrelate EXISTS + OR-to-UNION composite
- **shared_dimension_multi_channel** (Q80, 1.30x): Shared dim CTEs across channels

### Counter-Examples Added (when_not_to_use)
- **date_cte_isolate**: Don't use when optimizer already pushes predicates (Q31: 0.49x)
- **prefetch_fact_join**: Don't use on <50ms queries or window-dominated (Q25: 0.50x)
- **materialize_cte**: NEVER for EXISTS (Q16: 0.14x = 7x slowdown)
- **multi_dimension_prefetch**: No unfiltered dim CTEs (Q67: 0.85x)
- **or_to_union**: Don't split same-column OR (Q90: 0.59x)

---

## TIER 1: High-Value Targets

*63 queries - NEUTRAL/REGRESSION with significant runtime. Highest ROI.*

### Q67 — NEUTRAL (best: 1.00x) — baseline: 4509ms

**Chain**: 1.00x (baseline) -> 1.00x (v2_standard/date_cte_isolate) [+] -> 0.85x (dsr1/multi_dimension_prefetch) [-]

**DSR1**: 0.85x using `multi_dimension_prefetch` (regression)

**Transforms tried**:
  = `date_cte_isolate` (neutral)
  - `multi_dimension_prefetch` (failed/regression)

**Untried applicable patterns**: prefetch_fact_join, early_filter, shared_dimension_multi_channel

**Recommendations**:
  1. **early_filter** — gold 4.00x, success rate: 3/9
  2. **prefetch_fact_join** — gold 3.77x, success rate: 5/20
  3. **shared_dimension_multi_channel** — gold 1.30x, success rate: no data

### Q64 — NEUTRAL (best: 1.01x) — baseline: 3841ms

**Chain**: 1.00x (baseline) -> 1.01x (kimi/unknown) [=] -> 1.00x (v2_standard/pushdown) [+] -> 0.00x (dsr1/multi_date_range_cte) [X]

**DSR1**: 0.00x using `multi_date_range_cte` (error)

**Transforms tried**:
  - `multi_date_range_cte` (failed/regression)
  = `pushdown` (neutral)

**Untried applicable patterns**: None — all structural matches tried

**Recommendations**: All applicable patterns exhausted. Needs novel approach or composite strategy.

### Q23 — NEUTRAL (best: 1.06x) — baseline: 1854ms

**Chain**: 1.00x (baseline) -> 1.06x (kimi/unknown) [=] -> 1.00x (v2_standard/date_cte_isolate) [+] -> 1.02x (dsr1/date_cte_isolate) [=]

**DSR1**: 1.02x using `date_cte_isolate` (neutral)

**Transforms tried**:
  = `date_cte_isolate` (neutral)

**Untried applicable patterns**: decorrelate, composite_decorrelate_union, prefetch_fact_join

**Recommendations**:
  1. **prefetch_fact_join** — gold 3.77x, success rate: 5/20
  2. **decorrelate** — gold 2.92x, success rate: 4/13
  3. **composite_decorrelate_union** — gold 2.42x, success rate: no data

### Q4 — IMPROVED (best: 1.12x) — baseline: 1839ms

**Chain**: 1.00x (baseline) -> 1.03x (kimi/unknown) [=] -> 0.35x (retry3w_3/unknown) [-] -> 1.12x (dsr1/pushdown) [+]

**DSR1**: 1.12x using `pushdown` (success)

**Transforms tried**:
  + `pushdown` (succeeded)

**Untried applicable patterns**: multi_date_range_cte, decorrelate, composite_decorrelate_union, date_cte_isolate

**Recommendations**:
  1. **date_cte_isolate** — gold 4.00x, success rate: 5/59
  2. **decorrelate** — gold 2.92x, success rate: 4/13
  3. **composite_decorrelate_union** — gold 2.42x, success rate: no data
  4. **multi_date_range_cte** — gold 2.35x, success rate: 0/2

### Q51 — NEUTRAL (best: 1.00x) — baseline: 1424ms

**Chain**: 1.00x (baseline) -> 1.00x (v2_standard/date_cte_isolate) [+] -> 0.87x (dsr1/prefetch_fact_join) [-]

**DSR1**: 0.87x using `prefetch_fact_join` (regression)

**Transforms tried**:
  = `date_cte_isolate` (neutral)
  - `prefetch_fact_join` (failed/regression)

**Untried applicable patterns**: None — all structural matches tried

**Recommendations**: All applicable patterns exhausted. Needs novel approach or composite strategy.

### Q13 — NEUTRAL (best: 1.01x) — baseline: 981ms

**Chain**: 1.00x (baseline) -> 1.01x (kimi/unknown) [=] -> 1.00x (v2_standard/or_to_union) [+] -> 0.00x (dsr1/multi_dimension_prefetch) [X]

**DSR1**: 0.00x using `multi_dimension_prefetch` (error)

**Transforms tried**:
  - `multi_dimension_prefetch` (failed/regression)
  = `or_to_union` (neutral)

**Untried applicable patterns**: dimension_cte_isolate, early_filter, shared_dimension_multi_channel, prefetch_fact_join

**Recommendations**:
  1. **early_filter** — gold 4.00x, success rate: 3/9
  2. **prefetch_fact_join** — gold 3.77x, success rate: 5/20
  3. **dimension_cte_isolate** — gold 1.93x, success rate: 1/3
  4. **shared_dimension_multi_channel** — gold 1.30x, success rate: no data

### Q11 — NEUTRAL (best: 1.06x) — baseline: 953ms

**Chain**: 1.00x (baseline) -> 0.98x (kimi/unknown) [=] -> 1.06x (dsr1/date_cte_isolate) [=]

**DSR1**: 1.06x using `date_cte_isolate` (neutral)

**Transforms tried**:
  = `date_cte_isolate` (neutral)

**Untried applicable patterns**: decorrelate, composite_decorrelate_union, prefetch_fact_join, multi_date_range_cte

**Recommendations**:
  1. **prefetch_fact_join** — gold 3.77x, success rate: 5/20
  2. **decorrelate** — gold 2.92x, success rate: 4/13
  3. **composite_decorrelate_union** — gold 2.42x, success rate: no data
  4. **multi_date_range_cte** — gold 2.35x, success rate: 0/2

### Q2 — NEUTRAL (best: 1.00x) — baseline: 937ms

**Chain**: 1.00x (baseline) -> 1.00x (v2_standard/pushdown) [+] -> 0.00x (dsr1/date_cte_isolate) [X]

**DSR1**: 0.00x using `date_cte_isolate` (error)

**Transforms tried**:
  - `date_cte_isolate` (failed/regression)
  = `pushdown` (neutral)

**Untried applicable patterns**: prefetch_fact_join

**Recommendations**:
  1. **prefetch_fact_join** — gold 3.77x, success rate: 5/20

### Q78 — NEUTRAL (best: 1.08x) — baseline: 936ms

**Chain**: 1.00x (baseline) -> 1.01x (kimi/unknown) [=] -> 1.00x (v2_standard/pushdown) [+] -> 1.08x (dsr1/date_cte_isolate) [=]

**DSR1**: 1.08x using `date_cte_isolate` (neutral)

**Transforms tried**:
  = `date_cte_isolate` (neutral)
  = `pushdown` (neutral)

**Untried applicable patterns**: prefetch_fact_join

**Recommendations**:
  1. **prefetch_fact_join** — gold 3.77x, success rate: 5/20

### Q24 — NEUTRAL (best: 1.00x) — baseline: 780ms

**Chain**: 1.00x (baseline) -> 0.87x (kimi/unknown) [-] -> 1.00x (v2_standard/pushdown) [+] -> 0.00x (dsr1/decorrelate) [X]

**DSR1**: 0.00x using `decorrelate` (error)

**Transforms tried**:
  - `decorrelate` (failed/regression)
  = `pushdown` (neutral)

**Untried applicable patterns**: composite_decorrelate_union, date_cte_isolate

**Recommendations**:
  1. **date_cte_isolate** — gold 4.00x, success rate: 5/59
  2. **composite_decorrelate_union** — gold 2.42x, success rate: no data

### Q14 — IMPROVED (best: 1.40x) — baseline: 691ms

**Chain**: 1.00x (baseline) -> 0.95x (kimi/unknown) [=] -> 1.40x (dsr1/intersect_to_exists+date_cte_isolate) [+]

**DSR1**: 1.40x using `intersect_to_exists+date_cte_isolate` (success)

**Transforms tried**:
  + `date_cte_isolate` (succeeded)
  + `intersect_to_exists` (succeeded)

**Untried applicable patterns**: prefetch_fact_join

**Recommendations**:
  1. **prefetch_fact_join** — gold 3.77x, success rate: 5/20

### Q36 — NEUTRAL (best: 1.00x) — baseline: 567ms

**Chain**: 1.00x (baseline) -> 0.96x (kimi/unknown) [=] -> 1.00x (v2_standard/multi_push_predicate) [+] -> 0.91x (dsr1/prefetch_fact_join) [-]

**DSR1**: 0.91x using `prefetch_fact_join` (regression)

**Transforms tried**:
  = `multi_push_predicate` (neutral)
  - `prefetch_fact_join` (failed/regression)

**Untried applicable patterns**: date_cte_isolate, dimension_cte_isolate, multi_dimension_prefetch, early_filter, shared_dimension_multi_channel

**Recommendations**:
  1. **date_cte_isolate** — gold 4.00x, success rate: 5/59
  2. **early_filter** — gold 4.00x, success rate: 3/9
  3. **multi_dimension_prefetch** — gold 2.71x, success rate: 6/25
  4. **dimension_cte_isolate** — gold 1.93x, success rate: 1/3
  5. **shared_dimension_multi_channel** — gold 1.30x, success rate: no data

### Q76 — IMPROVED (best: 1.10x) — baseline: 513ms

**Chain**: 1.00x (baseline) -> 1.10x (kimi/unknown) [+] -> 1.00x (v2_standard/pushdown) [+] -> 0.00x (dsr1/date_cte_isolate) [X]

**DSR1**: 0.00x using `date_cte_isolate` (error)

**Transforms tried**:
  - `date_cte_isolate` (failed/regression)
  = `pushdown` (neutral)

**Untried applicable patterns**: prefetch_fact_join, or_to_union, composite_decorrelate_union, union_cte_split

**Recommendations**:
  1. **prefetch_fact_join** — gold 3.77x, success rate: 5/20
  2. **or_to_union** — gold 3.17x, success rate: 1/18
  3. **composite_decorrelate_union** — gold 2.42x, success rate: no data
  4. **union_cte_split** — gold 1.36x, success rate: no data

### Q74 — IMPROVED (best: 1.36x) — baseline: 493ms

**Chain**: 1.00x (baseline) -> 1.36x (kimi/pushdown) [+] -> 1.00x (v2_standard/pushdown) [+] -> 0.68x (dsr1/pushdown) [-]

**DSR1**: 0.68x using `pushdown` (regression)

**Transforms tried**:
  + `pushdown` (succeeded)

**Untried applicable patterns**: union_cte_split, date_cte_isolate, prefetch_fact_join

**Recommendations**:
  1. **date_cte_isolate** — gold 4.00x, success rate: 5/59
  2. **prefetch_fact_join** — gold 3.77x, success rate: 5/20
  3. **union_cte_split** — gold 1.36x, success rate: no data

### Q18 — IMPROVED (best: 1.14x) — baseline: 424ms

**Chain**: 1.00x (baseline) -> 1.14x (kimi/unknown) [+] -> 1.00x (v2_standard/date_cte_isolate) [+] -> 0.00x (dsr1/prefetch_fact_join) [X]

**DSR1**: 0.00x using `prefetch_fact_join` (error)

**Transforms tried**:
  = `date_cte_isolate` (neutral)
  - `prefetch_fact_join` (failed/regression)

**Untried applicable patterns**: dimension_cte_isolate, multi_dimension_prefetch, early_filter, shared_dimension_multi_channel

**Recommendations**:
  1. **early_filter** — gold 4.00x, success rate: 3/9
  2. **multi_dimension_prefetch** — gold 2.71x, success rate: 6/25
  3. **dimension_cte_isolate** — gold 1.93x, success rate: 1/3
  4. **shared_dimension_multi_channel** — gold 1.30x, success rate: no data

### Q47 — NEUTRAL (best: 1.00x) — baseline: 415ms

**Chain**: 1.00x (baseline) -> 1.00x (kimi/unknown) [=] -> 1.00x (v2_standard/or_to_union) [+] -> 0.91x (dsr1/date_cte_isolate) [-]

**DSR1**: 0.91x using `date_cte_isolate` (regression)

**Transforms tried**:
  - `date_cte_isolate` (failed/regression)
  = `or_to_union` (neutral)

**Untried applicable patterns**: prefetch_fact_join, early_filter, multi_dimension_prefetch, shared_dimension_multi_channel

**Recommendations**:
  1. **early_filter** — gold 4.00x, success rate: 3/9
  2. **prefetch_fact_join** — gold 3.77x, success rate: 5/20
  3. **multi_dimension_prefetch** — gold 2.71x, success rate: 6/25
  4. **shared_dimension_multi_channel** — gold 1.30x, success rate: no data

### Q95 — IMPROVED (best: 1.37x) — baseline: 390ms

**Chain**: 1.00x (baseline) -> 1.37x (kimi/unknown) [+] -> 1.00x (v2_standard/semantic_rewrite) [+] -> 0.54x (dsr1/materialize_cte) [-]

**DSR1**: 0.54x using `materialize_cte` (regression)

**Transforms tried**:
  - `materialize_cte` (failed/regression)
  = `semantic_rewrite` (neutral)

**Untried applicable patterns**: composite_decorrelate_union, date_cte_isolate, prefetch_fact_join

**Recommendations**:
  1. **date_cte_isolate** — gold 4.00x, success rate: 5/59
  2. **prefetch_fact_join** — gold 3.77x, success rate: 5/20
  3. **composite_decorrelate_union** — gold 2.42x, success rate: no data

### Q54 — NEUTRAL (best: 1.03x) — baseline: 389ms

**Chain**: 1.00x (baseline) -> 1.03x (kimi/unknown) [=] -> 1.00x (v2_standard/date_cte_isolate) [+] -> 0.00x (dsr1/prefetch_fact_join) [X]

**DSR1**: 0.00x using `prefetch_fact_join` (error)

**Transforms tried**:
  = `date_cte_isolate` (neutral)
  - `prefetch_fact_join` (failed/regression)

**Untried applicable patterns**: None — all structural matches tried

**Recommendations**: All applicable patterns exhausted. Needs novel approach or composite strategy.

### Q60 — NEUTRAL (best: 1.02x) — baseline: 378ms

**Chain**: 1.00x (baseline) -> 1.02x (kimi/unknown) [=] -> 1.00x (v2_standard/date_cte_isolate) [+] -> 0.00x (dsr1/multi_dimension_prefetch) [X]

**DSR1**: 0.00x using `multi_dimension_prefetch` (error)

**Transforms tried**:
  = `date_cte_isolate` (neutral)
  - `multi_dimension_prefetch` (failed/regression)

**Untried applicable patterns**: prefetch_fact_join, dimension_cte_isolate, early_filter, shared_dimension_multi_channel, union_cte_split

**Recommendations**:
  1. **early_filter** — gold 4.00x, success rate: 3/9
  2. **prefetch_fact_join** — gold 3.77x, success rate: 5/20
  3. **dimension_cte_isolate** — gold 1.93x, success rate: 1/3
  4. **union_cte_split** — gold 1.36x, success rate: no data
  5. **shared_dimension_multi_channel** — gold 1.30x, success rate: no data

### Q8 — IMPROVED (best: 1.16x) — baseline: 362ms

**Chain**: 1.00x (baseline) -> 1.03x (kimi/unknown) [=] -> 1.16x (dsr1/date_cte_isolate) [+]

**DSR1**: 1.16x using `date_cte_isolate` (success)

**Transforms tried**:
  + `date_cte_isolate` (succeeded)

**Untried applicable patterns**: decorrelate, composite_decorrelate_union, prefetch_fact_join

**Recommendations**:
  1. **prefetch_fact_join** — gold 3.77x, success rate: 5/20
  2. **decorrelate** — gold 2.92x, success rate: 4/13
  3. **composite_decorrelate_union** — gold 2.42x, success rate: no data

### Q72 — NEUTRAL (best: 1.00x) — baseline: 348ms

**Chain**: 1.00x (baseline) -> 0.97x (kimi/unknown) [=] -> 1.00x (v2_standard/semantic_rewrite) [+] -> 0.77x (dsr1/multi_dimension_prefetch) [-]

**DSR1**: 0.77x using `multi_dimension_prefetch` (regression)

**Transforms tried**:
  - `multi_dimension_prefetch` (failed/regression)
  = `semantic_rewrite` (neutral)

**Untried applicable patterns**: multi_date_range_cte, dimension_cte_isolate, early_filter, shared_dimension_multi_channel

**Recommendations**:
  1. **early_filter** — gold 4.00x, success rate: 3/9
  2. **multi_date_range_cte** — gold 2.35x, success rate: 0/2
  3. **dimension_cte_isolate** — gold 1.93x, success rate: 1/3
  4. **shared_dimension_multi_channel** — gold 1.30x, success rate: no data

### Q28 — IMPROVED (best: 1.33x) — baseline: 327ms

**Chain**: 1.00x (baseline) -> 1.33x (kimi/unknown) [+] -> 1.00x (v2_standard/semantic_rewrite) [+] -> 0.92x (dsr1/semantic_rewrite) [-]

**DSR1**: 0.92x using `semantic_rewrite` (regression)

**Transforms tried**:
  - `semantic_rewrite` (failed/regression)

**Untried applicable patterns**: single_pass_aggregation, pushdown

**Recommendations**:
  1. **single_pass_aggregation** — gold 4.47x, success rate: 1/1
  2. **pushdown** — gold 2.11x, success rate: 3/15

### Q75 — NEUTRAL (best: 1.00x) — baseline: 325ms

**Chain**: 1.00x (baseline) -> 0.94x (kimi/unknown) [-] -> 1.00x (v2_standard/pushdown) [+] -> 0.67x (retry3w_2/unknown) [-] -> 0.97x (dsr1/semantic_rewrite) [=]

**DSR1**: 0.97x using `semantic_rewrite` (neutral)

**Transforms tried**:
  = `pushdown` (neutral)
  = `semantic_rewrite` (neutral)

**Untried applicable patterns**: date_cte_isolate, prefetch_fact_join, union_cte_split

**Recommendations**:
  1. **date_cte_isolate** — gold 4.00x, success rate: 5/59
  2. **prefetch_fact_join** — gold 3.77x, success rate: 5/20
  3. **union_cte_split** — gold 1.36x, success rate: no data

### Q97 — NEUTRAL (best: 1.00x) — baseline: 273ms

**Chain**: 1.00x (baseline) -> 0.98x (kimi/unknown) [=] -> 1.00x (v2_standard/date_cte_isolate) [+] -> 0.90x (dsr1/early_filter) [-]

**DSR1**: 0.90x using `early_filter` (regression)

**Transforms tried**:
  = `date_cte_isolate` (neutral)
  - `early_filter` (failed/regression)

**Untried applicable patterns**: prefetch_fact_join, union_cte_split

**Recommendations**:
  1. **prefetch_fact_join** — gold 3.77x, success rate: 5/20
  2. **union_cte_split** — gold 1.36x, success rate: no data

### Q82 — IMPROVED (best: 1.18x) — baseline: 265ms

**Chain**: 1.00x (baseline) -> 0.97x (kimi/unknown) [=] -> 1.00x (v2_standard/date_cte_isolate) [+] -> 1.18x (retry3w_1/unknown) [+] -> 0.00x (dsr1/early_filter) [X]

**DSR1**: 0.00x using `early_filter` (error)

**Transforms tried**:
  = `date_cte_isolate` (neutral)
  - `early_filter` (failed/regression)

**Untried applicable patterns**: dimension_cte_isolate, multi_dimension_prefetch, shared_dimension_multi_channel, prefetch_fact_join

**Recommendations**:
  1. **prefetch_fact_join** — gold 3.77x, success rate: 5/20
  2. **multi_dimension_prefetch** — gold 2.71x, success rate: 6/25
  3. **dimension_cte_isolate** — gold 1.93x, success rate: 1/3
  4. **shared_dimension_multi_channel** — gold 1.30x, success rate: no data

### Q87 — NEUTRAL (best: 1.00x) — baseline: 254ms

**Chain**: 1.00x (baseline) -> 0.86x (kimi/unknown) [-] -> 1.00x (v2_standard/date_cte_isolate) [+] -> 0.97x (dsr1/multi_dimension_prefetch) [=]

**DSR1**: 0.97x using `multi_dimension_prefetch` (neutral)

**Transforms tried**:
  = `date_cte_isolate` (neutral)
  = `multi_dimension_prefetch` (neutral)

**Untried applicable patterns**: prefetch_fact_join, dimension_cte_isolate, early_filter, shared_dimension_multi_channel

**Recommendations**:
  1. **early_filter** — gold 4.00x, success rate: 3/9
  2. **prefetch_fact_join** — gold 3.77x, success rate: 5/20
  3. **dimension_cte_isolate** — gold 1.93x, success rate: 1/3
  4. **shared_dimension_multi_channel** — gold 1.30x, success rate: no data

### Q52 — NEUTRAL (best: 1.08x) — baseline: 239ms

**Chain**: 1.00x (baseline) -> 1.08x (kimi/unknown) [=] -> 1.00x (v2_standard/date_cte_isolate) [+] -> 0.00x (dsr1/multi_dimension_prefetch) [X]

**DSR1**: 0.00x using `multi_dimension_prefetch` (error)

**Transforms tried**:
  = `date_cte_isolate` (neutral)
  - `multi_dimension_prefetch` (failed/regression)

**Untried applicable patterns**: prefetch_fact_join, early_filter, shared_dimension_multi_channel

**Recommendations**:
  1. **early_filter** — gold 4.00x, success rate: 3/9
  2. **prefetch_fact_join** — gold 3.77x, success rate: 5/20
  3. **shared_dimension_multi_channel** — gold 1.30x, success rate: no data

### Q39 — NEUTRAL (best: 1.05x) — baseline: 234ms

**Chain**: 1.00x (baseline) -> 0.99x (kimi/pushdown) [=] -> 1.00x (v2_standard/date_cte_isolate) [+] -> 1.05x (dsr1/pushdown) [=]

**DSR1**: 1.05x using `pushdown` (neutral)

**Transforms tried**:
  = `date_cte_isolate` (neutral)
  = `pushdown` (neutral)

**Untried applicable patterns**: prefetch_fact_join, early_filter, multi_dimension_prefetch, shared_dimension_multi_channel

**Recommendations**:
  1. **early_filter** — gold 4.00x, success rate: 3/9
  2. **prefetch_fact_join** — gold 3.77x, success rate: 5/20
  3. **multi_dimension_prefetch** — gold 2.71x, success rate: 6/25
  4. **shared_dimension_multi_channel** — gold 1.30x, success rate: no data

### Q57 — IMPROVED (best: 1.20x) — baseline: 218ms

**Chain**: 1.00x (baseline) -> 1.02x (kimi/unknown) [=] -> 1.00x (v2_standard/date_cte_isolate) [+] -> 1.20x (dsr1/prefetch_fact_join) [+]

**DSR1**: 1.20x using `prefetch_fact_join` (success)

**Transforms tried**:
  = `date_cte_isolate` (neutral)
  + `prefetch_fact_join` (succeeded)

**Untried applicable patterns**: early_filter, multi_dimension_prefetch, shared_dimension_multi_channel

**Recommendations**:
  1. **early_filter** — gold 4.00x, success rate: 3/9
  2. **multi_dimension_prefetch** — gold 2.71x, success rate: 6/25
  3. **shared_dimension_multi_channel** — gold 1.30x, success rate: no data

### Q70 — IMPROVED (best: 1.15x) — baseline: 207ms

**Chain**: 1.00x (baseline) -> 0.75x (kimi/unknown) [-] -> 1.00x (v2_standard/date_cte_isolate) [+] -> 1.15x (dsr1/decorrelate) [+]

**DSR1**: 1.15x using `decorrelate` (success)

**Transforms tried**:
  = `date_cte_isolate` (neutral)
  + `decorrelate` (succeeded)

**Untried applicable patterns**: prefetch_fact_join, composite_decorrelate_union

**Recommendations**:
  1. **prefetch_fact_join** — gold 3.77x, success rate: 5/20
  2. **composite_decorrelate_union** — gold 2.42x, success rate: no data

### Q80 — IMPROVED (best: 1.30x) — baseline: 186ms

**Chain**: 1.00x (baseline) -> 1.03x (kimi/unknown) [=] -> 1.00x (v2_standard/date_cte_isolate) [+] -> 1.30x (dsr1/prefetch_fact_join) [+]

**DSR1**: 1.30x using `prefetch_fact_join` (success)

**Transforms tried**:
  = `date_cte_isolate` (neutral)
  + `prefetch_fact_join` (succeeded)

**Untried applicable patterns**: multi_date_range_cte, union_cte_split

**Recommendations**:
  1. **multi_date_range_cte** — gold 2.35x, success rate: 0/2
  2. **union_cte_split** — gold 1.36x, success rate: no data

### Q46 — IMPROVED (best: 1.23x) — baseline: 184ms

**Chain**: 1.00x (baseline) -> 1.02x (kimi/unknown) [=] -> 1.00x (v2_standard/or_to_union) [+] -> 1.23x (dsr1/multi_dimension_prefetch) [+]

**DSR1**: 1.23x using `multi_dimension_prefetch` (success)

**Transforms tried**:
  + `multi_dimension_prefetch` (succeeded)
  = `or_to_union` (neutral)

**Untried applicable patterns**: dimension_cte_isolate, early_filter, shared_dimension_multi_channel, prefetch_fact_join

**Recommendations**:
  1. **early_filter** — gold 4.00x, success rate: 3/9
  2. **prefetch_fact_join** — gold 3.77x, success rate: 5/20
  3. **dimension_cte_isolate** — gold 1.93x, success rate: 1/3
  4. **shared_dimension_multi_channel** — gold 1.30x, success rate: no data

### Q38 — IMPROVED (best: 1.44x) — baseline: 174ms

**Chain**: 1.00x (baseline) -> 0.99x (kimi/unknown) [=] -> 1.00x (v2_standard/date_cte_isolate) [+] -> 1.44x (retry3w_2/unknown) [+] -> 1.00x (dsr1/prefetch_fact_join) [=]

**DSR1**: 1.00x using `prefetch_fact_join` (neutral)

**Transforms tried**:
  = `date_cte_isolate` (neutral)
  = `prefetch_fact_join` (neutral)

**Untried applicable patterns**: intersect_to_exists

**Recommendations**:
  1. **intersect_to_exists** — gold 1.83x, success rate: 1/1

### Q50 — IMPROVED (best: 1.11x) — baseline: 153ms

**Chain**: 1.00x (baseline) -> 0.91x (kimi/unknown) [-] -> 1.00x (v2_standard/date_cte_isolate) [+] -> 1.11x (dsr1/multi_dimension_prefetch) [+]

**DSR1**: 1.11x using `multi_dimension_prefetch` (success)

**Transforms tried**:
  = `date_cte_isolate` (neutral)
  + `multi_dimension_prefetch` (succeeded)

**Untried applicable patterns**: prefetch_fact_join, early_filter, shared_dimension_multi_channel

**Recommendations**:
  1. **early_filter** — gold 4.00x, success rate: 3/9
  2. **prefetch_fact_join** — gold 3.77x, success rate: 5/20
  3. **shared_dimension_multi_channel** — gold 1.30x, success rate: no data

### Q48 — NEUTRAL (best: 1.00x) — baseline: 151ms

**Chain**: 1.00x (baseline) -> 1.00x (kimi/unknown) [=] -> 1.00x (v2_standard/or_to_union) [+] -> 0.24x (retry3w_1/unknown) [-] -> 0.90x (dsr1/dimension_cte_isolate) [-]

**DSR1**: 0.90x using `dimension_cte_isolate` (regression)

**Transforms tried**:
  - `dimension_cte_isolate` (failed/regression)
  = `or_to_union` (neutral)

**Untried applicable patterns**: multi_dimension_prefetch, early_filter, shared_dimension_multi_channel, composite_decorrelate_union

**Recommendations**:
  1. **early_filter** — gold 4.00x, success rate: 3/9
  2. **multi_dimension_prefetch** — gold 2.71x, success rate: 6/25
  3. **composite_decorrelate_union** — gold 2.42x, success rate: no data
  4. **shared_dimension_multi_channel** — gold 1.30x, success rate: no data

### Q68 — NEUTRAL (best: 1.02x) — baseline: 141ms

**Chain**: 1.00x (baseline) -> 0.95x (kimi/unknown) [=] -> 1.00x (v2_standard/or_to_union) [+] -> 1.02x (dsr1/multi_dimension_prefetch) [=]

**DSR1**: 1.02x using `multi_dimension_prefetch` (neutral)

**Transforms tried**:
  = `multi_dimension_prefetch` (neutral)
  = `or_to_union` (neutral)

**Untried applicable patterns**: dimension_cte_isolate, early_filter, shared_dimension_multi_channel, prefetch_fact_join

**Recommendations**:
  1. **early_filter** — gold 4.00x, success rate: 3/9
  2. **prefetch_fact_join** — gold 3.77x, success rate: 5/20
  3. **dimension_cte_isolate** — gold 1.93x, success rate: 1/3
  4. **shared_dimension_multi_channel** — gold 1.30x, success rate: no data

### Q94 — NEUTRAL (best: 1.00x) — baseline: 141ms

**Chain**: 1.00x (baseline) -> 0.08x (kimi/unknown) [-] -> 1.00x (v2_standard/date_cte_isolate) [+] -> 0.24x (retry3w_2/unknown) [-] -> 0.00x (dsr1/materialize_cte) [X]

**DSR1**: 0.00x using `materialize_cte` (error)

**Transforms tried**:
  = `date_cte_isolate` (neutral)
  - `materialize_cte` (failed/regression)

**Untried applicable patterns**: prefetch_fact_join, composite_decorrelate_union

**Recommendations**:
  1. **prefetch_fact_join** — gold 3.77x, success rate: 5/20
  2. **composite_decorrelate_union** — gold 2.42x, success rate: no data

### Q79 — NEUTRAL (best: 1.05x) — baseline: 134ms

**Chain**: 1.00x (baseline) -> 1.05x (kimi/unknown) [=] -> 1.00x (v2_standard/or_to_union) [+] -> 0.98x (dsr1/multi_dimension_prefetch) [=]

**DSR1**: 0.98x using `multi_dimension_prefetch` (neutral)

**Transforms tried**:
  = `multi_dimension_prefetch` (neutral)
  = `or_to_union` (neutral)

**Untried applicable patterns**: dimension_cte_isolate, early_filter, shared_dimension_multi_channel, prefetch_fact_join

**Recommendations**:
  1. **early_filter** — gold 4.00x, success rate: 3/9
  2. **prefetch_fact_join** — gold 3.77x, success rate: 5/20
  3. **dimension_cte_isolate** — gold 1.93x, success rate: 1/3
  4. **shared_dimension_multi_channel** — gold 1.30x, success rate: no data

### Q7 — NEUTRAL (best: 1.05x) — baseline: 106ms

**Chain**: 1.00x (baseline) -> 1.00x (v2_standard/date_cte_isolate) [+] -> 1.05x (dsr1/multi_dimension_prefetch) [=] -> 1.00x (retry3w_w2/unknown) [=]

**DSR1**: 1.05x using `multi_dimension_prefetch` (neutral)

**Transforms tried**:
  = `date_cte_isolate` (neutral)
  = `multi_dimension_prefetch` (neutral)

**Untried applicable patterns**: prefetch_fact_join, dimension_cte_isolate, early_filter, shared_dimension_multi_channel

**Recommendations**:
  1. **early_filter** — gold 4.00x, success rate: 3/9
  2. **prefetch_fact_join** — gold 3.77x, success rate: 5/20
  3. **dimension_cte_isolate** — gold 1.93x, success rate: 1/3
  4. **shared_dimension_multi_channel** — gold 1.30x, success rate: no data

### Q17 — IMPROVED (best: 1.19x) — baseline: 106ms

**Chain**: 1.00x (baseline) -> 1.19x (kimi/unknown) [+] -> 0.90x (dsr1/multi_date_range_cte) [-]

**DSR1**: 0.90x using `multi_date_range_cte` (regression)

**Transforms tried**:
  - `multi_date_range_cte` (failed/regression)

**Untried applicable patterns**: prefetch_fact_join, early_filter, multi_dimension_prefetch, shared_dimension_multi_channel

**Recommendations**:
  1. **early_filter** — gold 4.00x, success rate: 3/9
  2. **prefetch_fact_join** — gold 3.77x, success rate: 5/20
  3. **multi_dimension_prefetch** — gold 2.71x, success rate: 6/25
  4. **shared_dimension_multi_channel** — gold 1.30x, success rate: no data

### Q31 — NEUTRAL (best: 1.04x) — baseline: 99ms

**Chain**: 1.00x (baseline) -> 1.04x (kimi/unknown) [=] -> 1.00x (v2_standard/pushdown) [+] -> 0.49x (dsr1/date_cte_isolate) [-]

**DSR1**: 0.49x using `date_cte_isolate` (regression)

**Transforms tried**:
  - `date_cte_isolate` (failed/regression)
  = `pushdown` (neutral)

**Untried applicable patterns**: prefetch_fact_join

**Recommendations**:
  1. **prefetch_fact_join** — gold 3.77x, success rate: 5/20

### Q98 — NEUTRAL (best: 1.00x) — baseline: 97ms

**Chain**: 1.00x (baseline) -> 0.96x (kimi/unknown) [=] -> 1.00x (v2_standard/date_cte_isolate) [+] -> 0.97x (dsr1/multi_dimension_prefetch) [=]

**DSR1**: 0.97x using `multi_dimension_prefetch` (neutral)

**Transforms tried**:
  = `date_cte_isolate` (neutral)
  = `multi_dimension_prefetch` (neutral)

**Untried applicable patterns**: prefetch_fact_join, early_filter, shared_dimension_multi_channel

**Recommendations**:
  1. **early_filter** — gold 4.00x, success rate: 3/9
  2. **prefetch_fact_join** — gold 3.77x, success rate: 5/20
  3. **shared_dimension_multi_channel** — gold 1.30x, success rate: no data

### Q34 — NEUTRAL (best: 1.08x) — baseline: 88ms

**Chain**: 1.00x (baseline) -> 0.29x (kimi/unknown) [-] -> 1.00x (v2_standard/or_to_union) [+] -> 0.80x (retry3w_2/unknown) [-] -> 1.08x (dsr1/multi_dimension_prefetch) [=]

**DSR1**: 1.08x using `multi_dimension_prefetch` (neutral)

**Transforms tried**:
  = `multi_dimension_prefetch` (neutral)
  = `or_to_union` (neutral)

**Untried applicable patterns**: dimension_cte_isolate, early_filter, shared_dimension_multi_channel, prefetch_fact_join

**Recommendations**:
  1. **early_filter** — gold 4.00x, success rate: 3/9
  2. **prefetch_fact_join** — gold 3.77x, success rate: 5/20
  3. **dimension_cte_isolate** — gold 1.93x, success rate: 1/3
  4. **shared_dimension_multi_channel** — gold 1.30x, success rate: no data

### Q49 — NEUTRAL (best: 1.02x) — baseline: 86ms

**Chain**: 1.00x (baseline) -> 1.02x (kimi/unknown) [=] -> 1.00x (v2_standard/date_cte_isolate) [+] -> 0.98x (dsr1/prefetch_fact_join+materialize_cte) [=]

**DSR1**: 0.98x using `prefetch_fact_join+materialize_cte` (neutral)

**Transforms tried**:
  = `date_cte_isolate` (neutral)
  = `materialize_cte` (neutral)
  = `prefetch_fact_join` (neutral)

**Untried applicable patterns**: union_cte_split

**Recommendations**:
  1. **union_cte_split** — gold 1.36x, success rate: no data

### Q71 — NEUTRAL (best: 1.00x) — baseline: 82ms

**Chain**: 1.00x (baseline) -> 0.96x (kimi/unknown) [=] -> 1.00x (v2_standard/or_to_union) [+] -> 0.89x (dsr1/date_cte_isolate) [-]

**DSR1**: 0.89x using `date_cte_isolate` (regression)

**Transforms tried**:
  - `date_cte_isolate` (failed/regression)
  = `or_to_union` (neutral)

**Untried applicable patterns**: prefetch_fact_join, dimension_cte_isolate, multi_dimension_prefetch, early_filter, shared_dimension_multi_channel

**Recommendations**:
  1. **early_filter** — gold 4.00x, success rate: 3/9
  2. **prefetch_fact_join** — gold 3.77x, success rate: 5/20
  3. **multi_dimension_prefetch** — gold 2.71x, success rate: 6/25
  4. **dimension_cte_isolate** — gold 1.93x, success rate: 1/3
  5. **shared_dimension_multi_channel** — gold 1.30x, success rate: no data

### Q89 — NEUTRAL (best: 1.00x) — baseline: 82ms

**Chain**: 1.00x (baseline) -> 0.60x (kimi/unknown) [-] -> 1.00x (v2_standard/or_to_union) [+] -> 0.94x (dsr1/date_cte_isolate) [-]

**DSR1**: 0.94x using `date_cte_isolate` (regression)

**Transforms tried**:
  - `date_cte_isolate` (failed/regression)
  = `or_to_union` (neutral)

**Untried applicable patterns**: prefetch_fact_join, early_filter, multi_dimension_prefetch, shared_dimension_multi_channel

**Recommendations**:
  1. **early_filter** — gold 4.00x, success rate: 3/9
  2. **prefetch_fact_join** — gold 3.77x, success rate: 5/20
  3. **multi_dimension_prefetch** — gold 2.71x, success rate: 6/25
  4. **shared_dimension_multi_channel** — gold 1.30x, success rate: no data

### Q85 — NEUTRAL (best: 1.00x) — baseline: 82ms

**Chain**: 1.00x (baseline) -> 1.00x (kimi/unknown) [=] -> 1.00x (v2_standard/or_to_union) [+] -> 0.95x (dsr1/multi_dimension_prefetch) [-]

**DSR1**: 0.95x using `multi_dimension_prefetch` (regression)

**Transforms tried**:
  - `multi_dimension_prefetch` (failed/regression)
  = `or_to_union` (neutral)

**Untried applicable patterns**: dimension_cte_isolate, early_filter, shared_dimension_multi_channel, prefetch_fact_join

**Recommendations**:
  1. **early_filter** — gold 4.00x, success rate: 3/9
  2. **prefetch_fact_join** — gold 3.77x, success rate: 5/20
  3. **dimension_cte_isolate** — gold 1.93x, success rate: 1/3
  4. **shared_dimension_multi_channel** — gold 1.30x, success rate: no data

### Q21 — NEUTRAL (best: 1.00x) — baseline: 71ms

**Chain**: 1.00x (baseline) -> 0.99x (kimi/unknown) [=] -> 1.00x (v2_standard/date_cte_isolate) [+] -> 0.00x (dsr1/multi_dimension_prefetch) [X]

**DSR1**: 0.00x using `multi_dimension_prefetch` (error)

**Transforms tried**:
  = `date_cte_isolate` (neutral)
  - `multi_dimension_prefetch` (failed/regression)

**Untried applicable patterns**: prefetch_fact_join, early_filter, shared_dimension_multi_channel

**Recommendations**:
  1. **early_filter** — gold 4.00x, success rate: 3/9
  2. **prefetch_fact_join** — gold 3.77x, success rate: 5/20
  3. **shared_dimension_multi_channel** — gold 1.30x, success rate: no data

### Q10 — NEUTRAL (best: 1.02x) — baseline: 59ms

**Chain**: 1.00x (baseline) -> 1.02x (kimi/unknown) [=] -> 1.00x (v2_standard/date_cte_isolate) [+] -> 0.95x (dsr1/semantic_rewrite) [-]

**DSR1**: 0.95x using `semantic_rewrite` (regression)

**Transforms tried**:
  = `date_cte_isolate` (neutral)
  - `semantic_rewrite` (failed/regression)

**Untried applicable patterns**: prefetch_fact_join, dimension_cte_isolate, multi_dimension_prefetch, early_filter, shared_dimension_multi_channel, materialize_cte, composite_decorrelate_union

**Recommendations**:
  1. **early_filter** — gold 4.00x, success rate: 3/9
  2. **prefetch_fact_join** — gold 3.77x, success rate: 5/20
  3. **multi_dimension_prefetch** — gold 2.71x, success rate: 6/25
  4. **composite_decorrelate_union** — gold 2.42x, success rate: no data
  5. **dimension_cte_isolate** — gold 1.93x, success rate: 1/3

### Q77 — NEUTRAL (best: 1.01x) — baseline: 58ms

**Chain**: 1.00x (baseline) -> 1.01x (kimi/unknown) [=] -> 1.00x (v2_standard/date_cte_isolate) [+] -> 0.99x (dsr1/date_cte_isolate) [=]

**DSR1**: 0.99x using `date_cte_isolate` (neutral)

**Transforms tried**:
  = `date_cte_isolate` (neutral)

**Untried applicable patterns**: prefetch_fact_join, union_cte_split

**Recommendations**:
  1. **prefetch_fact_join** — gold 3.77x, success rate: 5/20
  2. **union_cte_split** — gold 1.36x, success rate: no data

### Q19 — NEUTRAL (best: 1.04x) — baseline: 57ms

**Chain**: 1.00x (baseline) -> 1.04x (kimi/unknown) [=] -> 1.00x (v2_standard/date_cte_isolate) [+] -> 0.99x (dsr1/multi_dimension_prefetch) [=]

**DSR1**: 0.99x using `multi_dimension_prefetch` (neutral)

**Transforms tried**:
  = `date_cte_isolate` (neutral)
  = `multi_dimension_prefetch` (neutral)

**Untried applicable patterns**: prefetch_fact_join, dimension_cte_isolate, early_filter, shared_dimension_multi_channel

**Recommendations**:
  1. **early_filter** — gold 4.00x, success rate: 3/9
  2. **prefetch_fact_join** — gold 3.77x, success rate: 5/20
  3. **dimension_cte_isolate** — gold 1.93x, success rate: 1/3
  4. **shared_dimension_multi_channel** — gold 1.30x, success rate: no data

### Q33 — NEUTRAL (best: 1.08x) — baseline: 49ms

**Chain**: 1.00x (baseline) -> 1.05x (kimi/unknown) [=] -> 1.00x (v2_standard/materialize_cte) [+] -> 1.08x (dsr1/dimension_cte_isolate) [=]

**DSR1**: 1.08x using `dimension_cte_isolate` (neutral)

**Transforms tried**:
  = `dimension_cte_isolate` (neutral)
  = `materialize_cte` (neutral)

**Untried applicable patterns**: date_cte_isolate, prefetch_fact_join, multi_dimension_prefetch, early_filter, shared_dimension_multi_channel, union_cte_split

**Recommendations**:
  1. **date_cte_isolate** — gold 4.00x, success rate: 5/59
  2. **early_filter** — gold 4.00x, success rate: 3/9
  3. **prefetch_fact_join** — gold 3.77x, success rate: 5/20
  4. **multi_dimension_prefetch** — gold 2.71x, success rate: 6/25
  5. **union_cte_split** — gold 1.36x, success rate: no data

### Q58 — NEUTRAL (best: 1.06x) — baseline: 46ms

**Chain**: 1.00x (baseline) -> 1.06x (kimi/unknown) [=] -> 1.00x (v2_standard/materialize_cte) [+] -> 1.01x (retry3w_2/unknown) [=] -> 0.78x (dsr1/date_cte_isolate) [-]

**DSR1**: 0.78x using `date_cte_isolate` (regression)

**Transforms tried**:
  - `date_cte_isolate` (failed/regression)
  = `materialize_cte` (neutral)

**Untried applicable patterns**: prefetch_fact_join, multi_date_range_cte

**Recommendations**:
  1. **prefetch_fact_join** — gold 3.77x, success rate: 5/20
  2. **multi_date_range_cte** — gold 2.35x, success rate: 0/2

### Q86 — NEUTRAL (best: 1.00x) — baseline: 45ms

**Chain**: 1.00x (baseline) -> 0.92x (kimi/unknown) [-] -> 1.00x (v2_standard/date_cte_isolate) [+] -> 0.98x (dsr1/multi_dimension_prefetch) [=]

**DSR1**: 0.98x using `multi_dimension_prefetch` (neutral)

**Transforms tried**:
  = `date_cte_isolate` (neutral)
  = `multi_dimension_prefetch` (neutral)

**Untried applicable patterns**: dimension_cte_isolate, early_filter, shared_dimension_multi_channel, prefetch_fact_join

**Recommendations**:
  1. **early_filter** — gold 4.00x, success rate: 3/9
  2. **prefetch_fact_join** — gold 3.77x, success rate: 5/20
  3. **dimension_cte_isolate** — gold 1.93x, success rate: 1/3
  4. **shared_dimension_multi_channel** — gold 1.30x, success rate: no data

### Q3 — NEUTRAL (best: 1.04x) — baseline: 37ms

**Chain**: 1.00x (baseline) -> 0.98x (kimi/unknown) [=] -> 1.04x (dsr1/prefetch_fact_join) [=]

**DSR1**: 1.04x using `prefetch_fact_join` (neutral)

**Transforms tried**:
  = `prefetch_fact_join` (neutral)

**Untried applicable patterns**: date_cte_isolate, early_filter, multi_dimension_prefetch, shared_dimension_multi_channel

**Recommendations**:
  1. **date_cte_isolate** — gold 4.00x, success rate: 5/59
  2. **early_filter** — gold 4.00x, success rate: 3/9
  3. **multi_dimension_prefetch** — gold 2.71x, success rate: 6/25
  4. **shared_dimension_multi_channel** — gold 1.30x, success rate: no data

### Q42 — NEUTRAL (best: 1.00x) — baseline: 36ms

**Chain**: 1.00x (baseline) -> 0.94x (kimi/unknown) [-] -> 1.00x (v2_standard/date_cte_isolate) [+] -> 0.97x (retry3w_1/unknown) [=] -> 1.00x (dsr1/prefetch_fact_join) [=]

**DSR1**: 1.00x using `prefetch_fact_join` (neutral)

**Transforms tried**:
  = `date_cte_isolate` (neutral)
  = `prefetch_fact_join` (neutral)

**Untried applicable patterns**: early_filter, multi_dimension_prefetch, shared_dimension_multi_channel

**Recommendations**:
  1. **early_filter** — gold 4.00x, success rate: 3/9
  2. **multi_dimension_prefetch** — gold 2.71x, success rate: 6/25
  3. **shared_dimension_multi_channel** — gold 1.30x, success rate: no data

### Q55 — NEUTRAL (best: 1.03x) — baseline: 34ms

**Chain**: 1.00x (baseline) -> 0.94x (kimi/unknown) [-] -> 1.00x (v2_standard/date_cte_isolate) [+] -> 1.03x (dsr1/prefetch_fact_join) [=]

**DSR1**: 1.03x using `prefetch_fact_join` (neutral)

**Transforms tried**:
  = `date_cte_isolate` (neutral)
  = `prefetch_fact_join` (neutral)

**Untried applicable patterns**: early_filter, multi_dimension_prefetch, shared_dimension_multi_channel

**Recommendations**:
  1. **early_filter** — gold 4.00x, success rate: 3/9
  2. **multi_dimension_prefetch** — gold 2.71x, success rate: 6/25
  3. **shared_dimension_multi_channel** — gold 1.30x, success rate: no data

### Q25 — NEUTRAL (best: 1.00x) — baseline: 31ms

**Chain**: 1.00x (baseline) -> 0.98x (kimi/unknown) [=] -> 1.00x (v2_standard/date_cte_isolate) [+] -> 0.69x (retry3w_2/unknown) [-] -> 0.50x (dsr1/prefetch_fact_join) [-]

**DSR1**: 0.50x using `prefetch_fact_join` (regression)

**Transforms tried**:
  = `date_cte_isolate` (neutral)
  - `prefetch_fact_join` (failed/regression)

**Untried applicable patterns**: multi_date_range_cte, early_filter, multi_dimension_prefetch, shared_dimension_multi_channel

**Recommendations**:
  1. **early_filter** — gold 4.00x, success rate: 3/9
  2. **multi_dimension_prefetch** — gold 2.71x, success rate: 6/25
  3. **multi_date_range_cte** — gold 2.35x, success rate: 0/2
  4. **shared_dimension_multi_channel** — gold 1.30x, success rate: no data

### Q20 — NEUTRAL (best: 1.07x) — baseline: 31ms

**Chain**: 1.00x (baseline) -> 1.07x (kimi/unknown) [=] -> 1.00x (v2_standard/date_cte_isolate) [+] -> 1.01x (dsr1/multi_dimension_prefetch) [=]

**DSR1**: 1.01x using `multi_dimension_prefetch` (neutral)

**Transforms tried**:
  = `date_cte_isolate` (neutral)
  = `multi_dimension_prefetch` (neutral)

**Untried applicable patterns**: prefetch_fact_join, early_filter, shared_dimension_multi_channel

**Recommendations**:
  1. **early_filter** — gold 4.00x, success rate: 3/9
  2. **prefetch_fact_join** — gold 3.77x, success rate: 5/20
  3. **shared_dimension_multi_channel** — gold 1.30x, success rate: no data

### Q91 — NEUTRAL (best: 1.03x) — baseline: 31ms

**Chain**: 1.00x (baseline) -> 0.66x (kimi/unknown) [-] -> 1.00x (v2_standard/or_to_union) [+] -> 0.89x (retry3w_1/unknown) [-] -> 1.03x (dsr1/early_filter) [=]

**DSR1**: 1.03x using `early_filter` (neutral)

**Transforms tried**:
  = `early_filter` (neutral)
  = `or_to_union` (neutral)

**Untried applicable patterns**: date_cte_isolate, prefetch_fact_join, multi_dimension_prefetch, shared_dimension_multi_channel

**Recommendations**:
  1. **date_cte_isolate** — gold 4.00x, success rate: 5/59
  2. **prefetch_fact_join** — gold 3.77x, success rate: 5/20
  3. **multi_dimension_prefetch** — gold 2.71x, success rate: 6/25
  4. **shared_dimension_multi_channel** — gold 1.30x, success rate: no data

### Q92 — NEUTRAL (best: 1.00x) — baseline: 28ms

**Chain**: 1.00x (baseline) -> 0.95x (kimi/unknown) [=] -> 1.00x (v2_standard/decorrelate) [+] -> 0.92x (dsr1/decorrelate) [-]

**DSR1**: 0.92x using `decorrelate` (regression)

**Transforms tried**:
  - `decorrelate` (failed/regression)

**Untried applicable patterns**: date_cte_isolate, prefetch_fact_join, early_filter, multi_dimension_prefetch, shared_dimension_multi_channel

**Recommendations**:
  1. **date_cte_isolate** — gold 4.00x, success rate: 5/59
  2. **early_filter** — gold 4.00x, success rate: 3/9
  3. **prefetch_fact_join** — gold 3.77x, success rate: 5/20
  4. **multi_dimension_prefetch** — gold 2.71x, success rate: 6/25
  5. **shared_dimension_multi_channel** — gold 1.30x, success rate: no data

### Q16 — NEUTRAL (best: 1.00x) — baseline: 18ms

**Chain**: 1.00x (baseline) -> 1.00x (v2_standard/semantic_rewrite) [+] -> 0.14x (dsr1/date_cte_isolate) [-]

**DSR1**: 0.14x using `date_cte_isolate` (regression)

**Transforms tried**:
  - `date_cte_isolate` (failed/regression)
  = `semantic_rewrite` (neutral)

**Untried applicable patterns**: prefetch_fact_join, materialize_cte, composite_decorrelate_union

**Recommendations**:
  1. **prefetch_fact_join** — gold 3.77x, success rate: 5/20
  2. **composite_decorrelate_union** — gold 2.42x, success rate: no data
  3. **materialize_cte** — gold 1.37x, success rate: 2/12

### Q32 — NEUTRAL (best: 1.00x) — baseline: 14ms

**Chain**: 1.00x (baseline) -> 0.27x (kimi/unknown) [-] -> 1.00x (v2_standard/decorrelate) [+] -> 0.82x (dsr1/semantic_rewrite) [-]

**DSR1**: 0.82x using `semantic_rewrite` (regression)

**Transforms tried**:
  = `decorrelate` (neutral)
  - `semantic_rewrite` (failed/regression)

**Untried applicable patterns**: composite_decorrelate_union, date_cte_isolate, prefetch_fact_join

**Recommendations**:
  1. **date_cte_isolate** — gold 4.00x, success rate: 5/59
  2. **prefetch_fact_join** — gold 3.77x, success rate: 5/20
  3. **composite_decorrelate_union** — gold 2.42x, success rate: no data

---

## TIER 2: Medium-Value Targets

*19 queries - IMPROVED, could push to WIN threshold (1.5x).*

### Q22 — WIN (best: 1.69x) — baseline: 4230ms

**Chain**: 1.00x (baseline) -> 0.98x (kimi/unknown) [=] -> 1.00x (v2_standard/date_cte_isolate) [+] -> 1.69x (retry3w_2/unknown) [+] -> 1.10x (dsr1/prefetch_fact_join) [+]
**DSR1**: 1.10x using `prefetch_fact_join` (success)

### Q9 — WIN (best: 4.47x) — baseline: 798ms

**Chain**: 1.00x (baseline) -> 0.42x (kimi/unknown) [-] -> 4.47x (retry3w_2/unknown) [+] -> 1.28x (dsr1/pushdown) [+]
**DSR1**: 1.28x using `pushdown` (success)
**Untried**: single_pass_aggregation
**Top recs**: `single_pass_aggregation` (4.5x)

### Q69 — IMPROVED (best: 1.13x) — baseline: 96ms

**Chain**: 1.00x (baseline) -> 1.03x (kimi/unknown) [=] -> 1.00x (v2_standard/decorrelate) [+] -> 1.13x (dsr1/semantic_rewrite) [+]
**DSR1**: 1.13x using `semantic_rewrite` (success)
**Untried**: dimension_cte_isolate, multi_dimension_prefetch, early_filter, shared_dimension_multi_channel, prefetch_fact_join
**Top recs**: `early_filter` (4.0x), `prefetch_fact_join` (3.8x), `multi_dimension_prefetch` (2.7x)

### Q81 — IMPROVED (best: 1.20x) — baseline: 92ms

**Chain**: 1.00x (baseline) -> 1.00x (v2_standard/decorrelate) [+] -> 1.20x (dsr1/decorrelate, date_cte_isolate, pushdown) [+]
**DSR1**: 1.20x using `decorrelate, date_cte_isolate, pushdown` (success)
**Untried**: composite_decorrelate_union, date_cte_isolate, prefetch_fact_join
**Top recs**: `date_cte_isolate` (4.0x), `prefetch_fact_join` (3.8x), `composite_decorrelate_union` (2.4x)

### Q99 — IMPROVED (best: 1.11x) — baseline: 80ms

**Chain**: 1.00x (baseline) -> 1.00x (kimi/unknown) [=] -> 1.00x (v2_standard/date_cte_isolate) [+] -> 1.11x (dsr1/prefetch_fact_join) [+]
**DSR1**: 1.11x using `prefetch_fact_join` (success)
**Untried**: dimension_cte_isolate, multi_dimension_prefetch, early_filter, shared_dimension_multi_channel
**Top recs**: `early_filter` (4.0x), `multi_dimension_prefetch` (2.7x), `dimension_cte_isolate` (1.9x)

### Q45 — IMPROVED (best: 1.19x) — baseline: 76ms

**Chain**: 1.00x (baseline) -> 1.08x (kimi/unknown) [=] -> 1.00x (v2_standard/or_to_union) [+] -> 1.19x (dsr1/date_cte_isolate) [+]
**DSR1**: 1.19x using `date_cte_isolate` (success)
**Untried**: composite_decorrelate_union, prefetch_fact_join, decorrelate
**Top recs**: `prefetch_fact_join` (3.8x), `decorrelate` (2.9x), `composite_decorrelate_union` (2.4x)

### Q66 — IMPROVED (best: 1.23x) — baseline: 67ms

**Chain**: 1.00x (baseline) -> 1.23x (kimi/unknown) [+] -> 1.00x (v2_standard/date_cte_isolate) [+] -> 1.21x (dsr1/multi_dimension_prefetch) [+]
**DSR1**: 1.21x using `multi_dimension_prefetch` (success)
**Untried**: prefetch_fact_join, dimension_cte_isolate, early_filter, shared_dimension_multi_channel, union_cte_split
**Top recs**: `early_filter` (4.0x), `prefetch_fact_join` (3.8x), `dimension_cte_isolate` (1.9x)

### Q56 — IMPROVED (best: 1.16x) — baseline: 64ms

**Chain**: 1.00x (baseline) -> 0.92x (kimi/unknown) [-] -> 1.00x (v2_standard/date_cte_isolate) [+] -> 1.16x (dsr1/multi_dimension_prefetch) [+]
**DSR1**: 1.16x using `multi_dimension_prefetch` (success)
**Untried**: dimension_cte_isolate, early_filter, shared_dimension_multi_channel, union_cte_split
**Top recs**: `early_filter` (4.0x), `dimension_cte_isolate` (1.9x), `union_cte_split` (1.4x)

### Q30 — IMPROVED (best: 1.15x) — baseline: 63ms

**Chain**: 1.00x (baseline) -> 1.00x (v2_standard/decorrelate) [+] -> 1.15x (dsr1/decorrelate) [+]
**DSR1**: 1.15x using `decorrelate` (success)
**Untried**: composite_decorrelate_union, date_cte_isolate, prefetch_fact_join
**Top recs**: `date_cte_isolate` (4.0x), `prefetch_fact_join` (3.8x), `composite_decorrelate_union` (2.4x)

### Q53 — IMPROVED (best: 1.12x) — baseline: 59ms

**Chain**: 1.00x (baseline) -> 0.51x (kimi/unknown) [-] -> 1.00x (v2_standard/or_to_union) [+] -> 0.10x (retry3w_1/unknown) [-] -> 1.12x (dsr1/date_cte_isolate) [+]
**DSR1**: 1.12x using `date_cte_isolate` (success)
**Untried**: prefetch_fact_join, early_filter, multi_dimension_prefetch, shared_dimension_multi_channel
**Top recs**: `early_filter` (4.0x), `prefetch_fact_join` (3.8x), `multi_dimension_prefetch` (2.7x)

### Q40 — IMPROVED (best: 1.15x) — baseline: 51ms

**Chain**: 1.00x (baseline) -> 1.07x (kimi/unknown) [=] -> 1.00x (v2_standard/date_cte_isolate) [+] -> 1.15x (dsr1/multi_dimension_prefetch) [+]
**DSR1**: 1.15x using `multi_dimension_prefetch` (success)
**Untried**: prefetch_fact_join, early_filter, shared_dimension_multi_channel
**Top recs**: `early_filter` (4.0x), `prefetch_fact_join` (3.8x), `shared_dimension_multi_channel` (1.3x)

### Q6 — IMPROVED (best: 1.33x) — baseline: 50ms

**Chain**: 1.00x (baseline) -> 1.33x (kimi/unknown) [+] -> 0.85x (dsr1/date_cte_isolate) [-]
**DSR1**: 0.85x using `date_cte_isolate` (regression)
**Untried**: decorrelate, composite_decorrelate_union, prefetch_fact_join
**Top recs**: `prefetch_fact_join` (3.8x), `decorrelate` (2.9x), `composite_decorrelate_union` (2.4x)

### Q62 — IMPROVED (best: 1.23x) — baseline: 44ms

**Chain**: 1.00x (baseline) -> 1.23x (kimi/unknown) [+] -> 1.00x (v2_standard/date_cte_isolate) [+] -> 1.00x (dsr1/multi_dimension_prefetch) [=]
**DSR1**: 1.00x using `multi_dimension_prefetch` (neutral)
**Untried**: prefetch_fact_join, early_filter, shared_dimension_multi_channel
**Top recs**: `early_filter` (4.0x), `prefetch_fact_join` (3.8x), `shared_dimension_multi_channel` (1.3x)

### Q12 — IMPROVED (best: 1.27x) — baseline: 36ms

**Chain**: 1.00x (baseline) -> 1.01x (kimi/unknown) [=] -> 1.23x (retry3w_3/unknown) [+] -> 1.27x (dsr1/prefetch_fact_join) [+]
**DSR1**: 1.27x using `prefetch_fact_join` (success)
**Untried**: date_cte_isolate, early_filter, multi_dimension_prefetch, shared_dimension_multi_channel
**Top recs**: `date_cte_isolate` (4.0x), `early_filter` (4.0x), `multi_dimension_prefetch` (2.7x)

### Q37 — IMPROVED (best: 1.30x) — baseline: 27ms

**Chain**: 1.00x (baseline) -> 1.16x (kimi/unknown) [+] -> 1.00x (v2_standard/date_cte_isolate) [+] -> 1.30x (retry3w_2/unknown) [+] -> 1.10x (dsr1/semantic_rewrite) [=]
**DSR1**: 1.10x using `semantic_rewrite` (neutral)
**Untried**: prefetch_fact_join, dimension_cte_isolate, multi_dimension_prefetch, early_filter, shared_dimension_multi_channel
**Top recs**: `early_filter` (4.0x), `prefetch_fact_join` (3.8x), `multi_dimension_prefetch` (2.7x)

### Q83 — IMPROVED (best: 1.24x) — baseline: 25ms

**Chain**: 1.00x (baseline) -> 1.24x (kimi/unknown) [+] -> 1.00x (v2_standard/materialize_cte) [+] -> 1.16x (dsr1/semantic_rewrite) [+]
**DSR1**: 1.16x using `semantic_rewrite` (success)
**Untried**: dimension_cte_isolate, multi_dimension_prefetch, early_filter, shared_dimension_multi_channel, prefetch_fact_join
**Top recs**: `early_filter` (4.0x), `prefetch_fact_join` (3.8x), `multi_dimension_prefetch` (2.7x)

### Q84 — IMPROVED (best: 1.22x) — baseline: 22ms

**Chain**: 1.00x (baseline) -> 1.22x (kimi/unknown) [+] -> 1.00x (v2_standard/reorder_join) [+] -> 1.10x (dsr1/early_filter) [+]
**DSR1**: 1.10x using `early_filter` (success)
**Untried**: dimension_cte_isolate, multi_dimension_prefetch, shared_dimension_multi_channel, prefetch_fact_join
**Top recs**: `prefetch_fact_join` (3.8x), `multi_dimension_prefetch` (2.7x), `dimension_cte_isolate` (1.9x)

### Q61 — IMPROVED (best: 1.46x) — baseline: 14ms

**Chain**: 1.00x (baseline) -> 0.40x (kimi/unknown) [-] -> 1.00x (v2_standard/materialize_cte) [+] -> 1.46x (dsr1/dimension_cte_isolate) [+]
**DSR1**: 1.46x using `dimension_cte_isolate` (success)
**Untried**: date_cte_isolate, prefetch_fact_join, multi_dimension_prefetch, early_filter, shared_dimension_multi_channel
**Top recs**: `date_cte_isolate` (4.0x), `early_filter` (4.0x), `prefetch_fact_join` (3.8x)

### Q44 — IMPROVED (best: 1.37x) — baseline: 4ms

**Chain**: 1.00x (baseline) -> 1.00x (v2_standard/materialize_cte) [+] -> 1.37x (dsr1/materialize_cte) [+]
**DSR1**: 1.37x using `materialize_cte` (success)
**Untried**: single_pass_aggregation, pushdown
**Top recs**: `single_pass_aggregation` (4.5x), `pushdown` (2.1x)

---

## TIER 3: Already Winning / Low Runtime

*17 queries - WIN or very fast baseline. Lower priority.*

| Query | Best | Baseline | Best Attempt | Untried |
|-------|------|----------|--------------|---------|
| Q63 | 3.77x | 387ms | retry3w_2 | date_cte_isolate, prefetch_fact_join, multi_dimension_prefetch |
| Q65 | 1.60x | 355ms | dsr1 | prefetch_fact_join, early_filter, multi_dimension_prefetch |
| Q59 | 1.68x | 353ms | dsr1 | prefetch_fact_join |
| Q88 | 3.32x | 251ms | dsr1 | pushdown, or_to_union, composite_decorrelate_union |
| Q35 | 2.42x | 214ms | dsr1 | prefetch_fact_join, composite_decorrelate_union, dimension_cte_isolate |
| Q27 | 1.58x | 177ms | dsr1 | dimension_cte_isolate, early_filter, shared_dimension_multi_channel |
| Q26 | 1.93x | 156ms | retry3w_1 | dimension_cte_isolate, multi_dimension_prefetch, early_filter |
| Q29 | 2.35x | 121ms | retry3w_1 | multi_date_range_cte, early_filter, multi_dimension_prefetch |
| Q73 | 1.57x | 112ms | retry3w_2 | dimension_cte_isolate, early_filter, shared_dimension_multi_channel |
| Q5 | 1.89x | 110ms | retry3w_1 | date_cte_isolate, union_cte_split, dimension_cte_isolate |
| Q93 | 2.73x | 109ms | kimi | multi_dimension_prefetch, shared_dimension_multi_channel |
| Q43 | 2.71x | 86ms | retry3w_2 | date_cte_isolate, dimension_cte_isolate, multi_dimension_prefetch |
| Q15 | 2.78x | 51ms | kimi | composite_decorrelate_union, date_cte_isolate |
| Q96 | 1.64x | 28ms | retry3w_2 | dimension_cte_isolate, multi_dimension_prefetch, shared_dimension_multi_channel |
| Q41 | 1.63x | 22ms | dsr1 | composite_decorrelate_union, date_cte_isolate |
| Q1 | 2.92x | 22ms | kimi | composite_decorrelate_union, prefetch_fact_join, early_filter |
| Q90 | 1.57x | 16ms | kimi | date_cte_isolate, prefetch_fact_join, shared_dimension_multi_channel |

---

## Appendix: Complete Query Matrix

| Q# | Baseline(ms) | Best | Best From | DSR1 | #Tried | #Untried | Top Untried |
|----|-------------|------|-----------|------|--------|----------|-------------|
| Q1 | 22 | 2.92x | kimi | 0.71x | 2 | 5 | composite_decorrelate_union |
| Q2 | 937 | 1.00x | baseline | 0.00x | 2 | 1 | prefetch_fact_join |
| Q3 | 37 | 1.04x | dsr1 | 1.04x | 1 | 4 | date_cte_isolate |
| Q4 | 1839 | 1.12x | dsr1 | 1.12x | 1 | 4 | multi_date_range_cte |
| Q5 | 110 | 1.89x | retry3w_1 | 0.96x | 1 | 6 | date_cte_isolate |
| Q6 | 50 | 1.33x | kimi | 0.85x | 1 | 3 | decorrelate |
| Q7 | 106 | 1.05x | dsr1 | 1.05x | 2 | 4 | prefetch_fact_join |
| Q8 | 362 | 1.16x | dsr1 | 1.16x | 1 | 3 | decorrelate |
| Q9 | 798 | 4.47x | retry3w_2 | 1.28x | 1 | 1 | single_pass_aggregation |
| Q10 | 59 | 1.02x | kimi | 0.95x | 2 | 7 | prefetch_fact_join |
| Q11 | 953 | 1.06x | dsr1 | 1.06x | 1 | 4 | decorrelate |
| Q12 | 36 | 1.27x | dsr1 | 1.27x | 1 | 4 | date_cte_isolate |
| Q13 | 981 | 1.01x | kimi | 0.00x | 2 | 4 | dimension_cte_isolate |
| Q14 | 691 | 1.40x | dsr1 | 1.40x | 2 | 1 | prefetch_fact_join |
| Q15 | 51 | 2.78x | kimi | 1.09x | 2 | 2 | composite_decorrelate_union |
| Q16 | 18 | 1.00x | baseline | 0.14x | 2 | 3 | prefetch_fact_join |
| Q17 | 106 | 1.19x | kimi | 0.90x | 1 | 4 | prefetch_fact_join |
| Q18 | 424 | 1.14x | kimi | 0.00x | 2 | 4 | dimension_cte_isolate |
| Q19 | 57 | 1.04x | kimi | 0.99x | 2 | 4 | prefetch_fact_join |
| Q20 | 31 | 1.07x | kimi | 1.01x | 2 | 3 | prefetch_fact_join |
| Q21 | 71 | 1.00x | baseline | 0.00x | 2 | 3 | prefetch_fact_join |
| Q22 | 4230 | 1.69x | retry3w_2 | 1.10x | 2 | 0 | — |
| Q23 | 1854 | 1.06x | kimi | 1.02x | 1 | 3 | decorrelate |
| Q24 | 780 | 1.00x | baseline | 0.00x | 2 | 2 | composite_decorrelate_union |
| Q25 | 31 | 1.00x | baseline | 0.50x | 2 | 4 | multi_date_range_cte |
| Q26 | 156 | 1.93x | retry3w_1 | 1.01x | 2 | 5 | dimension_cte_isolate |
| Q27 | 177 | 1.58x | dsr1 | 1.58x | 2 | 4 | dimension_cte_isolate |
| Q28 | 327 | 1.33x | kimi | 0.92x | 1 | 2 | single_pass_aggregation |
| Q29 | 121 | 2.35x | retry3w_1 | 1.00x | 2 | 4 | multi_date_range_cte |
| Q30 | 63 | 1.15x | dsr1 | 1.15x | 1 | 3 | composite_decorrelate_union |
| Q31 | 99 | 1.04x | kimi | 0.49x | 2 | 1 | prefetch_fact_join |
| Q32 | 14 | 1.00x | baseline | 0.82x | 2 | 3 | composite_decorrelate_union |
| Q33 | 49 | 1.08x | dsr1 | 1.08x | 2 | 6 | date_cte_isolate |
| Q34 | 88 | 1.08x | dsr1 | 1.08x | 2 | 4 | dimension_cte_isolate |
| Q35 | 214 | 2.42x | dsr1 | 2.42x | 2 | 6 | prefetch_fact_join |
| Q36 | 567 | 1.00x | baseline | 0.91x | 2 | 5 | date_cte_isolate |
| Q37 | 27 | 1.30x | retry3w_2 | 1.10x | 2 | 5 | prefetch_fact_join |
| Q38 | 174 | 1.44x | retry3w_2 | 1.00x | 2 | 1 | intersect_to_exists |
| Q39 | 234 | 1.05x | dsr1 | 1.05x | 2 | 4 | prefetch_fact_join |
| Q40 | 51 | 1.15x | dsr1 | 1.15x | 2 | 3 | prefetch_fact_join |
| Q41 | 22 | 1.63x | dsr1 | 1.63x | 2 | 2 | composite_decorrelate_union |
| Q42 | 36 | 1.00x | baseline | 1.00x | 2 | 3 | early_filter |
| Q43 | 86 | 2.71x | retry3w_2 | 1.10x | 2 | 4 | date_cte_isolate |
| Q44 | 4 | 1.37x | dsr1 | 1.37x | 1 | 2 | single_pass_aggregation |
| Q45 | 76 | 1.19x | dsr1 | 1.19x | 2 | 3 | composite_decorrelate_union |
| Q46 | 184 | 1.23x | dsr1 | 1.23x | 2 | 4 | dimension_cte_isolate |
| Q47 | 415 | 1.00x | baseline | 0.91x | 2 | 4 | prefetch_fact_join |
| Q48 | 151 | 1.00x | baseline | 0.90x | 2 | 4 | multi_dimension_prefetch |
| Q49 | 86 | 1.02x | kimi | 0.98x | 3 | 1 | union_cte_split |
| Q50 | 153 | 1.11x | dsr1 | 1.11x | 2 | 3 | prefetch_fact_join |
| Q51 | 1424 | 1.00x | baseline | 0.87x | 2 | 0 | — |
| Q52 | 239 | 1.08x | kimi | 0.00x | 2 | 3 | prefetch_fact_join |
| Q53 | 59 | 1.12x | dsr1 | 1.12x | 2 | 4 | prefetch_fact_join |
| Q54 | 389 | 1.03x | kimi | 0.00x | 2 | 0 | — |
| Q55 | 34 | 1.03x | dsr1 | 1.03x | 2 | 3 | early_filter |
| Q56 | 64 | 1.16x | dsr1 | 1.16x | 2 | 4 | dimension_cte_isolate |
| Q57 | 218 | 1.20x | dsr1 | 1.20x | 2 | 3 | early_filter |
| Q58 | 46 | 1.06x | kimi | 0.78x | 2 | 2 | prefetch_fact_join |
| Q59 | 353 | 1.68x | dsr1 | 1.68x | 2 | 1 | prefetch_fact_join |
| Q60 | 378 | 1.02x | kimi | 0.00x | 2 | 5 | prefetch_fact_join |
| Q61 | 14 | 1.46x | dsr1 | 1.46x | 2 | 5 | date_cte_isolate |
| Q62 | 44 | 1.23x | kimi | 1.00x | 2 | 3 | prefetch_fact_join |
| Q63 | 387 | 3.77x | retry3w_2 | 1.00x | 2 | 4 | date_cte_isolate |
| Q64 | 3841 | 1.01x | kimi | 0.00x | 2 | 0 | — |
| Q65 | 355 | 1.60x | dsr1 | 1.60x | 2 | 4 | prefetch_fact_join |
| Q66 | 67 | 1.23x | kimi | 1.21x | 2 | 5 | prefetch_fact_join |
| Q67 | 4509 | 1.00x | baseline | 0.85x | 2 | 3 | prefetch_fact_join |
| Q68 | 141 | 1.02x | dsr1 | 1.02x | 2 | 4 | dimension_cte_isolate |
| Q69 | 96 | 1.13x | dsr1 | 1.13x | 2 | 5 | dimension_cte_isolate |
| Q70 | 207 | 1.15x | dsr1 | 1.15x | 2 | 2 | prefetch_fact_join |
| Q71 | 82 | 1.00x | baseline | 0.89x | 2 | 5 | prefetch_fact_join |
| Q72 | 348 | 1.00x | baseline | 0.77x | 2 | 4 | multi_date_range_cte |
| Q73 | 112 | 1.57x | retry3w_2 | 0.87x | 2 | 4 | dimension_cte_isolate |
| Q74 | 493 | 1.36x | kimi | 0.68x | 1 | 3 | union_cte_split |
| Q75 | 325 | 1.00x | baseline | 0.97x | 2 | 3 | date_cte_isolate |
| Q76 | 513 | 1.10x | kimi | 0.00x | 2 | 4 | prefetch_fact_join |
| Q77 | 58 | 1.01x | kimi | 0.99x | 1 | 2 | prefetch_fact_join |
| Q78 | 936 | 1.08x | dsr1 | 1.08x | 2 | 1 | prefetch_fact_join |
| Q79 | 134 | 1.05x | kimi | 0.98x | 2 | 4 | dimension_cte_isolate |
| Q80 | 186 | 1.30x | dsr1 | 1.30x | 2 | 2 | multi_date_range_cte |
| Q81 | 92 | 1.20x | dsr1 | 1.20x | 2 | 3 | composite_decorrelate_union |
| Q82 | 265 | 1.18x | retry3w_1 | 0.00x | 2 | 4 | dimension_cte_isolate |
| Q83 | 25 | 1.24x | kimi | 1.16x | 2 | 5 | dimension_cte_isolate |
| Q84 | 22 | 1.22x | kimi | 1.10x | 2 | 4 | dimension_cte_isolate |
| Q85 | 82 | 1.00x | baseline | 0.95x | 2 | 4 | dimension_cte_isolate |
| Q86 | 45 | 1.00x | baseline | 0.98x | 2 | 4 | dimension_cte_isolate |
| Q87 | 254 | 1.00x | baseline | 0.97x | 2 | 4 | prefetch_fact_join |
| Q88 | 251 | 3.32x | dsr1 | 3.32x | 2 | 3 | pushdown |
| Q89 | 82 | 1.00x | baseline | 0.94x | 2 | 4 | prefetch_fact_join |
| Q90 | 16 | 1.57x | kimi | 0.59x | 3 | 3 | date_cte_isolate |
| Q91 | 31 | 1.03x | dsr1 | 1.03x | 2 | 4 | date_cte_isolate |
| Q92 | 28 | 1.00x | baseline | 0.92x | 1 | 5 | date_cte_isolate |
| Q93 | 109 | 2.73x | kimi | 0.34x | 3 | 2 | multi_dimension_prefetch |
| Q94 | 141 | 1.00x | baseline | 0.00x | 2 | 2 | prefetch_fact_join |
| Q95 | 390 | 1.37x | kimi | 0.54x | 2 | 3 | composite_decorrelate_union |
| Q96 | 28 | 1.64x | retry3w_2 | 0.98x | 2 | 4 | dimension_cte_isolate |
| Q97 | 273 | 1.00x | baseline | 0.90x | 2 | 2 | prefetch_fact_join |
| Q98 | 97 | 1.00x | baseline | 0.97x | 2 | 3 | prefetch_fact_join |
| Q99 | 80 | 1.11x | dsr1 | 1.11x | 2 | 4 | dimension_cte_isolate |
