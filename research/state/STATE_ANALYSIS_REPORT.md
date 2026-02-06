# STATE ANALYSIS REPORT: TPC-DS Optimization Strategy

**Generated**: 2026-02-06

**Scope**: 99 TPC-DS Queries

**Strategy**: Prioritize by RUNTIME (absolute time savings), not speedup %

# EXECUTIVE DASHBOARD

## Progress Summary

- WIN: 0
- IMPROVED: 0
- NEUTRAL: 39
- REGRESSION: 35
- ERROR: 3
- GOLD_EXAMPLE: 6

## Top 20 Longest-Running Queries (Highest Value Targets)

1. Q23: 24404ms - 1.06x (NEUTRAL)
2. Q4: 10209ms - 1.03x (NEUTRAL)
3. Q14: 9211ms - 0.95x (REGRESSION)
4. Q78: 9002ms - 1.01x (NEUTRAL)
5. Q51: 7935ms - 1.00x (FAILS_VALIDATION)
6. Q22: 7655ms - 1.69x (REGRESSION)
7. Q11: 6017ms - 0.98x (REGRESSION)
8. Q95: 5151ms - 1.37x (MODERATE_WIN)
9. Q74: 4130ms - 1.36x (GOLD_EXAMPLE)
10. Q64: 3841ms - 1.01x (NEUTRAL)
11. Q28: 3731ms - 1.33x (MODERATE_WIN)
12. Q65: 3548ms - 1.00x (FAILS_VALIDATION)
13. Q59: 2873ms - 1.00x (FAILS_VALIDATION)
14. Q93: 2861ms - 2.73x (GOLD_EXAMPLE)
15. Q75: 2845ms - 1.00x (REGRESSION)
16. Q47: 2706ms - 1.00x (NEUTRAL)
17. Q97: 2643ms - 1.00x (REGRESSION)
18. Q9: 2206ms - 4.47x (REGRESSION)
19. Q88: 2070ms - 1.00x (REGRESSION)
20. Q87: 1822ms - 1.00x (REGRESSION)

## Transform Effectiveness

- prefetch_fact_join: 100% success (1/1), 3.77x avg
- union_cte_split: 100% success (1/1), 1.36x avg
- date_cte_isolate: 0% success (0/40), 4.00x avg
- decorrelate: 0% success (0/3), 2.92x avg
- dimension_cte_isolate: 0% success (0/2), 1.93x avg
- early_filter: 0% success (0/7), 4.00x avg
- intersect_to_exists: 0% success (0/1), 1.83x avg
- materialize_cte: 0% success (0/9), 1.37x avg
- multi_date_range_cte: 0% success (0/2), 2.35x avg
- multi_dimension_prefetch: 0% success (0/3), 2.71x avg


# TIER 1: HIGH-VALUE TARGETS (Priority > 70)


**Priority Score**: 90.0 (TOP_20%)

### Q88: Q88
**Classification**: REGRESSION
**Runtime**: 2069.9ms baseline (TOP_20%)
**Time Savings Potential**: 1035ms at 2x, 1380ms at 3x
**Current Best**: 1.00x (baseline)
**Gap to Expectation**: 5.00x

**State History**:
- baseline: 1.00x [none] - success
- kimi: 0.99x [none] - neutral
- v2_standard: 1.00x [materialize_cte] - success

**Transforms Attempted**:
- ✗ materialize_cte

**Gold Patterns NOT Tried**: date_cte_isolate, decorrelate, dimension_cte_isolate, early_filter, intersect_to_exists, multi_date_range_cte, multi_dimension_prefetch, or_to_union, prefetch_fact_join, pushdown, single_pass_aggregation, union_cte_split

**Top Recommendations**:

1. **prefetch_fact_join** [CONFIDENCE: 93%] [RISK: LOW]
   - Expected: 3.77x improvement
   - Success Rate: 100%
   - Rationale: Transform has 100% success rate with 3.77x average speedup

2. **single_pass_aggregation** [CONFIDENCE: 50%] [RISK: HIGH]
   - Expected: 4.47x improvement
   - Success Rate: 0%
   - Rationale: Transform has 0% success rate with 4.47x average speedup

**Priority Score**: 83.4 (TOP_20%)

### Q64: Q64
**Classification**: NEUTRAL
**Runtime**: 3841.1ms baseline (TOP_20%)
**Time Savings Potential**: 1921ms at 2x, 2561ms at 3x
**Current Best**: 1.01x (baseline)
**Gap to Expectation**: 1.70x

**State History**:
- baseline: 1.00x [none] - success
- kimi: 1.01x [none] - neutral
- v2_standard: 1.00x [pushdown] - success

**Transforms Attempted**:
- ✗ pushdown

**Gold Patterns NOT Tried**: date_cte_isolate, decorrelate, dimension_cte_isolate, early_filter, intersect_to_exists, materialize_cte, multi_date_range_cte, multi_dimension_prefetch, or_to_union, prefetch_fact_join, single_pass_aggregation, union_cte_split

**Top Recommendations**:

1. **prefetch_fact_join** [CONFIDENCE: 93%] [RISK: LOW]
   - Expected: 3.77x improvement
   - Success Rate: 100%
   - Rationale: Transform has 100% success rate with 3.77x average speedup

2. **single_pass_aggregation** [CONFIDENCE: 50%] [RISK: HIGH]
   - Expected: 4.47x improvement
   - Success Rate: 0%
   - Rationale: Transform has 0% success rate with 4.47x average speedup

**Priority Score**: 82.5 (TOP_20%)

### Q47: Q47
**Classification**: NEUTRAL
**Runtime**: 2705.8ms baseline (TOP_20%)
**Time Savings Potential**: 1353ms at 2x, 1804ms at 3x
**Current Best**: 1.00x (baseline)
**Gap to Expectation**: 1.50x

**State History**:
- baseline: 1.00x [none] - success
- kimi: 1.00x [none] - neutral
- v2_standard: 1.00x [or_to_union] - success

**Transforms Attempted**:
- ✗ or_to_union

**Gold Patterns NOT Tried**: date_cte_isolate, decorrelate, dimension_cte_isolate, early_filter, intersect_to_exists, materialize_cte, multi_date_range_cte, multi_dimension_prefetch, prefetch_fact_join, pushdown, single_pass_aggregation, union_cte_split

**Top Recommendations**:

1. **prefetch_fact_join** [CONFIDENCE: 93%] [RISK: LOW]
   - Expected: 3.77x improvement
   - Success Rate: 100%
   - Rationale: Transform has 100% success rate with 3.77x average speedup

2. **single_pass_aggregation** [CONFIDENCE: 50%] [RISK: HIGH]
   - Expected: 4.47x improvement
   - Success Rate: 0%
   - Rationale: Transform has 0% success rate with 4.47x average speedup

**Priority Score**: 82.3 (TOP_20%)

### Q78: Q78
**Classification**: NEUTRAL
**Runtime**: 9002.1ms baseline (TOP_20%)
**Time Savings Potential**: 4501ms at 2x, 6001ms at 3x
**Current Best**: 1.01x (baseline)
**Gap to Expectation**: 1.49x

**State History**:
- baseline: 1.00x [none] - success
- kimi: 1.01x [none] - neutral
- v2_standard: 1.00x [pushdown] - success

**Transforms Attempted**:
- ✗ pushdown

**Gold Patterns NOT Tried**: date_cte_isolate, decorrelate, dimension_cte_isolate, early_filter, intersect_to_exists, materialize_cte, multi_date_range_cte, multi_dimension_prefetch, or_to_union, prefetch_fact_join, single_pass_aggregation, union_cte_split

**Top Recommendations**:

1. **prefetch_fact_join** [CONFIDENCE: 93%] [RISK: LOW]
   - Expected: 3.77x improvement
   - Success Rate: 100%
   - Rationale: Transform has 100% success rate with 3.77x average speedup

2. **single_pass_aggregation** [CONFIDENCE: 50%] [RISK: HIGH]
   - Expected: 4.47x improvement
   - Success Rate: 0%
   - Rationale: Transform has 0% success rate with 4.47x average speedup

**Priority Score**: 80.0 (TOP_20%)

### Q97: Q97
**Classification**: REGRESSION
**Runtime**: 2642.7ms baseline (TOP_20%)
**Time Savings Potential**: 1321ms at 2x, 1762ms at 3x
**Current Best**: 1.00x (baseline)
**Gap to Expectation**: 2.00x

**State History**:
- baseline: 1.00x [none] - success
- kimi: 0.98x [none] - neutral
- v2_standard: 1.00x [date_cte_isolate] - success

**Transforms Attempted**:
- ✗ date_cte_isolate

**Gold Patterns NOT Tried**: decorrelate, dimension_cte_isolate, early_filter, intersect_to_exists, materialize_cte, multi_date_range_cte, multi_dimension_prefetch, or_to_union, prefetch_fact_join, pushdown, single_pass_aggregation, union_cte_split

**Top Recommendations**:

1. **prefetch_fact_join** [CONFIDENCE: 93%] [RISK: LOW]
   - Expected: 3.77x improvement
   - Success Rate: 100%
   - Rationale: Transform has 100% success rate with 3.77x average speedup

2. **single_pass_aggregation** [CONFIDENCE: 50%] [RISK: HIGH]
   - Expected: 4.47x improvement
   - Success Rate: 0%
   - Rationale: Transform has 0% success rate with 4.47x average speedup

**Priority Score**: 77.5 (TOP_20%)

### Q75: Q75
**Classification**: REGRESSION
**Runtime**: 2844.6ms baseline (TOP_20%)
**Time Savings Potential**: 1422ms at 2x, 1896ms at 3x
**Current Best**: 1.00x (W2)
**Gap to Expectation**: 1.50x

**State History**:
- baseline: 1.00x [none] - success
- kimi: 0.94x [none] - regression
- v2_standard: 1.00x [pushdown] - success

**Transforms Attempted**:
- ✗ pushdown

**Gold Patterns NOT Tried**: date_cte_isolate, decorrelate, dimension_cte_isolate, early_filter, intersect_to_exists, materialize_cte, multi_date_range_cte, multi_dimension_prefetch, or_to_union, prefetch_fact_join, single_pass_aggregation, union_cte_split

**Top Recommendations**:

1. **prefetch_fact_join** [CONFIDENCE: 93%] [RISK: LOW]
   - Expected: 3.77x improvement
   - Success Rate: 100%
   - Rationale: Transform has 100% success rate with 3.77x average speedup

2. **single_pass_aggregation** [CONFIDENCE: 50%] [RISK: HIGH]
   - Expected: 4.47x improvement
   - Success Rate: 0%
   - Rationale: Transform has 0% success rate with 4.47x average speedup

**Priority Score**: 76.7 (TOP_20%)

### Q28: Q28
**Classification**: MODERATE_WIN
**Runtime**: 3730.7ms baseline (TOP_20%)
**Time Savings Potential**: 1865ms at 2x, 2487ms at 3x
**Current Best**: 1.33x (baseline)
**Gap to Expectation**: 4.47x

**State History**:
- baseline: 1.00x [none] - success
- kimi: 1.33x [none] - success
- v2_standard: 1.00x [semantic_rewrite] - success

**Transforms Attempted**:
- ✗ semantic_rewrite

**Gold Patterns NOT Tried**: date_cte_isolate, decorrelate, dimension_cte_isolate, early_filter, intersect_to_exists, materialize_cte, multi_date_range_cte, multi_dimension_prefetch, or_to_union, prefetch_fact_join, pushdown, single_pass_aggregation, union_cte_split

**Top Recommendations**:

1. **prefetch_fact_join** [CONFIDENCE: 93%] [RISK: LOW]
   - Expected: 3.77x improvement
   - Success Rate: 100%
   - Rationale: Transform has 100% success rate with 3.77x average speedup

2. **single_pass_aggregation** [CONFIDENCE: 50%] [RISK: HIGH]
   - Expected: 4.47x improvement
   - Success Rate: 0%
   - Rationale: Transform has 0% success rate with 4.47x average speedup

**Priority Score**: 76.6 (TOP_20%)

### Q23: Q23
**Classification**: NEUTRAL
**Runtime**: 24403.6ms baseline (TOP_20%)
**Time Savings Potential**: 12202ms at 2x, 16269ms at 3x
**Current Best**: 1.06x (baseline)
**Gap to Expectation**: 0.44x

**State History**:
- baseline: 1.00x [none] - success
- kimi: 1.06x [none] - neutral
- v2_standard: 1.00x [date_cte_isolate] - success

**Transforms Attempted**:
- ✗ date_cte_isolate

**Gold Patterns NOT Tried**: decorrelate, dimension_cte_isolate, early_filter, intersect_to_exists, materialize_cte, multi_date_range_cte, multi_dimension_prefetch, or_to_union, prefetch_fact_join, pushdown, single_pass_aggregation, union_cte_split

**Top Recommendations**:

1. **prefetch_fact_join** [CONFIDENCE: 93%] [RISK: LOW]
   - Expected: 3.77x improvement
   - Success Rate: 100%
   - Rationale: Transform has 100% success rate with 3.77x average speedup

2. **single_pass_aggregation** [CONFIDENCE: 50%] [RISK: HIGH]
   - Expected: 4.47x improvement
   - Success Rate: 0%
   - Rationale: Transform has 0% success rate with 4.47x average speedup

**Priority Score**: 74.7 (TOP_20%)

### Q4: Q4
**Classification**: NEUTRAL
**Runtime**: 10208.8ms baseline (TOP_20%)
**Time Savings Potential**: 5104ms at 2x, 6806ms at 3x
**Current Best**: 1.03x (W3)
**Gap to Expectation**: 0.03x

**State History**:
- baseline: 1.00x [none] - success
- kimi: 1.03x [none] - neutral

**Gold Patterns NOT Tried**: date_cte_isolate, decorrelate, dimension_cte_isolate, early_filter, intersect_to_exists, materialize_cte, multi_date_range_cte, multi_dimension_prefetch, or_to_union, prefetch_fact_join, pushdown, single_pass_aggregation, union_cte_split

**Top Recommendations**:

1. **prefetch_fact_join** [CONFIDENCE: 93%] [RISK: LOW]
   - Expected: 3.77x improvement
   - Success Rate: 100%
   - Rationale: Transform has 100% success rate with 3.77x average speedup

2. **single_pass_aggregation** [CONFIDENCE: 50%] [RISK: HIGH]
   - Expected: 4.47x improvement
   - Success Rate: 0%
   - Rationale: Transform has 0% success rate with 4.47x average speedup

**Priority Score**: 72.5 (TOP_20%)

### Q59: Q59
**Classification**: FAILS_VALIDATION
**Runtime**: 2873.2ms baseline (TOP_20%)
**Time Savings Potential**: 1437ms at 2x, 1915ms at 3x
**Current Best**: 1.00x (baseline)
**Gap to Expectation**: 2.50x

**State History**:
- baseline: 1.00x [none] - success
- v2_standard: 1.00x [pushdown] - success

**Transforms Attempted**:
- ✗ pushdown

**Gold Patterns NOT Tried**: date_cte_isolate, decorrelate, dimension_cte_isolate, early_filter, intersect_to_exists, materialize_cte, multi_date_range_cte, multi_dimension_prefetch, or_to_union, prefetch_fact_join, single_pass_aggregation, union_cte_split

**Top Recommendations**:

1. **prefetch_fact_join** [CONFIDENCE: 93%] [RISK: LOW]
   - Expected: 3.77x improvement
   - Success Rate: 100%
   - Rationale: Transform has 100% success rate with 3.77x average speedup

2. **single_pass_aggregation** [CONFIDENCE: 50%] [RISK: HIGH]
   - Expected: 4.47x improvement
   - Success Rate: 0%
   - Rationale: Transform has 0% success rate with 4.47x average speedup

**Priority Score**: 71.5 (TOP_20%)

### Q22: Q22
**Classification**: REGRESSION
**Runtime**: 7654.7ms baseline (TOP_20%)
**Time Savings Potential**: 3827ms at 2x, 5103ms at 3x
**Current Best**: 1.69x (W2)
**Gap to Expectation**: 1.31x

**State History**:
- baseline: 1.00x [none] - success
- kimi: 0.98x [none] - neutral
- v2_standard: 1.00x [date_cte_isolate] - success

**Transforms Attempted**:
- ✗ date_cte_isolate

**Gold Patterns NOT Tried**: decorrelate, dimension_cte_isolate, early_filter, intersect_to_exists, materialize_cte, multi_date_range_cte, multi_dimension_prefetch, or_to_union, prefetch_fact_join, pushdown, single_pass_aggregation, union_cte_split

**Top Recommendations**:

1. **prefetch_fact_join** [CONFIDENCE: 93%] [RISK: LOW]
   - Expected: 3.77x improvement
   - Success Rate: 100%
   - Rationale: Transform has 100% success rate with 3.77x average speedup

2. **single_pass_aggregation** [CONFIDENCE: 50%] [RISK: HIGH]
   - Expected: 4.47x improvement
   - Success Rate: 0%
   - Rationale: Transform has 0% success rate with 4.47x average speedup

**Priority Score**: 71.0 (TOP_20%)

### Q65: Q65
**Classification**: FAILS_VALIDATION
**Runtime**: 3547.9ms baseline (TOP_20%)
**Time Savings Potential**: 1774ms at 2x, 2365ms at 3x
**Current Best**: 1.00x (baseline)
**Gap to Expectation**: 2.20x

**State History**:
- baseline: 1.00x [none] - success
- v2_standard: 1.00x [date_cte_isolate] - success

**Transforms Attempted**:
- ✗ date_cte_isolate

**Gold Patterns NOT Tried**: decorrelate, dimension_cte_isolate, early_filter, intersect_to_exists, materialize_cte, multi_date_range_cte, multi_dimension_prefetch, or_to_union, prefetch_fact_join, pushdown, single_pass_aggregation, union_cte_split

**Top Recommendations**:

1. **prefetch_fact_join** [CONFIDENCE: 93%] [RISK: LOW]
   - Expected: 3.77x improvement
   - Success Rate: 100%
   - Rationale: Transform has 100% success rate with 3.77x average speedup

2. **single_pass_aggregation** [CONFIDENCE: 50%] [RISK: HIGH]
   - Expected: 4.47x improvement
   - Success Rate: 0%
   - Rationale: Transform has 0% success rate with 4.47x average speedup

**Priority Score**: 70.8 (TOP_20%)

### Q14: Q14
**Classification**: REGRESSION
**Runtime**: 9210.5ms baseline (TOP_20%)
**Time Savings Potential**: 4605ms at 2x, 6140ms at 3x
**Current Best**: 0.95x (baseline)
**Gap to Expectation**: 0.05x

**State History**:
- baseline: 1.00x [none] - success
- kimi: 0.95x [none] - neutral

**Gold Patterns NOT Tried**: date_cte_isolate, decorrelate, dimension_cte_isolate, early_filter, intersect_to_exists, materialize_cte, multi_date_range_cte, multi_dimension_prefetch, or_to_union, prefetch_fact_join, pushdown, single_pass_aggregation, union_cte_split

**Top Recommendations**:

1. **prefetch_fact_join** [CONFIDENCE: 93%] [RISK: LOW]
   - Expected: 3.77x improvement
   - Success Rate: 100%
   - Rationale: Transform has 100% success rate with 3.77x average speedup

2. **single_pass_aggregation** [CONFIDENCE: 50%] [RISK: HIGH]
   - Expected: 4.47x improvement
   - Success Rate: 0%
   - Rationale: Transform has 0% success rate with 4.47x average speedup

**Priority Score**: 70.3 (TOP_20%)

### Q11: Q11
**Classification**: REGRESSION
**Runtime**: 6017.1ms baseline (TOP_20%)
**Time Savings Potential**: 3009ms at 2x, 4011ms at 3x
**Current Best**: 0.98x (baseline)
**Gap to Expectation**: 0.02x

**State History**:
- baseline: 1.00x [none] - success
- kimi: 0.98x [none] - neutral

**Gold Patterns NOT Tried**: date_cte_isolate, decorrelate, dimension_cte_isolate, early_filter, intersect_to_exists, materialize_cte, multi_date_range_cte, multi_dimension_prefetch, or_to_union, prefetch_fact_join, pushdown, single_pass_aggregation, union_cte_split

**Top Recommendations**:

1. **prefetch_fact_join** [CONFIDENCE: 93%] [RISK: LOW]
   - Expected: 3.77x improvement
   - Success Rate: 100%
   - Rationale: Transform has 100% success rate with 3.77x average speedup

2. **single_pass_aggregation** [CONFIDENCE: 50%] [RISK: HIGH]
   - Expected: 4.47x improvement
   - Success Rate: 0%
   - Rationale: Transform has 0% success rate with 4.47x average speedup


# TIER 2: INCREMENTAL OPPORTUNITIES (Priority 40-70)


**Priority Score**: 66.9 (TOP_20%)

### Q95: Q95
**Classification**: MODERATE_WIN
**Runtime**: 5151.0ms baseline (TOP_20%)
**Time Savings Potential**: 2576ms at 2x, 3434ms at 3x
**Current Best**: 1.37x (baseline)
**Gap to Expectation**: 2.13x

**State History**:
- baseline: 1.00x [none] - success
- kimi: 1.37x [none] - success
- v2_standard: 1.00x [semantic_rewrite] - success

**Transforms Attempted**:
- ✗ semantic_rewrite

**Gold Patterns NOT Tried**: date_cte_isolate, decorrelate, dimension_cte_isolate, early_filter, intersect_to_exists, materialize_cte, multi_date_range_cte, multi_dimension_prefetch, or_to_union, prefetch_fact_join, pushdown, single_pass_aggregation, union_cte_split

**Top Recommendations**:

1. **prefetch_fact_join** [CONFIDENCE: 93%] [RISK: LOW]
   - Expected: 3.77x improvement
   - Success Rate: 100%
   - Rationale: Transform has 100% success rate with 3.77x average speedup

2. **single_pass_aggregation** [CONFIDENCE: 50%] [RISK: HIGH]
   - Expected: 4.47x improvement
   - Success Rate: 0%
   - Rationale: Transform has 0% success rate with 4.47x average speedup

**Priority Score**: 65.0 (TOP_20%)

### Q9: Q9
**Classification**: REGRESSION
**Runtime**: 2206.2ms baseline (TOP_20%)
**Time Savings Potential**: 1103ms at 2x, 1471ms at 3x
**Current Best**: 4.47x (W2)
**Gap to Expectation**: 3.47x

**State History**:
- baseline: 1.00x [none] - success
- kimi: 0.42x [none] - regression

**Gold Patterns NOT Tried**: date_cte_isolate, decorrelate, dimension_cte_isolate, early_filter, intersect_to_exists, materialize_cte, multi_date_range_cte, multi_dimension_prefetch, or_to_union, prefetch_fact_join, pushdown, single_pass_aggregation, union_cte_split

**Top Recommendations**:

1. **prefetch_fact_join** [CONFIDENCE: 93%] [RISK: LOW]
   - Expected: 3.77x improvement
   - Success Rate: 100%
   - Rationale: Transform has 100% success rate with 3.77x average speedup

2. **single_pass_aggregation** [CONFIDENCE: 50%] [RISK: HIGH]
   - Expected: 4.47x improvement
   - Success Rate: 0%
   - Rationale: Transform has 0% success rate with 4.47x average speedup

**Priority Score**: 62.5 (TOP_20%)

### Q51: Q51
**Classification**: FAILS_VALIDATION
**Runtime**: 7935.1ms baseline (TOP_20%)
**Time Savings Potential**: 3968ms at 2x, 5290ms at 3x
**Current Best**: 1.00x (baseline)
**Gap to Expectation**: 0.50x

**State History**:
- baseline: 1.00x [none] - success
- v2_standard: 1.00x [date_cte_isolate] - success

**Transforms Attempted**:
- ✗ date_cte_isolate

**Gold Patterns NOT Tried**: decorrelate, dimension_cte_isolate, early_filter, intersect_to_exists, materialize_cte, multi_date_range_cte, multi_dimension_prefetch, or_to_union, prefetch_fact_join, pushdown, single_pass_aggregation, union_cte_split

**Top Recommendations**:

1. **prefetch_fact_join** [CONFIDENCE: 93%] [RISK: LOW]
   - Expected: 3.77x improvement
   - Success Rate: 100%
   - Rationale: Transform has 100% success rate with 3.77x average speedup

2. **single_pass_aggregation** [CONFIDENCE: 50%] [RISK: HIGH]
   - Expected: 4.47x improvement
   - Success Rate: 0%
   - Rationale: Transform has 0% success rate with 4.47x average speedup

**Priority Score**: 62.4 (TOP_50%)

### Q27: Q27
**Classification**: NEUTRAL
**Runtime**: 703.0ms baseline (TOP_50%)
**Time Savings Potential**: 351ms at 2x, 469ms at 3x
**Current Best**: 1.01x (baseline)
**Gap to Expectation**: 2.49x

**State History**:
- baseline: 1.00x [none] - success
- kimi: 1.01x [none] - neutral
- v2_standard: 1.00x [date_cte_isolate] - success

**Transforms Attempted**:
- ✗ date_cte_isolate

**Gold Patterns NOT Tried**: decorrelate, dimension_cte_isolate, early_filter, intersect_to_exists, materialize_cte, multi_date_range_cte, multi_dimension_prefetch, or_to_union, prefetch_fact_join, pushdown, single_pass_aggregation, union_cte_split

**Top Recommendations**:

1. **prefetch_fact_join** [CONFIDENCE: 93%] [RISK: LOW]
   - Expected: 3.77x improvement
   - Success Rate: 100%
   - Rationale: Transform has 100% success rate with 3.77x average speedup

2. **single_pass_aggregation** [CONFIDENCE: 50%] [RISK: HIGH]
   - Expected: 4.47x improvement
   - Success Rate: 0%
   - Rationale: Transform has 0% success rate with 4.47x average speedup

**Priority Score**: 62.1 (TOP_20%)

### Q74: Q74
**Classification**: GOLD_EXAMPLE
**Runtime**: 4129.7ms baseline (TOP_20%)
**Time Savings Potential**: 2065ms at 2x, 2753ms at 3x
**Current Best**: 1.36x (baseline)
**Gap to Expectation**: 1.14x

**State History**:
- baseline: 1.00x [none] - success
- kimi: 1.36x [pushdown] - success
- v2_standard: 1.00x [pushdown] - success

**Transforms Attempted**:
- ✓ pushdown

**Gold Patterns NOT Tried**: date_cte_isolate, decorrelate, dimension_cte_isolate, early_filter, intersect_to_exists, materialize_cte, multi_date_range_cte, multi_dimension_prefetch, or_to_union, prefetch_fact_join, single_pass_aggregation, union_cte_split

**Top Recommendations**:

1. **prefetch_fact_join** [CONFIDENCE: 93%] [RISK: LOW]
   - Expected: 3.77x improvement
   - Success Rate: 100%
   - Rationale: Transform has 100% success rate with 3.77x average speedup

2. **single_pass_aggregation** [CONFIDENCE: 50%] [RISK: HIGH]
   - Expected: 4.47x improvement
   - Success Rate: 0%
   - Rationale: Transform has 0% success rate with 4.47x average speedup

**Priority Score**: 61.9 (TOP_50%)

### Q31: Q31
**Classification**: NEUTRAL
**Runtime**: 661.3ms baseline (TOP_50%)
**Time Savings Potential**: 331ms at 2x, 441ms at 3x
**Current Best**: 1.04x (baseline)
**Gap to Expectation**: 2.46x

**State History**:
- baseline: 1.00x [none] - success
- kimi: 1.04x [none] - neutral
- v2_standard: 1.00x [pushdown] - success

**Transforms Attempted**:
- ✗ pushdown

**Gold Patterns NOT Tried**: date_cte_isolate, decorrelate, dimension_cte_isolate, early_filter, intersect_to_exists, materialize_cte, multi_date_range_cte, multi_dimension_prefetch, or_to_union, prefetch_fact_join, single_pass_aggregation, union_cte_split

**Top Recommendations**:

1. **prefetch_fact_join** [CONFIDENCE: 93%] [RISK: LOW]
   - Expected: 3.77x improvement
   - Success Rate: 100%
   - Rationale: Transform has 100% success rate with 3.77x average speedup

2. **single_pass_aggregation** [CONFIDENCE: 50%] [RISK: HIGH]
   - Expected: 4.47x improvement
   - Success Rate: 0%
   - Rationale: Transform has 0% success rate with 4.47x average speedup

**Priority Score**: 60.0 (TOP_50%)

### Q48: Q48
**Classification**: NEUTRAL
**Runtime**: 933.9ms baseline (TOP_50%)
**Time Savings Potential**: 467ms at 2x, 623ms at 3x
**Current Best**: 1.00x (W1)
**Gap to Expectation**: 2.00x

**State History**:
- baseline: 1.00x [none] - success
- kimi: 1.00x [none] - neutral
- v2_standard: 1.00x [or_to_union] - success

**Transforms Attempted**:
- ✗ or_to_union

**Gold Patterns NOT Tried**: date_cte_isolate, decorrelate, dimension_cte_isolate, early_filter, intersect_to_exists, materialize_cte, multi_date_range_cte, multi_dimension_prefetch, prefetch_fact_join, pushdown, single_pass_aggregation, union_cte_split

**Top Recommendations**:

1. **prefetch_fact_join** [CONFIDENCE: 93%] [RISK: LOW]
   - Expected: 3.77x improvement
   - Success Rate: 100%
   - Rationale: Transform has 100% success rate with 3.77x average speedup

2. **single_pass_aggregation** [CONFIDENCE: 50%] [RISK: HIGH]
   - Expected: 4.47x improvement
   - Success Rate: 0%
   - Rationale: Transform has 0% success rate with 4.47x average speedup

**Priority Score**: 59.8 (TOP_50%)

### Q13: Q13
**Classification**: NEUTRAL
**Runtime**: 980.7ms baseline (TOP_50%)
**Time Savings Potential**: 490ms at 2x, 654ms at 3x
**Current Best**: 1.01x (baseline)
**Gap to Expectation**: 1.97x

**State History**:
- baseline: 1.00x [none] - success
- kimi: 1.01x [none] - neutral
- v2_standard: 1.00x [or_to_union] - success

**Transforms Attempted**:
- ✗ or_to_union

**Gold Patterns NOT Tried**: date_cte_isolate, decorrelate, dimension_cte_isolate, early_filter, intersect_to_exists, materialize_cte, multi_date_range_cte, multi_dimension_prefetch, prefetch_fact_join, pushdown, single_pass_aggregation, union_cte_split

**Top Recommendations**:

1. **prefetch_fact_join** [CONFIDENCE: 93%] [RISK: LOW]
   - Expected: 3.77x improvement
   - Success Rate: 100%
   - Rationale: Transform has 100% success rate with 3.77x average speedup

2. **single_pass_aggregation** [CONFIDENCE: 50%] [RISK: HIGH]
   - Expected: 4.47x improvement
   - Success Rate: 0%
   - Rationale: Transform has 0% success rate with 4.47x average speedup

**Priority Score**: 59.5 (TOP_50%)

### Q80: Q80
**Classification**: NEUTRAL
**Runtime**: 1553.3ms baseline (TOP_50%)
**Time Savings Potential**: 777ms at 2x, 1036ms at 3x
**Current Best**: 1.03x (baseline)
**Gap to Expectation**: 1.97x

**State History**:
- baseline: 1.00x [none] - success
- kimi: 1.03x [none] - neutral
- v2_standard: 1.00x [date_cte_isolate] - success

**Transforms Attempted**:
- ✗ date_cte_isolate

**Gold Patterns NOT Tried**: decorrelate, dimension_cte_isolate, early_filter, intersect_to_exists, materialize_cte, multi_date_range_cte, multi_dimension_prefetch, or_to_union, prefetch_fact_join, pushdown, single_pass_aggregation, union_cte_split

**Top Recommendations**:

1. **prefetch_fact_join** [CONFIDENCE: 93%] [RISK: LOW]
   - Expected: 3.77x improvement
   - Success Rate: 100%
   - Rationale: Transform has 100% success rate with 3.77x average speedup

2. **single_pass_aggregation** [CONFIDENCE: 50%] [RISK: HIGH]
   - Expected: 4.47x improvement
   - Success Rate: 0%
   - Rationale: Transform has 0% success rate with 4.47x average speedup

**Priority Score**: 57.5 (TOP_50%)

### Q36: Q36
**Classification**: REGRESSION
**Runtime**: 896.6ms baseline (TOP_50%)
**Time Savings Potential**: 448ms at 2x, 598ms at 3x
**Current Best**: 1.00x (baseline)
**Gap to Expectation**: 2.50x

**State History**:
- baseline: 1.00x [none] - success
- kimi: 0.96x [none] - neutral
- v2_standard: 1.00x [multi_push_predicate] - success

**Transforms Attempted**:
- ✗ multi_push_predicate

**Gold Patterns NOT Tried**: date_cte_isolate, decorrelate, dimension_cte_isolate, early_filter, intersect_to_exists, materialize_cte, multi_date_range_cte, multi_dimension_prefetch, or_to_union, prefetch_fact_join, pushdown, single_pass_aggregation, union_cte_split

**Top Recommendations**:

1. **prefetch_fact_join** [CONFIDENCE: 93%] [RISK: LOW]
   - Expected: 3.77x improvement
   - Success Rate: 100%
   - Rationale: Transform has 100% success rate with 3.77x average speedup

2. **single_pass_aggregation** [CONFIDENCE: 50%] [RISK: HIGH]
   - Expected: 4.47x improvement
   - Success Rate: 0%
   - Rationale: Transform has 0% success rate with 4.47x average speedup

**Priority Score**: 57.5 (TOP_50%)

### Q72: Q72
**Classification**: REGRESSION
**Runtime**: 1467.3ms baseline (TOP_50%)
**Time Savings Potential**: 734ms at 2x, 978ms at 3x
**Current Best**: 1.00x (baseline)
**Gap to Expectation**: 2.50x

**State History**:
- baseline: 1.00x [none] - success
- kimi: 0.97x [none] - neutral
- v2_standard: 1.00x [semantic_rewrite] - success

**Transforms Attempted**:
- ✗ semantic_rewrite

**Gold Patterns NOT Tried**: date_cte_isolate, decorrelate, dimension_cte_isolate, early_filter, intersect_to_exists, materialize_cte, multi_date_range_cte, multi_dimension_prefetch, or_to_union, prefetch_fact_join, pushdown, single_pass_aggregation, union_cte_split

**Top Recommendations**:

1. **prefetch_fact_join** [CONFIDENCE: 93%] [RISK: LOW]
   - Expected: 3.77x improvement
   - Success Rate: 100%
   - Rationale: Transform has 100% success rate with 3.77x average speedup

2. **single_pass_aggregation** [CONFIDENCE: 50%] [RISK: HIGH]
   - Expected: 4.47x improvement
   - Success Rate: 0%
   - Rationale: Transform has 0% success rate with 4.47x average speedup

**Priority Score**: 57.2 (TOP_50%)

### Q46: Q46
**Classification**: NEUTRAL
**Runtime**: 859.9ms baseline (TOP_50%)
**Time Savings Potential**: 430ms at 2x, 573ms at 3x
**Current Best**: 1.02x (baseline)
**Gap to Expectation**: 1.48x

**State History**:
- baseline: 1.00x [none] - success
- kimi: 1.02x [none] - neutral
- v2_standard: 1.00x [or_to_union] - success

**Transforms Attempted**:
- ✗ or_to_union

**Gold Patterns NOT Tried**: date_cte_isolate, decorrelate, dimension_cte_isolate, early_filter, intersect_to_exists, materialize_cte, multi_date_range_cte, multi_dimension_prefetch, prefetch_fact_join, pushdown, single_pass_aggregation, union_cte_split

**Top Recommendations**:

1. **prefetch_fact_join** [CONFIDENCE: 93%] [RISK: LOW]
   - Expected: 3.77x improvement
   - Success Rate: 100%
   - Rationale: Transform has 100% success rate with 3.77x average speedup

2. **single_pass_aggregation** [CONFIDENCE: 50%] [RISK: HIGH]
   - Expected: 4.47x improvement
   - Success Rate: 0%
   - Rationale: Transform has 0% success rate with 4.47x average speedup

**Priority Score**: 57.2 (TOP_50%)

### Q49: Q49
**Classification**: NEUTRAL
**Runtime**: 534.3ms baseline (TOP_50%)
**Time Savings Potential**: 267ms at 2x, 356ms at 3x
**Current Best**: 1.02x (baseline)
**Gap to Expectation**: 1.48x

**State History**:
- baseline: 1.00x [none] - success
- kimi: 1.02x [none] - neutral
- v2_standard: 1.00x [date_cte_isolate] - success

**Transforms Attempted**:
- ✗ date_cte_isolate

**Gold Patterns NOT Tried**: decorrelate, dimension_cte_isolate, early_filter, intersect_to_exists, materialize_cte, multi_date_range_cte, multi_dimension_prefetch, or_to_union, prefetch_fact_join, pushdown, single_pass_aggregation, union_cte_split

**Top Recommendations**:

1. **prefetch_fact_join** [CONFIDENCE: 93%] [RISK: LOW]
   - Expected: 3.77x improvement
   - Success Rate: 100%
   - Rationale: Transform has 100% success rate with 3.77x average speedup

2. **single_pass_aggregation** [CONFIDENCE: 50%] [RISK: HIGH]
   - Expected: 4.47x improvement
   - Success Rate: 0%
   - Rationale: Transform has 0% success rate with 4.47x average speedup

**Priority Score**: 57.2 (TOP_50%)

### Q57: Q57
**Classification**: NEUTRAL
**Runtime**: 1316.8ms baseline (TOP_50%)
**Time Savings Potential**: 658ms at 2x, 878ms at 3x
**Current Best**: 1.02x (baseline)
**Gap to Expectation**: 1.48x

**State History**:
- baseline: 1.00x [none] - success
- kimi: 1.02x [none] - neutral
- v2_standard: 1.00x [date_cte_isolate] - success

**Transforms Attempted**:
- ✗ date_cte_isolate

**Gold Patterns NOT Tried**: decorrelate, dimension_cte_isolate, early_filter, intersect_to_exists, materialize_cte, multi_date_range_cte, multi_dimension_prefetch, or_to_union, prefetch_fact_join, pushdown, single_pass_aggregation, union_cte_split

**Top Recommendations**:

1. **prefetch_fact_join** [CONFIDENCE: 93%] [RISK: LOW]
   - Expected: 3.77x improvement
   - Success Rate: 100%
   - Rationale: Transform has 100% success rate with 3.77x average speedup

2. **single_pass_aggregation** [CONFIDENCE: 50%] [RISK: HIGH]
   - Expected: 4.47x improvement
   - Success Rate: 0%
   - Rationale: Transform has 0% success rate with 4.47x average speedup

**Priority Score**: 56.8 (TOP_50%)

### Q79: Q79
**Classification**: NEUTRAL
**Runtime**: 939.8ms baseline (TOP_50%)
**Time Savings Potential**: 470ms at 2x, 626ms at 3x
**Current Best**: 1.05x (baseline)
**Gap to Expectation**: 1.45x

**State History**:
- baseline: 1.00x [none] - success
- kimi: 1.05x [none] - neutral
- v2_standard: 1.00x [or_to_union] - success

**Transforms Attempted**:
- ✗ or_to_union

**Gold Patterns NOT Tried**: date_cte_isolate, decorrelate, dimension_cte_isolate, early_filter, intersect_to_exists, materialize_cte, multi_date_range_cte, multi_dimension_prefetch, prefetch_fact_join, pushdown, single_pass_aggregation, union_cte_split

**Top Recommendations**:

1. **prefetch_fact_join** [CONFIDENCE: 93%] [RISK: LOW]
   - Expected: 3.77x improvement
   - Success Rate: 100%
   - Rationale: Transform has 100% success rate with 3.77x average speedup

2. **single_pass_aggregation** [CONFIDENCE: 50%] [RISK: HIGH]
   - Expected: 4.47x improvement
   - Success Rate: 0%
   - Rationale: Transform has 0% success rate with 4.47x average speedup

**Priority Score**: 56.4 (TOP_20%)

### Q93: Q93
**Classification**: GOLD_EXAMPLE
**Runtime**: 2860.6ms baseline (TOP_20%)
**Time Savings Potential**: 1430ms at 2x, 1907ms at 3x
**Current Best**: 2.73x (baseline)
**Gap to Expectation**: 0.27x

**State History**:
- baseline: 1.00x [none] - success
- kimi: 2.73x [early_filter] - success
- v2_standard: 1.00x [decorrelate] - success

**Transforms Attempted**:
- ✗ decorrelate
- ✓ early_filter

**Gold Patterns NOT Tried**: date_cte_isolate, dimension_cte_isolate, intersect_to_exists, materialize_cte, multi_date_range_cte, multi_dimension_prefetch, or_to_union, prefetch_fact_join, pushdown, single_pass_aggregation, union_cte_split

**Top Recommendations**:

1. **prefetch_fact_join** [CONFIDENCE: 93%] [RISK: LOW]
   - Expected: 3.77x improvement
   - Success Rate: 100%
   - Rationale: Transform has 100% success rate with 3.77x average speedup

2. **single_pass_aggregation** [CONFIDENCE: 50%] [RISK: HIGH]
   - Expected: 4.47x improvement
   - Success Rate: 0%
   - Rationale: Transform has 0% success rate with 4.47x average speedup

**Priority Score**: 56.0 (TOP_50%)

### Q87: Q87
**Classification**: REGRESSION
**Runtime**: 1821.9ms baseline (TOP_50%)
**Time Savings Potential**: 911ms at 2x, 1215ms at 3x
**Current Best**: 1.00x (baseline)
**Gap to Expectation**: 2.20x

**State History**:
- baseline: 1.00x [none] - success
- kimi: 0.86x [none] - regression
- v2_standard: 1.00x [date_cte_isolate] - success

**Transforms Attempted**:
- ✗ date_cte_isolate

**Gold Patterns NOT Tried**: decorrelate, dimension_cte_isolate, early_filter, intersect_to_exists, materialize_cte, multi_date_range_cte, multi_dimension_prefetch, or_to_union, prefetch_fact_join, pushdown, single_pass_aggregation, union_cte_split

**Top Recommendations**:

1. **prefetch_fact_join** [CONFIDENCE: 93%] [RISK: LOW]
   - Expected: 3.77x improvement
   - Success Rate: 100%
   - Rationale: Transform has 100% success rate with 3.77x average speedup

2. **single_pass_aggregation** [CONFIDENCE: 50%] [RISK: HIGH]
   - Expected: 4.47x improvement
   - Success Rate: 0%
   - Rationale: Transform has 0% success rate with 4.47x average speedup

**Priority Score**: 56.0 (TOP_50%)

### Q89: Q89
**Classification**: REGRESSION
**Runtime**: 521.0ms baseline (TOP_50%)
**Time Savings Potential**: 261ms at 2x, 347ms at 3x
**Current Best**: 1.00x (baseline)
**Gap to Expectation**: 2.20x

**State History**:
- baseline: 1.00x [none] - success
- kimi: 0.60x [none] - regression
- v2_standard: 1.00x [or_to_union] - success

**Transforms Attempted**:
- ✗ or_to_union

**Gold Patterns NOT Tried**: date_cte_isolate, decorrelate, dimension_cte_isolate, early_filter, intersect_to_exists, materialize_cte, multi_date_range_cte, multi_dimension_prefetch, prefetch_fact_join, pushdown, single_pass_aggregation, union_cte_split

**Top Recommendations**:

1. **prefetch_fact_join** [CONFIDENCE: 93%] [RISK: LOW]
   - Expected: 3.77x improvement
   - Success Rate: 100%
   - Rationale: Transform has 100% success rate with 3.77x average speedup

2. **single_pass_aggregation** [CONFIDENCE: 50%] [RISK: HIGH]
   - Expected: 4.47x improvement
   - Success Rate: 0%
   - Rationale: Transform has 0% success rate with 4.47x average speedup

**Priority Score**: 53.5 (TOP_50%)

### Q24: Q24
**Classification**: REGRESSION
**Runtime**: 780.3ms baseline (TOP_50%)
**Time Savings Potential**: 390ms at 2x, 520ms at 3x
**Current Best**: 1.00x (baseline)
**Gap to Expectation**: 1.70x

**State History**:
- baseline: 1.00x [none] - success
- kimi: 0.87x [none] - regression
- v2_standard: 1.00x [pushdown] - success

**Transforms Attempted**:
- ✗ pushdown

**Gold Patterns NOT Tried**: date_cte_isolate, decorrelate, dimension_cte_isolate, early_filter, intersect_to_exists, materialize_cte, multi_date_range_cte, multi_dimension_prefetch, or_to_union, prefetch_fact_join, single_pass_aggregation, union_cte_split

**Top Recommendations**:

1. **prefetch_fact_join** [CONFIDENCE: 93%] [RISK: LOW]
   - Expected: 3.77x improvement
   - Success Rate: 100%
   - Rationale: Transform has 100% success rate with 3.77x average speedup

2. **single_pass_aggregation** [CONFIDENCE: 50%] [RISK: HIGH]
   - Expected: 4.47x improvement
   - Success Rate: 0%
   - Rationale: Transform has 0% success rate with 4.47x average speedup

**Priority Score**: 52.5 (TOP_50%)

### Q25: Q25
**Classification**: REGRESSION
**Runtime**: 515.4ms baseline (TOP_50%)
**Time Savings Potential**: 258ms at 2x, 344ms at 3x
**Current Best**: 1.00x (W2)
**Gap to Expectation**: 1.50x

**State History**:
- baseline: 1.00x [none] - success
- kimi: 0.98x [none] - neutral
- v2_standard: 1.00x [date_cte_isolate] - success

**Transforms Attempted**:
- ✗ date_cte_isolate

**Gold Patterns NOT Tried**: decorrelate, dimension_cte_isolate, early_filter, intersect_to_exists, materialize_cte, multi_date_range_cte, multi_dimension_prefetch, or_to_union, prefetch_fact_join, pushdown, single_pass_aggregation, union_cte_split

**Top Recommendations**:

1. **prefetch_fact_join** [CONFIDENCE: 93%] [RISK: LOW]
   - Expected: 3.77x improvement
   - Success Rate: 100%
   - Rationale: Transform has 100% success rate with 3.77x average speedup

2. **single_pass_aggregation** [CONFIDENCE: 50%] [RISK: HIGH]
   - Expected: 4.47x improvement
   - Success Rate: 0%
   - Rationale: Transform has 0% success rate with 4.47x average speedup

**Priority Score**: 52.5 (TOP_50%)

### Q34: Q34
**Classification**: REGRESSION
**Runtime**: 539.9ms baseline (TOP_50%)
**Time Savings Potential**: 270ms at 2x, 360ms at 3x
**Current Best**: 1.00x (W2)
**Gap to Expectation**: 1.50x

**State History**:
- baseline: 1.00x [none] - success
- kimi: 0.29x [none] - regression
- v2_standard: 1.00x [or_to_union] - success

**Transforms Attempted**:
- ✗ or_to_union

**Gold Patterns NOT Tried**: date_cte_isolate, decorrelate, dimension_cte_isolate, early_filter, intersect_to_exists, materialize_cte, multi_date_range_cte, multi_dimension_prefetch, prefetch_fact_join, pushdown, single_pass_aggregation, union_cte_split

**Top Recommendations**:

1. **prefetch_fact_join** [CONFIDENCE: 93%] [RISK: LOW]
   - Expected: 3.77x improvement
   - Success Rate: 100%
   - Rationale: Transform has 100% success rate with 3.77x average speedup

2. **single_pass_aggregation** [CONFIDENCE: 50%] [RISK: HIGH]
   - Expected: 4.47x improvement
   - Success Rate: 0%
   - Rationale: Transform has 0% success rate with 4.47x average speedup

**Priority Score**: 52.5 (TOP_50%)

### Q50: Q50
**Classification**: REGRESSION
**Runtime**: 1007.8ms baseline (TOP_50%)
**Time Savings Potential**: 504ms at 2x, 672ms at 3x
**Current Best**: 1.00x (baseline)
**Gap to Expectation**: 1.50x

**State History**:
- baseline: 1.00x [none] - success
- kimi: 0.91x [none] - regression
- v2_standard: 1.00x [date_cte_isolate] - success

**Transforms Attempted**:
- ✗ date_cte_isolate

**Gold Patterns NOT Tried**: decorrelate, dimension_cte_isolate, early_filter, intersect_to_exists, materialize_cte, multi_date_range_cte, multi_dimension_prefetch, or_to_union, prefetch_fact_join, pushdown, single_pass_aggregation, union_cte_split

**Top Recommendations**:

1. **prefetch_fact_join** [CONFIDENCE: 93%] [RISK: LOW]
   - Expected: 3.77x improvement
   - Success Rate: 100%
   - Rationale: Transform has 100% success rate with 3.77x average speedup

2. **single_pass_aggregation** [CONFIDENCE: 50%] [RISK: HIGH]
   - Expected: 4.47x improvement
   - Success Rate: 0%
   - Rationale: Transform has 0% success rate with 4.47x average speedup

**Priority Score**: 52.5 (TOP_50%)

### Q68: Q68
**Classification**: REGRESSION
**Runtime**: 889.7ms baseline (TOP_50%)
**Time Savings Potential**: 445ms at 2x, 593ms at 3x
**Current Best**: 1.00x (baseline)
**Gap to Expectation**: 1.50x

**State History**:
- baseline: 1.00x [none] - success
- kimi: 0.95x [none] - neutral
- v2_standard: 1.00x [or_to_union] - success

**Transforms Attempted**:
- ✗ or_to_union

**Gold Patterns NOT Tried**: date_cte_isolate, decorrelate, dimension_cte_isolate, early_filter, intersect_to_exists, materialize_cte, multi_date_range_cte, multi_dimension_prefetch, prefetch_fact_join, pushdown, single_pass_aggregation, union_cte_split

**Top Recommendations**:

1. **prefetch_fact_join** [CONFIDENCE: 93%] [RISK: LOW]
   - Expected: 3.77x improvement
   - Success Rate: 100%
   - Rationale: Transform has 100% success rate with 3.77x average speedup

2. **single_pass_aggregation** [CONFIDENCE: 50%] [RISK: HIGH]
   - Expected: 4.47x improvement
   - Success Rate: 0%
   - Rationale: Transform has 0% success rate with 4.47x average speedup

**Priority Score**: 52.5 (TOP_50%)

### Q71: Q71
**Classification**: REGRESSION
**Runtime**: 579.4ms baseline (TOP_50%)
**Time Savings Potential**: 290ms at 2x, 386ms at 3x
**Current Best**: 1.00x (baseline)
**Gap to Expectation**: 1.50x

**State History**:
- baseline: 1.00x [none] - success
- kimi: 0.96x [none] - neutral
- v2_standard: 1.00x [or_to_union] - success

**Transforms Attempted**:
- ✗ or_to_union

**Gold Patterns NOT Tried**: date_cte_isolate, decorrelate, dimension_cte_isolate, early_filter, intersect_to_exists, materialize_cte, multi_date_range_cte, multi_dimension_prefetch, prefetch_fact_join, pushdown, single_pass_aggregation, union_cte_split

**Top Recommendations**:

1. **prefetch_fact_join** [CONFIDENCE: 93%] [RISK: LOW]
   - Expected: 3.77x improvement
   - Success Rate: 100%
   - Rationale: Transform has 100% success rate with 3.77x average speedup

2. **single_pass_aggregation** [CONFIDENCE: 50%] [RISK: HIGH]
   - Expected: 4.47x improvement
   - Success Rate: 0%
   - Rationale: Transform has 0% success rate with 4.47x average speedup

**Priority Score**: 50.0 (TOP_50%)

### Q70: Q70
**Classification**: REGRESSION
**Runtime**: 1299.7ms baseline (TOP_50%)
**Time Savings Potential**: 650ms at 2x, 866ms at 3x
**Current Best**: 1.00x (baseline)
**Gap to Expectation**: 1.00x

**State History**:
- baseline: 1.00x [none] - success
- kimi: 0.75x [none] - regression
- v2_standard: 1.00x [date_cte_isolate] - success

**Transforms Attempted**:
- ✗ date_cte_isolate

**Gold Patterns NOT Tried**: decorrelate, dimension_cte_isolate, early_filter, intersect_to_exists, materialize_cte, multi_date_range_cte, multi_dimension_prefetch, or_to_union, prefetch_fact_join, pushdown, single_pass_aggregation, union_cte_split

**Top Recommendations**:

1. **prefetch_fact_join** [CONFIDENCE: 93%] [RISK: LOW]
   - Expected: 3.77x improvement
   - Success Rate: 100%
   - Rationale: Transform has 100% success rate with 3.77x average speedup

2. **single_pass_aggregation** [CONFIDENCE: 50%] [RISK: HIGH]
   - Expected: 4.47x improvement
   - Success Rate: 0%
   - Rationale: Transform has 0% success rate with 4.47x average speedup

**Priority Score**: 49.7 (TOP_50%)

### Q8: Q8
**Classification**: NEUTRAL
**Runtime**: 704.3ms baseline (TOP_50%)
**Time Savings Potential**: 352ms at 2x, 470ms at 3x
**Current Best**: 1.03x (baseline)
**Gap to Expectation**: 0.03x

**State History**:
- baseline: 1.00x [none] - success
- kimi: 1.03x [none] - neutral

**Gold Patterns NOT Tried**: date_cte_isolate, decorrelate, dimension_cte_isolate, early_filter, intersect_to_exists, materialize_cte, multi_date_range_cte, multi_dimension_prefetch, or_to_union, prefetch_fact_join, pushdown, single_pass_aggregation, union_cte_split

**Top Recommendations**:

1. **prefetch_fact_join** [CONFIDENCE: 93%] [RISK: LOW]
   - Expected: 3.77x improvement
   - Success Rate: 100%
   - Rationale: Transform has 100% success rate with 3.77x average speedup

2. **single_pass_aggregation** [CONFIDENCE: 50%] [RISK: HIGH]
   - Expected: 4.47x improvement
   - Success Rate: 0%
   - Rationale: Transform has 0% success rate with 4.47x average speedup

**Priority Score**: 48.1 (TOP_50%)

### Q17: Q17
**Classification**: NEUTRAL
**Runtime**: 864.4ms baseline (TOP_50%)
**Time Savings Potential**: 432ms at 2x, 576ms at 3x
**Current Best**: 1.19x (baseline)
**Gap to Expectation**: 0.19x

**State History**:
- baseline: 1.00x [none] - success
- kimi: 1.19x [none] - success

**Gold Patterns NOT Tried**: date_cte_isolate, decorrelate, dimension_cte_isolate, early_filter, intersect_to_exists, materialize_cte, multi_date_range_cte, multi_dimension_prefetch, or_to_union, prefetch_fact_join, pushdown, single_pass_aggregation, union_cte_split

**Top Recommendations**:

1. **prefetch_fact_join** [CONFIDENCE: 93%] [RISK: LOW]
   - Expected: 3.77x improvement
   - Success Rate: 100%
   - Rationale: Transform has 100% success rate with 3.77x average speedup

2. **single_pass_aggregation** [CONFIDENCE: 50%] [RISK: HIGH]
   - Expected: 4.47x improvement
   - Success Rate: 0%
   - Rationale: Transform has 0% success rate with 4.47x average speedup

**Priority Score**: 47.5 (TOP_50%)

### Q7: Q7
**Classification**: FAILS_VALIDATION
**Runtime**: 660.0ms baseline (TOP_50%)
**Time Savings Potential**: 330ms at 2x, 440ms at 3x
**Current Best**: 1.00x (W2)
**Gap to Expectation**: 2.50x

**State History**:
- baseline: 1.00x [none] - success
- v2_standard: 1.00x [date_cte_isolate] - success

**Transforms Attempted**:
- ✗ date_cte_isolate

**Gold Patterns NOT Tried**: decorrelate, dimension_cte_isolate, early_filter, intersect_to_exists, materialize_cte, multi_date_range_cte, multi_dimension_prefetch, or_to_union, prefetch_fact_join, pushdown, single_pass_aggregation, union_cte_split

**Top Recommendations**:

1. **prefetch_fact_join** [CONFIDENCE: 93%] [RISK: LOW]
   - Expected: 3.77x improvement
   - Success Rate: 100%
   - Rationale: Transform has 100% success rate with 3.77x average speedup

2. **single_pass_aggregation** [CONFIDENCE: 50%] [RISK: HIGH]
   - Expected: 4.47x improvement
   - Success Rate: 0%
   - Rationale: Transform has 0% success rate with 4.47x average speedup

**Priority Score**: 45.9 (TOP_50%)

### Q38: Q38
**Classification**: REGRESSION
**Runtime**: 1598.8ms baseline (TOP_50%)
**Time Savings Potential**: 799ms at 2x, 1066ms at 3x
**Current Best**: 1.44x (W2)
**Gap to Expectation**: 1.06x

**State History**:
- baseline: 1.00x [none] - success
- kimi: 0.99x [none] - neutral
- v2_standard: 1.00x [date_cte_isolate] - success

**Transforms Attempted**:
- ✗ date_cte_isolate

**Gold Patterns NOT Tried**: decorrelate, dimension_cte_isolate, early_filter, intersect_to_exists, materialize_cte, multi_date_range_cte, multi_dimension_prefetch, or_to_union, prefetch_fact_join, pushdown, single_pass_aggregation, union_cte_split

**Top Recommendations**:

1. **prefetch_fact_join** [CONFIDENCE: 93%] [RISK: LOW]
   - Expected: 3.77x improvement
   - Success Rate: 100%
   - Rationale: Transform has 100% success rate with 3.77x average speedup

2. **single_pass_aggregation** [CONFIDENCE: 50%] [RISK: HIGH]
   - Expected: 4.47x improvement
   - Success Rate: 0%
   - Rationale: Transform has 0% success rate with 4.47x average speedup

**Priority Score**: 45.0 (TOP_50%)

### Q5: Q5
**Classification**: NEUTRAL
**Runtime**: 1169.5ms baseline (TOP_50%)
**Time Savings Potential**: 585ms at 2x, 780ms at 3x
**Current Best**: 1.89x (W1)
**Gap to Expectation**: 0.89x

**State History**:
- baseline: 1.00x [none] - success
- kimi: 1.09x [none] - neutral

**Gold Patterns NOT Tried**: date_cte_isolate, decorrelate, dimension_cte_isolate, early_filter, intersect_to_exists, materialize_cte, multi_date_range_cte, multi_dimension_prefetch, or_to_union, prefetch_fact_join, pushdown, single_pass_aggregation, union_cte_split

**Top Recommendations**:

1. **prefetch_fact_join** [CONFIDENCE: 93%] [RISK: LOW]
   - Expected: 3.77x improvement
   - Success Rate: 100%
   - Rationale: Transform has 100% success rate with 3.77x average speedup

2. **single_pass_aggregation** [CONFIDENCE: 50%] [RISK: HIGH]
   - Expected: 4.47x improvement
   - Success Rate: 0%
   - Rationale: Transform has 0% success rate with 4.47x average speedup


# TIER 3: MATURE WINS (Priority < 40)


**50 queries with low priority** (mostly short-running or already optimized)

These queries are not recommended for immediate focus due to:
- Short baseline runtime (<500ms) → lower absolute time savings potential
- Already at or near expected speedup targets
- Limited remaining optimization opportunities



# APPENDIX: METHODOLOGY & INTERPRETATION GUIDE


## Priority Scoring Formula

`Priority = Runtime_Percentile(50pts) + Gap_To_Expectation(20pts) + Win_Potential(20pts) + Untried_Patterns(5pts) + Category_Bonus(15pts)`


### Runtime Percentile (50 points - DOMINANT FACTOR)

- **Top 20% by baseline runtime**: 50 points
- **Top 21-50% by baseline runtime**: 25 points
- **Bottom 50% by baseline runtime**: 0 points

**Key insight**: A 1.2x speedup on a 10,000ms query saves more absolute time than 3x speedup on a 100ms query.


### Time Savings Potential

Shown for each query:
- **At 2x speedup**: Original_ms / 2 seconds saved
- **At 3x speedup**: Original_ms * 2 / 3 seconds saved


### Confidence Scores

- **90-100%**: Very high confidence - proven pattern with high success rate
- **75-89%**: High confidence - successful pattern, likely to work
- **60-74%**: Good confidence - proven technique, moderate risk
- **40-59%**: Moderate confidence - less evidence but promising
- **<40%**: Low confidence - experimental, use as last resort


### Risk Assessment

- **LOW**: >80% historical success rate
- **MEDIUM**: 50-80% success rate
- **HIGH**: <50% success rate or untested pattern


## How to Use This Report

1. **Start with Tier 1** (highest priority scores) - these are longest-running queries with proven patterns
2. **Check Time Savings Potential** - focus on queries where potential savings are largest
3. **Review Top Recommendations** - follow highest-confidence transforms first
4. **Validate improvements** using 3-run (discard warmup, avg last 2) or 5-run trimmed mean methodology
5. **Move to Tier 2** only after exhausting high-value targets in Tier 1
