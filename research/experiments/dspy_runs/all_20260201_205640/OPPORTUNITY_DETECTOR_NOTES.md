# Optimization Opportunity Detector - Research Notes

**Date:** 2026-02-02
**Status:** Initial implementation complete, needs refinement

---

## Detection Summary

| Metric | Value |
|--------|-------|
| Total queries with opportunities | 73/99 |
| Wins detected (≥1.2x) | 15/20 (75%) |
| Undetected wins | 5 |

---

## Rule Effectiveness

| Rule | Description | Detections | Wins | Win Rate | Regressions |
|------|-------------|------------|------|----------|-------------|
| `QT-OPT-006` | COUNT(*)>0 → EXISTS | 1 | 1 | **100%** | 0 |
| `QT-OPT-001` | OR → UNION ALL | 16 | 3 | 19% | 4 |
| `QT-OPT-002` | Late date filter → Early CTE | 77 | 13 | 17% | 23 |
| `QT-OPT-003` | Repeated subquery → Materialized CTE | 28 | 3 | 11% | 8 |

---

## Undetected Wins (TODO: Add patterns)

### q39 (2.44x) - Filter pushed into existing CTE
```
Original: CTE groups by all months, main query filters to months 1,2
Optimized: Added d_moy IN (1, 2) inside CTE's inner query
```
**Pattern needed:** Detect when main query filters on CTE columns that could be pushed into CTE

### q24 (2.16x) - UNION added to existing CTE structure
```
Original: CTE with complex joins
Optimized: Added UNION and early i_color filtering
```
**Pattern needed:** Detect CTE that could benefit from UNION decomposition

### q4 (1.25x) - Complex aggregation restructuring
```
Original: Complex customer lifetime value aggregation
Optimized: Restructured aggregation with early date filtering
```
**Pattern needed:** Complex, may need LLM judgment

### q62 (1.24x) - Date CTE added (no date_dim in original)
```
Original: Uses ws_ship_date_sk arithmetic, no date_dim table
Optimized: Added filtered_dates CTE joining to date_dim
```
**Pattern needed:** Detect date_sk columns without date_dim join

### q17 (1.23x) - Date CTEs added (no date_dim in original)
```
Original: Uses d_quarter but no date_dim table
Optimized: Created filtered CTEs for date_dim by quarter
```
**Pattern needed:** Same as q62 - date filtering without date_dim

---

## False Positives Analysis

### Problem: High detection, low win rate

The detector finds many "opportunities" but only ~15-20% actually produce speedups.

### Key Question: If we tell DeepSeek about the pattern, will it use it?

**Hypothesis:** Yes, but we need to be selective about WHICH patterns we tell it.

**Current approach:**
- DSPy prompt includes general optimization strategies
- LLM decides which to apply based on query structure

**Proposed approach:**
1. Run opportunity detector on input query
2. Only include patterns that are DETECTED as relevant
3. Add confidence scores based on pattern characteristics
4. Include counter-examples where pattern REGRESSED

### Patterns that REGRESS (important for LLM guidance)

#### QT-OPT-001 OR→UNION regressions:
| Query | Speedup | OR Pattern | Why it regressed |
|-------|---------|------------|------------------|
| q46 | 0.79x | `hd_dep_count \| hd_vehicle_count` | Small cardinality dimension columns |
| q68 | 0.71x | `hd_dep_count \| hd_vehicle_count` | Same pattern, DuckDB handles well |
| q78 | 0.79x | `ws_qty \| cs_qty` | Quantity columns, low selectivity |
| q7 | 0.96x | `p_channel_email \| p_channel_event` | Boolean flags, optimizer handles |

**Insight:** OR on small cardinality or boolean columns should NOT be converted to UNION.

#### QT-OPT-002 Date CTE regressions:
Many queries already have efficient date filtering. Adding CTEs can:
- Add overhead for small date ranges
- Interfere with existing optimizer plans
- Create unnecessary materialization

**When date CTE helps:**
- Large fact table scans (catalog_sales, store_sales, web_sales)
- Date range spans significant portion of data
- Multiple fact tables joined with same date filter

**When date CTE hurts:**
- Query already has efficient date predicates
- Small date ranges (single day/week)
- Complex existing CTE structure

---

## Recommendations for DSPy Prompt

### 1. Pattern-specific guidance

Instead of generic "consider UNION ALL", provide:
```
UNION ALL decomposition (2-3x speedup potential):
- GOOD: OR across different INDEXED columns (e.g., ca_zip OR ca_state)
- BAD: OR on small cardinality columns (e.g., boolean flags, status codes)
- BAD: OR on same column with different values (use IN instead)
```

### 2. Include regression examples

```
WARNING - These patterns often REGRESS:
- UNION ALL on household_demographics columns (hd_dep_count, hd_vehicle_count)
- Adding date CTE when original already filters efficiently
- Materializing small subqueries
```

### 3. Confidence-based application

```python
def get_optimization_guidance(sql: str) -> str:
    opportunities = detect_opportunities(sql)
    guidance = []

    for opp in opportunities:
        if opp.rule_id == "QT-OPT-006":  # COUNT→EXISTS
            guidance.append(f"HIGH CONFIDENCE: {opp.suggestion}")
        elif opp.rule_id == "QT-OPT-001":  # OR→UNION
            if not has_small_cardinality_columns(opp):
                guidance.append(f"MEDIUM CONFIDENCE: {opp.suggestion}")
        # etc.

    return "\n".join(guidance)
```

---

## DeepSeek Missed Opportunities Analysis

Cases where we detected an opportunity but DeepSeek did something else:

### Q28 (1.00x) - DeepSeek was RIGHT to skip
- **Detected:** OR→UNION on `ss_list_price | ss_coupon_amt | ss_wholesale_cost` (6 times!)
- **DeepSeek did:** Kept original
- **Result:** Correct decision - OR on price columns doesn't benefit from UNION
- **Learning:** Our detector has false positives, DeepSeek's judgment was better here

### Q83 (0.77x) - DeepSeek chose WRONG pattern
- **Detected:** Repeated subquery (date_dim), Date CTE
- **DeepSeek did:** Date CTE only
- **Result:** REGRESSED - should have used Materialized CTE for repeated date_dim subqueries
- **Learning:** When multiple patterns detected, need to guide which to prioritize

### Q57 (0.54x) - DeepSeek OVERCOMPLICATED
- **Detected:** Date CTE
- **DeepSeek did:** Date CTE + replaced self-joins with LAG/LEAD window functions
- **Result:** HUGE regression (almost 2x slower)
- **Learning:** Combining multiple transformations can backfire badly

### Q54, Q60, Q5, Q80 (all regressed) - DeepSeek chose UNION when Date CTE was available
- **Detected:** Date CTE opportunities
- **DeepSeek did:** UNION decomposition
- **Result:** All regressed (0.91x-0.97x)
- **Learning:** UNION isn't always the answer even when OR is present

### Q46, Q68 (0.71x-0.79x) - Detector warned correctly
- **Detected:** OR→UNION on `hd_dep_count | hd_vehicle_count`
- **DeepSeek did:** Predicate pushdown instead
- **Result:** Still regressed with PRED
- **Learning:** These household_demographics queries are hard to optimize

### Key Insights

1. **DeepSeek sometimes knows better than our rules** (Q28)
2. **When multiple opportunities exist, priority matters** (Q83)
3. **Combining transformations is risky** (Q57)
4. **UNION is overused** - DeepSeek defaults to it but Date CTE often works better

### Recommendation for Prompting

Instead of telling DeepSeek about ALL detected patterns, we should:
1. Rank patterns by confidence
2. Tell it which patterns to AVOID for this specific query
3. Include negative examples: "Don't use UNION on price columns like Q28"

---

## Next Steps

1. **Add missing patterns:**
   - Filter pushdown into existing CTE (q39)
   - Date SK without date_dim join (q17, q62)

2. **Refine existing patterns:**
   - QT-OPT-001: Skip small cardinality columns
   - QT-OPT-002: Check if date filtering already efficient

3. **Integrate with DSPy:**
   - Pass detected opportunities to prompt
   - Include confidence scores
   - Add regression warnings

4. **Test hypothesis:**
   - Run same queries with pattern-specific prompts
   - Measure if detection → LLM application → speedup

---

## Files

- Opportunity rules: `packages/qt-sql/qt_sql/analyzers/ast_detector/rules/opportunity_rules.py`
- Registry: `packages/qt-sql/qt_sql/analyzers/ast_detector/registry.py`
- Results: `research/experiments/dspy_runs/all_20260201_205640/RESULTS.md`
