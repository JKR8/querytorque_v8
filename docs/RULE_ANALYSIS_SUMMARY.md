# Rule Effectiveness Analysis Summary

**Analysis Date:** 2026-02-04
**Dataset:** TPC-DS SF100 Benchmark (98 queries)
**Benchmark Source:** Kimi benchmark run (20260202_221828)

---

## Executive Summary

This analysis correlates AST detector rules with actual query optimization speedups to identify which rules are most predictive of successful optimization.

### Key Statistics

- **Total Queries Analyzed:** 98
- **Successful Optimizations (>1.2x):** 17 queries (17.3%)
- **Regressions (<1.0x):** 42 queries (42.9%)
- **Neutral (1.0-1.2x):** 39 queries (39.8%)
- **Average Speedup:** 1.06x
- **Unique Rules Detected:** 46

### Speedup Distribution Breakdown

| Category | Count | Percentage | Description |
|----------|-------|------------|-------------|
| **Big Wins** | 5 | 5.1% | >2.0x speedup |
| **Good Wins** | 12 | 12.2% | 1.2-2.0x speedup |
| **Neutral** | 39 | 39.8% | 1.0-1.2x speedup |
| **Regressions** | 42 | 42.9% | <1.0x speedup |

---

## Top Predictive Rules

### High-Value Rules (Focus Here)

These rules have the strongest correlation with successful optimization:

| Rank | Rule | Win Rate | Avg Speedup | Appearances | Use Case |
|------|------|----------|-------------|-------------|----------|
| 1 | `SQL-DUCK-007` | 75.0% | 1.54x | 4 | DuckDB-specific optimization |
| 2 | `SQL-SUB-001` | 60.0% | 1.95x | 5 | Correlated subquery in WHERE |
| 3 | `SQL-CTE-005` | 50.0% | 1.56x | 2 | CTE optimization opportunity |
| 4 | `QT-OPT-002` | 42.9% | 1.57x | 7 | Correlated subquery to pre-computed CTE |

**Insight:** When `SQL-SUB-001` (correlated subquery in WHERE) is detected, there's a 60% chance of >1.2x speedup with an average of 1.95x improvement. This is a high-priority optimization target.

---

## Risky Rules (Use Caution)

These rules are strongly associated with regressions:

| Rank | Rule | Win Rate | Loss Rate | Avg Speedup | Appearances |
|------|------|----------|-----------|-------------|-------------|
| 1 | `QT-PLAN-001` | 16.7% | 66.7% | 0.61x | 6 |
| 2 | `SQL-DUCK-001` | 12.5% | 62.5% | 0.68x | 8 |
| 3 | `SQL-ORD-001` | 20.0% | 60.0% | 0.66x | 5 |
| 4 | `QT-OPT-011` | 20.0% | 60.0% | 0.66x | 5 |

**Insight:** When `QT-PLAN-001` or `SQL-DUCK-001` are detected, there's a >60% chance of regression. These queries require conservative optimization approaches and thorough validation.

---

## Most Powerful Rule Combinations

Rule combinations with 75% win rate and 2.16x average speedup:

### Core Pattern (All Variations)

The following combinations all appear in the same 4 queries and achieve identical results:

1. **`QT-AGG-002 + QT-OPT-002`** - Aggregate after join + Correlated subquery to CTE
2. **`QT-AGG-002 + SQL-SUB-001`** - Aggregate after join + Correlated subquery in WHERE
3. **`QT-OPT-002 + QT-OPT-009`** - Correlated subquery to CTE + Join reordering
4. **`QT-OPT-003 + SQL-SUB-001`** - [Rule] + Correlated subquery in WHERE
5. **`QT-OPT-009 + SQL-SUB-001`** - Join reordering + Correlated subquery in WHERE

**Success Rate:** 75% win rate (3 wins, 1 loss out of 4 queries)
**Average Speedup:** 2.16x
**Example Queries:** Query 1 (2.92x), Query 27 (2.43x), Query 60 (1.40x)

### What Makes This Pattern Special

This pattern represents queries with:
- Correlated subqueries that can be pre-computed
- Aggregations performed after joins (opportunity for push-down)
- Potential for join reordering

When all these conditions are met, the LLM can apply multiple complementary optimizations.

---

## Volume vs. Effectiveness Analysis

### Most Common Rules

| Rule | Appearances | Win Rate | Analysis |
|------|-------------|----------|----------|
| `QT-AGG-002` | 79 | 16.5% | High volume, low win rate - not predictive |
| `QT-OPT-003` | 56 | 16.1% | Common but low effectiveness |
| `SQL-DUCK-002` | 40 | 10.0% | Very common, poor predictor |
| `QT-OPT-009` | 39 | 20.5% | Join reordering - use selectively |

**Insight:** The most common rules (`QT-AGG-002`, `QT-OPT-003`) have LOW win rates (16-17%). Don't assume frequency = importance. Focus on high win-rate rules even if they're less common.

---

## Actionable Recommendations

### 1. Priority Targeting Strategy

**Focus LLM optimization efforts when these rules are detected:**

- `SQL-SUB-001` (Correlated subquery in WHERE) - 60% win rate, 1.95x avg
- `SQL-DUCK-007` (DuckDB-specific) - 75% win rate, 1.54x avg
- `QT-OPT-002` (Correlated subquery to CTE) - 43% win rate, 1.57x avg

### 2. Conservative Approach for Risky Rules

**When these rules appear, validate thoroughly:**

- `QT-PLAN-001`, `SQL-DUCK-001`, `SQL-ORD-001`, `QT-OPT-011`
- All have >60% loss rates
- Consider simpler optimizations or skip these queries

### 3. Leverage High-Value Combinations

**When you detect the winning combination pattern:**

- `QT-AGG-002 + QT-OPT-002` (or any of the 75% combinations)
- Apply CTE extraction for correlated subqueries
- Consider pre-aggregation before joins
- Test join reordering with filtered tables first

### 4. Join Reordering Caution

**`QT-OPT-009` (Join Reordering) appears in 39 queries but only 20.5% win rate:**

- Don't blindly reorder joins
- Only reorder when there are highly selective filters
- Prioritize filtered tables early in join sequence
- More likely to succeed when combined with subquery optimizations

### 5. Volume ≠ Value

**Don't prioritize rules by frequency alone:**

- `QT-AGG-002` appears 79 times but only 16.5% win rate
- `SQL-SUB-001` appears only 5 times but 60% win rate
- Focus on high win-rate rules regardless of frequency

---

## Statistical Insights

### Rules with >50% Win Rate (High Priority)

| Rule | Appearances | Win Rate | Avg Speedup |
|------|-------------|----------|-------------|
| `SQL-DUCK-007` | 4 | 75.0% | 1.54x |
| `SQL-SUB-001` | 5 | 60.0% | 1.95x |

Only 2 rules (with 4+ appearances) have >50% win rate!

### Rules with >50% Loss Rate (High Risk)

| Rule | Appearances | Loss Rate | Avg Speedup |
|------|-------------|-----------|-------------|
| `QT-PLAN-001` | 6 | 66.7% | 0.61x |
| `SQL-DUCK-001` | 8 | 62.5% | 0.68x |
| `SQL-ORD-001` | 5 | 60.0% | 0.66x |
| `QT-OPT-011` | 5 | 60.0% | 0.66x |

7 rules total have >50% loss rate (including low-appearance rules).

### Rule Combinations with >70% Win Rate

**15 combinations** achieve >70% win rate, all centered around the core pattern of:
- Correlated subqueries + Aggregates + Join reordering

---

## Case Studies

### Big Win Example: Query 1 (2.92x speedup)

**Original Time:** 239ms → **Optimized Time:** 82ms

**Rules Detected:**
- `QT-AGG-002` - Aggregate after join
- `QT-OPT-002` - Correlated subquery to CTE
- `QT-OPT-009` - Join reordering opportunity
- `SQL-SUB-001` - Correlated subquery in WHERE
- `QT-OPT-003` - Additional optimization opportunity

**What Worked:**
- Extracted correlated AVG subquery into separate CTE with GROUP BY
- Pre-aggregated before joining
- Reordered joins to put filtered tables first

### Regression Example: Query 16 (0.01x speedup - 113x slower!)

**Original Time:** 40ms → **Optimized Time:** 4520ms

**Rules Detected:**
- `SQL-AGG-006`
- `QT-OPT-003`
- `QT-OPT-009`
- `QT-DIST-001`
- `SQL-SUB-006`

**What Went Wrong:**
- Query was already fast (40ms)
- Over-optimization added complexity
- Lesson: Be conservative with already-fast queries

---

## Files Generated

1. **`rule_effectiveness_analysis.csv`** - Complete rule scores with metrics
2. **`rule_effectiveness_report.md`** - Formatted report with top/bottom rules
3. **`rule_effectiveness_detailed.md`** - Enhanced analysis with category breakdowns
4. **`rule_combinations.json`** - Top 20 rule combinations in JSON format
5. **`RULE_ANALYSIS_SUMMARY.md`** - This document (executive summary)

---

## Next Steps

### For LLM Optimization Pipeline

1. **Implement Rule-Based Prioritization:**
   - Weight queries with `SQL-SUB-001` or `SQL-DUCK-007` higher
   - Reduce priority for queries with only `QT-PLAN-001` or `SQL-DUCK-001`

2. **Add Combination Detection:**
   - Detect the 75% win-rate pattern (correlated subquery + aggregate + join)
   - Apply the full optimization suite when pattern is detected

3. **Add Safety Guards:**
   - Skip or use conservative approaches for queries with risky rules
   - Extra validation for queries with >60% loss rate rules

4. **A/B Testing:**
   - Test rule-weighted optimization vs. uniform approach
   - Measure improvement in win rate and average speedup

### For Further Analysis

1. Analyze why `QT-AGG-002` (most common rule) has low win rate
2. Investigate query characteristics that make `QT-PLAN-001` risky
3. Deep-dive into the 4 queries with the 75% winning pattern
4. Analyze correlation between query complexity and optimization success

---

**Generated by:** Rule Effectiveness Analysis Pipeline
**Scripts:** `analyze_rule_effectiveness.py`, `enhanced_rule_analysis.py`
