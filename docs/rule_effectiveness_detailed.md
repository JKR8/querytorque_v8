# Enhanced Rule Effectiveness Analysis

Detailed correlation analysis between AST detector rules and query optimization success.

## Executive Summary

**Total Queries:** 98

### Speedup Distribution

- **Big Wins (>2.0x):** 5 queries (5.1%)
- **Good Wins (1.2-2.0x):** 12 queries (12.2%)
- **Neutral (1.0-1.2x):** 39 queries (39.8%)
- **Regressions (<1.0x):** 42 queries (42.9%)

## Key Findings

### Most Predictive Rules

Rules with highest correlation to successful optimization:

1. **`SQL-DUCK-007`**
   - Win Rate: 75.0%
   - Average Speedup: 1.54x
   - Appearances: 4

2. **`SQL-SUB-001`**
   - Win Rate: 60.0%
   - Average Speedup: 1.95x
   - Appearances: 5

3. **`SQL-AGG-007`**
   - Win Rate: 50.0%
   - Average Speedup: 1.09x
   - Appearances: 2

4. **`SQL-CTE-005`**
   - Win Rate: 50.0%
   - Average Speedup: 1.56x
   - Appearances: 2

5. **`QT-OPT-002`**
   - Win Rate: 42.9%
   - Average Speedup: 1.57x
   - Appearances: 7

### Most Risky Rules

Rules associated with regressions:

1. **`SQL-SEL-003`**
   - Loss Rate: 100.0%
   - Win Rate: 0.0%
   - Average Speedup: 0.42x
   - Appearances: 1

2. **`SQL-SUB-005`**
   - Loss Rate: 100.0%
   - Win Rate: 0.0%
   - Average Speedup: 0.42x
   - Appearances: 1

3. **`SQL-SEL-002`**
   - Loss Rate: 100.0%
   - Win Rate: 0.0%
   - Average Speedup: 0.42x
   - Appearances: 1

4. **`QT-PLAN-001`**
   - Loss Rate: 66.7%
   - Win Rate: 16.7%
   - Average Speedup: 0.61x
   - Appearances: 6

5. **`SQL-DUCK-001`**
   - Loss Rate: 62.5%
   - Win Rate: 12.5%
   - Average Speedup: 0.68x
   - Appearances: 8

## Analysis by Speedup Category

### Big Wins (>2.0x)

**5 queries**

Top rules in this category:

| Rule | Appearances | % of Category |
|------|-------------|---------------|
| `QT-AGG-002` | 5 | 100.0% |
| `QT-OPT-003` | 3 | 60.0% |
| `QT-OPT-002` | 2 | 40.0% |
| `QT-OPT-009` | 2 | 40.0% |
| `SQL-SUB-001` | 2 | 40.0% |
| `SQL-WHERE-004` | 1 | 20.0% |
| `QT-BOOL-001` | 1 | 20.0% |
| `QT-OPT-001` | 1 | 20.0% |
| `SQL-WHERE-001` | 1 | 20.0% |
| `SQL-DUCK-007` | 1 | 20.0% |

Example queries:
- **Query 1**: 2.92x speedup (239ms → 82ms)
  Rules: `QT-AGG-002, QT-OPT-002, QT-OPT-009, SQL-SUB-001, QT-OPT-003`
- **Query 15**: 2.78x speedup (150ms → 54ms)
  Rules: `QT-AGG-002, QT-OPT-003, SQL-WHERE-004, QT-BOOL-001, QT-OPT-001` +1 more
- **Query 2**: 2.10x speedup (937ms → 447ms)
  Rules: `SQL-DUCK-007, QT-AGG-002, QT-OPT-006, QT-OPT-007, SQL-JOIN-010` +3 more

### Good Wins (1.2-2.0x)

**12 queries**

Top rules in this category:

| Rule | Appearances | % of Category |
|------|-------------|---------------|
| `QT-AGG-002` | 8 | 66.7% |
| `SQL-AGG-006` | 6 | 50.0% |
| `QT-OPT-007` | 6 | 50.0% |
| `QT-OPT-003` | 6 | 50.0% |
| `QT-OPT-009` | 6 | 50.0% |
| `SQL-DUCK-002` | 4 | 33.3% |
| `QT-DIST-001` | 3 | 25.0% |
| `SQL-DUCK-006` | 3 | 25.0% |
| `SQL-JOIN-010` | 3 | 25.0% |
| `QT-CTE-002` | 2 | 16.7% |

Example queries:
- **Query 28**: 1.33x speedup (3731ms → 2814ms)
  Rules: `SQL-AGG-006, QT-OPT-007, QT-DIST-001, SQL-WHERE-004, QT-BOOL-001` +1 more
- **Query 35**: 1.51x speedup (1148ms → 761ms)
  Rules: `SQL-AGG-006, SQL-DUCK-006, QT-AGG-002, QT-OPT-003, QT-OPT-009` +2 more
- **Query 51**: 1.51x speedup (7935ms → 5247ms)
  Rules: `QT-PLAN-001, QT-AGG-002, QT-OPT-007, SQL-ORD-001, SQL-DUCK-001` +2 more

### Neutral (1.0-1.2x)

**39 queries**

Top rules in this category:

| Rule | Appearances | % of Category |
|------|-------------|---------------|
| `QT-AGG-002` | 34 | 87.2% |
| `QT-OPT-003` | 25 | 64.1% |
| `SQL-DUCK-002` | 19 | 48.7% |
| `SQL-AGG-006` | 14 | 35.9% |
| `QT-OPT-009` | 13 | 33.3% |
| `QT-CTE-002` | 12 | 30.8% |
| `SQL-JOIN-007` | 9 | 23.1% |
| `QT-OPT-007` | 7 | 17.9% |
| `SQL-WHERE-004` | 6 | 15.4% |
| `SQL-DUCK-006` | 5 | 12.8% |

Example queries:
- **Query 10**: 1.02x speedup (290ms → 285ms)
  Rules: `SQL-AGG-006, SQL-DUCK-006, QT-AGG-002, QT-OPT-003, QT-OPT-009` +2 more
- **Query 12**: 1.01x speedup (110ms → 109ms)
  Rules: `SQL-AGG-006, QT-AGG-002, QT-OPT-003, SQL-DUCK-002`
- **Query 13**: 1.01x speedup (981ms → 974ms)
  Rules: `QT-OPT-003, SQL-WHERE-004`

### Regressions (<1.0x)

**42 queries**

Top rules in this category:

| Rule | Appearances | % of Category |
|------|-------------|---------------|
| `QT-AGG-002` | 32 | 76.2% |
| `QT-OPT-003` | 22 | 52.4% |
| `QT-OPT-009` | 18 | 42.9% |
| `SQL-DUCK-002` | 17 | 40.5% |
| `SQL-AGG-006` | 17 | 40.5% |
| `SQL-DUCK-006` | 7 | 16.7% |
| `QT-OPT-007` | 7 | 16.7% |
| `SQL-JOIN-007` | 6 | 14.3% |
| `SQL-JOIN-010` | 5 | 11.9% |
| `SQL-DUCK-001` | 5 | 11.9% |

Example queries:
- **Query 11**: 0.98x speedup (6017ms → 6119ms)
  Rules: `QT-AGG-002, QT-OPT-009, SQL-DUCK-006, SQL-DUCK-002`
- **Query 14**: 0.95x speedup (9211ms → 9708ms)
  Rules: `SQL-JOIN-007, SQL-SUB-004, SQL-AGG-006, SQL-AGG-007, QT-AGG-002` +7 more
- **Query 16**: 0.01x speedup (40ms → 4520ms)
  Rules: `SQL-AGG-006, QT-OPT-003, QT-OPT-009, QT-DIST-001, SQL-SUB-006`

## Strongly Correlated Rules

### Rules Strongly Associated with Wins

Rules that appear predominantly in successful optimizations:

| Rule | Win Appearances | Total | Win % |
|------|-----------------|-------|-------|
| `SQL-SUB-001` | 3 | 5 | 60.0% |

### Rules Strongly Associated with Regressions

Rules that appear predominantly in queries with regressions:

| Rule | Loss Appearances | Total | Loss % |
|------|------------------|-------|--------|
| `QT-PLAN-001` | 4 | 6 | 66.7% |
| `SQL-DUCK-001` | 5 | 8 | 62.5% |
| `SQL-ORD-001` | 3 | 5 | 60.0% |
| `QT-OPT-011` | 3 | 5 | 60.0% |

## Rule Combination Analysis

### Top Performing Combinations

1. **`QT-AGG-002 + QT-OPT-002`**
   - Appearances: 4
   - Win Rate: 75.0%
   - Average Speedup: 2.16x
   - Net Score: +2

2. **`QT-AGG-002 + SQL-SUB-001`**
   - Appearances: 4
   - Win Rate: 75.0%
   - Average Speedup: 2.16x
   - Net Score: +2

3. **`QT-OPT-002 + QT-OPT-009`**
   - Appearances: 4
   - Win Rate: 75.0%
   - Average Speedup: 2.16x
   - Net Score: +2

4. **`QT-OPT-003 + SQL-SUB-001`**
   - Appearances: 4
   - Win Rate: 75.0%
   - Average Speedup: 2.16x
   - Net Score: +2

5. **`QT-OPT-009 + SQL-SUB-001`**
   - Appearances: 4
   - Win Rate: 75.0%
   - Average Speedup: 2.16x
   - Net Score: +2

6. **`QT-AGG-002 + QT-OPT-002 + QT-OPT-003`**
   - Appearances: 4
   - Win Rate: 75.0%
   - Average Speedup: 2.16x
   - Net Score: +2

7. **`QT-AGG-002 + QT-OPT-002 + QT-OPT-009`**
   - Appearances: 4
   - Win Rate: 75.0%
   - Average Speedup: 2.16x
   - Net Score: +2

8. **`QT-AGG-002 + QT-OPT-002 + SQL-SUB-001`**
   - Appearances: 4
   - Win Rate: 75.0%
   - Average Speedup: 2.16x
   - Net Score: +2

9. **`QT-AGG-002 + QT-OPT-003 + SQL-SUB-001`**
   - Appearances: 4
   - Win Rate: 75.0%
   - Average Speedup: 2.16x
   - Net Score: +2

10. **`QT-AGG-002 + QT-OPT-009 + SQL-SUB-001`**
   - Appearances: 4
   - Win Rate: 75.0%
   - Average Speedup: 2.16x
   - Net Score: +2

## Actionable Insights

### Priority Rules for LLM Focus

When these rules are detected, the LLM should prioritize optimization efforts:

1. **`SQL-DUCK-007`** - 75% win rate, 1.54x avg speedup
2. **`SQL-SUB-001`** - 60% win rate, 1.95x avg speedup
3. **`QT-OPT-002`** - 43% win rate, 1.57x avg speedup
4. **`QT-OPT-004`** - 25% win rate, 1.59x avg speedup

### Rules Requiring Caution

When these rules are detected, carefully validate optimizations:

1. **`SQL-ORD-001`** - 60% loss rate, 0.66x avg speedup
2. **`QT-OPT-011`** - 60% loss rate, 0.66x avg speedup
3. **`QT-PLAN-001`** - 67% loss rate, 0.61x avg speedup
4. **`SQL-DUCK-001`** - 62% loss rate, 0.68x avg speedup

### Optimization Strategy Recommendations

1. **High Priority Patterns**: Focus on queries with correlated subqueries (SQL-SUB-001) and aggregate-after-join patterns (QT-AGG-002 + QT-OPT-002)

2. **Pre-computation Strategy**: Queries with QT-OPT-002 (Correlated Subquery to CTE) show 1.57x average speedup - prioritize CTE extraction

3. **Join Reordering**: QT-OPT-009 appears in 39 queries but only 20.5% win rate - be selective about when to reorder joins

4. **Risk Mitigation**: Queries with SQL-DUCK-001, SQL-ORD-001, or QT-PLAN-001 have >60% loss rate - use conservative optimizations

## Statistical Summary

- **Total Rules Analyzed:** 46
- **Rules with >50% Win Rate:** 2
- **Rules with >50% Loss Rate:** 7
- **Rule Combinations Analyzed:** 20
- **Combinations with >70% Win Rate:** 15

---

*Generated by enhanced_rule_analysis.py*
