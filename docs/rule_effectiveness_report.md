# Rule Effectiveness Analysis Report

**Generated:** analyze_rule_effectiveness.py

## Summary

- **Total Queries Analyzed:** 98
- **Wins (>1.2x speedup):** 17 (17.3%)
- **Losses (<1.0x speedup):** 42 (42.9%)
- **Neutral (1.0-1.2x):** 39 (39.8%)
- **Average Speedup:** 1.06x
- **Unique Rules Detected:** 46

## Top 20 Rules by Predictive Score

Rules most likely to indicate successful optimization (>1.2x speedup).

| Rank | Rule | Appearances | Win Rate | Avg Speedup | Predictive Score | Net Score |
|------|------|-------------|----------|-------------|------------------|----------|
| 1 | `SQL-DUCK-007` | 4 | 75.0% | 1.54x | 0.750 | +2 |
| 2 | `SQL-SUB-001` | 5 | 60.0% | 1.95x | 0.600 | +2 |
| 3 | `SQL-AGG-007` | 2 | 50.0% | 1.09x | 0.500 | +0 |
| 4 | `SQL-CTE-005` | 2 | 50.0% | 1.56x | 0.500 | +1 |
| 5 | `QT-OPT-002` | 7 | 42.9% | 1.57x | 0.429 | +0 |
| 6 | `SQL-JOIN-010` | 10 | 40.0% | 1.17x | 0.400 | -1 |
| 7 | `QT-OPT-007` | 21 | 33.3% | 1.08x | 0.333 | +0 |
| 8 | `QT-DIST-001` | 9 | 33.3% | 0.90x | 0.333 | -1 |
| 9 | `SQL-AGG-002` | 3 | 33.3% | 1.10x | 0.333 | +1 |
| 10 | `SQL-AGG-008` | 3 | 33.3% | 1.14x | 0.333 | +1 |
| 11 | `SQL-DUCK-006` | 16 | 25.0% | 1.19x | 0.250 | -3 |
| 12 | `QT-OPT-005` | 4 | 25.0% | 1.12x | 0.250 | +0 |
| 13 | `QT-OPT-004` | 4 | 25.0% | 1.59x | 0.250 | -1 |
| 14 | `QT-OPT-009` | 39 | 20.5% | 1.11x | 0.205 | -10 |
| 15 | `SQL-SUB-006` | 5 | 20.0% | 0.73x | 0.200 | -1 |
| 16 | `SQL-WHERE-004` | 10 | 20.0% | 1.17x | 0.200 | +0 |
| 17 | `SQL-SUB-002` | 10 | 20.0% | 1.05x | 0.200 | -1 |
| 18 | `SQL-DUCK-018` | 10 | 20.0% | 1.05x | 0.200 | -1 |
| 19 | `QT-OPT-010` | 5 | 20.0% | 1.23x | 0.200 | +0 |
| 20 | `SQL-ORD-001` | 5 | 20.0% | 0.66x | 0.200 | -2 |

## Bottom 20 Rules by Win Rate

Rules most associated with regressions or neutral results.

| Rank | Rule | Appearances | Win Rate | Loss Rate | Avg Speedup | Net Score |
|------|------|-------------|----------|-----------|-------------|----------|
| 1 | `QT-OPT-008` | 1 | 0.0% | 0.0% | 1.14x | +0 |
| 2 | `SQL-SUB-003` | 3 | 0.0% | 33.3% | 0.68x | -1 |
| 3 | `SQL-SUB-007` | 1 | 0.0% | 0.0% | 1.02x | +0 |
| 4 | `SQL-UNION-001` | 2 | 0.0% | 50.0% | 0.98x | -1 |
| 5 | `SQL-UNION-002` | 1 | 0.0% | 0.0% | 1.09x | +0 |
| 6 | `QT-AGG-003` | 1 | 0.0% | 0.0% | 1.01x | +0 |
| 7 | `SQL-CTE-003` | 1 | 0.0% | 0.0% | 1.01x | +0 |
| 8 | `SQL-WHERE-008` | 1 | 0.0% | 0.0% | 1.01x | +0 |
| 9 | `QT-FILT-001` | 1 | 0.0% | 0.0% | 1.03x | +0 |
| 10 | `SQL-SEL-003` | 1 | 0.0% | 100.0% | 0.42x | -1 |
| 11 | `SQL-SUB-005` | 1 | 0.0% | 100.0% | 0.42x | -1 |
| 12 | `SQL-SEL-002` | 1 | 0.0% | 100.0% | 0.42x | -1 |
| 13 | `SQL-DUCK-002` | 40 | 10.0% | 42.5% | 1.02x | -13 |
| 14 | `SQL-JOIN-007` | 17 | 11.8% | 35.3% | 0.99x | -4 |
| 15 | `SQL-SUB-004` | 8 | 12.5% | 37.5% | 0.91x | -2 |
| 16 | `SQL-DUCK-001` | 8 | 12.5% | 62.5% | 0.68x | -4 |
| 17 | `SQL-WHERE-001` | 7 | 14.3% | 28.6% | 1.25x | -1 |
| 18 | `QT-OPT-003` | 56 | 16.1% | 39.3% | 1.07x | -13 |
| 19 | `SQL-AGG-006` | 37 | 16.2% | 45.9% | 0.93x | -11 |
| 20 | `QT-AGG-002` | 79 | 16.5% | 40.5% | 1.10x | -19 |

## Top 20 Rule Combinations

Rule combinations most predictive of successful optimization.

| Rank | Combination | Appearances | Win Rate | Avg Speedup | Net Score |
|------|-------------|-------------|----------|-------------|----------|
| 1 | `QT-AGG-002 + QT-OPT-002` | 4 | 75.0% | 2.16x | +2 |
| 2 | `QT-AGG-002 + SQL-SUB-001` | 4 | 75.0% | 2.16x | +2 |
| 3 | `QT-OPT-002 + QT-OPT-009` | 4 | 75.0% | 2.16x | +2 |
| 4 | `QT-OPT-003 + SQL-SUB-001` | 4 | 75.0% | 2.16x | +2 |
| 5 | `QT-OPT-009 + SQL-SUB-001` | 4 | 75.0% | 2.16x | +2 |
| 6 | `QT-AGG-002 + QT-OPT-002 + QT-OPT-003` | 4 | 75.0% | 2.16x | +2 |
| 7 | `QT-AGG-002 + QT-OPT-002 + QT-OPT-009` | 4 | 75.0% | 2.16x | +2 |
| 8 | `QT-AGG-002 + QT-OPT-002 + SQL-SUB-001` | 4 | 75.0% | 2.16x | +2 |
| 9 | `QT-AGG-002 + QT-OPT-003 + SQL-SUB-001` | 4 | 75.0% | 2.16x | +2 |
| 10 | `QT-AGG-002 + QT-OPT-009 + SQL-SUB-001` | 4 | 75.0% | 2.16x | +2 |
| 11 | `QT-OPT-002 + QT-OPT-003 + QT-OPT-009` | 4 | 75.0% | 2.16x | +2 |
| 12 | `QT-OPT-002 + QT-OPT-003 + SQL-SUB-001` | 4 | 75.0% | 2.16x | +2 |
| 13 | `QT-OPT-002 + QT-OPT-009 + SQL-SUB-001` | 4 | 75.0% | 2.16x | +2 |
| 14 | `QT-OPT-003 + QT-OPT-009 + SQL-SUB-001` | 4 | 75.0% | 2.16x | +2 |
| 15 | `QT-AGG-002 + SQL-DUCK-007` | 4 | 75.0% | 1.54x | +2 |
| 16 | `QT-BOOL-001 + SQL-WHERE-004` | 3 | 66.7% | 1.50x | +1 |
| 17 | `QT-OPT-001 + SQL-WHERE-004` | 3 | 66.7% | 1.50x | +1 |
| 18 | `QT-BOOL-001 + QT-OPT-001 + SQL-WHERE-004` | 3 | 66.7% | 1.50x | +1 |
| 19 | `QT-OPT-002 + SQL-SUB-001` | 5 | 60.0% | 1.95x | +2 |
| 20 | `QT-DIST-001 + SQL-AGG-006` | 5 | 60.0% | 0.82x | +1 |

## Statistical Insights

### High-Value Rules (0 rules)

Rules with >70% win rate and 5+ appearances:


### Risky Rules (4 rules)

Rules with >50% loss rate and 5+ appearances:

- **`SQL-ORD-001`**: 3/5 losses (60.0%), avg 0.66x speedup
- **`QT-OPT-011`**: 3/5 losses (60.0%), avg 0.66x speedup
- **`QT-PLAN-001`**: 4/6 losses (66.7%), avg 0.61x speedup
- **`SQL-DUCK-001`**: 5/8 losses (62.5%), avg 0.68x speedup

## Recommendations

### Prioritize These Rules for Optimization

Focus LLM attention on queries where these high-value rules are detected:

1. **`SQL-DUCK-007`** - 75% win rate, 1.54x avg speedup
2. **`SQL-SUB-001`** - 60% win rate, 1.95x avg speedup
5. **`QT-OPT-002`** - 43% win rate, 1.57x avg speedup
6. **`SQL-JOIN-010`** - 40% win rate, 1.17x avg speedup
7. **`QT-OPT-007`** - 33% win rate, 1.08x avg speedup
8. **`QT-DIST-001`** - 33% win rate, 0.90x avg speedup
9. **`SQL-AGG-002`** - 33% win rate, 1.10x avg speedup
10. **`SQL-AGG-008`** - 33% win rate, 1.14x avg speedup

### Use Caution with These Rules

These rules may indicate queries that are harder to optimize or have edge cases:

1. **`SQL-SUB-003`** - 0% win rate, 33% loss rate

### Rule Combination Insights

When these rule combinations appear together, optimization success is highly likely:

1. **`QT-AGG-002 + QT-OPT-002`** - 75% win rate, 2.16x avg speedup
2. **`QT-AGG-002 + SQL-SUB-001`** - 75% win rate, 2.16x avg speedup
3. **`QT-OPT-002 + QT-OPT-009`** - 75% win rate, 2.16x avg speedup
4. **`QT-OPT-003 + SQL-SUB-001`** - 75% win rate, 2.16x avg speedup
5. **`QT-OPT-009 + SQL-SUB-001`** - 75% win rate, 2.16x avg speedup
