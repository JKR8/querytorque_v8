# Rule Effectiveness Quick Reference

**Quick lookup table for AST detector rule effectiveness**

---

## Legend

- **Win Rate:** % of queries with >1.2x speedup when this rule is present
- **Loss Rate:** % of queries with <1.0x speedup (regressions)
- **Avg Speedup:** Average speedup across all queries with this rule
- **Priority:** HIGH (>50% win rate), MEDIUM (30-50%), LOW (<30%), RISKY (>50% loss rate)

---

## Priority Classification

### HIGH PRIORITY ⭐⭐⭐

**Focus optimization efforts here - high success rate**

| Rule | Win Rate | Loss Rate | Avg Speedup | Appearances |
|------|----------|-----------|-------------|-------------|
| `SQL-DUCK-007` | 75.0% | 25.0% | 1.54x | 4 |
| `SQL-SUB-001` | 60.0% | 20.0% | 1.95x | 5 |
| `SQL-CTE-005` | 50.0% | 0.0% | 1.56x | 2 |
| `SQL-AGG-007` | 50.0% | 50.0% | 1.09x | 2 |

### MEDIUM PRIORITY ⭐⭐

**Good targets but less reliable**

| Rule | Win Rate | Loss Rate | Avg Speedup | Appearances |
|------|----------|-----------|-------------|-------------|
| `QT-OPT-002` | 42.9% | 42.9% | 1.57x | 7 |
| `SQL-JOIN-010` | 40.0% | 50.0% | 1.17x | 10 |
| `QT-OPT-007` | 33.3% | 33.3% | 1.08x | 21 |
| `QT-DIST-001` | 33.3% | 44.4% | 0.90x | 9 |
| `SQL-AGG-002` | 33.3% | 0.0% | 1.10x | 3 |
| `SQL-AGG-008` | 33.3% | 0.0% | 1.14x | 3 |

### LOW PRIORITY ⭐

**Common but low effectiveness - optimize selectively**

| Rule | Win Rate | Loss Rate | Avg Speedup | Appearances |
|------|----------|-----------|-------------|-------------|
| `SQL-DUCK-006` | 25.0% | 43.8% | 1.19x | 16 |
| `QT-OPT-009` | 20.5% | 46.2% | 1.11x | 39 |
| `SQL-WHERE-004` | 20.0% | 20.0% | 1.17x | 10 |
| `QT-CTE-002` | 17.6% | 11.8% | 1.13x | 17 |
| `QT-AGG-002` | 16.5% | 40.5% | 1.10x | 79 |
| `SQL-AGG-006` | 16.2% | 45.9% | 0.93x | 37 |
| `QT-OPT-003` | 16.1% | 39.3% | 1.07x | 56 |

### RISKY ⚠️

**High regression rate - use extreme caution**

| Rule | Win Rate | Loss Rate | Avg Speedup | Appearances |
|------|----------|-----------|-------------|-------------|
| `QT-PLAN-001` | 16.7% | 66.7% | 0.61x | 6 |
| `SQL-DUCK-001` | 12.5% | 62.5% | 0.68x | 8 |
| `SQL-ORD-001` | 20.0% | 60.0% | 0.66x | 5 |
| `QT-OPT-011` | 20.0% | 60.0% | 0.66x | 5 |
| `SQL-DUCK-002` | 10.0% | 42.5% | 1.02x | 40 |
| `SQL-SEL-003` | 0.0% | 100.0% | 0.42x | 1 |
| `SQL-SUB-005` | 0.0% | 100.0% | 0.42x | 1 |
| `SQL-SEL-002` | 0.0% | 100.0% | 0.42x | 1 |

---

## Rule Descriptions

### High Priority Rules

**`SQL-DUCK-007`** - DuckDB-specific optimization opportunity
- **When to apply:** DuckDB-specific syntax or function usage
- **Why it works:** Leverages DuckDB's optimizations
- **Example:** Using DuckDB's native functions vs. generic SQL

**`SQL-SUB-001`** - Correlated subquery in WHERE clause
- **When to apply:** Subquery references outer query in WHERE
- **Why it works:** Convert to JOIN or CTE eliminates per-row execution
- **Example:** `WHERE col > (SELECT AVG(x) FROM t2 WHERE t2.id = t1.id)`

**`SQL-CTE-005`** - CTE optimization opportunity
- **When to apply:** Repeated subquery expressions or complex logic
- **Why it works:** CTE can be materialized once and reused
- **Example:** Multiple references to same subquery

**`QT-OPT-002`** - Correlated subquery to pre-computed CTE
- **When to apply:** Correlated aggregate subquery
- **Why it works:** Pre-compute aggregates in CTE, then JOIN
- **Example:** Convert correlated AVG to GROUP BY CTE

### Risky Rules (Avoid/Use Caution)

**`QT-PLAN-001`** - Query plan structure concerns
- **Why risky:** May indicate already-optimized query
- **Risk:** 66.7% loss rate, 0.61x avg speedup
- **Recommendation:** Skip or use minimal changes

**`SQL-DUCK-001`** - DuckDB-specific anti-pattern
- **Why risky:** May break DuckDB's native optimizations
- **Risk:** 62.5% loss rate, 0.68x avg speedup
- **Recommendation:** Conservative approach, validate thoroughly

**`SQL-ORD-001`** / **`QT-OPT-011`** - ORDER BY optimizations
- **Why risky:** Sort operations are sensitive to data distribution
- **Risk:** 60% loss rate, 0.66x avg speedup
- **Recommendation:** Only optimize if ORDER BY is clearly redundant

---

## Top 10 Rule Combinations (75% Win Rate)

All of these patterns achieve **75% win rate** and **2.16x average speedup**:

1. `QT-AGG-002 + QT-OPT-002` - Aggregate after join + Correlated subquery to CTE
2. `QT-AGG-002 + SQL-SUB-001` - Aggregate after join + Correlated subquery in WHERE
3. `QT-OPT-002 + QT-OPT-009` - Correlated subquery to CTE + Join reordering
4. `QT-OPT-003 + SQL-SUB-001` - [Optimization] + Correlated subquery
5. `QT-OPT-009 + SQL-SUB-001` - Join reordering + Correlated subquery
6. `QT-AGG-002 + QT-OPT-002 + QT-OPT-003` - Triple combination
7. `QT-AGG-002 + QT-OPT-002 + QT-OPT-009` - Triple combination
8. `QT-AGG-002 + QT-OPT-002 + SQL-SUB-001` - Triple combination
9. `QT-AGG-002 + QT-OPT-003 + SQL-SUB-001` - Triple combination
10. `QT-OPT-002 + QT-OPT-003 + QT-OPT-009` - Triple combination

**Pattern:** All involve correlated subqueries (`SQL-SUB-001` or `QT-OPT-002`) combined with aggregate and/or join optimizations.

---

## Decision Tree for LLM

```
Query Analysis
    │
    ├─ Contains SQL-SUB-001 or SQL-DUCK-007?
    │   └─ YES → HIGH PRIORITY (60-75% win rate)
    │       └─ Apply: CTE extraction, JOIN conversion
    │
    ├─ Contains QT-AGG-002 + QT-OPT-002 combination?
    │   └─ YES → VERY HIGH PRIORITY (75% win rate)
    │       └─ Apply: Pre-aggregate in CTE + JOIN reordering
    │
    ├─ Contains QT-PLAN-001 or SQL-DUCK-001?
    │   └─ YES → RISKY (60-67% loss rate)
    │       └─ Apply: Conservative changes only, validate thoroughly
    │
    ├─ Contains only common rules (QT-AGG-002, QT-OPT-003)?
    │   └─ YES → LOW PRIORITY (16% win rate)
    │       └─ Apply: Selective optimization, focus on obvious wins
    │
    └─ Query already fast (<100ms)?
        └─ YES → SKIP or CONSERVATIVE
            └─ Risk of over-optimization (see Query 16: 40ms → 4520ms)
```

---

## Statistics at a Glance

| Metric | Value |
|--------|-------|
| **Total Queries** | 98 |
| **Total Rules** | 46 |
| **Rules with >50% Win Rate** | 2 |
| **Rules with >50% Loss Rate** | 7 |
| **Most Common Rule** | `QT-AGG-002` (79 appearances, 16.5% win rate) |
| **Best Win Rate** | `SQL-DUCK-007` (75% win rate) |
| **Best Avg Speedup** | `SQL-SUB-001` (1.95x avg) |
| **Worst Loss Rate** | `QT-PLAN-001` (66.7% loss rate) |

---

## Key Insights

1. **Frequency ≠ Effectiveness:** Most common rule (`QT-AGG-002`, 79 appearances) has only 16.5% win rate
2. **Correlated Subqueries = Gold:** `SQL-SUB-001` and `QT-OPT-002` are top performers
3. **Combinations Matter:** 75% win rate achieved by specific rule combinations
4. **Join Reordering Caution:** `QT-OPT-009` appears in 39 queries but only 20.5% win rate
5. **Already-Fast Queries:** Be conservative with queries <100ms (risk of over-optimization)

---

**Last Updated:** 2026-02-04
**Data Source:** TPC-DS SF100 Kimi Benchmark (98 queries)
