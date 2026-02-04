# Rule Effectiveness Analysis (Revised)

**Critical insight:** Many high-value transforms are NOT detected by AST rules.

## Detection Coverage for Winning Transforms

| Query | Speedup | Transform | Expected Rule | Detected? | Rules Found |
|-------|---------|-----------|---------------|-----------|-------------|
| Q1 | **2.81x** | decorrelate | QT-OPT-002 | ✓ | QT-AGG-002, SQL-SUB-001, QT-OPT-009 +2 more |
| Q93 | **2.71x** | early_filter | MISSING | ✗ | QT-AGG-002 |
| Q15 | **2.67x** | or_to_union | QT-OPT-001 | ✓ | QT-BOOL-001, SQL-WHERE-004, QT-AGG-002 +3 more |
| Q90 | **1.84x** | early_filter | MISSING | ✗ | QT-OPT-007, SQL-JOIN-010, QT-OPT-009 +1 more |
| Q74 | **1.42x** | pushdown | QT-OPT-004 | ✗ | QT-OPT-003, QT-OPT-009, SQL-DUCK-002 |
| Q80 | **1.24x** | early_filter | MISSING | ✗ | SQL-JOIN-007, QT-OPT-003, QT-AGG-002 +1 more |
| Q73 | **1.24x** | pushdown | QT-OPT-004 | ✗ | QT-AGG-002 |
| Q27 | **1.23x** | early_filter | MISSING | ✗ | QT-OPT-003, QT-AGG-002 |
| Q78 | **1.21x** | projection_prune | MISSING | ✗ | QT-BOOL-001, QT-AGG-002, SQL-WHERE-008 +5 more |

**Coverage: 2/9 (22%)**

### ⚠️ HIGH-VALUE TRANSFORMS NOT DETECTED

These transforms produced big wins but have no AST detection:

- **early_filter** (2.71x speedup) - MISSING
- **early_filter** (1.84x speedup) - MISSING
- **pushdown** (1.42x speedup) - QT-OPT-004
- **early_filter** (1.24x speedup) - MISSING
- **pushdown** (1.24x speedup) - QT-OPT-004
- **early_filter** (1.23x speedup) - MISSING
- **projection_prune** (1.21x speedup) - MISSING

## Over-Optimized Rules

Rules that appear frequently but optimizer makes them WORSE:

| Rule ID | Total | Improvements | Regressions | Over-Opt Score |
|---------|-------|--------------|-------------|----------------|
| **SQL-DUCK-006** | 11 | 1 | 5 | 0.36 |
| **SQL-DUCK-002** | 39 | 3 | 17 | 0.36 |
| **SQL-AGG-006** | 33 | 4 | 15 | 0.33 |
| **QT-OPT-009** | 33 | 5 | 15 | 0.30 |
| **QT-AGG-002** | 68 | 7 | 27 | 0.29 |
| **SQL-JOIN-007** | 17 | 2 | 6 | 0.24 |
| **QT-OPT-003** | 50 | 7 | 18 | 0.22 |
| **QT-OPT-007** | 15 | 3 | 5 | 0.13 |
| **SQL-DUCK-018** | 10 | 2 | 3 | 0.10 |
| **SQL-SUB-002** | 10 | 2 | 3 | 0.10 |

**Interpretation:** These rules detect patterns where DuckDB's optimizer already does well. Our LLM applying additional transforms makes queries WORSE.

## Rule Co-Occurrence (Top 15 Pairs)

Rules that frequently appear together (not independent):

| Rule 1 | Rule 2 | Co-occurrences |
|--------|--------|----------------|
| QT-AGG-002 | QT-OPT-003 | 40 |
| QT-AGG-002 | SQL-DUCK-002 | 36 |
| QT-AGG-002 | QT-OPT-009 | 27 |
| QT-AGG-002 | SQL-AGG-006 | 24 |
| QT-OPT-003 | SQL-DUCK-002 | 22 |
| QT-OPT-003 | SQL-AGG-006 | 20 |
| QT-OPT-003 | QT-OPT-009 | 19 |
| QT-OPT-009 | SQL-DUCK-002 | 19 |
| QT-OPT-009 | SQL-AGG-006 | 16 |
| QT-AGG-002 | SQL-JOIN-007 | 15 |
| QT-AGG-002 | QT-CTE-002 | 15 |
| SQL-AGG-006 | SQL-DUCK-002 | 15 |
| QT-CTE-002 | QT-OPT-003 | 12 |
| QT-AGG-002 | SQL-DUCK-006 | 11 |
| QT-AGG-002 | QT-OPT-007 | 11 |

## Recommendations

### 1. Add Missing Detection Rules

Priority transforms to add AST detection for:

- **early_filter**: 2.71x, 1.84x, 1.24x, 1.23x wins (4 big wins, NO detection!)
- **projection_prune**: 1.21x win (no detection)

### 2. Reduce False Positives

Rules with high over-optimization scores need refinement:

- **SQL-DUCK-006**: 36% over-optimization rate
- **SQL-DUCK-002**: 36% over-optimization rate
- **SQL-AGG-006**: 33% over-optimization rate
- **QT-OPT-009**: 30% over-optimization rate
- **QT-AGG-002**: 29% over-optimization rate

### 3. Treat Co-Occurring Rules as Patterns

Rules that co-occur >15 times should be evaluated together, not independently.

---

*Analysis based on TPC-DS SF100 benchmark (Kimi K2.5, 87 validated queries)*