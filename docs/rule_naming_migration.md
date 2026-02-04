# Rule Naming Migration: GLD-XXX vs SQL-XXX

**Standard naming scheme:**
- `GLD-XXX` = Gold standard rules with proven benchmark speedups
- `SQL-XXX` = Pattern detection rules (not yet verified or negative correlation)

## Gold Rules (GLD-XXX) - Verified Transforms

These rules detect patterns that have produced proven speedups in benchmarks:

| New ID | Old ID | Name | Transform | Proven Speedup | Evidence |
|--------|--------|------|-----------|----------------|----------|
| **GLD-001** | QT-OPT-002 | Decorrelate Subquery to CTE | decorrelate | 2.81x (Q1) | Strong predictor, avg 1.32x speedup |
| **GLD-002** | QT-OPT-001 | OR to UNION ALL | or_to_union | 2.67x (Q15) | Avg 1.18x speedup |
| **GLD-005** | SQL-SUB-001 | Correlated Subquery in WHERE | correlated_subquery | 1.80x avg | 67% win rate, strongest SQL-* predictor |

## ⚠️ Missing Gold Rules (Need Creation)

These high-value transforms are NOT detected by current AST rules:

| New ID | Name | Transform | Proven Speedup | Priority |
|--------|------|-----------|----------------|----------|
| **GLD-003** | Early Filter Pushdown (Dimension Before Fact) | early_filter | 2.71x (Q93), 1.84x (Q90) | 🔴 CRITICAL |
| **GLD-004** | Projection Pruning | projection_prune | 1.21x (Q78) | 🔴 CRITICAL |

## SQL Detection Rules (SQL-XXX)

Pattern detection rules (may have negative or neutral correlation with speedups):

<details>
<summary>Click to expand complete mapping</summary>

| New ID | Old ID | Frequency | Notes |
|--------|--------|-----------|-------|
| SQL-001 | QT-AGG-002 | 131 | Aggregation pattern |
| SQL-002 | QT-AGG-003 | 1 | Aggregation pattern |
| SQL-003 | QT-BOOL-001 | 16 |  |
| SQL-004 | QT-CTE-002 | 17 |  |
| SQL-005 | QT-DIST-001 | 10 |  |
| SQL-006 | QT-FILT-001 | 1 |  |
| SQL-007 | QT-OPT-003 | 77 | Over-optimized pattern |
| SQL-008 | QT-OPT-004 | 4 | Over-optimized pattern |
| SQL-009 | QT-OPT-005 | 4 | Over-optimized pattern |
| SQL-010 | QT-OPT-006 | 6 | Over-optimized pattern |
| SQL-011 | QT-OPT-007 | 28 | Over-optimized pattern |
| SQL-012 | QT-OPT-008 | 1 | Over-optimized pattern |
| SQL-013 | QT-OPT-009 | 41 | Over-optimized pattern |
| SQL-014 | QT-OPT-010 | 5 | Over-optimized pattern |
| SQL-015 | QT-OPT-011 | 6 | Over-optimized pattern |
| SQL-016 | QT-PLAN-001 | 6 |  |
| SQL-017 | SQL-AGG-002 | 3 |  |
| SQL-018 | SQL-AGG-006 | 42 |  |
| SQL-019 | SQL-AGG-007 | 2 |  |
| SQL-020 | SQL-AGG-008 | 3 |  |
| SQL-021 | SQL-CTE-003 | 1 |  |
| SQL-022 | SQL-CTE-005 | 2 |  |
| SQL-023 | SQL-DUCK-001 | 8 | DuckDB-specific |
| SQL-024 | SQL-DUCK-002 | 44 | DuckDB-specific |
| SQL-025 | SQL-DUCK-006 | 17 | DuckDB-specific |
| SQL-026 | SQL-DUCK-007 | 6 | DuckDB-specific |
| SQL-027 | SQL-DUCK-018 | 25 | DuckDB-specific |
| SQL-028 | SQL-JOIN-007 | 17 |  |
| SQL-029 | SQL-JOIN-010 | 10 |  |
| SQL-030 | SQL-ORD-001 | 6 |  |
| SQL-031 | SQL-SEL-002 | 2 |  |
| SQL-032 | SQL-SEL-003 | 1 |  |
| SQL-033 | SQL-SUB-002 | 25 |  |
| SQL-034 | SQL-SUB-003 | 3 |  |
| SQL-035 | SQL-SUB-004 | 8 |  |
| SQL-036 | SQL-SUB-005 | 1 |  |
| SQL-037 | SQL-SUB-006 | 5 |  |
| SQL-038 | SQL-SUB-007 | 1 |  |
| SQL-039 | SQL-UNION-001 | 2 |  |
| SQL-040 | SQL-UNION-002 | 1 |  |
| SQL-041 | SQL-WHERE-001 | 8 |  |
| SQL-042 | SQL-WHERE-004 | 15 |  |
| SQL-043 | SQL-WHERE-008 | 2 |  |

</details>

## Migration Plan

### Phase 1: Create Missing Gold Rules (Priority)

1. **GLD-003 (Early Filter)**: Create AST detector for dimension filter before fact join
   - Pattern: `WITH filtered_dim AS (SELECT ... WHERE selective_filter) SELECT ... FROM fact JOIN filtered_dim`
   - Detection: Dimension table with selective filter joined to fact table

2. **GLD-004 (Projection Pruning)**: Create detector for unused columns in CTEs
   - Pattern: CTE selects columns that aren't used in main query
   - Detection: Column in CTE SELECT list not referenced later

### Phase 2: Rename Existing Rules

**Code changes required:**

1. Update rule_id in all rule classes:
   - `packages/qt-sql/qt_sql/analyzers/ast_detector/rules/*.py`

2. Update references in:
   - Knowledge base mappings
   - Documentation
   - Test cases

### Phase 3: Update Rule Categories

**New severity/category scheme:**

- `GLD-XXX` rules:
  - `severity: 'gold'`
  - `category: 'verified_optimization'`
  - Higher priority in scoring

- `SQL-XXX` rules:
  - `severity: 'low' | 'medium' | 'info'`
  - `category: 'pattern_detection'`
  - Lower priority, informational

## Summary

- **Gold rules (existing):** 3
- **Gold rules (missing):** 2 ⚠️
- **SQL detection rules:** 43

**Action items:**
1. Create GLD-003 and GLD-004 AST detectors (highest priority)
2. Rename QT-OPT-002 → GLD-001
3. Rename QT-OPT-001 → GLD-002
4. Rename SQL-SUB-001 → GLD-005
5. Rename 43 rules to SQL-XXX scheme

---

*Generated from TPC-DS benchmark analysis*