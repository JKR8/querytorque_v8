# SQL Optimization Results

> **Knowledge Base**: [Working Optimizations](WORKING_OPTIMIZATIONS.md) ← patterns with examples
>
> **Architecture**: [Prompt Architecture](PROMPT_ARCHITECTURE.md) | [Agentic Loop Design](AGENTIC_LOOP_DESIGN.md)
>
> **Details**: [Q23 Optimization Process](Q23_OPTIMIZATION_PROCESS.md) | [Prompt & System Design](PROMPT_AND_SYSTEM_DESIGN.md)
>
> **Prompts**: [Q1](prompts/q1_prompt.md) | [Q23](prompts/q23_prompt.md)

## Summary

| Query | Status | Speedup | Best Model | Optimization Type |
|-------|--------|---------|------------|-------------------|
| [Q1](q1_tpcds.md) | ✅ SUCCESS | 2.10x | DeepSeek-reasoner | Predicate pushdown |
| [Q2](q2_tpcds.md) | ✅ SUCCESS | 2.09x | Gemini | Filter pushdown |
| [Q23](q23_tpcds.md) | ✅ SUCCESS | 2.18x | Gemini | Join elimination |

## Key Findings

### AST Rules vs EXPLAIN Plan

| Query | AST Issues Detected | Issues That Mattered | Overlap |
|-------|---------------------|----------------------|---------|
| Q1 | 9 | 1 (predicate pushdown) | **0%** |
| Q2 | 13 | 1 (filter pushdown) | **0%** |

**Conclusion:** AST rules detect syntax patterns. EXPLAIN plans reveal performance bottlenecks. They are completely different.

### Model Comparison

| Model | Q1 | Q2 | Q23 | Notes |
|-------|----|----|-----|-------|
| DeepSeek-reasoner | ✅ 2.10x | - | - | Conservative, correct |
| Gemini | - | ✅ 2.09x | ✅ 2.18x | Join elimination with IS NOT NULL |
| OSS | - | ❌ 3.48x (wrong) | - | Aggressive, broke semantics |

### Critical Finding: Join Elimination Requires IS NOT NULL

Q23 demonstrated the importance of **preserving NULL filtering semantics** when eliminating joins:

| Pattern | Naive Approach | Correct Approach |
|---------|----------------|------------------|
| FK-only join to dimension | Remove join | Remove join + add `WHERE fk IS NOT NULL` |
| "Missing" year filter | Add year filter | Leave alone (intentional: lifetime vs periodic) |

**Lesson:** When a join is only used for FK validation (no columns selected), replace with `IS NOT NULL` to preserve the implicit NULL filtering.

### Validation Gaps

Sample database validation **did not catch** Q23 issues because:
- Both original and optimized returned NULL on 1% sample
- HAVING clause conditions weren't met with less data
- Need larger samples or full-data validation for complex queries

### Optimization Patterns

| Pattern | Description | Example |
|---------|-------------|---------|
| **Predicate Pushdown** | Move filter INTO CTE before GROUP BY | Q1: s_state='SD' into CTE |
| **Filter Pushdown** | Add selective filter to reduce rows before aggregation | Q2: d_year IN subquery |
| **Scan Consolidation** | Combine multiple scans with CASE WHEN | Q23: filtered + all-time in one pass |
| **Join Reordering** | Filter by smallest result first | Q23: date + customer before items |
| **Join Elimination** | Remove redundant dimension join | Q23: ss_customer_sk IS NOT NULL |

### Prompt Structure That Works

```
## Algorithm
1. ANALYZE: Find where rows/cost are largest
2. OPTIMIZE: For each bottleneck, ask "what could reduce it earlier?"
3. VERIFY: Result must be semantically equivalent

## Plan
<operators by cost>
<table scans with filter status>

## Data Flow
<CTE dependencies>

## SQL
<query>

## Output
<patch JSON format>
```

## Benchmark Methodology

- **Database:** TPC-DS SF100 on DuckDB
- **Runs:** 3 runs after cache warmup
- **Metric:** Average execution time
- **Verification:** Row count and value comparison
