# Prompt Architecture for SQL Optimization

## Overview

Two prompt modes for LLM-based SQL optimization:

1. **One-shot prompt**: Includes accumulated knowledge (patterns that worked). Used first.
2. **Agentic prompt**: More exploratory. Used when one-shot fails. Iterates with sample DB feedback.

---

## Knowledge Base

Patterns that produced >2x speedups on TPC-DS:

| Pattern | Source | Description |
|---------|--------|-------------|
| Predicate pushdown | Q1, Q2 | Join small filtered dimension INSIDE CTE before GROUP BY |
| Scan consolidation | Q23 | Single scan with CASE WHEN for multiple conditional aggregates |
| Join elimination | Q23 | Remove FK-only join, add `WHERE fk IS NOT NULL` |
| Correlated subquery to window | Q1 | Replace correlated subquery with window function in CTE |

These are included as hints in the one-shot prompt.

---

## Prompt Structure

### One-Shot Prompt

```
1. Execution Plan (from EXPLAIN ANALYZE)
   - Operators by cost (what's expensive)
   - Table scans with filter status
   - Cardinality misestimates

2. Block Map (query structure)
   - CTEs and their clauses
   - Repeated scans
   - Filter gaps

3. Optimization Patterns (accumulated knowledge)
   - Concise descriptions of what worked
   - Enough detail to apply, not benchmark-specific

4. SQL (the query)

5. Output Format (operations JSON)
```

### Agentic Prompt

Same structure, but:
- More exploratory guidance
- Encourages trying multiple approaches
- Receives feedback after each iteration:
  - Speedup achieved
  - Semantic correctness
  - Suggestions for next iteration

---

## Scaling Strategy

### Current State (3 queries)
- All hints fit in prompt
- No filtering needed

### Future State (100+ queries)
When hints grow too large:

1. **Build a scanner** that analyzes the input query
2. **Detect which patterns apply** (e.g., has repeated scans? has FK-only joins?)
3. **Inject only relevant hints** into the prompt
4. Keep prompt size manageable

```
┌─────────────┐     ┌─────────────┐     ┌─────────────┐
│ Input Query │────▶│   Scanner   │────▶│  Relevant   │
└─────────────┘     │  (detects   │     │   Hints     │
                    │  patterns)  │     └──────┬──────┘
                    └─────────────┘            │
                                               ▼
                    ┌─────────────────────────────────┐
                    │         LLM Prompt              │
                    │  - Execution Plan               │
                    │  - Block Map                    │
                    │  - Relevant Hints (filtered)    │
                    │  - SQL                          │
                    └─────────────────────────────────┘
```

---

## Validation

### Sample DB (1% of full data)
- Fast iteration (~0.3s per query)
- Compare results for semantic correctness
- Measure speedup ratio

### Promotion Threshold
- **≥2x speedup on sample** → test on full database
- **<2x on sample** → often doesn't hold on full data

### Full DB Test
- Confirms optimization works at scale
- Final validation before accepting

---

## Files

| File | Purpose |
|------|---------|
| `qt_sql/optimization/block_map.py` | Generates Block Map + builds prompt |
| `qt_sql/optimization/iterative_optimizer.py` | Agentic loop with sample DB feedback |
| `qt_sql/execution/plan_parser.py` | Parses EXPLAIN output into summary |

---

## Adding New Patterns

When a new optimization works:

1. Document it in the knowledge base (this file)
2. Add a concise hint to `block_map.py` in the Optimization Patterns section
3. If it's query-specific, wait to see if it generalizes before adding

The goal is patterns that work across many queries, not one-off tricks.
