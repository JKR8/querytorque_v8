# {Engine} Plan-Space Scanner Analysis

Below are observations from toggling planner flags (SET LOCAL) across {N}
{Benchmark} benchmark queries on {Engine} {Version} ({Scale}). Each row is one (query, flag-combo)
that produced a DIFFERENT plan than baseline — neutral combos are excluded.

Column key:
- **query**: query ID
- **combo**: flag(s) toggled
- **cost_ratio**: baseline_cost / combo_cost (>1 = combo cheaper, <1 = combo worse)
- **wall_speedup**: baseline_ms / combo_ms (>1 = faster, <1 = regression). Only for queries with wall-clock data.
- **vulns**: vulnerability types detected
- **plans**: number of distinct plans discovered for this query

## Your Task

Extract 10-30 **findings** — generalizable claims about how this engine
behaves on {workload_type}. Focus on join sensitivity, memory/spill,
JIT, parallelism, cost model accuracy, join reorder, and flag interactions.

## Output Format

Return ONLY a JSON array. No markdown fences, no explanation. Example:

```json
[
  {
    "id": "SF-001",                          // sequential SF-001, SF-002, ...
    "claim": "Disabling nested loops causes >4x regression on dim-heavy star queries",
    "category": "join_sensitivity",           // join_sensitivity|memory|parallelism|jit|cost_model|join_order|interaction|scan_method
    "supporting_queries": ["query001_multi_i1", "query065_multi_i1", "query080_multi_i1"],
    "evidence_summary": "8/10 queries with nested loop baseline regress >4x",
    "evidence_count": 8,
    "contradicting_count": 2,
    "boundaries": ["Applies when baseline uses nested loops for dimension PK lookups"],
    "mechanism": "Nested loops exploit dim PK indexes; hash join must full-scan dimension tables",
    "confidence": "high",                     // high (>5 queries, consistent) | medium (3-5 or contradictions) | low (<3)
    "confidence_rationale": "Consistent across 8 queries with cost + wall-clock evidence",
    "implication": "Do NOT restructure joins that eliminate nested loop index lookups on dimension tables"
  }
]
```

---

## {category} ({N} observations)

{observations in compact TSV format}
