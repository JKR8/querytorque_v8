# Gold Examples: Few-Shot Learning Material

> **Status**: Target state specification
> **Extends**: `qt_sql/examples/duckdb/*.json` (16 DuckDB), `qt_sql/examples/postgres/*.json` (3 PG)
> **Loaded by**: `knowledge.py:TagRecommender.find_similar_examples()`, `pipeline.py:_find_examples()`
> **Injected at**: `analyst_briefing.py` lines 919-936 (analyst), `worker.py` section [6] (worker)

---

## Current Format

Works well. From `qt_sql/examples/duckdb/or_to_union.json`:

```json
{
  "id": "or_to_union",
  "name": "OR to UNION ALL",
  "description": "...",
  "verified_speedup": "3.17x",
  "principle": "OR-to-UNION Decomposition: split OR conditions...",
  "example": {
    "opportunity": "OR_TO_UNION + EARLY_FILTER",
    "input_slice": "[main_query]: ...",
    "output": { "rewrite_sets": [{ "id": "rs_01", "transform": "or_to_union", ... }] },
    "key_insight": "...",
    "when_not_to_use": "..."
  },
  "original_sql": "...",
  "optimized_sql": "..."
}
```

## What to Add

### 1. Four-Part Explanation

The LLM needs all four parts to generalize correctly:

```json
{
  "explanation": {
    "what": "Split 3 OR branches into separate UNION ALL branches with shared date CTE",
    "why": "DuckDB evaluates OR as single filter, cannot use different access paths per branch. UNION ALL lets optimizer independently optimize each branch.",
    "when": "OR conditions span different columns/tables, each branch benefits from different access path, 2-3 branches",
    "when_not": "Same-column OR (0.59x Q90), >3 branches (0.23x Q13 with 9 branches)"
  }
}
```

| Field | Purpose | If Missing |
|-------|---------|------------|
| `what` | Names specific transforms | Worker copies SQL but can't generalize |
| `why` | Explains performance mechanism | Worker applies blindly without understanding |
| `when` | Specifies conditions + diagnostic | Worker overgeneralizes |
| `when_not` | Counter-indications with evidence | Worker causes regressions |

### 2. Gap Demonstration Links

```json
{
  "demonstrates_gaps": ["CROSS_CTE_PREDICATE_BLINDNESS", "CROSS_COLUMN_OR_DECOMPOSITION"]
}
```

Enables gap-weighted scoring (see `05_DETECTION_AND_MATCHING.md`).

### 3. Classification + Provenance

```json
{
  "classification": {
    "tags": ["or_to_union", "predicate_pushdown"],
    "archetype": "star_schema_groupby_topn",
    "transforms": ["or_to_union", "date_cte_isolate"],
    "complexity": "moderate"
  },
  "outcome": {
    "speedup": 3.17,
    "original_ms": 240.0,
    "optimized_ms": 76.0,
    "validated_at_sf": 10,
    "validation_confidence": "high",
    "rows_match": true,
    "checksum_match": true
  },
  "provenance": {
    "source_run": "swarm_batch_20260208_102033",
    "worker_id": 2,
    "model": "deepseek-reasoner",
    "promoted_at": "2026-02-09T10:00:00Z",
    "promoted_by": "human",
    "analysis_session": "AS-DUCK-002"
  }
}
```

See `templates/gold_example_template.json` for the complete template with all fields.

---

## How to Promote a Gold Example

### Prerequisites

1. You've run a benchmark batch and reviewed the blackboard
2. You've completed an analysis session with findings
3. A finding identifies a WIN or CRITICAL_HIT as gold example candidate

### Steps

1. **Check quality gate** — the observation must have all 5 knowledge atom components:
   - SQL structure identifiable
   - Semantic intent clear
   - EXPLAIN evidence available
   - Principle (why it works) you can articulate
   - Conditions (when/when_not) with evidence

2. **Check diversity** — don't promote 3 examples that all teach the same thing. Each example should teach something new or show a different aspect of a technique.

3. **Fill in the template** — copy `templates/gold_example_template.json`, fill in all fields. The 4-part explanation is required.

4. **Write the 4-part explanation** yourself. This is the most important part. The `what` names the transforms. The `why` explains the performance mechanism. The `when` gives conditions + the EXPLAIN diagnostic. The `when_not` cites specific regressions with query IDs and speedups.

5. **Add `demonstrates_gaps[]`** — which engine profile gaps does this example exploit? Look at the gap IDs in the engine profile.

6. **Place the file** in `qt_sql/examples/{dialect}/` and optionally in `ado/examples/{dialect}/` for the ADO system.

7. **Validate** — `qt validate-example {id}` checks schema compliance.

8. **Record** in analysis session — note which example was promoted and from which finding.

---

## Regression Examples

Already exist in `qt_sql/examples/duckdb/regressions/`. Format is good:

```json
{
  "id": "regression_or_to_union_same_column",
  "type": "regression",
  "regression_mechanism": "Same-column OR split duplicates fact table scan with no selectivity benefit",
  "transform_attempted": "or_to_union",
  "observed_regression": 0.59,
  "query_id": "q90",
  "when_it_fails": "OR branches filter the same column on the same table"
}
```

Regression examples feed into `when_not` conditions and are served through `_find_regression_warnings()`. Keep creating these whenever you observe a regression worth documenting.

---

## Backward Compatibility

The current format fields (`principle`, `example.key_insight`, `example.when_not_to_use`) continue to work. The new fields are additive:

1. Existing examples keep current fields
2. `explanation` section added alongside (not replacing) existing fields
3. `analyst_briefing.py` reads `explanation` if present, falls back to `principle` + `key_insight`
4. `demonstrates_gaps[]` populated when you promote examples

No existing example files need to be rewritten immediately.

---

## Current Gold Example Inventory

### DuckDB (16 examples)

| ID | Speedup | Transform | From |
|----|---------|-----------|------|
| `channel_bitmap_aggregation` | 6.24x | single_pass_agg + dimension_cte | Q88 |
| `early_filter` | 4.00x | early_filter | Q4 |
| `date_cte_isolate` | 4.00x | date_cte_isolate + category_avg | Q6 |
| `prefetch_fact_join` | 3.77x | prefetch_fact_join | Q63 |
| `or_to_union` | 3.17x | or_to_union | Q15 |
| `decorrelate` | 2.92x | decorrelate + pushdown | Q1 |
| `composite_decorrelate_union` | 2.42x | decorrelate + or_to_union | Q35 |
| `dimension_cte_isolate` | 1.93x | dimension_cte_isolate | Q27 |
| `intersect_to_exists` | 1.83x | intersect_to_exists | Q38 |
| + 7 more | | | |

### PostgreSQL (3 examples)

| ID | Speedup | Transform |
|----|---------|-----------|
| `pg_early_filter_decorrelate` | 1.13x | early_filter + decorrelate |
| `pg_cte_inline` | — | CTE inlining |
| `pg_join_reorder` | — | Join reorder |

### Storage Location

- `qt_sql/examples/duckdb/` — 16 gold + 10 regressions
- `qt_sql/examples/postgres/` — 6 examples
