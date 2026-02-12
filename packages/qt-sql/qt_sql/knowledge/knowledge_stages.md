# Knowledge Stages

How raw experimental data becomes actionable rewrite intelligence.
Each stage distills the previous one — hundreds of trials compress to
a handful of pathology cards that an LLM analyst can walk in seconds.

---

## Stage 1: Transform & Trial JSON

**Purpose:** Raw experimental results. Every rewrite attempt, win or lose.

**Status:** IMPLEMENTED — `knowledge/trials.jsonl` (25 gold), full corpus in benchmark learning dirs

```json
{
  "query_id": "Q39",
  "transform": "self_join_decomposition",
  "trial_round": "swarm_w2",
  "original_sql": "WITH inv AS (...) SELECT ... FROM inv inv1, inv inv2 WHERE inv1.d_moy=1 AND inv2.d_moy=2",
  "rewritten_sql": "WITH month1_stats AS (...), month2_stats AS (...) SELECT ... JOIN ...",
  "baseline_ms": 142.3,
  "rewrite_ms": 29.9,
  "speedup": "4.76x",
  "rows_match": true,
  "explain_before": { "..." : "..." },
  "explain_after": { "..." : "..." },
  "tags": ["self_join_cte", "discriminator_filter", "comma_join"],
  "preconditions_matched": ["SELF_JOIN_CTE_DISCRIMINATOR"]
}
```

One per attempt. Hundreds of these. Most are noise — failed attempts,
marginal wins, regressions.

---

## Stage 2: Gold Examples + Regressions

**Purpose:** Promoted specimens. Each is a verified win or an instructive
failure, with the reasoning extracted.

**Status:** IMPLEMENTED — `examples/duckdb/` (16 wins + 10 regressions), `examples/postgres/` (6 wins)

```json
{
  "id": "self_join_decomposition",
  "type": "win",
  "verified_speedup": "4.76x",
  "benchmark_queries": ["Q39"],

  "example": {
    "input_slice": "CTE self-joined with inv1.d_moy=1 AND inv2.d_moy=2",
    "output": {
      "nodes": {
        "month1_stats": "... WHERE d_moy = 1 GROUP BY ...",
        "month2_stats": "... WHERE d_moy = 2 GROUP BY ...",
        "main_query": "... FROM month1_stats JOIN month2_stats ON ..."
      }
    },
    "key_insight": "Optimizer materializes CTE for ALL months then post-filters. Splitting embeds filter into each CTE, processing 1/12th of data.",
    "when_not_to_use": "Only when CTE is self-joined with different discriminator values. Same filter on both aliases = no benefit."
  },

  "original_sql": "...",
  "optimized_sql": "...",
  "explain_before_summary": "Sequential scan on inventory (7.2M rows) → GROUP BY → materialize → filter d_moy=1 (keeps 600K)",
  "explain_after_summary": "Sequential scan on inventory with d_moy=1 pushed (600K rows) → GROUP BY → hash join"
}
```

Curated. ~30 total. Each one is evidence for a specific pathology.

---

## Stage 3: Pathology Cards

**Purpose:** The reusable knowledge. Explains WHY something is expensive
at the engine level, not just WHAT to do about it. This is where
generalization lives — the IMPLICATION section connects individual gold
examples to the underlying engine limitation and predicts novel
manifestations.

**Status:** IMPLEMENTED — `knowledge/decisions.md` (10 cards), `knowledge/decision_card_template.md`

```yaml
pathology: full_materialization_with_late_filter

surface_cost: >
  CTE materializes N rows, outer query filters to N/K afterward.
  Work wasted: (K-1)/K of materialization is discarded.

engine_gap: CROSS_CTE_PREDICATE_BLINDNESS
engine_mechanism: >
  DuckDB plans each CTE as an independent subplan. Predicates
  from outer query (WHERE, JOIN ON, self-join discriminators)
  cannot propagate backward into CTE definitions. The CTE
  materializes blind to how its output will be consumed.

implication: >
  Same gap manifests as:
  - Dimension filters not reaching fact table CTEs
  - Self-join discriminator not reaching CTE GROUP BY
  - Scalar subquery results not flowing into referencing CTEs
  - HAVING thresholds not constraining CTE materialization
  Any time information from one scope needs to constrain
  another scope's materialization, this gap applies.

detection:
  explain_signals:
    - CTE scan output >> post-filter output (ratio > 5:1)
    - Filter node appears ABOVE materialization node
    - Same CTE referenced 2+ times with different WHERE
  sql_signals:
    - CTE self-joined with different literal values
    - WHERE on CTE alias column that could have been in CTE definition

restructuring_principle: >
  Move the discriminating predicate INTO the CTE definition.
  If self-joined, split into N separate CTEs each embedding
  their discriminator. Result: each CTE materializes only
  the rows it needs.

risk_calibration:
  expected_range: "1.3x - 4.8x"
  worst_regression: "0.85x (Q67 — ROLLUP blocked pushdown)"
  regression_signals:
    - ROLLUP or WINDOW in same query (blocks further pushdown)
    - Baseline < 100ms (CTE overhead exceeds savings)
    - 3+ fact tables (materializing early locks join order)

gold_example_ids:
  wins: [self_join_decomposition, date_cte_isolate, dimension_cte_isolate]
  regressions: [regression_q25_date_cte_isolate, regression_q67]
```

~10 of these, one per engine gap. This is the layer that connects
individual gold examples to the underlying engine limitation and
predicts novel manifestations.

---

## Stage 4: Distilled Algorithm

**Purpose:** What the analyst and workers actually see. Compressed from
the pathology cards into a walkable tree with global guards, pathology
detection, decision gates, and gold example routing.

**Status:** IMPLEMENTED — `knowledge/duckdb.md` (11KB), `knowledge/postgresql.md` (4KB)

```markdown
# DuckDB Query Rewrite Decision Tree

## GLOBAL GUARDS (check always, before any rewrite)
- EXISTS/NOT EXISTS → never materialize (0.14x Q16)
- Same-column OR → never split (0.23x Q13)
- Baseline < 100ms → skip CTE-based rewrites
- 3+ fact table joins → do not pre-materialize facts

## PATHOLOGY DETECTION (read explain plan, identify expensive nodes)

### P1: Repeated scan of same table
  Explain signal: same table name appears N times in plan
  → DECISION: Consolidate to single pass with CASE WHEN
  → Gates: max 8 branches, all scans must share same join structure
  → Expected: 1.3x-6.2x | Worst: no known regressions
  → Workers get: Q88 (6.24x), Q9 (4.47x), Q90 (1.47x)

### P2: CTE materialization >> post-filter output
  Explain signal: materialize node rows >> WHERE-filtered rows
  → DECISION A: Self-joined CTE? Split by discriminator
    → Gates: 2-4 discriminator values, ratio > 5:1
    → Expected: 1.3x-4.8x | Worst: 0.85x (ROLLUP present)
    → Workers get: Q39 (4.76x), Q67 warning
  → DECISION B: Dimension filter above join? Isolate into CTE
    → Gates: selectivity < 10%, not 3+ fact joins
    → Expected: 1.3x-4.0x | Worst: 0.0076x (dim cross-join)
    → Workers get: Q6 (4.00x), Q43 (2.71x), Q80 warning

### P3: Nested loop with high outer cardinality
  Explain signal: nested loop, outer > 1000 rows, inner is scan
  → DECISION: Decorrelate to CTE + GROUP BY + hash join
  → Gates: not EXISTS, not already hash join, filter preserved
  → Expected: 2.0x-2.9x | Worst: 0.34x (was already semi-join)
  → Workers get: Q1 (2.92x), Q93 regression, Q35 (2.42x)

### P4: Aggregation over large joined result
  Explain signal: GROUP BY input rows >> distinct key count
  → DECISION: Push aggregation below join
  → Gates: GROUP BY keys ⊇ join keys, reconstruct AVG from SUM/COUNT
  → Expected: 2x-43x | Worst: no known regressions
  → Workers get: Q22 (42.90x)

### P5: LEFT JOIN with NULL-eliminating WHERE
  Explain signal: LEFT JOIN output ≈ INNER JOIN output
  → DECISION: Convert to INNER JOIN
  → Gates: WHERE references right-table column, no CASE WHEN IS NULL
  → Expected: 2x-3.4x | Worst: no known regressions
  → Workers get: Q93 (3.44x)

### P6: Cross-column OR forcing full scan
  Explain signal: sequential scan with complex OR, no index usage
  → DECISION: Split to UNION ALL per branch
  → Gates: max 3 branches, different columns, no self-join
  → Expected: 1.5x-6.3x | Worst: 0.23x (9 branches from nested OR)
  → Workers get: Q15 (3.17x), Q13 regression

### NO MATCH
  Record: which pathologies were checked, which gates failed
  Nearest miss: closest pathology + why it didn't qualify
  Features present: [list structural features for future pattern discovery]
  → Workers get: broad gold example set, analyst's manual reasoning
```

One document per engine. This replaces the old constraint files and
engine profiles as the primary analyst intelligence.

---

## Stage 5: Tree Pruning by EXPLAIN Plan (FUTURE STATE)

**Purpose:** At inference time, prune branches the analyst never needs
to read. Based on what's actually in the EXPLAIN plan, not the SQL.

**Status:** NOT IMPLEMENTED — requires EXPLAIN plan parsing infrastructure
and dynamic prompt assembly. Significant engineering effort.

```yaml
pruning_rules:
  # Explain-plan based (preferred — more reliable than SQL parsing)
  - if: no nested_loop nodes in plan
    prune: P3 (decorrelation)

  - if: each table appears only once in plan
    prune: P1 (repeated scans)

  - if: no CTE materialization nodes in plan
    prune: P2 (CTE materialization issues)

  - if: no LEFT JOIN nodes in plan
    prune: P5 (LEFT→INNER conversion)

  - if: no OR predicates in plan filter nodes
    prune: P6 (OR decomposition)

  # SQL-based fallback (when explain plan isn't detailed enough)
  - if: no GROUP BY in SQL
    prune: P4 (aggregate pushdown)

  - if: no WINDOW/OVER in SQL
    prune: deferred_window_aggregation path

  # Combined
  - if: baseline_ms < 100
    prune: ALL CTE-based rewrites (overhead exceeds savings)
```

Pruning is by EXPLAIN plan content first, SQL content as fallback.
EXPLAIN is more reliable because it shows what the optimizer actually
did, not what the SQL suggests. A query might have a correlated
subquery in the SQL but the EXPLAIN shows a hash join — the optimizer
already fixed it, so pruning P3 is correct.

**Engineering required:** EXPLAIN plan parser (per engine), pruning
rule evaluator, dynamic prompt assembler that emits only relevant
branches + their gold examples.

---

## The Full Pipeline

```
Trial JSON (hundreds)
  → promote/curate →
Gold Examples (20-40)
  → extract engine mechanisms →
Pathology Cards (7-10)
  → compress to walkable tree →
Distilled Algorithm (1 doc per engine)
  → [future] prune at inference by EXPLAIN plan →
Analyst sees only relevant branches
  → analyst picks pathologies, routes gold examples →
Workers produce rewrites
  → validate → new Trial JSON (loop)
```
