# QueryTorque Optimization Pipeline

Iterative state-based optimization of 99 TPC-DS queries on DuckDB.

```
state_0 (baseline) ──> state_1 ──> state_2 ──> state_N
   5 workers           1-shot      1-shot      done
   discovery           refine      refine
```

## The Loop

Every state runs 5 steps:

| Step | What | Output |
|------|------|--------|
| 1. PROMPT | Gather EXPLAIN plan, table stats, FAISS top-3 gold examples, history. Build one prompt per query. | `prompts/` |
| 2. API | Send prompts to LLM. Save responses one-by-one as they complete. | `responses/` |
| 3. VALIDATE | Run 3x or 5x timing on SF10. Pick best worker per query. | `validation/` + `leaderboard.json` |
| 4. LEARN | Find new gold patterns. Track what worked/failed. Flag systemic issues (e.g. "40% syntax errors"). Attach history to query metadata. | Updates `history.json` |
| 5. PROMOTE | >= 1.05x moves forward, rest keeps original. Create next state folder. | `state_N+1/queries/` |

### Step 1: PROMPT (the brain)

The prompt step does all the intelligence gathering per query:
- Runs `EXPLAIN ANALYZE` on the query (SF10) and embeds the plan
- Reads table cardinalities and column stats from the database
- FAISS similarity search → attaches top 3 gold examples
- Reads `history.json` for this query's attempt history (what worked, what failed, what had no effect)
- Structural analysis → matches query features to gold patterns
- Ranks queries by runtime (longest = highest value targets)
- Outputs one self-contained prompt file per query

### Step 4: LEARN (the feedback loop)

After validation, before promotion:
- **New gold patterns**: If a worker achieved >1.5x with a novel approach, extract it as a candidate gold example
- **What worked/failed**: Per-query history updated with this state's results
- **Systemic issues**: Cross-worker analysis. E.g. "40% of W3 responses had syntax errors" → add constraint for next state. "or_to_union >3 branches always regresses" → block it.
- **History attachment**: Each query's metadata now carries its full attempt log so the next state's PROMPT step can read it directly

## Folder Structure

```
research/pipeline/
  PIPELINE.md           # This doc
  history.json          # Cumulative learning across all states

  state_0/
    queries/            # q1.sql ... q99.sql (starting SQL)
    prompts/            # q1_prompt.txt ... (self-contained, includes EXPLAIN + stats + examples)
    responses/          # q1_w1.sql, q1_w2.sql ... q1_w5.sql
    validation/         # q1_validation.json ... + summary.json
    leaderboard.json    # Ranked results for this state

  state_1/
    queries/            # Promoted winners + unchanged originals
    prompts/            # New prompts using updated history
    responses/
    validation/
    leaderboard.json
```

## State Rules

**State 0** = Discovery mode
- Start from original TPC-DS baselines
- 5 workers per query (explore strategy space)
- No history yet — FAISS + structural analysis guide recommendations

**State 1+** = Production mode
- Queries >= 1.05x promoted (optimized SQL replaces original)
- Queries < 1.05x carry forward unchanged
- 1-shot per query (history says exactly what to try)
- Full attempt history from all prior states embedded in prompt
- Learning insights (constraints, error patterns) applied

## Promotion

After validation + learning, for each query:
- **Speedup >= 1.05x**: Best worker's SQL becomes the query in next state
- **Speedup < 1.05x**: Original SQL carries forward unchanged

Queries never regress. The pipeline can only improve or hold steady.

## history.json

Cumulative learning record. Every attempt across every state.

```json
{
  "metadata": {
    "total_states": 1,
    "database": "/mnt/d/TPC-DS/tpcds_sf10.duckdb",
    "learnings": [
      "40% of W4 responses produced syntax errors — add DuckDB syntax constraint",
      "or_to_union with >3 branches always regresses — hard block",
      "date_cte_isolate is most reliable pattern (85% success rate)"
    ]
  },
  "queries": {
    "q1": {
      "baseline_ms": 239,
      "current_speedup": 2.92,
      "current_state": 1,
      "attempts": [
        {"state": 0, "worker": 1, "speedup": 2.92, "transforms": ["decorrelate"], "status": "pass"},
        {"state": 0, "worker": 2, "speedup": 1.05, "transforms": ["date_cte_isolate"], "status": "neutral"},
        {"state": 0, "worker": 3, "speedup": 0.90, "transforms": ["or_to_union"], "status": "regression",
         "error": "9 UNION branches caused 9x fact table scans"}
      ]
    }
  }
}
```

## leaderboard.json

Per-state results ranked by speedup.

```json
{
  "state": 0,
  "summary": {"wins": 34, "improved": 25, "neutral": 30, "regression": 10, "avg_speedup": 1.42},
  "queries": [
    {"query": "q88", "speedup": 6.28, "worker": 3, "transforms": ["or_to_union"], "promoted": true},
    {"query": "q9",  "speedup": 4.47, "worker": 2, "transforms": ["single_pass_aggregation"], "promoted": true}
  ]
}
```

## Validation Rules

Only 2 valid methods:
1. **3-run**: Run 3x, discard 1st (warmup), average last 2
2. **5-run trimmed mean**: Run 5x, remove min/max, average middle 3

Never use single-run comparisons.

## Gold Examples (13 patterns)

| Pattern | Speedup | Mechanism |
|---------|---------|-----------|
| single_pass_aggregation | 4.47x | Consolidate repeated scans |
| date_cte_isolate | 4.00x | Pre-filter date_dim into CTE |
| early_filter | 4.00x | Push filters into CTEs |
| prefetch_fact_join | 3.77x | Pre-join filtered dates with fact |
| or_to_union | 3.17x | OR to UNION ALL (max 3 branches) |
| decorrelate | 2.92x | Correlated subquery to JOIN |
| multi_dimension_prefetch | 2.71x | Pre-filter date + store dims |
| multi_date_range_cte | 2.35x | Separate CTEs per date alias |
| pushdown | 2.11x | Push filters outer to inner |
| dimension_cte_isolate | 1.93x | Pre-filter ALL dimensions |
| intersect_to_exists | 1.83x | INTERSECT to EXISTS |
| materialize_cte | 1.37x | Force CTE materialization |
| union_cte_split | 1.36x | Split UNION ALL by year |

## Config

| Setting | Default | Notes |
|---------|---------|-------|
| Database | `/mnt/d/TPC-DS/tpcds_sf10.duckdb` | SF10 primary, SF100 for final validation |
| Workers | 5 (state_0), 1 (state_1+) | |
| Validation | 3-run | 3 or 5 |
| Promote threshold | 1.05x | |
| Provider | anthropic | anthropic, openai, deepseek |
| Model | claude-sonnet-4-5 | |
| Timeout | 300s per query | |
| FAISS examples | top 3 | From similarity index |
