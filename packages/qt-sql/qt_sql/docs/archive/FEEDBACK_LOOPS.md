# ADO Feedback Loops

Four feedback loops that improve the swarm system over time. The key insight
is that Loops 2 and 3 feed back into the **analyst** (orchestrator), not just
the workers. Most systems (E^3, etc.) only improve the generator; ours
improves the orchestrator too.

## Architecture

```
                    +-------------------------+
                    |    QUERY + EXPLAIN       |
                    +------------+------------+
                                 |
                                 v
                    +------------------------+
                    |   ANALYST (frontier)    |<---- Strategy Leaderboard
                    |                        |<---- Constraint List (auto-growing)
                    |                        |<---- Example Pool (curated)
                    +------------+-----------+
                                 |
              +----------+-------+-------+----------+
              v          v               v          v
         +--------+ +--------+     +--------+ +--------+
         |Worker 1| |Worker 2|     |Worker 3| |Worker 4|
         +---+----+ +---+----+     +---+----+ +---+----+
             |          |              |          |
             v          v              v          v
        +---------------------------------------------+
        |          COMPARATIVE EVALUATION              |
        |  (parse -> execute -> equivalence -> timing) |
        +-----------------+---------------------------+
                          |
          +---------------+-------------------+
          v               v                   v
   +------------+  +-------------+    +--------------+
   |SUCCESS POOL|  |FAILURE CORPS|    |REGRESSION    |
   |(demo pool) |  |(diagnostic) |    |CORPUS        |
   +------+-----+  +------+------+    +------+-------+
          |               |                  |
          v               v                  v
   +------------+  +-------------+    +------------------+
   |Example     |  |Error Pattern|    |Frontier Model:   |
   |Retirement +|  |Clustering   |    |Root Cause        |
   |Coverage    |  |             |    |Analysis          |
   |Matrix      |  |             |    |                  |
   +------+-----+  +------+------+    +------+-----------+
          |               |                  |
          v               v                  v
   +------------+  +-------------+    +--------------+
   |Curated     |  |Worker Model |    |Auto-generated|
   |Example Pool|  |Fine-tuning  |    |Constraints + |
   |            |  |(DPO pairs)  |    |Anti-patterns |
   +------+-----+  +-------------+    +------+-------+
          |                                  |
          +------------------+---------------+
                             |
                             v
                  +---------------------+
                  | STRATEGY LEADERBOARD|
                  | per (archetype x    |
                  |      transform)     |
                  +---------------------+
                             |
                             | feeds back into
                             v
                        ANALYST PROMPT
                        (next query)
```

---

## Loop 1: Example Pool Evolution

**Flow:** Success pool -> coverage matrix -> retire weak examples -> curated pool -> analyst prompt.

**Cadence:** After every benchmark run.

**What to do (manual):**

1. After a swarm batch completes, look at `duckdb_tpcds.json` for new WIN queries.
2. Check if the winning transform already has a gold example in
   `ado/examples/duckdb/`. If yes, compare speedups — keep the stronger one.
3. Check the coverage matrix: does every archetype have at least 2 gold
   examples? If an archetype is under-served, promote the best new win.
4. Retire examples that haven't contributed to a win in 3+ consecutive batches
   (move to `ado/examples/duckdb/retired/`).
5. Rebuild the tag index: `PYTHONPATH=packages/qt-shared:packages/qt-sql:. python3 -m ado.tag_index`

**Key files:**
- `ado/examples/duckdb/*.json` — gold examples
- `qt_sql/optimization/examples/*.json` — V5 CLI copies (must stay in sync)
- `ado/models/similarity_tags.json` — tag index

**When it matters:** Always. Stale examples waste prompt tokens and mislead workers.

---

## Loop 2: Constraint Auto-Generation

**Flow:** Regression corpus -> cluster by transform type -> frontier model root cause analysis -> new constraint rules -> analyst prompt.

**Cadence:** Weekly. Requires 10+ regressions per cluster for statistical validity.

**What to do (manual):**

1. Query `duckdb_tpcds.json` for all attempts with `speedup < 0.95` and
   `rows_match == true`. Group by transform.
2. For each transform cluster with 10+ regressions, look for the common
   pattern. Examples:
   - `or_to_union` regressions cluster around queries with >3 OR branches
   - `materialize_cte` regressions cluster around queries where DuckDB
     already inlines the CTE
3. Write a new constraint JSON in `ado/constraints/`. Template:
   ```json
   {
     "id": "CONSTRAINT_NAME",
     "severity": "HIGH",
     "engine": "duckdb",
     "prompt_instruction": "Do NOT ... because ...",
     "observed_failures": [
       {"query_id": "query_48", "speedup": 0.23, "transform": "or_to_union"}
     ]
   }
   ```
4. Verify the constraint is loaded: check count in `generate_sample.py` output.

**Key files:**
- `ado/constraints/*.json` — constraint rules (auto-loaded, engine-filtered)
- `ado/benchmarks/duckdb_tpcds/knowledge/duckdb_tpcds.json` — regression source

**When it matters:** After accumulating enough regressions. The elimination
table in `strategy_leaderboard.json` is a good early-warning signal — any
cell with success_rate < 15% and 5+ attempts is a constraint candidate.

---

## Loop 3: Strategy Leaderboard

**Flow:** Comparative evaluation -> (archetype x transform) success rates -> strategy leaderboard -> analyst uses for transform selection.

**Cadence:** After every benchmark run. Starts being useful after ~50 queries per archetype.

**What to do (manual):**

1. After a swarm batch, rebuild the leaderboard:
   ```bash
   cd /mnt/c/Users/jakc9/Documents/QueryTorque_V8
   PYTHONPATH=packages/qt-shared:packages/qt-sql:. \
     python3 -m ado.benchmarks.duckdb_tpcds.build_strategy_leaderboard
   ```
2. Check the output for sanity:
   - Are all 101 queries classified?
   - Does the elimination table catch known bad combos?
   - Are win rates plausible (not 0% or 100% across the board)?
3. Spot-check with `generate_sample.py` to see the table in the analyst prompt.

**Key files:**
- `ado/benchmarks/duckdb_tpcds/build_strategy_leaderboard.py` — builder script
- `ado/benchmarks/duckdb_tpcds/strategy_leaderboard.json` — output (auto-loaded by swarm)
- `ado/prompts/analyst_briefing.py` — renders the table in the analyst prompt

**Current state (Feb 2026):**
- 101 queries, 821 attempts, 4 archetypes
- 4 eliminations in `aggregation_rewrite` (or_to_union, intersect_to_exists,
  materialize_cte, multi_date_range_cte)
- Top transform per archetype:
  - `filter_pushdown`: multi_date_range_cte (83% win)
  - `aggregation_rewrite`: decorrelate (47% win)
  - `set_operations`: intersect_to_exists (50% win)
  - `general`: decorrelate (38% win)

**When it matters:** After ~50 attempts per archetype. Below that the rates
are noisy. The `attempts >= 3` filter in the table rendering already handles
very-low-signal cells.

---

## Loop 4: Worker Model Improvement

**Flow:** Preference pairs from comparative evaluation -> DPO fine-tuning on worker model -> better SQL generation within target DAGs.

**Cadence:** Monthly. Requires ~500 preference pairs for meaningful update.

**What to do (manual / future):**

1. From `duckdb_tpcds.json`, extract preference pairs:
   - For each query with multiple attempts, the highest-speedup attempt
     with `rows_match == true` is "chosen", the lowest is "rejected".
   - Each pair = (analyst briefing + worker prompt, chosen SQL, rejected SQL).
2. Format as DPO training data (prompt, chosen, rejected).
3. Fine-tune the worker model on these pairs.
4. Re-run a benchmark batch with the fine-tuned model and compare aggregate
   win rate.

**Not implemented yet.** Requires:
- ~500 clean preference pairs (we have ~400 from 821 attempts)
- Fine-tuning infrastructure (DeepSeek API or local model)
- A/B evaluation framework

**When it matters:** When Loops 1-3 plateau. If the analyst is giving good
briefings but workers still produce regressions, the worker model is the
bottleneck.

---

---

## Problem 6: Analyst Feedback Gap

E^3 doesn't have an analyst layer — it's a single model. Our system has
analyst -> workers. But we're only learning on worker output. The analyst's
quality is equally important and currently unoptimized.

**How to tell if the analyst produced a good briefing:** By worker outcomes.
If all 4 workers fail, the analyst probably gave bad guidance (wrong strategy
selection, missing hazard, bad DAG design). If 3/4 succeed, the analyst was
mostly right.

### What to Track (per query, per batch)

```
analyst_briefing_id: abc123
query: q74
worker_outcomes:
  worker_1: 1.4x success
  worker_2: semantic_break (missing column)
  worker_3: 0.9x regression
  worker_4: 2.1x success

analyst_quality_signals:
  strategy_selection: 3/4 applicable (worker_3's strategy wrong for archetype)
  hazard_coverage: 1 semantic break -> missed hazard about column completeness
  constraint_relevance: constraints correct for workers 1,2,4
  dag_correctness: 3/4 valid DAGs
```

### Analyst Failure Patterns to Watch For

Over time, cluster these signals to find systematic analyst weaknesses:

- "Analyst consistently misses CTE_COLUMN_COMPLETENESS hazards when target
  DAG has >4 nodes." -> Prompt fix: add a checklist step.
- "Analyst assigns decorrelate to queries with no correlated subqueries
  30% of the time." -> Prompt fix: reinforce applicability check.
- "Analyst's BOTTLENECK_DIAGNOSIS disagrees with EXPLAIN 40% of the time
  for aggregation_rewrite queries." -> Prompt fix: add EXPLAIN-first rule.
- "Worker 4 (novel strategies) has 80% failure rate." -> Strategy selection
  fix: assign conservative fallback to W4 slot instead.

### How to Compute (manual, per batch)

1. From the swarm session output, collect per-worker outcomes:
   - `status`: success / pass / fail / error
   - `speedup`: float
   - `rows_match`: bool
   - `error_category`: syntax / semantic / timeout / execution
2. Per query, score the analyst:
   - **strategy_hit_rate**: fraction of workers that produced speedup >= 1.0
   - **semantic_break_rate**: fraction with `rows_match == false`
   - **regression_rate**: fraction with speedup < 0.95
3. Aggregate across queries:
   - Overall analyst quality = avg strategy_hit_rate
   - Worst failure mode = most common error_category among failures
4. Compare across batches to detect drift.

### When to Act

- **strategy_hit_rate < 0.5 for an archetype**: The analyst is
  systematically picking wrong transforms for that query type. Check the
  Strategy Leaderboard — is the analyst ignoring the data?
- **semantic_break_rate > 0.25**: The analyst's NODE_CONTRACTS are
  incomplete. Add a column-completeness verification step to the prompt.
- **One worker slot consistently fails**: The analyst may be assigning
  that slot an overly ambitious strategy. Consider constraining it.

**Not automated yet.** The data exists in swarm session logs and
`duckdb_tpcds.json` (all_attempts per query with worker_id + status +
speedup). A scoring script could be added to compute these metrics
after each batch.

---

## Weekly Checklist

After each benchmark batch:

- [ ] **Loop 1**: Check for new gold example candidates. Retire weak ones.
      Rebuild tag index.
- [ ] **Loop 3**: Run `build_strategy_leaderboard.py`. Check elimination
      table for new bad combos.

Weekly (if 10+ new regressions accumulated):

- [ ] **Loop 2**: Cluster regressions by transform. Write new constraints
      for any cluster with a clear pattern.

Monthly (when ~500 pairs available):

- [ ] **Loop 4**: Extract preference pairs. Evaluate fine-tuning feasibility.
