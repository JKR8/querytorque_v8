# ADO Workflow Documentation

## System Flow (ASCII)

```text
+----------------------------------------------------------------------------------+
|                        ADO Optimization Workflow                                 |
+----------------------------------------------------------------------------------+

Inputs:
  - benchmark config (engine, db_path/dsn, workers, promote threshold)
  - baseline SQL queries (benchmarks/<name>/queries/*.sql)
  - gold examples (ado/examples/<engine>/*.json) + constraints + FAISS index

            |
            v
+-----------------------------+
| Phase 1: Parse SQL -> DAG   |
| - DagBuilder + CostAnalyzer |
| - node graph + cost profile |
+-----------------------------+
            |
            v
+-----------------------------------------------+
| Phase 2: FAISS Example Retrieval              |
| - fingerprint SQL (literals->constants)       |
| - vectorize AST (90-dim feature vector)       |
| - find top-k gold examples for this engine    |
| - DuckDB queries -> DuckDB examples only      |
| - PostgreSQL queries -> PG examples only      |
+-----------------------------------------------+
            |
            v
+-----------------------------------------------+
| (Optional) LLM Analyst (use_analyst=True)     |
| - 1 extra API call per query                  |
| - sees FAISS picks + full example catalogue   |
| - can accept or override FAISS selections     |
| - generates expert structural analysis        |
| - use for stubborn/regression queries only    |
+-----------------------------------------------+
            |
            v
+--------------------------------------------------------------+
| Phase 3: Build Prompt + Generate N Candidates                |
| - prompt sections (attention-ordered):                       |
|   1. Role/Task  2. Full SQL  3. DAG  4. Performance         |
|   5. History  5b. Global Learnings  6. Examples/Analyst      |
|   7. Constraints (sandwich)  8. Output Format                |
| - CandidateGenerator runs N workers (parallel LLM calls)    |
| - SQLRewriter extracts optimized SQL from each response       |
| - first valid non-trivial candidate is selected              |
+--------------------------------------------------------------+
            |
            v
+-----------------------------------------+
| Phase 4: Syntax Gate                    |
| - parse candidate SQL (sqlglot)         |
| - fallback to original SQL on parse fail|
+-----------------------------------------+
            |
            v
+------------------------------------------------------+
| Phase 5: Validate + Score                            |
| - semantic + runtime validation                      |
| - DuckDB: qt_sql.validation.sql_validator            |
| - PostgreSQL: executor-based PG validator             |
| - status buckets: WIN / IMPROVED / NEUTRAL / etc.   |
| - speedup computed and recorded                      |
+------------------------------------------------------+
            |
            v
+------------------------------------------------------+
| Persist + Learn                                      |
| - save prompt/response/sql/validation artifacts      |
| - create learning record (per query)                 |
| - update state leaderboard + benchmark leaderboard   |
| - update learning summary                            |
+------------------------------------------------------+
            |
            v
+------------------------------------------------------+
| Promote to Next State (optional, per promote())      |
| - if speedup >= promote_threshold, carry optimized   |
|   SQL into next state baseline                        |
| - generate LLM promotion analysis for next-round     |
|   context (what worked + what to try next)            |
+------------------------------------------------------+
```

## Phase Details

### Phase 1: Parse
- Module: `ado/pipeline.py` (`Pipeline._parse_dag`)
- Uses `DagBuilder` and `CostAnalyzer` from `qt_sql.optimization.dag_v2`.
- Output: DAG nodes/edges and per-node cost breakdown.

### Phase 2: FAISS Example Retrieval
- Module: `ado/pipeline.py` (`Pipeline._find_examples`) -> `ado/knowledge.py` (`ADOFAISSRecommender`).
- Fingerprints the input SQL: replaces literals with neutral constants (`0`, `'x'`), lowercases identifiers.
- Vectorizes the AST into a 90-dimensional feature vector (node type counts, depth metrics, cardinality, pattern indicators, complexity).
- Searches FAISS index (IndexFlatL2 with z-score + L2 normalization for cosine similarity).
- **Engine-specific**: strict filtering ensures DuckDB queries only get DuckDB gold examples, PostgreSQL queries only get PostgreSQL gold examples.
- Returns top-k examples (default k=3).
- Index: `ado/models/similarity_index.faiss` (95 vectors, 90 dimensions).
- Rebuild: `python3 -m ado.faiss_builder` from `packages/qt-sql/`.

### LLM Analyst (Optional)
- Module: `ado/pipeline.py` (`Pipeline._run_analyst`) + `ado/analyst.py`.
- **Off by default** — costs 1 extra API call per query. Set `use_analyst=True` on Pipeline or per-call.
- Sees: FAISS-selected examples, full catalogue of available gold examples for the engine, DAG, costs, history.
- Can: accept FAISS picks, or override with specific example IDs via `EXAMPLES: id1, id2, id3` in response.
- Provides expert structural analysis injected into the Phase 3 prompt (replaces examples section).

### Phase 3: Prompt + Candidate Generation
- Prompt assembly: `ado/node_prompter.py` (`Prompter.build_prompt`).
- Prompt sections (attention-ordered): Role/Task, Full SQL, DAG Topology, Performance Profile, History, Global Learnings (from benchmark runs), Examples or Expert Analysis, Constraints (sandwich-ordered from JSON), Output Format.
- Candidate generation: `ado/generate.py` (`CandidateGenerator.generate`) with N parallel workers via `ThreadPoolExecutor`.
- Response application: `ado/sql_rewriter.py` (`SQLRewriter`) extracts optimized SQL from LLM response.
- **Candidate selection**: first syntactically valid, non-trivial candidate is picked (not best-of-N by speedup).

### Phase 4: Syntax Gate
- Pipeline parses selected candidate SQL via `sqlglot.parse_one(sql, dialect=dialect)`.
- If parsing fails, candidate is discarded and original SQL is used as fallback.

### Phase 5: Validate + Score
- Validator: `ado/validate.py` (`Validator`).
- DuckDB: uses `qt_sql.validation.sql_validator.SQLValidator`.
- PostgreSQL: uses executor-based PG validator (`Validator._create_pg_validator()`).
- Pipeline status mapping:
  - `WIN`: speedup >= `1.10`
  - `IMPROVED`: speedup >= `1.05`
  - `NEUTRAL`: speedup >= `0.95`
  - `REGRESSION`: speedup < `0.95`
  - `ERROR`: validation error/failure

## State Lifecycle

### `run_state(state_num)`
- For each query:
  - load baseline SQL (`queries/*.sql` or promoted SQL from previous state)
  - execute all phases: parse → FAISS → (analyst) → prompt+generate → syntax gate → validate
  - persist validation + artifacts
  - create learning record
- Writes:
  - `state_N/validation/<query_id>.json`
  - `state_N/<query_id>/worker_00/{prompt.txt,response.txt,optimized.sql,validation.json}`
  - `state_N/leaderboard.json`
- Also updates benchmark-level:
  - `benchmarks/<name>/leaderboard.json`
  - `benchmarks/<name>/leaderboard.md`
- Also updates learning:
  - `benchmarks/<name>/learning/` (learning records + summary)
  - `benchmarks/<name>/history.json` (cumulative learnings for analyst)

### Promotion (`promote(state_num)`)
- Threshold from config: `promote_threshold` (default 1.05).
- Winners (`speedup >= threshold`) produce:
  - `state_{N+1}/<query_id>_promoted.sql`
  - `state_{N+1}/promotion_context/<query_id>.json` (LLM-generated analysis of what worked + suggestions)
- Non-winners keep original baseline from `queries/`.
- Seed rules (knowledge) carry forward from `state_N/seed/` to `state_{N+1}/seed/`.

### History Loading
- `Pipeline._load_history(state_num)` aggregates all previous states.
- Per query: list of attempts (status, speedup, transforms, SQL) + most recent promotion analysis.
- Injected into the prompt so the LLM knows what was already tried.

## Core Artifacts
- Config: `ado/benchmarks/<name>/config.json`
- Query input: `ado/benchmarks/<name>/queries/*.sql`
- State outputs: `ado/benchmarks/<name>/state_*`
- FAISS index: `ado/models/similarity_index.faiss` + `similarity_metadata.json`
- Gold examples: `ado/examples/<engine>/*.json` (duckdb: 16, postgres: 5)
- Constraints: `ado/constraints/*.json` (11 files: 6 CRITICAL, 4 HIGH, 1 MEDIUM)
- Learning journal: `ado/benchmarks/<name>/learning/`
- Benchmark history: `ado/benchmarks/<name>/history.json` (auto-generated)
- Analyst sessions: `ado/benchmarks/<name>/analyst_sessions/{query_id}/`

## Analyst Deep-Dive Mode

### Overview
Iterative single-query optimization loop where the LLM analyst steers direction, reviews output, and retries with intelligence. Critical invariant: **always optimizes from the ORIGINAL query** with full history of all iterations — never from intermediate results.

### Entry Points
- **Pipeline**: `p.run_analyst_session("query_88", sql, max_iterations=5, target_speedup=1.5, n_workers=3)`
- **ADORunner**: `runner.run_analyst("query_88", sql, max_iterations=5, target_speedup=1.5, n_workers=3)`
- **Direct**: `from ado.analyst_session import AnalystSession`

### How It Works
Each iteration:
1. Parse the ORIGINAL SQL into DAG (never intermediate)
2. FAISS example retrieval (on original SQL)
3. LLM analyst generates structural analysis with full history of all previous iterations
4. Build prompt with iteration history + global learnings
5. Generate N candidates via parallel workers
6. Validate best candidate
7. Record iteration result

### Stopping Criteria
- Target speedup reached
- Max iterations exhausted
- Converged (requires 3+ iterations; last 2 made no meaningful progress — within 2% of best)

### Session Persistence
- Sessions saved to `benchmark_dir/analyst_sessions/{query_id}/`
- Each iteration's prompt, SQL, validation, and analysis saved individually
- Sessions can be resumed via `AnalystSession.load_session(pipeline, session_dir)`

### Module
- `ado/analyst_session.py` — `AnalystSession`, `AnalystIteration`

## Dynamic Constraints

### Overview
Constraints are loaded dynamically from `ado/constraints/*.json` instead of being hardcoded. Each constraint file contains an `id`, `severity` (CRITICAL/HIGH/MEDIUM), and a `prompt_instruction` that is injected into the rewrite prompt.

### Sandwich Pattern
Constraints are ordered in the prompt using an attention-optimized sandwich pattern:
- **Top**: CRITICAL constraints (correctness guards)
- **Middle**: HIGH + MEDIUM constraints (performance rules)
- **Bottom**: CRITICAL constraints repeated (recency reinforcement)

### Adding New Constraints
Create a JSON file in `ado/constraints/` with this structure:
```json
{
  "id": "MY_CONSTRAINT",
  "severity": "HIGH",
  "description": "What this constraint prevents",
  "prompt_instruction": "The text injected into prompts",
  "observed_failures": [],
  "constraint_rules": []
}
```

## Global Learnings

### Overview
Aggregate learnings from benchmark runs are automatically loaded from the learning journal and injected into rewrite prompts. This includes transform effectiveness, known anti-patterns, and example success rates.

### Data Flow
1. Learning records saved per-query during `run_query()`
2. `Learner.build_learning_summary()` aggregates all records
3. Summary injected into prompt via `Prompter._section_global_learnings()`
4. `Learner.generate_benchmark_history()` creates `history.json` for the analyst

### history.json Structure
```json
{
  "cumulative_learnings": {
    "effective_patterns": {
      "date_cte_isolate": {"wins": 12, "avg_speedup": 1.34, ...}
    },
    "known_regressions": {
      "OR_TO_UNION_GUARD_Q90": "0.59x. Doubles the fact table scan..."
    }
  }
}
```

## Operational Defaults (duckdb_tpcds)
- Engine: `duckdb`
- Scale factor: `10`
- Database: `/mnt/d/TPC-DS/tpcds_sf10.duckdb`
- Validation method: `3-run` (discard 1st warmup, average runs 2-3)
- State 0 workers: `5` (discovery)
- State N workers: `1` (refinement)
- Promotion threshold: `1.05`
- Analyst: off (opt-in per query)
