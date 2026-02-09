# ADO Workflow Documentation

## System Flow (ASCII)

```text
+----------------------------------------------------------------------------------+
|                        ADO Optimization Workflow                                 |
+----------------------------------------------------------------------------------+

Inputs:
  - benchmark config (engine, db_path/dsn, workers, promote threshold)
  - baseline SQL queries (benchmarks/<name>/queries/*.sql)
  - gold examples (ado/examples/<engine>/*.json) + regression anti-patterns
  - constraints (ado/constraints/*.json)
  - FAISS index (ado/models/similarity_index.faiss)
  - LLM provider (configured via QT_LLM_PROVIDER env var or ADOConfig)

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
| - find regression warnings (similar queries   |
|   that regressed — anti-patterns)             |
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
|   6b. Regression Warnings  7. Constraints  8. Output Format  |
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
| - semantic + runtime validation (1-1-2-2 pattern)   |
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
| - update learning summary + history.json             |
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

## Module Reference

| Module | Purpose |
|--------|---------|
| `pipeline.py` | Core orchestrator — all 5 phases, state management, promote, analyst |
| `runner.py` | Thin wrapper — `ADORunner`, `ADOConfig`, `ADOResult` for simplified API |
| `node_prompter.py` | Prompt builder — attention-ordered sections, dynamic constraints, regression warnings |
| `generate.py` | LLM candidate generation — parallel workers via `ThreadPoolExecutor` |
| `sql_rewriter.py` | SQL extraction — parses LLM response, extracts rewritten SQL + transforms |
| `validate.py` | Validation + scoring — semantic equivalence, runtime benchmarking, error categorization |
| `analyst.py` | Analyst prompt builder — structural analysis, bottleneck identification, example overrides |
| `analyst_session.py` | Deep-dive iterative mode — `AnalystSession`, `AnalystIteration` |
| `knowledge.py` | FAISS recommender — `ADOFAISSRecommender` for similarity search |
| `faiss_builder.py` | FAISS index builder — vectorize examples, build/rebuild index |
| `learn.py` | Learning system — `Learner`, `LearningRecord`, summary analytics |
| `schemas.py` | Data models — `PipelineResult`, `BenchmarkConfig`, `PromotionAnalysis`, `ValidationResult`, `ValidationStatus`, `EdgeContract`, `NodeRewriteResult` |
| `store.py` | Artifact persistence — saves prompt/response/sql/validation per worker |
| `context.py` | Context builder — `ContextBundle` for EXPLAIN plans and table stats (optional, not in main pipeline) |
| `parse_dsb_catalog.py` | DSB catalog parser — utility for importing DSB benchmark rules |

## Phase Details

### Phase 1: Parse
- Module: `ado/pipeline.py` (`Pipeline._parse_dag`)
- Uses `DagBuilder` and `CostAnalyzer` from `qt_sql.optimization.dag_v2`.
- Output: DAG nodes/edges and per-node cost breakdown.

### Phase 2: FAISS Example Retrieval
- Module: `ado/pipeline.py` (`Pipeline._find_examples`, `Pipeline._find_regression_warnings`) -> `ado/knowledge.py` (`ADOFAISSRecommender`).
- Fingerprints the input SQL: replaces literals with neutral constants (`0`, `'x'`), lowercases identifiers.
- Vectorizes the AST into a 90-dimensional feature vector (node type counts, depth metrics, cardinality, pattern indicators, complexity).
- Searches FAISS index (IndexFlatL2 with z-score + L2 normalization for cosine similarity).
- **Engine-specific**: strict filtering ensures DuckDB queries only get DuckDB gold examples, PostgreSQL queries only get PostgreSQL gold examples.
- **Gold examples** (type=gold): Returns top-k (default k=3) — proven rewrites to emulate.
- **Regression warnings** (type=regression): Returns top-k (default k=2, min similarity 0.3) — failed rewrites to avoid. Shown as anti-patterns in the prompt.
- Index: `ado/models/similarity_index.faiss` (105 vectors, 90 dimensions).
  - 68 metadata entries: 16 DuckDB gold + 5 PG gold + 37 seed catalog rules + 10 DuckDB regressions
  - 37 additional anonymous vectors from multi-dialect vectorization of seed rules
- Rebuild: `python3 -m ado.faiss_builder` from `packages/qt-sql/` (use `--stats` to inspect).

### LLM Analyst (Optional)
- Module: `ado/pipeline.py` (`Pipeline._run_analyst`) + `ado/analyst.py`.
- **Off by default** — costs 1 extra API call per query. Set `use_analyst=True` on Pipeline or per-call.
- Sees: FAISS-selected examples, full catalogue of available gold examples for the engine, DAG, costs, history, effective_patterns, known_regressions.
- Can: accept FAISS picks, or override with specific example IDs via `EXAMPLES: id1, id2, id3` in response.
- Output: parsed into sections (structural breakdown, bottleneck, proposed optimizations, recommended strategy, example selection).
- Formatted analysis injected into the Phase 3 prompt (replaces examples section with expert structural guidance).

### Phase 3: Prompt + Candidate Generation
- Prompt assembly: `ado/node_prompter.py` (`Prompter.build_prompt`).
- Prompt sections (attention-ordered):
  1. **Role/Task** — frames the LLM as a SQL rewrite engine
  2. **Full SQL** — pretty-printed complete query with query_id
  3. **DAG Topology** — nodes, edges, depths, flags, costs as SQL comments
  4. **Performance Profile** — per-node cost %, row estimates, operators
  5. **History** — previous attempts (status, speedup, transforms) + promotion context
  5b. **Global Learnings** — aggregate benchmark stats (transform effectiveness, anti-patterns, example success rates, error patterns)
  6. **Examples** (default) or **Expert Analysis** (analyst mode) — FAISS-matched gold BEFORE/AFTER pairs, or LLM analyst structural guidance
  6b. **Regression Warnings** — FAISS-matched anti-patterns showing similar queries that regressed (original SQL, regressed rewrite, regression mechanism)
  7. **Constraints** — sandwich-ordered from JSON (CRITICAL top+bottom, HIGH+MEDIUM middle)
  8. **Output Format** — return complete rewritten SQL + changes summary
- Candidate generation: `ado/generate.py` (`CandidateGenerator.generate`) with N parallel workers via `ThreadPoolExecutor`.
- Response application: `ado/sql_rewriter.py` (`SQLRewriter`) extracts optimized SQL from LLM response, with fallback AST-based transform inference.
- **Candidate selection**: first syntactically valid, non-trivial candidate is picked (not best-of-N by speedup).

### Phase 4: Syntax Gate
- Pipeline parses selected candidate SQL via `sqlglot.parse_one(sql, dialect=dialect)`.
- If parsing fails, candidate is discarded and original SQL is used as fallback.

### Phase 5: Validate + Score
- Validator: `ado/validate.py` (`Validator`).
- DuckDB: uses `qt_sql.validation.sql_validator.SQLValidator`.
- PostgreSQL: uses executor-based PG validator (`Validator._create_pg_validator()`).
- Validation pattern: 1-1-2-2 (warmup runs, then measurement runs).
- Error categorization: `categorize_error()` classifies errors as syntax|semantic|timeout|execution|unknown.
- Pipeline status mapping:
  - `WIN`: speedup >= `1.10`
  - `IMPROVED`: speedup >= `1.05`
  - `NEUTRAL`: speedup >= `0.95`
  - `REGRESSION`: speedup < `0.95`
  - `ERROR`: validation error/failure

## LLM Configuration

### Provider Setup
LLM inference is configured via environment variables (`.env` file at project root):

```bash
QT_LLM_PROVIDER=deepseek          # Provider: deepseek | openrouter | anthropic | openai | groq | gemini-api
QT_LLM_MODEL=deepseek-reasoner    # Model name (provider-specific)
QT_DEEPSEEK_API_KEY=sk-...        # API key for chosen provider
```

### Supported Providers
| Provider | Default Model | Env Key |
|----------|--------------|---------|
| `deepseek` | `deepseek-reasoner` | `QT_DEEPSEEK_API_KEY` |
| `openrouter` | `moonshotai/kimi-k2.5` | `QT_OPENROUTER_API_KEY` |
| `anthropic` | `claude-sonnet-4-5-20250929` | `QT_ANTHROPIC_API_KEY` |
| `openai` | `gpt-4o` | `QT_OPENAI_API_KEY` |
| `groq` | `llama-3.3-70b-versatile` | `QT_GROQ_API_KEY` |
| `gemini-api` | `gemini-3-flash-preview` | `QT_GEMINI_API_KEY` |

### Programmatic Override
```python
config = ADOConfig(
    benchmark_dir="ado/benchmarks/duckdb_tpcds",
    provider="anthropic",
    model="claude-sonnet-4-5-20250929",
)
# Or pass a custom function:
config = ADOConfig(
    benchmark_dir="ado/benchmarks/duckdb_tpcds",
    analyze_fn=my_custom_llm_function,
)
```

Infrastructure: `qt_shared/llm/` (`create_llm_client()` factory, `LLMClient` protocol).

## Entry Points

### ADORunner (recommended)
```python
from ado import ADORunner, ADOConfig

config = ADOConfig(benchmark_dir="ado/benchmarks/duckdb_tpcds")
runner = ADORunner(config)

# Single query
result = runner.run_query("query_1", sql)

# Batch (full state)
results = runner.run_batch(state_num=0)

# Multiple queries with progress callback
results = runner.run_queries({"q1": sql1, "q2": sql2})

# Analyst deep-dive
result = runner.run_analyst("query_88", sql, max_iterations=5, target_speedup=1.5)

# Promote winners to next state
runner.promote(state_num=0)
```

### Pipeline (direct)
```python
from ado import Pipeline

p = Pipeline("ado/benchmarks/duckdb_tpcds", use_analyst=True)
result = p.run_query("query_1", sql, n_workers=5)
results = p.run_state(state_num=0)
session = p.run_analyst_session("query_88", sql, max_iterations=5)
```

## State Lifecycle

### `run_state(state_num)`
- For each query:
  - load baseline SQL (`queries/*.sql` or promoted SQL from previous state)
  - execute all phases: parse -> FAISS -> (analyst) -> prompt+generate -> syntax gate -> validate
  - persist validation + artifacts via `Store`
  - create learning record via `Learner`
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

| Artifact | Location |
|----------|----------|
| Benchmark config | `ado/benchmarks/<name>/config.json` |
| Query inputs | `ado/benchmarks/<name>/queries/*.sql` |
| State outputs | `ado/benchmarks/<name>/state_*` |
| FAISS index | `ado/models/similarity_index.faiss` + `similarity_metadata.json` + `feature_stats.json` |
| Gold examples | `ado/examples/<engine>/*.json` (duckdb: 16, postgres: 5) |
| Regression examples | `ado/examples/<engine>/regressions/*.json` (duckdb: 10) |
| Constraints | `ado/constraints/*.json` (11 files: 6 CRITICAL, 4 HIGH, 1 MEDIUM) |
| Learning journal | `ado/benchmarks/<name>/learning/` |
| Benchmark history | `ado/benchmarks/<name>/history.json` (auto-generated for analyst) |
| Analyst sessions | `ado/benchmarks/<name>/analyst_sessions/{query_id}/` |
| Leaderboards | `ado/benchmarks/<name>/leaderboard.json` + `leaderboard.md` |

## Analyst Deep-Dive Mode

### Overview
Iterative single-query optimization loop where the LLM analyst steers direction, reviews output, and retries with intelligence. Critical invariant: **always optimizes from the ORIGINAL query** with full history of all iterations — never from intermediate results.

### Entry Points
- **ADORunner**: `runner.run_analyst("query_88", sql, max_iterations=5, target_speedup=1.5, n_workers=3)`
- **Pipeline**: `p.run_analyst_session("query_88", sql, max_iterations=5, target_speedup=1.5, n_workers=3)`
- **Direct**: `from ado.analyst_session import AnalystSession`

### How It Works
Each iteration:
1. Parse the ORIGINAL SQL into DAG (never intermediate)
2. FAISS example retrieval (on original SQL) + regression warning matching
3. LLM analyst generates structural analysis with full history of all previous iterations
4. Build prompt with iteration history + global learnings + regression warnings
5. Generate N candidates via parallel workers
6. Validate best candidate
7. Record iteration result + learning record

### Stopping Criteria
- Target speedup reached
- Max iterations exhausted
- Converged (requires 3+ iterations; last 2 made no meaningful progress — within 2% of best)

### Session Persistence
- Sessions saved to `benchmark_dir/analyst_sessions/{query_id}/`
- Each iteration: `iteration_NN/{prompt.txt, optimized.sql, validation.json, analysis.txt}`
- Session metadata: `session.json` (query_id, best_speedup, best_sql, n_iterations)
- Sessions can be resumed via `AnalystSession.load_session(pipeline, session_dir)`

### Module
- `ado/analyst_session.py` — `AnalystSession`, `AnalystIteration`

## Dynamic Constraints

### Overview
Constraints are loaded dynamically from `ado/constraints/*.json` instead of being hardcoded. Each constraint file contains an `id`, `severity` (CRITICAL/HIGH/MEDIUM), and a `prompt_instruction` that is injected into the rewrite prompt.

### Current Constraints (11)
| ID | Severity | Purpose |
|----|----------|---------|
| COMPLETE_OUTPUT | CRITICAL | Preserve all output columns |
| CTE_COLUMN_COMPLETENESS | CRITICAL | CTE must include all downstream columns |
| KEEP_EXISTS_AS_EXISTS | CRITICAL | Don't convert EXISTS to IN/JOIN |
| LITERAL_PRESERVATION | CRITICAL | Copy all literals exactly |
| NO_MATERIALIZE_EXISTS | CRITICAL | Don't materialize EXISTS into CTEs |
| SEMANTIC_EQUIVALENCE | CRITICAL | Same rows, columns, ordering |
| MIN_BASELINE_THRESHOLD | HIGH | Be conservative on fast queries |
| NO_UNFILTERED_DIMENSION_CTE | HIGH | Every CTE must have a WHERE filter |
| OR_TO_UNION_GUARD | HIGH | Max 3 UNION branches, same-column ORs left alone |
| REMOVE_REPLACED_CTES | HIGH | Remove dead CTEs after rewriting |
| EXPLICIT_JOINS | MEDIUM | Prefer JOIN ON over comma joins |

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
Then rebuild the FAISS index if the constraint references new regression patterns.

## Regression Warning System

### Overview
Past benchmark regressions are indexed into FAISS alongside gold examples. When a new query is structurally similar to a query that previously regressed, the regression is shown as an anti-pattern in the prompt.

### How It Works
1. Regression examples stored in `ado/examples/<engine>/regressions/*.json`
2. Each has `type: "regression"` — indexed into FAISS with this metadata
3. `Pipeline._find_regression_warnings()` searches FAISS for type=regression matches (min similarity 0.3)
4. `Prompter._section_regression_warnings()` renders: original query, regressed rewrite, regression mechanism
5. LLM sees what went wrong and avoids repeating the same mistake

### Adding New Regressions
Create a JSON file in `ado/examples/<engine>/regressions/` with:
```json
{
  "id": "regression_qNN_transform",
  "type": "regression",
  "name": "QNN regression: transform (0.XXx)",
  "description": "Anti-pattern description",
  "verified_speedup": "0.XXx",
  "query_id": "qNN",
  "transform_attempted": "transform_name",
  "regression_mechanism": "Why it regressed — specific structural explanation",
  "original_sql": "-- complete original SQL",
  "example": {
    "before_sql": "-- original SQL",
    "after_sql": "-- the regressed rewrite",
    "key_insight": "What NOT to do"
  }
}
```
Then rebuild the FAISS index: `python3 -m ado.faiss_builder` from `packages/qt-sql/`.

### Current Regressions (10 DuckDB)
| ID | Speedup | Transform | Key Anti-Pattern |
|----|---------|-----------|-----------------|
| Q16 | 0.14x | semantic_rewrite | Don't materialize EXISTS into CTEs |
| Q93 | 0.34x | decorrelate | Don't decorrelate when join expands rows |
| Q31 | 0.49x | pushdown | Don't leave dead CTEs after rewriting |
| Q25 | 0.50x | date_cte_isolate | Don't isolate date when filter is already in WHERE |
| Q95 | 0.54x | semantic_rewrite | Don't decouple correlated EXISTS pairs into CTEs |
| Q90 | 0.59x | materialize_cte | Don't materialize tightly-coupled OR conditions |
| Q74 | 0.68x | pushdown | Don't duplicate CTEs instead of replacing |
| Q1 | 0.71x | decorrelate | Don't pre-aggregate when optimizer computes incrementally |
| Q67 | 0.85x | date_cte_isolate | Don't isolate date for queries with many window functions |
| Q51 | 0.87x | date_cte_isolate | Don't materialize running window aggregates into CTEs |

## Global Learnings

### Overview
Aggregate learnings from benchmark runs are automatically loaded from the learning journal and injected into rewrite prompts. This includes transform effectiveness, known anti-patterns, and example success rates.

### Data Flow
1. Learning records saved per-query during `run_query()` via `Learner.create_learning_record()`
2. `Learner.build_learning_summary()` aggregates all records into transform/example effectiveness stats
3. Summary injected into prompt via `Prompter._section_global_learnings()` (section 5b)
4. `Learner.generate_benchmark_history()` creates `history.json` for the analyst (effective_patterns + known_regressions)

### Learning Record Structure
```
LearningRecord: timestamp, query_id, query_pattern, examples_recommended,
  transforms_recommended, status, speedup, transforms_used, error_messages,
  error_category (syntax|semantic|timeout|execution|unknown)
```

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

## Benchmarks

### Configured Benchmarks
| Benchmark | Engine | Location |
|-----------|--------|----------|
| `duckdb_tpcds` | DuckDB | `ado/benchmarks/duckdb_tpcds/` |
| `postgres_dsb` | PostgreSQL | `ado/benchmarks/postgres_dsb/` |

### Benchmark Directory Structure
```
ado/benchmarks/<name>/
  config.json              # BenchmarkConfig (engine, db_path, workers, threshold)
  queries/*.sql            # Baseline SQL queries
  state_0/                 # Discovery state
    seed/                  # Unverified catalog rules (carry forward)
    prompts/               # Generated prompts
    responses/             # LLM responses
    validation/            # Per-query validation results
    leaderboard.json       # State leaderboard
  state_1/                 # Refinement state (after promote)
    promotion_context/     # LLM analysis of what worked
  learning/                # Learning records + summary
  analyst_sessions/        # Deep-dive session artifacts
  history.json             # Cumulative learnings for analyst
  leaderboard.json         # Best-of-all-states leaderboard
  leaderboard.md           # Human-readable leaderboard
```

## Operational Defaults (duckdb_tpcds)
- Engine: `duckdb`
- Scale factor: `10`
- Database: `/mnt/d/TPC-DS/tpcds_sf10.duckdb`
- Validation: `1-1-2-2` pattern (warmup + measurement via `qt_sql.validation.SQLValidator`)
- State 0 workers: `5` (discovery)
- State N workers: `1` (refinement)
- Promotion threshold: `1.05`
- Analyst: off by default (opt-in per query or per state)
- LLM: DeepSeek Reasoner (via `QT_LLM_PROVIDER=deepseek`)
