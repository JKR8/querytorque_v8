# ADO Workflow Documentation

## System Flow (ASCII)

```text
+----------------------------------------------------------------------------------+
|                        ADO 5-Phase Optimization Workflow                         |
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
| Phase 2: Annotate Rewrite Targets             |
| - heuristic by default (no LLM call)          |
| - optional LLM annotation (annotate_with_llm) |
| - assigns {node_id: pattern} hints            |
+-----------------------------------------------+
            |
            v
+-----------------------------------------------+
| FAISS Example Retrieval (engine-specific)     |
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
| - prompt includes: full SQL, DAG topology, cost data,        |
|   FAISS gold examples, constraints, history, analyst output   |
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

### Phase 2: Annotate
- Module: `ado/annotator.py` via `Pipeline._annotate()`.
- **Default: heuristic mode (no LLM call)**. Uses DAG node flags (CORRELATED, UNION_ALL, IN_SUBQUERY, etc.) and dimension table detection to assign patterns deterministically.
- Optional LLM mode: set `annotate_with_llm=True` on Pipeline. Sends DAG topology + costs (no SQL) to LLM, receives `{node: pattern}` JSON.
- Output: `AnnotationResult` with rewrite targets (`node_id -> pattern`) and skipped nodes.
- **Note**: Annotation patterns are used as prompt hints only. They do NOT drive example selection (FAISS handles that).

### FAISS Example Retrieval
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
- Provides expert structural analysis injected into the Phase 3 prompt (replaces pattern hints + examples section).

### Phase 3: Prompt + Candidate Generation
- Prompt assembly: `ado/node_prompter.py` (`Prompter.build_prompt`).
- Prompt includes: full SQL, DAG topology, cost data, FAISS gold examples (before/after SQL pairs), constraints, history from previous states, analyst output (if enabled).
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
  - execute all phases: parse → annotate → FAISS → (analyst) → prompt+generate → syntax gate → validate
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
- Constraints: `ado/constraints/*.json`
- Learning journal: `ado/benchmarks/<name>/learning/`

## Operational Defaults (duckdb_tpcds)
- Engine: `duckdb`
- Scale factor: `10`
- Database: `/mnt/d/TPC-DS/tpcds_sf10.duckdb`
- Validation method: `3-run` (discard 1st warmup, average runs 2-3)
- State 0 workers: `5` (discovery)
- State N workers: `1` (refinement)
- Promotion threshold: `1.05`
- Phase 2 annotation: heuristic (no LLM call)
- Analyst: off (opt-in per query)
