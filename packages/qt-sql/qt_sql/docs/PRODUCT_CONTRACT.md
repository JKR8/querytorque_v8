# qt_sql Product Contract

Engineering contract for the `qt_sql` SQL optimization pipeline.
Read this before changing any module.

---

## Master Flow

```
SQL + DSN
  │
  ▼
┌──────────────────────────────┐
│  Phase 1: Context Gathering  │  _parse_logical_tree(), _get_explain(), plan_analyzer, pg_tuning
│  SQL → logical tree + EXPLAIN│
└──────────┬───────────────────┘
           │  logical_tree, costs, explain_result
           ▼
┌──────────────────────────────┐
│  Phase 2: Knowledge          │  TagRecommender, _load_engine_profile(), _load_constraint_files()
│  Retrieval + Intelligence    │
└──────────┬───────────────────┘
           │  matched_examples, intelligence_briefing_local/global, engine_profile, constraints, regression_warnings
           ▼
┌──────────────────────────────┐
│  Phase 3: Prompt Generation  │  build_analyst_briefing_prompt(), build_worker_prompt()
│  Context → Prompt(s)         │
└──────────┬───────────────────┘
           │  prompt text(s) per worker
           ▼
┌──────────────────────────────┐
│  Phase 4: LLM Inference      │  CandidateGenerator._analyze()
│  Prompt → Raw Responses      │
└──────────┬───────────────────┘
           │  raw LLM response text per worker
           ▼
┌──────────────────────────────┐
│  Phase 5: Response           │  SQLRewriter.apply_response()
│  Processing                  │
└──────────┬───────────────────┘
           │  RewriteResult (optimized_sql, transform, set_local_commands)
           ▼
┌──────────────────────────────┐
│  Phase 6: Validation &       │  Validator.validate(), EquivalenceChecker, QueryBenchmarker
│  Benchmarking                │
└──────────┬───────────────────┘
           │  ValidationResult (status, speedup, error_category)
           ▼
┌──────────────────────────────┐
│  Phase 7: Outputs &          │  Store.save_candidate(), Learner, build_blackboard
│  Learning                    │
└──────────────────────────────┘
```

### Intelligence Workflows (Product-Defining)

#### A) PostgreSQL Scanner Intelligence Flow

`scanner (plan_scanner.py)`  
`-> blackboard/findings artifacts (scanner_knowledge/*)`  
`-> algorithm + query-specific finding`  
`-> analyst prompt section (plan_scanner_text / exploit algorithm context)`

Contract:
- Scanner outputs are evidence, not optional flavor text.
- Prompt must include scanner-derived intelligence when available for PostgreSQL.
- Missing scanner intelligence is an intelligence-gate failure for PostgreSQL optimization runs (unless explicit bootstrap override is enabled).

#### B) Optimization Blackboard Intelligence Flow

`optimization runs`  
`-> optimization blackboard (build_blackboard.py)`  
`-> findings/principles/anti-patterns`  
`-> intelligence briefing (local + global)`  
`-> prompt context + gold-example guidance`

Contract:
- Blackboard findings feed both local (query/run-adjacent) and global intelligence context.
- Gold examples are selected with intelligence context, not in isolation.
- Missing intelligence data is an intelligence-gate failure (unless explicit bootstrap override is enabled). No generic/synthetic fallback sections are allowed.

**Orchestrators** (choose one):
- `SwarmSession` — 4-worker fan-out + snipe refinement
- `ExpertSession` — iterative analyst + worker with failure analysis (default)
- `OneshotSession` — single LLM call, no iteration
- `ADORunner` / `Pipeline.run_query()` — batch harness wrapping any session

---

## Phase 1: Context Gathering

**Purpose:** Parse the input SQL into a logical tree, run EXPLAIN, extract plan signals.

| Field | Detail |
|-------|--------|
| CLI | `python3 -m qt_sql.plan_scanner`, `python3 -m qt_sql.scanner_knowledge.build_all` |
| Entry Point | `Pipeline._parse_logical_tree()` in `pipeline.py` |
| Input | SQL text (`str`) + DSN (`str`) + dialect (`str`) |
| Output | `logical_tree` + `costs: dict` + `explain_result: dict` |
| Success Condition | logical tree has >= 1 node; EXPLAIN returns valid plan or falls back gracefully |
| Failure Handling | If EXPLAIN ANALYZE fails, falls back to EXPLAIN (no ANALYZE). If that fails, uses heuristic cost splitting. Never blocks pipeline. |

**Key Modules:**

| Module | Responsibility |
|--------|----------------|
| `dag.py` | Query-structure parser/builder + `CostAnalyzer` — parse SQL into logical tree representation (nodes, edges, contracts) and assign cost percentages. |
| `plan_analyzer.py` | `analyze_plan_for_optimization()` — extract `OptimizationContext` (table scans, joins, CTE flows, bottlenecks) from EXPLAIN JSON. |
| `pg_tuning.py` | `PGSystemProfile`, `load_or_collect_profile()`, `build_resource_envelope()` — PostgreSQL system introspection, cached to disk. |
| `plan_scanner.py` | Three-layer plan-space scanner (hint scan, explore, knowledge) for PostgreSQL. |
| `scanner_knowledge/` | Knowledge pipeline that converts plan scanner findings into prompt-injectable text. |

**Key Data Structures:**

| Struct | Location | Key Fields |
|--------|----------|------------|
| `logical_tree` | `dag.py` | `nodes`, `edges`, `original_sql` |
| `logical_tree node` | `dag.py` | `node_id`, `node_type` (cte/main/subquery), `sql`, `tables`, `refs`, `flags`, `contract`, `cost` |
| `NodeContract` | `dag.py` | `output_columns`, `grain`, `required_predicates` |
| `OptimizationContext` | `plan_analyzer.py` | `table_scans`, `joins`, `cte_flows`, `bottleneck_operators` |

**Spec Links:** `plan_scanner_spec.yaml`, `scanner_knowledge/README.md`

---

## Phase 2: Knowledge Retrieval

**Purpose:** Find relevant gold examples and assemble intelligence briefing context (local + global), plus engine profiles, constraints, and regression warnings.

| Field | Detail |
|-------|--------|
| CLI | `python3 -m qt_sql.tag_index [--stats\|--rebuild]` |
| Entry Point | `Pipeline._find_examples()` in `pipeline.py`, `TagRecommender.find_similar_examples()` in `knowledge.py` |
| Input | SQL text + dialect |
| Output | `matched_examples: list[dict]`, `global_knowledge/intelligence_briefing_global`, `plan_scanner_text/intelligence_briefing_local` (PG), `engine_profile: dict`, `constraints: list[dict]`, `regression_warnings: list[dict]` |
| Success Condition | >= 1 example returned; local + global intelligence briefings are loaded; for PG, scanner intelligence + exploit algorithm are loaded; engine profile loaded for dialect |
| Failure Handling | Missing required intelligence inputs triggers hard failure via intelligence gates. Optional bootstrap override (`QT_ALLOW_INTELLIGENCE_BOOTSTRAP=1`) may downgrade to warning for controlled bootstrap/debug runs only. |

**Key Modules:**

| Module | Responsibility |
|--------|----------------|
| `knowledge.py` | `TagRecommender` — tag-based similarity matching from `models/similarity_tags.json`. |
| `tag_index.py` | `SQLNormalizer`, tag extraction, index builder. CLI: `python3 -m qt_sql.tag_index`. |
| `node_prompter.py` | `_load_engine_profile()`, `_load_constraint_files()`, `load_exploit_algorithm()` — load JSON profiles and constraints. |
| `models/similarity_tags.json` | Tag index for similarity matching (exact counts vary as the catalog evolves). |

**Data Files:**

| File | Content |
|------|---------|
| `constraints/engine_profile_duckdb.json` | 7 strengths, 6 gaps (CROSS_CTE_PREDICATE_BLINDNESS, etc.) |
| `constraints/engine_profile_postgresql.json` | 6 strengths, 5 gaps (COMMA_JOIN_WEAKNESS, etc.) |
| `examples/duckdb/*.json` | Gold optimization examples with verified speedups |
| `examples/postgres/*.json` | PostgreSQL gold examples |

---

## Phase 3: Prompt Generation

**Purpose:** Assemble structured prompts for the analyst and workers from Phase 1+2 outputs, including intelligence briefings (local + global).

| Field | Detail |
|-------|--------|
| CLI | `python3 -m qt_sql.prompts.samples.generate_sample` |
| Entry Point | `build_analyst_briefing_prompt()` in `prompts/analyst_briefing.py`, `build_worker_prompt()` in `prompts/worker.py`, `build_sniper_prompt()` in `prompts/swarm_snipe.py` |
| Input | Phase 1+2 context dict (from `Pipeline.gather_analyst_context()`): logical tree/EXPLAIN + matched examples + intelligence briefing local/global + constraints/profiles; mode; worker assignments |
| Output | Prompt text(s) — one analyst prompt + one per worker |
| Success Condition | All required sections present (semantic contract, target logical tree, examples, DAP output format) |
| Failure Handling | Missing required intelligence or required analyst briefing sections is a hard failure (no synthetic fallback briefing generation). |

**Key Modules:**

| Module | Responsibility |
|--------|----------------|
| `prompts/analyst_briefing.py` | `build_analyst_briefing_prompt()` — analyst role: distributes examples, assigns worker strategies. |
| `prompts/worker.py` | `build_worker_prompt()` — per-worker rewrite prompt with strategy, examples, DAP output format. Section [7b] handles SET LOCAL config for PG. |
| `prompts/swarm_snipe.py` | `build_snipe_analyst_prompt()`, `build_sniper_prompt()` — refinement after fan-out. Synthesizes failures. |
| `prompts/swarm_fan_out.py` | `build_fan_out_prompt()` — legacy fan-out prompt (pre-briefing). |
| `prompts/swarm_parsers.py` | `parse_briefing_response()` → `ParsedBriefing`, `parse_snipe_response()` → `SnipeAnalysis`, `parse_oneshot_response()` → `OneshotResult`. |
| `prompts/briefing_checks.py` | Section checklists for prompt validation — `build_analyst_section_checklist()`, `validate_parsed_briefing()`. |
| `prompts/dag_helpers.py` | Logical-tree formatting utilities for prompt text. |
| `prompts/pg_tuner.py` | PostgreSQL tuning prompt builder. |
| `prompts/swarm_common.py` | `build_worker_strategy_header()` — shared strategy text. |

**Parsed Structures:**

| Struct | Location | Key Fields |
|--------|----------|------------|
| `ParsedBriefing` | `prompts/swarm_parsers.py` | `shared: BriefingShared`, `workers: list[BriefingWorker]` |
| `BriefingShared` | `prompts/swarm_parsers.py` | Query analysis, semantic contract, intent text |
| `BriefingWorker` | `prompts/swarm_parsers.py` | `worker_id`, `strategy`, `assigned_examples`, `hint` |
| `SnipeAnalysis` | `prompts/swarm_parsers.py` | Failure synthesis, refined strategy for sniper worker |
| `WorkerAssignment` | `prompts/swarm_parsers.py` | Legacy fan-out assignment structure |

**Spec Links:** `prompts/sql_rewrite_spec.md` (DAP v1.0), `prompts/samples/PROMPT_SPEC.md`, `prompts/samples/V0/` (11 rendered samples)

---

## Phase 4: LLM Inference

**Purpose:** Send prompts to the LLM and collect raw responses.

| Field | Detail |
|-------|--------|
| CLI | `POST /api/sql/optimize` (API), `ADORunner.run_query()` (programmatic) |
| Entry Point | `CandidateGenerator._analyze()` in `generate.py` |
| Input | Prompt text(s) |
| Output | Raw LLM response strings per worker |
| Success Condition | >= 1 worker returns non-empty response |
| Failure Handling | Empty responses or API errors produce `Candidate` with `error` field set. Generator continues with remaining workers. |

**Key Modules:**

| Module | Responsibility |
|--------|----------------|
| `generate.py` | `CandidateGenerator` — parallel worker inference via `ThreadPoolExecutor`. Supports custom `analyze_fn` or `qt_shared.llm` client. |

**Key Data Structures:**

| Struct | Location | Key Fields |
|--------|----------|------------|
| `Candidate` | `generate.py` | `worker_id`, `prompt`, `response`, `optimized_sql`, `examples_used`, `transforms`, `set_local_commands`, `error` |

---

## Phase 5: Response Processing

**Purpose:** Parse LLM responses, extract SQL and transforms, validate structure.

| Field | Detail |
|-------|--------|
| CLI | Embedded in pipeline, no standalone command |
| Entry Point | `SQLRewriter.apply_response()` in `sql_rewriter.py` |
| Input | Raw LLM response (`str`) + original SQL (`str`) |
| Output | `RewriteResult` (`optimized_sql`, `transform`, `set_local_commands`) |
| Success Condition | Valid SQL extracted; output columns match original; base tables match |
| Failure Handling | Returns `RewriteResult(success=False, error=...)`. Malformed JSON, missing SQL, or failed AST checks all produce structured errors. |

**Key Modules:**

| Module | Responsibility |
|--------|----------------|
| `sql_rewriter.py` | `SQLRewriter` — parse DAP or legacy JSON from response, assemble SQL, validate output columns + base tables via sqlglot AST. `_split_set_local()` — extract SET LOCAL commands, validate against `PG_TUNABLE_PARAMS` whitelist. `extract_transforms_from_response()` — extract transform labels. `infer_transforms_from_sql_diff()` — AST-based transform inference fallback. |
| `logic_tree.py` | Logic tree generation for reasoning trace extraction. |

**Key Data Structures:**

| Struct | Location | Key Fields |
|--------|----------|------------|
| `RewriteResult` | `sql_rewriter.py` | `success`, `optimized_sql`, `transform`, `error`, `set_local_commands` |
| `RewriteSet` | `sql_rewriter.py` | `id`, `transform`, `nodes`, `invariants_kept`, `set_local` |
| `DAPComponent` | `sql_rewriter.py` | `component_id`, component SQL and metadata |

**Validation Gates (4):**

1. **JSON parse** — LLM response must contain valid JSON with expected keys
2. **SQL extraction** — assembled SQL must be non-empty
3. **Output columns match** — sqlglot AST comparison of SELECT columns
4. **Base tables match** — sqlglot AST comparison of FROM/JOIN tables

---

## Phase 6: Validation & Benchmarking

**Purpose:** Verify semantic equivalence and measure performance improvement.

| Field | Detail |
|-------|--------|
| CLI | `POST /api/sql/validate` |
| Entry Point | `Validator.validate()` in `validate.py` |
| Input | Original SQL + candidate SQL + executor (DSN) |
| Output | `ValidationResult` (`status`, `speedup`, `error_category`) |
| Success Condition | sqlglot gate passes; row counts match; checksums match (DuckDB). Performance is measured and classified separately (WIN/IMPROVED/NEUTRAL/REGRESSION). |
| Failure Handling | Returns structured `ValidationResult` with `ValidationStatus.ERROR` or `.FAIL`, `errors` list, and `error_category`. |

**Speedup Thresholds:**

| Label | Threshold |
|-------|-----------|
| WIN | >= 1.10x |
| IMPROVED | >= 1.05x |
| NEUTRAL | >= 0.95x |
| REGRESSION | < 0.95x |

**Timing Methods:**

| Method | Protocol | When |
|--------|----------|------|
| 3-run | Warmup, measure, measure — average last 2 | Default (DuckDB, PG) |
| 5-run trimmed mean | 5 runs, remove min/max, average middle 3 | Research validation |
| 4x triage (1-2-1-2) | Warmup orig, warmup opt, measure orig, measure opt | Mid-run screening, interleaved for drift control |

**Never** use single-run timing comparisons.

**Key Modules:**

| Module | Responsibility |
|--------|----------------|
| `validate.py` | `Validator` — orchestrates validation. `benchmark_baseline()` → `OriginalBaseline`. `validate_against_baseline()` for batch mode. `cost_rank_candidates()` for cost-based pre-screening. `categorize_error()` for learning. |
| `validation/sql_validator.py` | `SQLValidator` — DuckDB validation engine: syntax check, benchmarking, equivalence. |
| `validation/benchmarker.py` | `QueryBenchmarker` — timing harness (warmup + measurement pattern). |
| `validation/equivalence_checker.py` | `EquivalenceChecker` — row count + MD5 checksum comparison (DuckDB). `ChecksumResult`, `RowCountResult`, `ValueComparisonResult`. |
| `validation/schemas.py` | `ValidationStatus` (PASS/FAIL/WARN/ERROR) for the qt_sql validation layer. |

**Key Data Structures:**

| Struct | Location | Key Fields |
|--------|----------|------------|
| `ValidationResult` | `schemas.py` | `worker_id`, `status: ValidationStatus`, `speedup`, `error`, `errors`, `error_category` |
| `OriginalBaseline` | `validate.py` | `measured_time_ms`, `row_count`, `rows`, `checksum` |

---

## Phase 7: Outputs & Learning

**Purpose:** Persist artifacts, update leaderboards, capture learning records.

| Field | Detail |
|-------|--------|
| CLI | `python3 -m qt_sql.build_blackboard <batch_dir>`, `python3 -m qt_sql.build_blackboard --global` |
| Entry Point | `Store.save_candidate()` in `store.py`, `Learner.create_learning_record()` in `learn.py`, `Pipeline._update_benchmark_leaderboard()` in `pipeline.py` |
| Input | `ValidationResult`s + metadata (query_id, worker_id, prompts, responses) |
| Output | Artifacts (prompt.txt, response.txt, optimized.sql, validation.json), `LearningRecord`, leaderboard CSV, `BlackboardEntry` |
| Success Condition | All artifacts written; leaderboard updated if status improved; learning record captured |
| Failure Handling | Artifact write failures are logged but do not block pipeline. Leaderboard updates are atomic (write-then-rename). |

**Key Modules:**

| Module | Responsibility |
|--------|----------------|
| `store.py` | `Store` — persist prompt, response, SQL, validation JSON per worker. Returns `StoredArtifact` paths. |
| `learn.py` | `Learner` — create `LearningRecord`, save to journal, `build_learning_summary()`, `generate_benchmark_history()`. |
| `build_blackboard.py` | `BlackboardEntry` extraction from swarm batches. `KnowledgePrinciple`, `KnowledgeAntiPattern` collation. Global mode aggregates best-of-all-sources. No LLM calls. |
| `session_logging.py` | Per-run file logging — `attach_session_handler()`, `detach_session_handler()`. |
| `schemas.py` | `SessionResult` (per-session), `WorkerResult` (per-worker), `PipelineResult` (per-query), `RunMeta` (traceability). |

**Key Data Structures:**

| Struct | Location | Key Fields |
|--------|----------|------------|
| `StoredArtifact` | `store.py` | `prompt_path`, `response_path`, `optimized_sql_path`, `validation_path` |
| `LearningRecord` | `learn.py` | `query_id`, `transforms_used`, `status`, `speedup`, `error_category`, `error_messages`, `examples_recommended` |
| `BlackboardEntry` | `build_blackboard.py` | `query_id`, `worker_id`, `run_name`, speedup, transforms, reasoning |
| `SessionResult` | `schemas.py` | `query_id`, `mode`, `best_speedup`, `best_sql`, `status`, `n_iterations`, `n_api_calls` |
| `WorkerResult` | `schemas.py` | `worker_id`, `strategy`, `examples_used`, `speedup`, `status`, `transforms`, `set_local_config` |
| `RunMeta` | `schemas.py` | `run_id`, `model`, `provider`, `git_sha`, `queries_attempted`, `queries_improved`, `estimated_cost_usd` |

---

## CLI Quick Reference

| Command | What it does |
|---------|--------------|
| `python3 -m qt_sql.tag_index` | Build tag-based example index |
| `python3 -m qt_sql.tag_index --stats` | Show index statistics |
| `python3 -m qt_sql.tag_index --rebuild` | Force rebuild index |
| `python3 -m qt_sql.plan_scanner` | Run plan-space scanner (PG) |
| `python3 -m qt_sql.scanner_knowledge.build_all` | Build scanner knowledge from findings |
| `python3 -m qt_sql.prompts.samples.generate_sample` | Render prompt samples to `prompts/samples/V0/` |
| `python3 -m qt_sql.build_blackboard <batch_dir>` | Build blackboard from swarm batch |
| `python3 -m qt_sql.build_blackboard --global` | Build global best-of-all-sources blackboard |
| `python3 -m qt_sql.benchmarks.build_best` | Build best-of artifacts from leaderboard |
| `python3 -m qt_sql.benchmarks.migrate_leaderboards` | Migrate leaderboard formats |

**API Endpoints** (via `api/main.py`):

| Endpoint | Method |
|----------|--------|
| `/api/sql/optimize` | POST — run optimization pipeline |
| `/api/sql/validate` | POST — validate candidate SQL |

**Environment:**

```bash
# Required
PYTHONPATH=packages/qt-shared:packages/qt-sql:.
# Run from project root (QueryTorque_V8/) so pydantic-settings finds .env
```

---

## Module → Phase Mapping

| Module | Phase(s) | Primary Responsibility |
|--------|----------|----------------------|
| `dag.py` | 1 | SQL → logical tree parsing, cost analysis |
| `plan_analyzer.py` | 1 | EXPLAIN plan → OptimizationContext |
| `pg_tuning.py` | 1 | PostgreSQL system introspection |
| `plan_scanner.py` | 1 | Three-layer plan-space scanner (PG) |
| `scanner_knowledge/` | 1 | Scanner findings → prompt text |
| `knowledge.py` | 2 | Tag-based example matching |
| `tag_index.py` | 2 | Example index builder |
| `node_prompter.py` | 2, 3 | Load engine profiles, constraints; prompt utilities |
| `prompts/analyst_briefing.py` | 3 | Analyst briefing prompt builder |
| `prompts/worker.py` | 3 | Worker rewrite prompt builder |
| `prompts/swarm_snipe.py` | 3 | Snipe refinement prompt builder |
| `prompts/swarm_fan_out.py` | 3 | Legacy fan-out prompt builder |
| `prompts/swarm_parsers.py` | 3 | Response parsers (briefing, snipe, oneshot) |
| `prompts/briefing_checks.py` | 3 | Section validation checklists |
| `generate.py` | 4 | CandidateGenerator — parallel LLM inference |
| `sql_rewriter.py` | 5 | DAP/JSON parsing, SQL assembly, AST validation |
| `logic_tree.py` | 5 | Logic tree generation |
| `validate.py` | 6 | Validation orchestrator |
| `validation/sql_validator.py` | 6 | DuckDB validation engine |
| `validation/benchmarker.py` | 6 | Timing harness |
| `validation/equivalence_checker.py` | 6 | Row count + checksum equivalence |
| `store.py` | 7 | Artifact persistence |
| `learn.py` | 7 | Learning records + analytics |
| `build_blackboard.py` | 7 | Knowledge extraction from batches |
| `session_logging.py` | 7 | Per-run file logging |
| `schemas.py` | all | Shared data structures |
| `pipeline.py` | all | Pipeline orchestrator (all phases) |
| `runner.py` | all | `ADORunner` — batch harness |
| `sessions/swarm_session.py` | all | Swarm session orchestrator |
| `sessions/expert_session.py` | all | Expert session orchestrator |
| `sessions/oneshot_session.py` | all | Oneshot session orchestrator |
| `execution/factory.py` | 1, 6 | Database connector factory |
| `execution/duckdb_executor.py` | 1, 6 | DuckDB executor |
| `execution/postgres_executor.py` | 1, 6 | PostgreSQL executor |

---

## Known Gaps

1. **PG equivalence checking** — PostgreSQL path checks row counts only; checksum comparison not yet implemented. DuckDB path has full MD5 checksum verification.

2. **Formal SQL equivalence** — No formal prover handles CTEs + window functions (all TPC-DS queries use both). QED (VLDB 2024) and VeriEQL (OOPSLA 2024) both lack CTE support. We verify via result-set checksums.

3. **Cost-rank pre-screening** — `cost_rank_candidates()` uses EXPLAIN cost estimates for DuckDB only. PG pre-screening is not implemented.

4. **Bootstrap override risk** — `QT_ALLOW_INTELLIGENCE_BOOTSTRAP=1` intentionally bypasses intelligence hard gates for bootstrap/debug runs. This must stay off for performance-critical or SOTA claims.

5. **Timeout baseline handling** — When the original query times out (PG), baseline is set to the timeout ceiling. This can inflate speedup ratios for queries that were previously timing out.
