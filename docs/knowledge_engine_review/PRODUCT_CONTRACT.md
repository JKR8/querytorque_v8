# qt_sql Product Contract

Engineering contract for the `qt_sql` SQL optimization pipeline.
Read this before changing any module.

**Scope:** This contract covers the `qt_sql` package only. The ADO batch runner (`ado/`) is a separate system with its own contracts and is not covered here. The ADO runner consumes `qt_sql` as a library but is not part of this pipeline.

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
           │  logical_tree, costs, explain_result, context_confidence
           ▼
┌──────────────────────────────┐
│  Phase 2: Knowledge          │  TagRecommender, _load_engine_profile(), _load_constraint_files()
│  Retrieval + Intelligence    │
└──────────┬───────────────────┘
           │  matched_examples, intelligence_briefing_local/global, engine_profile, constraints, regression_warnings
           ▼
┌──────────────────────────────────────┐
│  Phase 2→3: Intelligence Handoff     │  gather_analyst_context() → context dict
│  Required vs optional field contract │
└──────────┬───────────────────────────┘
           │  analyst_context dict (see handoff contract below)
           ▼
┌──────────────────────────────┐
│  Phase 3: Prompt Generation  │  build_analyst_briefing_prompt(), build_worker_prompt()
│  Context → Prompt(s)         │
└──────────┬───────────────────┘
           │  prompt text(s) per worker (within token budget)
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
           │  ValidationResult (status, speedup, speedup_type, error_category, validation_confidence)
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

---

## Core Data Contract (`schemas.py`)

`schemas.py` defines the data structures that flow between every phase. All consumers must respect these contracts.

### Pipeline-Level Structures

| Struct | Key Fields | Phase(s) |
|--------|------------|----------|
| `ValidationStatus` (Enum) | `PASS`, `FAIL`, `ERROR` | 6, 7 |
| `ValidationResult` | `worker_id`, `status`, `speedup`, `speedup_type`, `error`, `errors: list[str]`, `error_category`, `optimized_sql`, `validation_confidence` | 6 → 7 |
| `PipelineResult` | `query_id`, `status`, `speedup`, `original_sql`, `optimized_sql`, `nodes_rewritten`, `transforms_applied`, `prompt`, `response`, `analysis` | all → output |
| `WorkerResult` | `worker_id`, `strategy`, `examples_used`, `optimized_sql`, `speedup`, `status`, `transforms`, `hint`, `error_messages: list[str]`, `error_category`, `exploratory: bool`, `set_local_config` | 4 → 7 |
| `SessionResult` | `query_id`, `mode`, `best_speedup`, `best_sql`, `original_sql`, `best_transforms`, `status`, `n_iterations`, `n_api_calls` | session → output |
| `RunMeta` | `run_id`, `model`, `provider`, `git_sha`, `queries_attempted`, `queries_improved`, `estimated_cost_usd` | output |

### Required vs Optional Fields

| Struct | Required (must be non-null) | Optional (may be None/empty) |
|--------|----------------------------|------------------------------|
| `ValidationResult` | `worker_id`, `status`, `speedup`, `optimized_sql` | `error`, `errors`, `error_category`, `speedup_type`, `validation_confidence` |
| `PipelineResult` | `query_id`, `status`, `speedup`, `original_sql`, `optimized_sql` | `transforms_applied`, `prompt`, `response`, `analysis` |
| `WorkerResult` | `worker_id`, `status`, `speedup`, `optimized_sql` | all others (empty list/string/None) |

### Backward Compatibility Rules

1. **Additive only** — new fields may be added with defaults; existing fields must not be removed or renamed without a migration.
2. **No silent semantic changes** — if a field's meaning changes (e.g., `speedup` gains a `speedup_type` qualifier), both fields must coexist during transition.
3. **Enum extension** — new `ValidationStatus` values may be added; consumers must handle unknown values gracefully (treat as `ERROR`).

---

## Phase 1: Context Gathering

**Purpose:** Parse the input SQL into a logical tree, run EXPLAIN, extract plan signals. Emit a quality signal so downstream phases can adjust their gate strictness.

| Field | Detail |
|-------|--------|
| CLI | `python3 -m qt_sql.plan_scanner`, `python3 -m qt_sql.scanner_knowledge.build_all` |
| Entry Point | `Pipeline._parse_logical_tree()` in `pipeline.py` |
| Input | SQL text (`str`) + DSN (`str`) + dialect (`str`) |
| Output | `logical_tree` + `costs: dict` + `explain_result: dict` + `context_confidence: str` |
| Success Condition | logical tree has >= 1 node; EXPLAIN returns valid plan or falls back gracefully |
| Failure Handling | Graceful degradation with quality signal (see below). Never blocks pipeline. |

### Context Confidence Signal

Phase 1 emits `context_confidence` so Phase 2 gates can be more nuanced than binary pass/fail:

| Value | Meaning | When |
|-------|---------|------|
| `high` | Full EXPLAIN ANALYZE succeeded; real execution stats available | EXPLAIN ANALYZE returns valid plan with actual rows/times |
| `degraded` | EXPLAIN (no ANALYZE) succeeded; cost estimates only, no actual execution stats | EXPLAIN ANALYZE failed, fell back to EXPLAIN |
| `heuristic` | No EXPLAIN data; using heuristic cost splitting from AST only | Both EXPLAIN variants failed; costs derived from logical tree structure |

**Contract:** Phase 2 intelligence gates **must** consult `context_confidence`:
- `high` — standard gate behavior (hard fail on missing intelligence)
- `degraded` — standard gate behavior, but prompt must include a note: "EXPLAIN plan is cost-estimate only (no actual execution stats)"
- `heuristic` — gates remain active but Phase 3 prompt must include: "No EXPLAIN plan available. Cost percentages are heuristic estimates from AST structure. Treat bottleneck identification as approximate."

This resolves the tension between Phase 1's "never blocks pipeline" philosophy and Phase 2's "hard gate" philosophy: Phase 1 never blocks, but it honestly reports its confidence level so downstream consumers can calibrate expectations.

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

**Purpose:** Find relevant gold examples and assemble intelligence briefing context (local + global), plus engine profiles, constraints, and regression warnings. Gate behavior adjusts based on Phase 1's `context_confidence`.

| Field | Detail |
|-------|--------|
| CLI | `python3 -m qt_sql.tag_index [--stats\|--rebuild]` |
| Entry Point | `Pipeline._find_examples()` in `pipeline.py`, `TagRecommender.find_similar_examples()` in `knowledge.py` |
| Input | SQL text + dialect + `context_confidence` from Phase 1 |
| Output | `matched_examples: list[dict]`, `global_knowledge/intelligence_briefing_global`, `plan_scanner_text/intelligence_briefing_local` (PG), `engine_profile: dict`, `constraints: list[dict]`, `regression_warnings: list[dict]` |
| Success Condition | >= 1 example returned; local + global intelligence briefings are loaded; for PG, scanner intelligence + exploit algorithm are loaded; engine profile loaded for dialect |
| Failure Handling | Missing required intelligence inputs triggers hard failure via intelligence gates. Optional bootstrap override (`QT_ALLOW_INTELLIGENCE_BOOTSTRAP=1`) may downgrade to warning for controlled bootstrap/debug runs only. |

**Key Modules:**

| Module | Responsibility |
|--------|----------------|
| `knowledge.py` | `TagRecommender` — tag-based similarity matching from `models/similarity_tags.json`. |
| `tag_index.py` | `SQLNormalizer`, tag extraction, index builder. CLI: `python3 -m qt_sql.tag_index`. |
| `prompter.py` | `_load_engine_profile()`, `_load_constraint_files()`, `load_exploit_algorithm()` — load JSON profiles and constraints. |
| `models/similarity_tags.json` | Tag index for similarity matching (exact counts vary as the catalog evolves). |

**Data Files:**

| File | Content |
|------|---------|
| `constraints/engine_profile_duckdb.json` | 7 strengths, 6 gaps (CROSS_CTE_PREDICATE_BLINDNESS, etc.) |
| `constraints/engine_profile_postgresql.json` | 6 strengths, 5 gaps (COMMA_JOIN_WEAKNESS, etc.) |
| `examples/duckdb/*.json` | Gold optimization examples with verified speedups |
| `examples/postgres/*.json` | PostgreSQL gold examples |

---

## Phase 2→3: Intelligence Handoff Contract

`gather_analyst_context()` produces a context dict that is the sole interface between knowledge retrieval and prompt generation. This section specifies which fields are required vs optional and what happens when fields are missing.

### Field Contract

| Field | Required? | If Missing | Impact on Prompt |
|-------|-----------|-----------|-----------------|
| `explain_plan_text` | Optional | Prompt says "not available, use logical-tree cost percentages as proxy" | Degraded bottleneck identification |
| `matched_examples` | **Required** | Intelligence gate hard fail (unless bootstrap override) | No prompt generated |
| `all_available_examples` | Optional | Analyst uses only matched subset | Slightly less strategy diversity |
| `global_knowledge` | **Required** | Intelligence gate hard fail (unless bootstrap override) | No prompt generated |
| `plan_scanner_text` (PG only) | **Required for PG** | Gate fails for PG (unless bootstrap override) | No PG prompt generated |
| `exploit_algorithm_text` (PG only) | **Required for PG** | Gate fails for PG (unless bootstrap override) | No PG prompt generated |
| `engine_profile` | Optional | Section omitted from prompt; analyst proceeds without gap-hunting guidance | Reduced optimization quality |
| `constraints` | Optional | No constraint sections in prompt | May produce known-bad patterns |
| `regression_warnings` | Optional | Section omitted | May repeat known regressions |
| `semantic_intents` | Optional | Analyst derives from SQL directly | Slightly more analyst work |
| `resource_envelope` (PG only) | Optional | Workers skip SET LOCAL section | No per-worker tuning |
| `strategy_leaderboard` | Optional | Omitted from prompt | Analyst picks strategies blind |
| `query_archetype` | Optional | Omitted from prompt | No archetype-specific guidance |
| `context_confidence` | **Required** | Defaults to `heuristic` (most conservative) | Prompt includes appropriate caveats |

### Partial Intelligence Rules

1. **Local present, global empty** — hard gate fail. Global intelligence is always required because it provides cross-query pattern knowledge.
2. **Global present, local empty (non-PG)** — proceeds. Local intelligence is PG-specific (scanner findings).
3. **Global present, local empty (PG)** — hard gate fail. PG requires scanner-derived local intelligence.
4. **All present but `context_confidence=heuristic`** — proceeds with caveat text injected into prompt.

---

## Phase 3: Prompt Generation

**Purpose:** Assemble structured prompts for the analyst and workers from Phase 1+2 outputs, including intelligence briefings (local + global). Manage token budgets to prevent silent quality degradation from context overflow.

| Field | Detail |
|-------|--------|
| CLI | `python3 -m qt_sql.prompts.samples.generate_sample` |
| Entry Point | `build_analyst_briefing_prompt()` in `prompts/analyst_briefing.py`, `build_worker_prompt()` in `prompts/worker.py`, `build_sniper_prompt()` in `prompts/swarm_snipe.py` |
| Input | Phase 1+2 context dict (from `Pipeline.gather_analyst_context()`): logical tree/EXPLAIN + matched examples + intelligence briefing local/global + constraints/profiles; mode; worker assignments |
| Output | Prompt text(s) — one analyst prompt + one per worker |
| Success Condition | All required sections present (semantic contract, target logical tree, examples, DAP output format); total prompt within token budget |
| Failure Handling | Missing required intelligence or required analyst briefing sections is a hard failure (no synthetic fallback briefing generation). Token budget overflow triggers truncation with warning. |

### Token Budget Contract

Generous limits (2x observed maximums) to avoid premature truncation while bounding degenerate cases:

| Prompt Type | Observed Size | Budget Limit | Truncation Strategy |
|-------------|---------------|--------------|---------------------|
| Analyst briefing | ~3,500–5,000 tokens | 10,000 tokens | Truncate EXPLAIN plan (150-line cap already enforced), then trim least-relevant examples |
| Worker briefing | ~2,000–2,500 tokens | 6,000 tokens | Truncate example SQL bodies first, then trim strategy context |
| Sniper prompt | ~3,000–4,000 tokens | 8,000 tokens | Truncate per-worker result summaries, cap error messages at 60 chars (already enforced) |
| PG tuner prompt | ~1,000 tokens | 3,000 tokens | Truncate resource envelope details |

**Existing truncation points** (already implemented):
- EXPLAIN plans: 150-line cap (`analyst_briefing.py`)
- Filter expressions: 80-char cap
- Join conditions: 60-char cap
- Worker error messages in snipe: 60-char cap (`swarm_snipe.py`)
- Semantic contract: validated to 80–150 tokens (`briefing_checks.py`)

**Contract rules:**
1. If a prompt exceeds its budget, log a warning with the prompt type and actual size.
2. Truncate the largest variable-size section first (EXPLAIN plan > example SQL bodies > strategy text).
3. Never truncate: semantic contract, output format spec, DAP instructions, constraint warnings.
4. If truncation still exceeds budget by >50%, fail the prompt generation (degenerate input).

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
| CLI | `POST /api/sql/optimize` (API), `Pipeline.run_query()` (programmatic) |
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

**Purpose:** Verify semantic equivalence and measure performance improvement. Validation strength varies by engine; all results carry a confidence tag.

| Field | Detail |
|-------|--------|
| CLI | `POST /api/sql/validate` |
| Entry Point | `Validator.validate()` in `validate.py` |
| Input | Original SQL + candidate SQL + executor (DSN) |
| Output | `ValidationResult` (`status`, `speedup`, `speedup_type`, `error_category`, `validation_confidence`) |
| Success Condition | sqlglot gate passes; equivalence check passes (see per-engine rules below). Performance is measured and classified separately (WIN/IMPROVED/NEUTRAL/REGRESSION). |
| Failure Handling | Returns structured `ValidationResult` with `ValidationStatus.ERROR` or `.FAIL`, `errors` list, and `error_category`. |

### Equivalence Verification by Engine

| Check | DuckDB | PostgreSQL |
|-------|--------|------------|
| Row count match | Yes | Yes |
| Full result-set MD5 checksum | Yes | **No** (known gap — see compensating controls) |
| Cost-rank pre-screening | Yes (`cost_rank_candidates()`) | **No** |
| `validation_confidence` | `high` | `row_count_only` |

**Contract:** Row counts alone are not sufficient for equivalence claims. Specific risks:
- Queries returning 0 rows pass trivially on row count (both return 0) but may have completely different semantics.
- Aggregation queries can return the correct number of rows with wrong values.
- JOIN reorderings can produce duplicate/missing rows that happen to sum to the same count.

### PostgreSQL Compensating Controls

Since PG lacks checksum verification, the following compensating controls apply:

1. **PG results carry lower confidence.** All PG `ValidationResult` objects must set `validation_confidence: "row_count_only"`. Downstream consumers (leaderboard, paper claims) must distinguish this from `"high"` confidence.
2. **Post-hoc DuckDB SF100 verification.** For PG winners intended for publication or SOTA claims, re-run the original and optimized SQL on DuckDB SF100 with full checksum verification. This can be done after the fact and is the definitive equivalence check.
3. **Zero-row query flag.** If both original and optimized return 0 rows, set `validation_confidence: "zero_row_unverified"`. These must be confirmed on DuckDB SF100 before any claims.
4. **No silent pass.** PG validation must never report `PASS` without logging that it used row-count-only checking.

### Speedup Classification

| Label | Threshold |
|-------|-----------|
| WIN | >= 1.10x |
| IMPROVED | >= 1.05x |
| NEUTRAL | >= 0.95x |
| REGRESSION | < 0.95x |

### Speedup Type Labeling

Every `ValidationResult` must include `speedup_type` to prevent misleading ratios:

| `speedup_type` | Meaning | When |
|----------------|---------|------|
| `measured` | Both original and optimized ran to completion; speedup is a real ratio of measured times | Default — both queries complete within timeout |
| `vs_timeout_ceiling` | Original query timed out; baseline is the timeout ceiling (e.g., 300s). Speedup ratio is inflated and not comparable to measured ratios. | Original hits timeout; optimized completes |
| `both_timeout` | Both original and optimized timed out | Neither completed — speedup is meaningless (set to 1.0) |

**Contract rules:**
1. `vs_timeout_ceiling` results must **never** be mixed with `measured` results in aggregate statistics (mean speedup, win rate, etc.) without explicit separation.
2. Leaderboard displays must show `speedup_type` alongside the ratio. A "4428x" `vs_timeout_ceiling` win is real but categorically different from a "3.93x" `measured` win.
3. Paper claims must report `vs_timeout_ceiling` results in a separate table or with explicit annotation.

### Timing Methods

| Method | Protocol | When |
|--------|----------|------|
| 3-run | Warmup, measure, measure — average last 2 | Default (DuckDB, PG) |
| 5-run trimmed mean | 5 runs, remove min/max, average middle 3 | Research validation |
| 4x triage (1-2-1-2) | Warmup orig, warmup opt, measure orig, measure opt | Mid-run screening, interleaved for drift control |

**Never** use single-run timing comparisons.

**Key Modules:**

| Module | Responsibility |
|--------|----------------|
| `validate.py` | `Validator` — orchestrates validation. `benchmark_baseline()` → `OriginalBaseline`. `validate_against_baseline()` for batch mode. `cost_rank_candidates()` for cost-based pre-screening (DuckDB only). `categorize_error()` for learning. |
| `validation/sql_validator.py` | `SQLValidator` — DuckDB validation engine: syntax check, benchmarking, equivalence. |
| `validation/benchmarker.py` | `QueryBenchmarker` — timing harness (warmup + measurement pattern). |
| `validation/equivalence_checker.py` | `EquivalenceChecker` — row count + MD5 checksum comparison (DuckDB). `ChecksumResult`, `RowCountResult`, `ValueComparisonResult`. |
| `validation/schemas.py` | `ValidationStatus` (PASS/FAIL/WARN/ERROR) for the qt_sql validation layer. |

**Key Data Structures:**

| Struct | Location | Key Fields |
|--------|----------|------------|
| `ValidationResult` | `schemas.py` | `worker_id`, `status: ValidationStatus`, `speedup`, `speedup_type`, `error`, `errors`, `error_category`, `validation_confidence` |
| `OriginalBaseline` | `validate.py` | `measured_time_ms`, `row_count`, `rows`, `checksum`, `is_timeout: bool` |

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

## API Contract

The FastAPI backend (`api/main.py`) serves the web UI and programmatic consumers. Version 2.0.0.

### `POST /api/sql/optimize`

Run the full optimization pipeline on a query.

**Request:**

```json
{
  "sql": "SELECT ... (required)",
  "dsn": "duckdb:///path.db | postgres://user:pass@host:port/db (required)",
  "mode": "swarm | expert | oneshot (optional, default: expert)",
  "query_id": "string (optional, auto-generated if omitted)",
  "max_iterations": 3,
  "target_speedup": 1.10
}
```

**Response (success):**

```json
{
  "status": "WIN | IMPROVED | NEUTRAL | REGRESSION | ERROR",
  "speedup": 1.45,
  "speedup_type": "measured | vs_timeout_ceiling | both_timeout",
  "validation_confidence": "high | row_count_only | zero_row_unverified",
  "optimized_sql": "SELECT ...",
  "original_sql": "SELECT ...",
  "transforms": ["decorrelate", "date_cte_isolate"],
  "workers": [ { "worker_id": 1, "strategy": "...", "speedup": 1.45, "status": "WIN", "transforms": [...] } ],
  "query_id": "q88",
  "n_iterations": 1,
  "n_api_calls": 5
}
```

**Response (error):**

```json
{
  "status": "ERROR",
  "error": "Intelligence gate failure: missing global_knowledge",
  "query_id": "q88"
}
```

**Contract:** The optimize endpoint must propagate `speedup_type` and `validation_confidence` from the pipeline's `SessionResult` / `ValidationResult`. These fields are defined in the response model but require the pipeline to populate them (see Phase 6 contract for field semantics).

### `POST /api/sql/validate`

Validate that optimized SQL is equivalent to original. Compares row counts, checksums, and values. Measures timing with warmup + measurement pattern.

**Request:**

```json
{
  "original_sql": "SELECT ... (required)",
  "optimized_sql": "SELECT ... (required)",
  "mode": "sample | full (default: sample)",
  "schema_sql": "CREATE TABLE ... (optional, for in-memory validation)",
  "session_id": "string (optional, existing DuckDB session)",
  "limit_strategy": "add_order | remove_limit (default: add_order)"
}
```

**Response:**

```json
{
  "status": "pass | fail | warn | error",
  "mode": "sample | full",
  "row_counts": { "original": 100, "optimized": 100 },
  "row_counts_match": true,
  "timing": { "original_ms": 45.2, "optimized_ms": 31.1 },
  "speedup": 1.45,
  "cost": { "original": 120.0, "optimized": 85.0 },
  "cost_reduction_pct": 29.17,
  "values_match": true,
  "checksum_match": true,
  "value_differences": [],
  "limit_detected": false,
  "errors": [],
  "warnings": []
}
```

### Database Session Routes

| Route | Method | Purpose |
|-------|--------|---------|
| `/api/database/connect/duckdb` | POST | Upload fixture file (SQL/CSV/Parquet) |
| `/api/database/connect/duckdb/quick` | POST | Connect via server-side path |
| `/api/database/status/{session_id}` | GET | Connection status |
| `/api/database/disconnect/{session_id}` | DELETE | Disconnect and clean up |
| `/api/database/execute/{session_id}` | POST | Execute SQL, return rows |
| `/api/database/explain/{session_id}` | POST | EXPLAIN plan with analysis |
| `/api/database/schema/{session_id}` | GET | Table + column schema |

### `GET /health`

Returns `{ "status": "ok", "version": "2.0.0", "llm_configured": true, "llm_provider": "deepseek" }`.

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
| `prompter.py` | 2, 3 | Load engine profiles, constraints; prompt utilities |
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
| `schemas.py` | all | Shared data structures (see Core Data Contract) |
| `pipeline.py` | all | Pipeline orchestrator (all phases) |
| `sessions/swarm_session.py` | all | Swarm session orchestrator |
| `sessions/expert_session.py` | all | Expert session orchestrator |
| `sessions/oneshot_session.py` | all | Oneshot session orchestrator |
| `execution/factory.py` | 1, 6 | Database connector factory |
| `execution/duckdb_executor.py` | 1, 6 | DuckDB executor |
| `execution/postgres_executor.py` | 1, 6 | PostgreSQL executor |

---

## Known Gaps

1. **PG equivalence checking** — PostgreSQL path checks row counts only; full result-set checksum comparison not yet implemented. DuckDB path has full MD5 checksum verification. **Compensating control:** PG results tagged `validation_confidence: "row_count_only"`; post-hoc DuckDB SF100 verification required for publication claims.

2. **Formal SQL equivalence** — No formal prover handles CTEs + window functions (all TPC-DS queries use both). QED (VLDB 2024) and VeriEQL (OOPSLA 2024) both lack CTE support. We verify via result-set checksums.

3. **Cost-rank pre-screening** — `cost_rank_candidates()` uses EXPLAIN cost estimates for DuckDB only. PG pre-screening is not implemented. **Impact:** PG runs validate all candidates (slower but more thorough).

4. **Bootstrap override risk** — `QT_ALLOW_INTELLIGENCE_BOOTSTRAP=1` intentionally bypasses intelligence hard gates for bootstrap/debug runs. This must stay off for performance-critical or SOTA claims.

5. **Timeout baseline handling** — When the original query times out (PG), baseline is set to the timeout ceiling. **Compensating control:** All such results labeled `speedup_type: "vs_timeout_ceiling"` and must be reported separately from measured speedups. See Phase 6 speedup type rules.

6. **Zero-row queries** — Queries returning 0 rows pass row-count validation trivially. **Compensating control:** Tagged `validation_confidence: "zero_row_unverified"` and must be confirmed on DuckDB SF100 post-hoc.
