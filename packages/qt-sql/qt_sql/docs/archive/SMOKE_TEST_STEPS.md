# ADO Swarm Pipeline — Smoke Test Step Reference

Each step is numbered, saved to a directory (`01_config/`, `02_query/`, ...),
and validates a set of structural contracts before the next step begins.

## Quick Reference

| Step | Name | Type | Input | Output | Finding |
|-----:|------|------|-------|--------|---------|
| 01 | Config | deterministic | `config.json` | `BenchmarkConfig` | F5 |
| 02 | Query | deterministic | `query_id` | SQL string | — |
| 03 | DAG | deterministic+DB | SQL | `(dag, costs, explain)` | — |
| 04 | FAISS | deterministic | SQL | gold examples (≤12) | — |
| 05 | Regressions | deterministic | SQL | anti-pattern examples | — |
| 06 | Fan-out Prompt | deterministic | dag+costs+examples | prompt text | — |
| 07 | Analyst Call | **LLM** | prompt | analyst response | F1 |
| 08 | Parse Assignments | deterministic | analyst response | 4 × WorkerAssignment | F2 |
| 09 | Worker Prompts | deterministic | assignments+dag+costs | 4 × prompt text | F6 |
| 10 | Generate | **LLM** (4×parallel) | 4 prompts | 4 × Candidate | F1 |
| 11 | Syntax Check | deterministic | 4 optimized SQLs | 4 × (sql, valid) | — |
| 12 | Baseline | **DB** | original SQL | `OriginalBaseline` | — |
| 13 | Validate | **DB** | baseline + 4 SQLs | 4 × `ValidationResult` | F4 |
| 14 | Learning | deterministic | validation results | 4 × `LearningRecord` | F4 |
| 15 | Session Save | deterministic | all iteration data | session directory | F1,F2,F4,F7 |

---

## Step 01: Load Configuration

**Purpose**: Load `BenchmarkConfig` from `config.json` and validate settings.

**Input**: `packages/qt-sql/ado/benchmarks/duckdb_tpcds/config.json`

**Output**: `01_config/output.json`
```json
{
  "engine": "duckdb",
  "benchmark": "tpcds",
  "db_path_or_dsn": "/mnt/d/TPC-DS/tpcds_sf10.duckdb",
  "scale_factor": 10,
  "timeout_seconds": 300,
  "validation_method": "3-run"
}
```

**Contract**:
| Check | Rule |
|-------|------|
| `engine_duckdb` | `engine == "duckdb"` |
| `db_exists` | Database file exists on disk |
| `timeout_positive` | `timeout_seconds > 0` |
| `validation_method_valid` | `∈ {"3-run", "5-run"}` |
| `scale_factor_10` | `scale_factor == 10` |
| `pipeline_has_logger` | **(F5)** `ado.pipeline.logger` is not `None` |

---

## Step 02: Load Query SQL

**Purpose**: Load query SQL from `queries/` directory and validate it parses.

**Input**: `query_id` (e.g. `query_42`)

**Output**: `02_query/output.sql` — raw SQL text

**Contract**:
| Check | Rule |
|-------|------|
| `sql_non_empty` | SQL string is not empty |
| `sql_has_select` | Contains `SELECT` keyword |
| `sql_parses` | `sqlglot.parse_one(sql, dialect="duckdb")` succeeds |

---

## Step 03: Parse DAG + EXPLAIN

**Purpose**: Parse SQL into a directed acyclic graph (DAG) with cost annotations
from cached EXPLAIN ANALYZE plans.

**Input**: `03_dag/input.sql` — original SQL

**Output**: `03_dag/output.json`
```json
{
  "n_nodes": 1,
  "n_edges": 0,
  "node_ids": ["main_query"],
  "cost_keys": ["main_query"],
  "has_explain": true
}
```

**Contract**:
| Check | Rule |
|-------|------|
| `dag_has_nodes` | `len(dag.nodes) > 0` |
| `costs_non_empty` | Costs dict / object is populated |

---

## Step 04: FAISS Example Retrieval

**Purpose**: Fingerprint the SQL, vectorize the AST, and retrieve structurally
similar gold examples from the FAISS index (95 vectors).

**Input**: SQL string + engine ("duckdb")

**Output**: `04_faiss/output.json` — list of matched examples with IDs + catalog

**Contract**:
| Check | Rule |
|-------|------|
| `faiss_returns_examples` | At least 1 example returned |
| `faiss_max_12` | At most 12 examples (k=12) |
| `faiss_ids_unique` | No duplicate example IDs |
| `all_examples_have_id` | Every example dict has an `"id"` key |
| `catalog_non_empty` | Full catalog has entries |

---

## Step 05: Regression Warnings

**Purpose**: Retrieve structurally similar queries that REGRESSED when rewritten,
to serve as anti-patterns in the prompt.

**Input**: SQL string + engine ("duckdb")

**Output**: `05_regressions/output.json`

**Contract**:
| Check | Rule |
|-------|------|
| `regressions_is_list` | Result is a list (may be empty) |
| `no_gold_regression_overlap` | No ID appears in both gold and regression sets |

---

## Step 06: Build Fan-out Prompt

**Purpose**: Assemble the analyst fan-out prompt: full query SQL, DAG topology,
bottleneck operators, top 12 FAISS examples, and full example catalog.

**Input**: query_id, SQL, dag, costs, faiss_examples, all_available

**Output**: `06_fan_out_prompt/output.txt` — full prompt text

**Contract**:
| Check | Rule |
|-------|------|
| `prompt_non_empty` | Prompt has content |
| `prompt_substantial` | `len(prompt) > 1000` chars |
| `prompt_contains_sql` | Contains the original query's `SELECT` |
| `prompt_mentions_workers` | References `WORKER` or `SPECIALIST` |

---

## Step 07: Analyst LLM Call

**Purpose**: Send the fan-out prompt to the LLM. The analyst distributes
12 FAISS examples across 4 workers and assigns each a unique strategy.

**Input**: `07_analyst_call/input_prompt.txt`

**Output**: `07_analyst_call/output_response.txt` + `meta.json`

**Contract**:
| Check | Rule |
|-------|------|
| `response_non_empty` | Response has content |
| `response_substantial` | `len > 100` chars |
| `response_has_worker_refs` | Contains "WORKER" in response |
| `analyst_prompt_saved` | **(F1)** Prompt file persisted and non-empty |
| `analyst_response_saved` | **(F1)** Response file persisted and non-empty |

---

## Step 08: Parse Worker Assignments

**Purpose**: Parse the analyst's free-text response into 4 structured
`WorkerAssignment` objects, each with `worker_id`, `strategy`, `examples`, `hint`.

**Input**: Analyst response text

**Output**: `08_parse_assignments/output.json`
```json
[
  {"worker_id": 1, "strategy": "date_cte_isolation", "examples": [...], "hint": "..."},
  {"worker_id": 2, "strategy": "pushdown_early_filter", "examples": [...], "hint": "..."},
  ...
]
```

**Contract**:
| Check | Rule |
|-------|------|
| `exactly_4_assignments` | **(F2)** Exactly 4 assignments returned |
| `worker_ids_unique` | **(F2)** All `worker_id` values are distinct |
| `worker_ids_are_1_to_4` | **(F2)** IDs are exactly `{1, 2, 3, 4}` |
| `w{N}_has_strategy` | Each worker has a non-empty `strategy` string |
| `w{N}_has_hint` | Each worker has a non-empty `hint` string |

---

## Step 09: Build Worker Prompts

**Purpose**: For each of the 4 assignments, build a specialized worker prompt:
strategy header (via shared `build_worker_strategy_header()`) + base DAG prompt
with assigned examples + regression warnings + global learnings.

**Input**: Assignments + dag + costs + examples + learnings

**Output**: `09_worker_prompts/worker_{N}_prompt.txt` (4 files)

**Contract**:
| Check | Rule |
|-------|------|
| `four_prompts_built` | 4 prompts created |
| `w{N}_prompt_substantial` | Each `> 1000` chars |
| `w{N}_contains_sql` | Each contains `SELECT` |
| `w{N}_header_has_strategy_title` | **(F6)** `"## Optimization Strategy: {strategy}"` present |
| `w{N}_header_has_approach` | **(F6)** `"**Your approach**:"` present |
| `w{N}_header_has_focus` | **(F6)** `"**Focus**:"` present |

---

## Step 10: Worker LLM Generation

**Purpose**: Fire 4 parallel LLM calls. Each worker receives its specialized
prompt and returns a JSON response containing `rewrite_sets`. The response is
parsed by `SQLRewriter.apply_response()` to produce optimized SQL, and
`extract_transforms_from_response()` identifies which transforms were applied.

**Input**: 4 worker prompts

**Output**: `10_generate/worker_{N}/` directories, each containing:
- `prompt.txt` — full prompt sent to LLM
- `response.txt` — raw LLM response
- `optimized.sql` — rewritten SQL
- `candidate.json` — metadata (transforms, error, sha256)

**Contract**:
| Check | Rule |
|-------|------|
| `four_candidates` | 4 candidates produced |
| `all_have_response` | All 4 have non-empty LLM responses |
| `at_least_one_changed` | ≥1 candidate differs from original SQL |
| `w{N}_prompt_persisted` | **(F1)** Prompt file saved and non-empty |
| `w{N}_response_persisted` | **(F1)** Response file saved and non-empty |

---

## Step 11: Syntax Validation

**Purpose**: Parse each candidate's optimized SQL with `sqlglot`. If parsing
fails, revert to the original SQL (safety net).

**Input**: 4 optimized SQL strings

**Output**: `11_syntax/output.json` — per-worker `{valid, sql_sha256, reverted_to_original}`

**Contract**:
| Check | Rule |
|-------|------|
| `at_least_one_valid` | ≥1 candidate has valid syntax |
| `w{N}_invalid_reverted` | Any invalid SQL is reverted to original |

---

## Step 12: Benchmark Original (Baseline)

**Purpose**: Time the original SQL using the 3-run warmup pattern:
run 3 times, discard 1st (warmup), average last 2. Also captures row count
and checksum for semantic comparison.

**Input**: `12_baseline/input.sql`

**Output**: `12_baseline/output.json`
```json
{
  "measured_time_ms": 42.3,
  "row_count": 100,
  "has_checksum": true,
  "has_rows": true
}
```

**Contract**:
| Check | Rule |
|-------|------|
| `time_positive` | `measured_time_ms > 0` |
| `rows_positive` | `row_count > 0` |
| `checksum_present` | Checksum is not `None` (DuckDB) |

---

## Step 13: Validate Candidates

**Purpose**: For each candidate, benchmark the optimized SQL against the cached
baseline. Checks: (1) row count match, (2) value/checksum match, (3) speedup.

**Input**: `OriginalBaseline` + 4 candidate SQLs

**Output**: `13_validate/worker_{N}.json` + `summary.json`

**Contract**:
| Check | Rule |
|-------|------|
| `all_validated` | All 4 candidates have a `ValidationResult` |
| `w{N}_has_status` | Status ∈ `{"pass", "fail", "error"}` |
| `w{N}_speedup_is_float` | Speedup is numeric |
| `w{N}_errors_is_list` | **(F4)** `errors` field is a `list`, not a string |
| `w{N}_error_category_set` | **(F4)** If errors non-empty, `error_category` is set |
| `at_least_one_passes` | ≥1 candidate has status `pass` |

---

## Step 14: Learning Records

**Purpose**: Create a `LearningRecord` for each worker capturing examples
recommended, transforms used, speedup, error messages, and error category.
Records are saved to the learning journal for future analytics.

**Input**: Validation results + candidate metadata

**Output**: `14_learning/records.json` — list of 4 `LearningRecord` dicts

**Contract**:
| Check | Rule |
|-------|------|
| `four_records_created` | 4 learning records created |
| `lr_w{N}_has_timestamp` | Each record has a timestamp |
| `lr_w{N}_has_query_id` | Each record has the correct `query_id` |
| `lr_w{N}_transforms_is_list` | `transforms_used` is a list |
| `lr_w{N}_error_msgs_is_list` | **(F4)** `error_messages` is a list |

---

## Step 15: Session Save + Audit Trail

**Purpose**: Save all session artifacts in the layout `SwarmSession.save_session()`
uses. Then audit that every expected file exists and is non-empty.

**Output directory layout**:
```
15_session_save/session_artifacts/
├── session.json
└── iteration_00_fan_out/
    ├── analyst_prompt.txt
    ├── analyst_response.txt
    ├── worker_01/
    │   ├── result.json
    │   ├── optimized.sql
    │   ├── prompt.txt
    │   └── response.txt
    ├── worker_02/ ...
    ├── worker_03/ ...
    └── worker_04/ ...
```

**Contract**:
| Check | Rule |
|-------|------|
| `session_json_exists` | `session.json` exists |
| `iteration_dir_exists` | `iteration_00_fan_out/` exists |
| `analyst_prompt_in_session` | **(F1)** Analyst prompt saved and non-empty |
| `analyst_response_in_session` | **(F1)** Analyst response saved and non-empty |
| `worker_{NN}_dir_exists` | **(F2)** All 4 worker directories exist |
| `worker_{NN}_result_json_saved` | **(F1)** `result.json` saved |
| `worker_{NN}_optimized_sql_saved` | **(F1)** `optimized.sql` saved |
| `worker_{NN}_prompt_txt_saved` | **(F1)** `prompt.txt` saved |
| `worker_{NN}_response_txt_saved` | **(F1)** `response.txt` saved |
| `worker_{NN}_result_has_status` | **(F4)** `result.json` contains `status` |
| `worker_{NN}_result_has_speedup` | **(F4)** `result.json` contains `speedup` |
| `api_call_count_correct` | **(F7)** Fan-out = 1 analyst + 4 workers = 5 calls |

---

## Code Review Finding Traceability

| Finding | Severity | Description | Steps That Test It |
|---------|----------|-------------|--------------------|
| F1 | High | Full prompt+response persistence at every iteration | 07, 10, 15 |
| F2 | High | All 4 worker IDs unique (no overwrites) | 08, 15 |
| F4 | Medium | Structured validation diagnostics (list, not string) | 13, 14, 15 |
| F5 | Medium | Structured logging (logger, not bare print) | 01 |
| F6 | Low | DRY: shared `build_worker_strategy_header()` | 09 |
| F7 | Low | API call count matches implementation | 15 |

## Running

```bash
cd /mnt/c/Users/jakc9/Documents/QueryTorque_V8
PYTHONPATH=packages/qt-shared:packages/qt-sql:. python3 -m ado.smoke_test
PYTHONPATH=packages/qt-shared:packages/qt-sql:. python3 -m ado.smoke_test --query query_67
```

Exit code: `0` = all checks pass, `1` = any check fails.

Output: `packages/qt-sql/ado/benchmarks/duckdb_tpcds/smoke_test_TIMESTAMP/`
