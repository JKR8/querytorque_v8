# ADO Final Clean State Design

Status: Active — implementing on `feature/ado-blackboard` worktree (`/mnt/d/qt-blackboard/`)
Date: 2026-02-08
Branch: `feature/ado-blackboard` (clean break, no legacy)

## 1) Principle

This worktree is the clean room. No backward compatibility. No fallback paths.
No `state_N`, no `queries/` outside seed, no `semantic_intents.json` monolith,
no try/except import shims for `qt_sql.*`. If something only works with the
old layout, it gets rewritten or deleted.

ADO is its own product. It depends on `ado.*` and `qt_shared.*` public APIs.
Nothing else at runtime.

## 2) Architecture

```text
                         +----------------------+
                         |      qt-shared       |
                         |----------------------|
                         | llm / config / obs   |
                         | auth / session APIs  |
                         +----------+-----------+
                                    ^
                                    |
              +---------------------+---------------------+
              |                                           |
   +----------+-----------+                   +-----------+----------+
   |   ADO Product Core   |                   |   DAX Product Core   |
   |----------------------|                   |----------------------|
   | ado.pipeline         |                   | (future)             |
   | ado.sessions         |                   |                      |
   | ado.validate         |                   |                      |
   | ado.prompting        |                   |                      |
   | ado.seed/run/board   |                   |                      |
   | ado.dag              |                   |                      |
   | ado.execution        |                   |                      |
   +----------+-----------+                   +-----------+----------+
              |
              v
   +----------------------+
   | SQL Engine Adapters  |
   | duckdb / postgres    |
   +----------------------+
```

Rules:
- ADO must NOT import `qt_sql.*` at runtime. No try/except fallbacks. Hard rule.
- `qt_shared.*` public modules only: `llm`, `config`, `observability`.
- Third-party libraries (sqlglot, faiss, pyyaml, etc.) are fine.

## 3) Dependency Rules

### 3.1 ADO allowed imports

| Source | Examples |
|--------|----------|
| `ado.*` | All ADO modules |
| `qt_shared.llm` | LLM provider clients |
| `qt_shared.config` | Settings, secrets |
| `qt_shared.observability` | Logging, telemetry |
| Third-party | sqlglot, faiss, numpy, pyyaml, duckdb, psycopg2 |

### 3.2 ADO forbidden imports

| Source | Reason |
|--------|--------|
| `qt_sql.*` | Product boundary. Fork what ADO needs into `ado.*`. |
| `qt_dax.*` | Product boundary. |
| Private `qt_shared` internals | Use public API only. |

### 3.3 What to fork from qt_sql into ado

Three modules currently cross the boundary. Fork the minimum needed:

| qt_sql source | Fork to | What ADO actually uses |
|---------------|---------|----------------------|
| `qt_sql.optimization.dag_v2` | `ado/dag.py` | `DagBuilder.build()`, `CostAnalyzer` — parse SQL into DAG nodes with EXPLAIN costs |
| `qt_sql.optimization.plan_analyzer` | `ado/dag.py` | `analyze_plan_for_optimization()` — extract plan context from EXPLAIN JSON |
| `qt_sql.execution.factory` | `ado/execution.py` | `create_executor_from_dsn()` — create DuckDB/Postgres executor |
| `qt_sql.execution.database_utils` | `ado/execution.py` | `run_explain_analyze()` — run EXPLAIN and cache |
| `qt_sql.validation.sql_validator` | `ado/validate.py` | `SQLValidator` — already mostly self-contained, just remove the import |
| `qt_sql.validation.schemas` | `ado/schemas.py` | `ValidationStatus`, `ValidationResult`, `ValidationMode` — copy the enums |

After forking: delete every `from qt_sql` and `import qt_sql` line in `ado/`.
No try/except wrappers. No `PassthroughValidator` fallback. ADO owns its own
validation, execution, and DAG parsing.

## 4) Data Layout

### 4.1 Seed (read-only initialization)

```text
benchmarks/<benchmark>/seed/
  manifest.yaml              # Checklist + inventory
  queries/*.sql              # All SQL files
  explains/*.json            # EXPLAIN plans
  intents/*.json             # Per-query semantic intents
  catalog_rules/*.json       # Unverified patterns
  config.json                # Engine, DSN, validation params
```

No other source of queries, explains, or intents. Seed is the single source.

### 4.2 Named runs (write)

```text
benchmarks/<benchmark>/runs/<run_name>/
  run.yaml                   # Config: mode, workers, query_filter
  results/<query_id>/
    worker_<id>/
      prompt.txt
      response.txt
      optimized.sql
      validation.json
    best.json
  blackboard/
    raw/<query_id>/worker_<id>.json
    collated.json
  leaderboard.json
  summary.json
  logs/run.log
```

No `state_0/`, `state_1/`, `swarm_sessions/`, or mode-specific paths.

### 4.3 Knowledge (accumulated)

```text
benchmarks/<benchmark>/knowledge/<dataset>.json
```

Built from blackboard collation. Loaded at pipeline init. Served in prompts.

## 5) Provenance

Every `best.json` and leaderboard entry includes:

```json
{
  "query_id": "query_88",
  "run_name": "discovery_20260208",
  "mode": "swarm",
  "iteration": 1,
  "worker_id": 3,
  "strategy": "W4_single_pass",
  "source": "run:discovery_20260208:mode:swarm:iter:1:worker:3",
  "status": "WIN",
  "speedup": 5.25,
  "transforms": ["or_to_union"]
}
```

`best_worker_id` must reflect the actual winning worker.
Current bug: `run_named_session()` hardcodes `best_worker_id=0`. Fix required.

## 6) Blackboard and Knowledge Lifecycle

```text
Worker validates → raw entry written → auto-collate at run end
                                              ↓
                                     collated.json (reviewed=false)
                                              ↓
                                     manual cleanup (reviewed=true)
                                              ↓
                                     merge → knowledge/<dataset>.json
                                              ↓
                                     next run loads → principles in prompt
```

Manual workflow documented in `ado/docs/blackboard_workflow.yaml`.

## 7) Observability

### 7.1 Console
Stage banners for human scanability in swarm and expert flows.

### 7.2 Structured artifacts
Every worker attempt persists: prompt, response, SQL, validation, blackboard entry.

### 7.3 Per-run log file
`runs/<run_name>/logs/run.log` with structured lines:
```
2026-02-08T14:30:00 query=query_88 iter=1 worker=3 status=WIN speedup=5.25 elapsed_ms=1234
```

## 8) Acceptance Gates

### 8.1 Import boundary
```bash
# Must return zero matches. Run from packages/qt-sql/.
grep -rn "from qt_sql\|import qt_sql" ado/ --include="*.py" | grep -v "^Binary"
```
Zero tolerance. No matches = pass.

### 8.2 Functional
1. `pytest packages/qt-sql/tests/` — all pass
2. Smoke test: single-query swarm run produces:
   - `runs/<name>/results/<query_id>/best.json` with correct `worker_id`
   - `runs/<name>/blackboard/raw/` populated
   - `runs/<name>/blackboard/collated.json` exists
   - `runs/<name>/leaderboard.json` has `source` lineage
   - `runs/<name>/logs/run.log` exists

### 8.3 Isolation
ADO runs with restricted PYTHONPATH (no qt_sql on path). No import errors.

## 9) Implementation Order

### Done
- [x] Seed folder: SeedLoader, SeedBuilder, SeedValidator, manifest.yaml
- [x] Named runs: RunManager, run.yaml, results/, best.json
- [x] Blackboard: writer, reader, auto-collate, cleanup, merge-to-global
- [x] Knowledge serving: intent_matcher, principles in prompts
- [x] Schemas: SeedManifest, BlackboardEntry, RunConfig, GlobalKnowledge
- [x] Workflow docs: blackboard_workflow.yaml

### Remaining
1. **Fork qt_sql dependencies** — create `ado/dag.py` and `ado/execution.py`
2. **Delete all qt_sql imports** — no fallbacks, no try/except shims
3. **Fix provenance** — `best_worker_id` from actual winner, `source` lineage string
4. **Per-run log file** — file handler in `run_named_session()`
5. **Delete legacy paths** — remove `state_N` refs, `queries/` fallback, `semantic_intents.json` fallback
6. **Import lint** — add grep check (gate 8.1)
7. **Smoke test script** — `python3 -m ado.smoke_test <benchmark> <run_name>`
8. **Run all tests** — verify zero regressions

## 10) Non-Goals

1. DAX product core (future, separate effort).
2. Moving ADO out of `packages/qt-sql/` directory (cosmetic, do later).
3. Changing optimization strategies or prompt design.
4. Backward compatibility with `state_N` or old layout.

