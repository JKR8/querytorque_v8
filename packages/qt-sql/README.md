# qt-sql

SQL optimization engine for QueryTorque. LLM-powered query rewriting with verified speedups.

Detailed engineering contract: `qt_sql/docs/PRODUCT_CONTRACT.md`

## System Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│  7-PHASE PIPELINE (pipeline.py)                                 │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│  1. Context:    SQL → logical tree + EXPLAIN + context_confidence│
│  2. Knowledge:  Tag-match examples + engine profiles + intel    │
│  3. Prompts:    Analyst briefing + per-worker prompts (DAP)     │
│  4. Inference:  Parallel LLM calls via CandidateGenerator       │
│  5. Parse:      DAP response → SQL + transforms (sqlglot gates) │
│  6. Validate:   Equivalence check + 3-run/5-run timing          │
│  7. Learn:      Artifacts + leaderboard + learning records      │
│                                                                 │
│  Sessions: OneshotSession | SwarmSession                        │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────────┐
│  API LAYER (api/main.py — FastAPI v2.0.0)                       │
├─────────────────────────────────────────────────────────────────┤
│  POST /api/sql/optimize    — Pipeline-backed optimization       │
│  POST /api/sql/validate    — Equivalence + timing validation    │
│  POST/GET/DELETE /api/database/* — DuckDB session management    │
│  GET  /health              — Health check                       │
│  Web UI consumes these endpoints                                │
└─────────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────────┐
│  EXECUTION                                                      │
├─────────────────────────────────────────────────────────────────┤
│  DuckDB Executor  │  PostgreSQL Executor  │  (Snowflake stub)   │
│  3-run / 5-run / 4x triage benchmarking                         │
│  Row count + MD5 checksum equivalence (DuckDB)                  │
│  Row count only (PG) — post-hoc SF100 for publication claims    │
└─────────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────────┐
│  KNOWLEDGE + INTELLIGENCE FLYWHEEL                              │
├─────────────────────────────────────────────────────────────────┤
│  Engine profiles (DuckDB/PG optimizer gaps + strengths)         │
│  Gold examples (25 verified: 19 DuckDB + 6 PG, up to 5.25x)   │
│  Tag-based similarity matching (108+ examples, no FAISS)        │
│  Regression warnings (anti-pattern avoidance)                   │
│  PG system introspection (SET LOCAL tuning, pg_hint_plan)       │
│                                                                 │
│  Intelligence flywheel:                                         │
│    WIN results → candidate gold examples → reviewed → catalog   │
│    Each run generates learning records for analytics            │
│                                                                 │
│  Scanner blackboard: PG planner flag exploration → findings     │
│  Optimization blackboard: Worker outcomes → principles          │
└─────────────────────────────────────────────────────────────────┘
```

## Optimization Modes

### Oneshot
1 LLM call — analyst analyzes AND produces optimized SQL in a single prompt. Cheapest mode.

```python
result = p.run_optimization_session("query_1", sql, mode=OptimizationMode.ONESHOT)
```

### Swarm
4-worker fan-out: analyst distributes diverse strategies across 4 workers. If none hit target, a snipe round synthesizes failures into a refined attempt.

```python
result = p.run_optimization_session("query_88", sql, mode=OptimizationMode.SWARM)
```

## Module Map

### Core Pipeline

| Module | Role |
|--------|------|
| `pipeline.py` | Top-level orchestrator — 7-phase pipeline from SQL to validated optimization |
| `dag.py` | SQL → logical tree parser (CTE topology, join contracts, column tracking) |
| `logic_tree.py` | DAP Logic Tree builder — structural representation for prompt output |
| `sql_rewriter.py` | DAP response parser — extracts SQL from Component Payload JSON, infers transforms |
| `schemas.py` | Core dataclasses: `OptimizationMode`, `WorkerResult`, `SessionResult`, `ValidationResult`, `PipelineResult` |
| `validate.py` | Timing + equivalence validation (3-run, 5-run, 4x triage protocols) |
| `generate.py` | LLM call orchestration — prompt → API → response |
| `prompter.py` | Query-level prompt builder — loads engine profiles, constraints, computes depths, builds prompt sections |

### Sessions

| Module | Role |
|--------|------|
| `sessions/base_session.py` | Abstract base with shared iteration logic |
| `sessions/oneshot_session.py` | Single-call analyst-as-worker mode |
| `sessions/swarm_session.py` | 4-worker fan-out + sniper synthesis |

### Prompts

| Module | Role |
|--------|------|
| `prompts/analyst_briefing.py` | Analyst prompt builder (swarm/oneshot modes) |
| `prompts/worker.py` | Worker prompt builder from analyst briefing sections |
| `prompts/swarm_fan_out.py` | Fan-out prompt — analyst distributes strategies to 4 workers |
| `prompts/swarm_snipe.py` | Sniper prompt — synthesizes worker failures into refined attempt |
| `prompts/swarm_common.py` | Shared utilities for swarm prompts |
| `prompts/swarm_parsers.py` | Parsers for analyst briefing output |
| `prompts/briefing_checks.py` | Briefing section validation checklists |
| `prompts/dag_helpers.py` | Logical-tree prompt helpers |
| `prompts/pg_tuner.py` | PostgreSQL-specific tuning prompt |
| `prompts/sql_rewrite_spec.md` | DAP spec reference |
| `prompts/samples/` | V0 Prompt Pack — 11 rendered prompt samples for review |

### Execution

| Module | Role |
|--------|------|
| `execution/factory.py` | `create_executor_from_dsn()` — auto-detects DuckDB/PG/Snowflake |
| `execution/duckdb_executor.py` | DuckDB execution + benchmarking |
| `execution/postgres_executor.py` | PostgreSQL execution + benchmarking + SET LOCAL support |
| `execution/duckdb_harness.py` | DuckDB test harness for batch runs |
| `execution/plan_parser.py` | DuckDB EXPLAIN plan parser |
| `execution/postgres_plan_parser.py` | PostgreSQL EXPLAIN ANALYZE plan parser |
| `execution/database_utils.py` | Shared DB utilities |
| `execution/base.py` | Abstract executor interface |

### Validation

| Module | Role |
|--------|------|
| `validation/benchmarker.py` | Timing benchmarks (3-run, 5-run, trimmed mean) |
| `validation/equivalence_checker.py` | Row count + MD5 checksum verification |
| `validation/query_normalizer.py` | SQL normalization for comparison |
| `validation/sql_validator.py` | sqlglot parse gate |
| `validation/schemas.py` | Validation-specific dataclasses |

### Knowledge Systems

| Module | Role |
|--------|------|
| `knowledge.py` | `TagRecommender` — tag-based example matching from similarity index |
| `knowledge/` | Static knowledge files (engine-specific JSON/YAML) |
| `examples/` | Gold examples — 25 verified transforms (19 DuckDB + 6 PG) with before/after SQL |
| `constraints/` | Engine profiles + correctness constraints |
| `models/` | `similarity_tags.json` + `similarity_metadata.json` — tag-based example index |
| `learnings/` | Learning YAML files |

### Scanner Knowledge Pipeline (PostgreSQL)

See [`scanner_knowledge/README.md`](qt_sql/scanner_knowledge/README.md) for full details.

| Module | Role |
|--------|------|
| `plan_scanner.py` | PG planner flag exploration — toggles 22 combos, collects EXPLAIN + timings |
| `plan_scanner_spec.yaml` | Plan scanner configuration |
| `plan_analyzer.py` | EXPLAIN plan analysis utilities |
| `scanner_knowledge/blackboard.py` | Layer 1: Raw observations → `scanner_blackboard.jsonl` |
| `scanner_knowledge/findings.py` | Layer 2: LLM-extracted claims → `scanner_findings.json` |
| `scanner_knowledge/schemas.py` | `ScannerObservation` (L1) + `ScannerFinding` (L2) dataclasses |
| `scanner_knowledge/build_all.py` | CLI entry — runs blackboard → findings pipeline |
| `scanner_knowledge/templates/` | Algorithm workflow, finding schema, prompt templates |
| `algorithms/` | Algorithm configs (e.g., `postgres_dsb_sf10_scanner.yaml`) |

### Optimization Blackboard

| Module | Role |
|--------|------|
| `build_blackboard.py` | Extracts learning from worker optimization outcomes (swarm or global) |
| | Outputs: `BlackboardEntry` per worker → `KnowledgePrinciple` + `KnowledgeAntiPattern` |

### Other

| Module | Role |
|--------|------|
| `tag_index.py` | Rebuilds tag-based example index (`python3 -m qt_sql.tag_index`) |
| `pg_tuning.py` | PG system introspection — `PGSystemProfile`, resource envelope, `PG_TUNABLE_PARAMS` whitelist |
| `learn.py` | Learning record system — captures speedup, transforms, errors per optimization attempt |
| `analyst.py` | Analyst utilities |
| `store.py` | Result storage |
| `session_logging.py` | Session-level logging |
| `script_parser.py` | ScriptParser — enterprise SQL multi-statement parsing |

## API

The FastAPI backend (`api/main.py`) serves the web UI and programmatic consumers.

```bash
# Start the API server
PYTHONPATH=packages/qt-shared:packages/qt-sql:. uvicorn api.main:app --port 8002

# Interactive docs at http://localhost:8002/api/docs
```

| Endpoint | Method | Purpose |
|----------|--------|---------|
| `/api/sql/optimize` | POST | Run 7-phase pipeline (sql + dsn + mode) |
| `/api/sql/validate` | POST | Validate candidate SQL equivalence + timing |
| `/api/database/connect/duckdb` | POST | Upload fixture file to create DuckDB session |
| `/api/database/connect/duckdb/quick` | POST | Connect via server-side path |
| `/api/database/status/{session_id}` | GET | Connection status check |
| `/api/database/disconnect/{session_id}` | DELETE | Disconnect and clean up session |
| `/api/database/execute/{session_id}` | POST | Execute SQL query |
| `/api/database/explain/{session_id}` | POST | Get EXPLAIN plan |
| `/api/database/schema/{session_id}` | GET | Get schema info |
| `/health` | GET | Health check |

## Knowledge Pipeline Commands

```bash
cd <repo-root>
PYTHONPATH=packages/qt-shared:packages/qt-sql:.

# Scanner knowledge (PG planner exploration → findings)
python3 -m qt_sql.scanner_knowledge.build_all benchmarks/postgres_dsb_76

# Optimization blackboard (worker outcomes → principles)
python3 -m qt_sql.build_blackboard <swarm_batch_dir>
python3 -m qt_sql.build_blackboard --global

# Rebuild tag-based example index
python3 -m qt_sql.tag_index

# Regenerate V0 prompt samples
python3 -m qt_sql.prompts.samples.generate_sample
```

## Usage

```python
from qt_sql.pipeline import Pipeline
from qt_sql.schemas import OptimizationMode

p = Pipeline(dsn="duckdb:///benchmarks/duckdb_tpcds/tpcds_sf10.duckdb")

result = p.run_optimization_session(
    "query_88", sql,
    mode=OptimizationMode.SWARM,
    max_iterations=3,
    target_speedup=2.0,
)
print(f"{result.status}: {result.best_speedup:.2f}x")
```
