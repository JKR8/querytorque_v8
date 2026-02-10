# qt-sql

SQL optimization and analysis engine for QueryTorque.

Current docs: `qt_sql/docs/README.md`  
Legacy docs archive: `qt_sql/docs/archive/`

## Optimization Modes

Three modes for LLM-powered query optimization:

### Oneshot
1 LLM call per iteration — the analyst analyzes the query AND produces optimized SQL in a single prompt. Cheapest mode. Optional retry with failure context on subsequent iterations.

```python
result = p.run_optimization_session("query_1", sql, mode=OptimizationMode.ONESHOT)
```

### Expert (default)
Iterative with analyst failure analysis. Each iteration: analyst briefing → 1 worker rewrite → validate → failure analysis if below target. History accumulates across iterations so the LLM learns from prior failures.

```python
result = p.run_optimization_session("query_1", sql, mode=OptimizationMode.EXPERT)
```

### Swarm
Multi-worker fan-out: analyst distributes 4 diverse strategies across 4 workers. If none hit target, a snipe round synthesizes failures into a refined attempt. Best for stubborn queries.

```python
result = p.run_optimization_session("query_88", sql, mode=OptimizationMode.SWARM)
```

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│  5-PHASE PIPELINE                                               │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│  1. Parse:     SQL → DAG (CTE topology, join contracts)        │
│  2. Retrieve:  Tag-based example matching (engine-specific)     │
│  3. Rewrite:   V2 analyst briefing → worker prompt → LLM       │
│  4. Syntax:    sqlglot parse gate (deterministic)              │
│  5. Validate:  Timing + row count + checksum (3-run or 5-run)  │
│                                                                 │
│  Sessions: OneshotSession | ExpertSession | SwarmSession       │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────────┐
│  EXECUTION                                                      │
├─────────────────────────────────────────────────────────────────┤
│  DuckDB Executor  │  PostgreSQL Executor  │  (Snowflake stub)  │
│  3-run / 5-run trimmed mean benchmarking                       │
│  Row count + MD5 checksum equivalence checking                 │
└─────────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────────┐
│  KNOWLEDGE (two blackboard systems)                             │
├─────────────────────────────────────────────────────────────────┤
│  Engine profiles (DuckDB/PG optimizer gaps + strengths)        │
│  Gold examples (19 verified transforms, up to 5.25x)           │
│  Tag-based similarity matching (108 examples, no FAISS)        │
│  Regression warnings (anti-pattern avoidance)                  │
│  PG system introspection (SET LOCAL tuning, pg_hint_plan)      │
│                                                                 │
│  Scanner blackboard: PG planner flag exploration → findings    │
│  Optimization blackboard: Worker outcomes → principles/anti-patterns │
└─────────────────────────────────────────────────────────────────┘
```

## Module Map

### Core Pipeline

| Module | Role |
|--------|------|
| `pipeline.py` | Top-level orchestrator — parse → retrieve → rewrite → validate |
| `dag.py` | SQL → DAG parser (CTE topology, join contracts, column tracking) |
| `logic_tree.py` | DAP Logic Tree builder — structural representation for prompt output |
| `sql_rewriter.py` | DAP response parser — extracts SQL from Component Payload JSON, infers transforms |
| `schemas.py` | Core dataclasses: `OptimizationMode`, `WorkerResult`, `SessionResult`, `ValidationResult`, `PipelineResult` |
| `validate.py` | Timing + equivalence validation (3-run, 5-run, 4x triage protocols) |
| `generate.py` | LLM call orchestration — prompt → API → response |
| `runner.py` | ADORunner wrapper — batch execution with learning integration |

### Sessions

| Module | Role |
|--------|------|
| `sessions/base_session.py` | Abstract base with shared iteration logic |
| `sessions/oneshot_session.py` | Single-call analyst-as-worker mode |
| `sessions/expert_session.py` | Iterative analyst → worker with failure history |
| `sessions/swarm_session.py` | 4-worker fan-out + sniper synthesis |

### Prompts

| Module | Role |
|--------|------|
| `prompts/analyst_briefing.py` | V2 analyst prompt builder (swarm/expert/oneshot/script modes) |
| `prompts/worker.py` | Worker prompt builder from analyst briefing sections |
| `prompts/swarm_fan_out.py` | Fan-out prompt — analyst distributes strategies to 4 workers |
| `prompts/swarm_snipe.py` | Sniper prompt — synthesizes worker failures into refined attempt |
| `prompts/swarm_common.py` | Shared utilities for swarm prompts |
| `prompts/swarm_parsers.py` | Parsers for analyst briefing output |
| `prompts/briefing_checks.py` | Worker rewrite checklist builder |
| `prompts/dag_helpers.py` | DAG-related prompt helpers |
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
| `knowledge.py` | Knowledge loader — engine profiles, constraints, regression warnings |
| `knowledge/` | Static knowledge files (engine-specific JSON/YAML) |
| `examples/` | Gold examples — 19 verified transforms (16 DuckDB + 3 PG) with before/after SQL |
| `constraints/` | Correctness constraints (4 validation gates) |
| `models/` | `similarity_tags.json` + `similarity_metadata.json` — tag-based example index (108 examples) |
| `learnings/` | General learning YAML files |

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
| | Also builds global best-of-all-sources knowledge (--global mode) |

### Other

| Module | Role |
|--------|------|
| `tag_index.py` | Rebuilds tag-based example index (`python3 -m qt_sql.tag_index`) |
| `pg_tuning.py` | PG system introspection — `PGSystemProfile`, resource envelope, `PG_TUNABLE_PARAMS` whitelist |
| `learn.py` | Learning record system — captures speedup, transforms, errors per optimization attempt |
| `analyst.py` | Analyst utilities |
| `analyst_session.py` | Analyst session management |
| `store.py` | Result storage |
| `session_logging.py` | Session-level logging |
| `script_parser.py` | ScriptParser — enterprise SQL multi-statement parsing |
| `node_prompter.py` | Legacy node-level prompt builder |

## CLI

```bash
qt-sql audit <file.sql>              # Static analysis (119 rules)
qt-sql optimize <file.sql>           # LLM-powered optimization
qt-sql validate <orig.sql> <opt.sql> # Validate optimization
```

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

p = Pipeline("packages/qt-sql/qt_sql/benchmarks/duckdb_tpcds")

# Run in any mode
result = p.run_optimization_session(
    "query_88", sql,
    mode=OptimizationMode.SWARM,
    max_iterations=3,
    target_speedup=2.0,
)
print(f"{result.status}: {result.best_speedup:.2f}x")

# Or via ADORunner wrapper
from qt_sql import ADORunner, ADOConfig
runner = ADORunner(ADOConfig(benchmark_dir="packages/qt-sql/qt_sql/benchmarks/duckdb_tpcds"))
result = runner.run_analyst("query_88", sql, mode=OptimizationMode.SWARM)
```
