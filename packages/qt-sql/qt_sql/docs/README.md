# qt_sql Documentation (Current)

This folder contains the **current** architecture documentation for `qt_sql`.

## Canonical Namespace

- Python module: `qt_sql`
- CLI module entry: `python3 -m qt_sql.<module>`
- Benchmark root: `packages/qt-sql/qt_sql/benchmarks/`

## Core Architecture

- Pipeline: `qt_sql/pipeline.py`
- Sessions:
  - `qt_sql/sessions/oneshot_session.py`
  - `qt_sql/sessions/expert_session.py`
  - `qt_sql/sessions/swarm_session.py`
- DAP rewrite parsing/assembly: `qt_sql/sql_rewriter.py`
- Logic Tree generation: `qt_sql/logic_tree.py`
- Prompt builders: `qt_sql/prompts/`
- Validation: `qt_sql/validate.py`, `qt_sql/validation/`
- Executors: `qt_sql/execution/`

## Operational Docs

- Scanner knowledge pipeline: `qt_sql/scanner_knowledge/README.md`
- Prompt spec and rendered samples: `qt_sql/prompts/samples/PROMPT_SPEC.md`, `qt_sql/prompts/samples/V0/`

## Legacy Docs

All legacy/historical docs were moved to:

- `qt_sql/docs/archive/`

These are preserved for reference and are not the source of truth for current architecture.
