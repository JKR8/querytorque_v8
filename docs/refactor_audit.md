# qt-sql Refactor Audit (Initial)

Date: 2026-02-04

## Scope
This document captures the initial structure audit and key inconsistencies across CLI, API, and Web UI for the `qt-sql` codebase. It is intended as a baseline for the refactor and unification effort.

## Top-Level Layout
- Core Python package: `packages/qt-sql/qt_sql`
- FastAPI service: `packages/qt-sql/api/main.py`
- CLI implementations:
  - Packaged entrypoint: `packages/qt-sql/qt_sql/cli/main.py` via `packages/qt-sql/pyproject.toml`
  - Secondary CLI module: `packages/qt-sql/cli/main.py`
- Web UI (Vite/React): `packages/qt-sql/web`

## Core Library Structure (`packages/qt-sql/qt_sql`)
- `analyzers/`: AST-based detector and rulebook integration
- `rewriters/`: deterministic AST rewriters and registry
- `optimization/`: DAG pipelines, plan analysis, v5 JSON adaptive optimizer, payload builder, knowledge base
- `execution/`: DuckDB/Postgres executors + plan parsers
- `validation/`: equivalence checking, benchmarking, normalization
- `renderers/` + `templates/`: HTML report rendering

## Key Runtime Flows Observed
- Packaged CLI (`packages/qt-sql/qt_sql/cli/main.py`)
  - `audit` uses `SQLAntiPatternDetector` and attempts to use an opportunity detector import.
  - `optimize --dag` calls `optimize_v5_json` in `packages/qt-sql/qt_sql/optimization/adaptive_rewriter_v5.py`.
  - `assess` runs plan-based analysis for opportunities.
- Secondary CLI (`packages/qt-sql/cli/main.py`)
  - Different DAG flow: manually builds prompts via `DagV2Pipeline` and calls LLM directly.
  - Batch optimization path and different validation wiring.
- API (`packages/qt-sql/api/main.py`)
  - `/api/sql/analyze` uses `SQLAntiPatternDetector` and maps AST-derived opportunities.
  - `/api/sql/optimize` uses a simple non-DAG prompt (legacy).
  - Database connect/explain endpoints exist.
- Web UI (`packages/qt-sql/web/src/api/client.ts`)
  - Calls endpoints like `/v2/optimize/*` and `/reports/*` that do not exist in the API.

## Structural Inconsistencies / Breakpoints
- Missing module import: `packages/qt-sql/qt_sql/cli/main.py` imports `qt_sql.analyzers.opportunity_detector`, which is not present in the codebase.
- Missing optimization schemas module: `packages/qt-sql/qt_sql/optimization/__init__.py` imports `.schemas`, but no `schemas.py` exists in that directory.
- Duplicate CLI implementations with diverging behavior:
  - `packages/qt-sql/cli/main.py` vs `packages/qt-sql/qt_sql/cli/main.py`.
- API vs Web mismatch:
  - Web expects `/v2/optimize/*` and `/reports/*`, API only exposes `/api/sql/*` and `/api/optimize/manual/validate`.
- Optimization pathway fragmentation:
  - `dag_v2.py`, `dag_v3.py`, `adaptive_rewriter_v5.py`, `iterative_optimizer.py` coexist with partial wiring.
  - Simple LLM prompt paths still exist in CLI/API but are not aligned with DAG v2 + v5 JSON vision.

## Context Notes
- `packages/qt-sql/ANTIPATTERN_ANALYSIS.txt` documents prior conclusions about the anti-pattern system. Per current guidance, the anti-pattern detector is considered fixed and should not be retired unless explicitly directed.

## Refactor Direction (From User)
- Retire the simple (legacy) LLM prompt setup.
- Bed down the final process first, then roll it out to CLI, Web UI, shared packages, and API.

## Open Decisions (To Resolve Before Refactor Execution)
- Final “single source of truth” pipeline definition (DAG v2 prompt + JSON v5 response structure and validation flow).
- Which CLI implementation becomes the unified entrypoint (likely consolidate into `qt_sql/cli/main.py`).
- API and Web contract alignment once the final pipeline is locked.

