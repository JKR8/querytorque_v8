# Qt-SQL ADO Product Refactor Plan

Status: Planning (no functional changes yet)  
Date: 2026-02-07

## 1) Scope and Product Boundaries

This plan follows the confirmed product boundaries:

1. `packages/qt-sql` is the SQL product and ADO runtime owner.
2. `packages/qt-dax` remains a separate product.
3. `packages/qt-shared` remains shared infrastructure for both products (including API key/provider config).
4. The web frontend remains shared UI for both products (SQL + DAX experiences).

## 2) Goals

1. Make ADO the clear core runtime inside the SQL product.
2. Reduce cross-module coupling and import fragility in `qt-sql`.
3. Keep `qt-shared` as shared infra, but use it through explicit integration boundaries.
4. Preserve behavior while refactoring (no silent regressions in parsing, rewriting, validation, or benchmarking).
5. Keep SQL and DAX independently deployable while sharing infra/UI.

## 3) Non-Goals

1. Merging `qt-dax` into `qt-sql`.
2. Removing `qt-shared`.
3. Replacing shared web with product-specific frontend forks.
4. Changing benchmark semantics or optimization policy during structural refactor.

## 4) Current Risks (Observed)

1. ADO core runtime has hard imports into `qt_sql` runtime modules and `qt_shared.llm`.
2. Package import side effects in `qt_sql/__init__.py` and `qt_shared/__init__.py` make partial module isolation brittle.
3. Mixed historical entry paths (`ado.*` and `qt_sql.*`) complicate dependency reasoning.
4. Runtime-critical behavior depends on external DB targets and provider configuration.

## 5) Target Architecture

### 5.1 Runtime Layers (Qt-SQL)

1. `qt_sql.ado`: orchestration and ADO domain logic (pipeline, prompts, learning, sessions).
2. `qt_sql.execution` / `qt_sql.validation` / `qt_sql.optimization`: SQL runtime services consumed by ADO.
3. `qt_sql.integrations`: thin adapters for external/shared services.
4. `qt_shared`: shared config + LLM clients + auth/billing/db infra used via `qt_sql.integrations`.

### 5.2 Frontend/Backend Split

1. Shared web UI routes to SQL API for ADO workflows.
2. Shared web UI routes to DAX API for DAX workflows.
3. Session/auth/provider config remains shared via `qt-shared`.

## 6) Execution Plan (Phased)

## Phase 0: Baseline Freeze

1. Tag baseline commit before structural moves.
2. Capture current smoke tests:
   - ADO import and runner initialization
   - `Pipeline._parse_dag`
   - `Pipeline.run_query` with deterministic `analyze_fn`
   - Validator path on DuckDB sample
3. Snapshot benchmark artifact contract (prompt/response/validation file formats).

## Phase 1: Boundary Hardening

1. Create explicit integration wrappers in `qt-sql` for:
   - LLM client creation
   - shared settings access
2. Replace direct scattered calls with wrapper usage in ADO runtime modules.
3. Keep functional behavior unchanged.

## Phase 2: Namespace Consolidation

1. Consolidate ADO into product namespace (`qt_sql.ado`) as the canonical path.
2. Keep temporary compatibility shim(s) for old `ado.*` imports.
3. Update internal imports to canonical namespace.

## Phase 3: Package Side-Effect Cleanup

1. Make `qt_sql/__init__.py` lightweight (remove heavy transitive imports).
2. Ensure importing ADO paths does not pull unrelated subsystems.
3. Keep public symbols stable where needed.

## Phase 4: API/CLI Alignment

1. Ensure SQL API exposes ADO workflows through stable endpoints.
2. Ensure SQL CLI entrypoint exposes ADO commands with one canonical execution path.
3. Remove duplicated legacy command paths after parity verification.

## Phase 5: Shared Web Contract Stabilization

1. Document SQL and DAX route/module boundaries in shared web.
2. Define stable API contracts consumed by web for each product.
3. Validate dual-product navigation/auth flow after backend refactor.

## Phase 6: Cleanup and Cutover

1. Remove temporary shims after usage reaches zero.
2. Delete deprecated internal paths only after test gates pass.
3. Update docs to reflect final canonical architecture and entrypoints.

## 7) Quality Gates (Required Before Cutover)

1. Unit tests for ADO modules pass.
2. Import smoke tests pass in clean environment.
3. Deterministic run-query smoke passes with mock/fixed `analyze_fn`.
4. Validation smoke passes on sample DB.
5. Shared web integration smoke passes for both SQL and DAX routes.
6. No runtime-critical import from deprecated paths.

## 8) Deliverables

1. Updated code layout with canonical `qt_sql.ado` ownership.
2. Compatibility shim and deprecation window plan.
3. Updated SQL API and CLI docs for ADO entrypoints.
4. Shared web integration notes for dual-product routing.
5. Final migration report with before/after dependency graph.

## 9) Change Control

1. Execute as small PR sequence (boundary hardening -> namespace -> cleanup).
2. No behavior changes bundled with structural changes in the same PR when avoidable.
3. Keep rollback-safe checkpoints at each phase boundary.

## 10) Notes

This document is the planning baseline only.  
Implementation starts after explicit go-ahead and PR sequencing.
