# ADO Delete Safety Manifest

Generated: 2026-02-07  
Scope: verify whether `packages/qt-sql/ado` can run after deleting other repo content.

## Critical Verdict

`ado` is **not standalone** in the current codebase.  
If you delete everything outside `packages/qt-sql/ado`, core execution fails.

## Verification Evidence

### Isolated `ado`-only runtime test (executed)

- `import ado`: PASS
- `Pipeline(...)`: PASS
- `Pipeline._parse_dag(...)`: FAIL (`ModuleNotFoundError: qt_sql`)
- `Pipeline.run_query(...)`: FAIL (`ModuleNotFoundError: qt_sql`)
- `Validator.validate(...)`: returns `ERROR` when `qt_sql.validation` is unavailable
- `CandidateGenerator` without `analyze_fn`: fails when `qt_shared.llm` is unavailable

### In-repo control test (executed)

- With `PYTHONPATH=packages/qt-sql`, `Pipeline._parse_dag("select 1")` works.

## Hard Must-Keep Paths (No Code Changes)

Keep these paths exactly (or keep equivalent import paths and update imports):

1. `packages/qt-sql/ado/**`
2. `packages/qt-sql/qt_sql/**`
3. `packages/qt-shared/qt_shared/**`

Why full `qt_sql` is required, not partial:

- `qt_sql` package import executes `qt_sql/__init__.py`, which imports analyzer modules.  
  Evidence: `packages/qt-sql/qt_sql/__init__.py:13`

Why full `qt_shared` is required, not partial:

- `ado` imports `qt_shared.llm`, but Python executes parent package init first.  
  `qt_shared/__init__.py` imports `auth` and `database` symbols.  
  Evidence: `packages/qt-shared/qt_shared/__init__.py:13`, `packages/qt-shared/qt_shared/__init__.py:14`, `packages/qt-shared/qt_shared/__init__.py:15`

Where `ado` hard-calls external packages:

- `qt_sql.optimization.dag_v2`: `packages/qt-sql/ado/pipeline.py:92`
- `qt_sql.optimization.plan_analyzer`: `packages/qt-sql/ado/pipeline.py:103`
- `qt_sql.execution.database_utils`: `packages/qt-sql/ado/pipeline.py:140`
- `qt_sql.validation.sql_validator`: `packages/qt-sql/ado/validate.py:73`
- `qt_sql.validation.schemas`: `packages/qt-sql/ado/validate.py:121`
- `qt_sql.execution.factory`: `packages/qt-sql/ado/validate.py:192`
- `qt_shared.llm`: `packages/qt-sql/ado/generate.py:61`

## External Runtime Prerequisites (Outside Repo)

These are required to run the product meaningfully:

1. Python packages:
   - required for core execution: `sqlglot`, `duckdb`, `psycopg2-binary`, `pydantic-settings`, `sqlalchemy`, `python-jose`, `stripe`, `pyyaml`
   - required for FAISS retrieval quality: `numpy`, `faiss` (or `faiss-cpu`)
   - required for chosen LLM provider:
     - OpenAI/DeepSeek/OpenRouter path: `openai`
     - Anthropic path: `anthropic`
     - Groq path: `groq`
     - Gemini API path: `google-generativeai`
2. Database targets in benchmark configs:
   - DuckDB file path in `packages/qt-sql/ado/benchmarks/duckdb_tpcds/config.json:4`
   - PostgreSQL DSN in `packages/qt-sql/ado/benchmarks/postgres_dsb/config.json:4`
3. LLM env config (`QT_LLM_PROVIDER`, provider API key vars) for non-manual generation.

Operational note:

- DuckDB executor sets `DUCKDB_TEMP_DIRECTORY` to `/mnt/d/duckdb_temp`.  
  Evidence: `packages/qt-sql/qt_sql/execution/duckdb_executor.py:85`

## Safe-To-Delete Paths (For ADO Python Runtime Only)

Safe to delete if your only goal is running ADO pipeline/runtime and you do not need non-ADO products:

1. `packages/qt-dax/**`
2. `packages/qt-ui-shared/**`
3. `packages/qt-sql/web/**`
4. `packages/qt-sql/api/**`
5. `packages/qt-sql/tests/**`
6. `packages/qt-shared/tests/**`
7. `packages/qt-sql/dsb_test/**`
8. `packages/qt-sql/qt-output/**`
9. `packages/qt-sql/scripts/**` (only if you do not use those scripts)
10. repo-level docs/landing/research artifacts not imported by runtime code

## Not Safe To Delete Yet

Do not delete these unless you first refactor ADO imports and package initializers:

1. `packages/qt-sql/qt_sql/**`
2. `packages/qt-shared/qt_shared/**`

## Minimal Survivable Layout (Current Implementation)

At minimum, keep:

1. `packages/qt-sql/ado`
2. `packages/qt-sql/qt_sql`
3. `packages/qt-shared/qt_shared`

Plus installed Python dependencies and reachable DB/LLM backends.
