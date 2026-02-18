# qt_sql Documentation

## Current Documents

| Document | Purpose |
|----------|---------|
| **[PRODUCT_CONTRACT.md](PRODUCT_CONTRACT.md)** | Pipeline phases, success conditions, CLI commands, module responsibilities. The engineering contract — read this before changing anything. |
| [`../prompts/sql_rewrite_spec.md`](../prompts/sql_rewrite_spec.md) | DAP (Decomposed Attention Protocol) v1.0 — output format spec for LLM rewrites |
| [`../prompts/archive/samples/PROMPT_SPEC.md`](../prompts/archive/samples/PROMPT_SPEC.md) | Archived prompt builder reference — legacy modes, parameters, token budgets |
| [`../prompts/archive/samples/V0/`](../prompts/archive/samples/V0/) | Archived rendered prompt samples covering legacy pipeline stages |
| [`../scanner_knowledge/README.md`](../scanner_knowledge/README.md) | Scanner knowledge pipeline (PG planner exploration → findings) |
| [`../plan_scanner_spec.yaml`](../plan_scanner_spec.yaml) | Three-layer plan-space scanner architecture |

## Data Files (referenced by code, not for reading)

| File | Used by |
|------|---------|
| `../constraints/engine_profile_duckdb.json` | `knowledge.py` — DuckDB optimizer gaps + strengths |
| `../constraints/engine_profile_postgresql.json` | `knowledge.py` — PG optimizer gaps + strengths |
| `../models/similarity_tags.json` | `tag_index.py` — tag-based example matching index |

## Archive

`archive/` — Historical V5 design docs and pre-qt_sql namespace docs. Preserved for reference, not source of truth.
