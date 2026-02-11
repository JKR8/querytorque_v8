# Scanner Knowledge Pipeline

PostgreSQL plan-space exploration → structured intelligence for optimization prompts.

## Two-Layer Architecture

```
Layer 1: Blackboard (raw observations)
  plan_explore/ + plan_scanner/ data
       ↓
  blackboard.py → scanner_blackboard.jsonl
       ↓
Layer 2: Findings (LLM-extracted claims)
  findings.py → scanner_findings.json
```

**Layer 1** (`blackboard.py`): Merges explore + scan + stacking data into `ScannerObservation` records. One JSONL line per (query, flags) pair. Machine-generated, no interpretation. Merge key: `(query_id, frozenset(flags.items()))`.

**Layer 2** (`findings.py`): Two-pass LLM extraction from blackboard observations. Pass 1 (reasoner) = free-form analysis. Pass 2 (chat model) = structured `ScannerFinding` JSON. Evidence-backed, human-reviewed before downstream use.

## Files

| File | Role |
|------|------|
| `schemas.py` | `ScannerObservation` (L1) + `ScannerFinding` (L2) dataclasses, `FLAG_CATEGORIES`, `derive_category()`, `derive_combo_name()` |
| `blackboard.py` | Layer 1 builder — reads plan_explore/, plan_scanner/, stacking data → `scanner_blackboard.jsonl` |
| `findings.py` | Layer 2 extractor — two-pass LLM (reasoner + chat) → `scanner_findings.json` |
| `build_all.py` | CLI entry point — runs blackboard then findings in sequence |
| `templates/` | YAML/JSON templates for algorithm workflows, finding schemas, prompt templates |

## Usage

```bash
cd /mnt/c/Users/jakc9/Documents/QueryTorque_V8
PYTHONPATH=packages/qt-shared:packages/qt-sql:.

# Full pipeline (blackboard → findings)
python3 -m qt_sql.scanner_knowledge.build_all \
    packages/qt-sql/qt_sql/benchmarks/postgres_dsb_76

# Blackboard only (Layer 1)
python3 -m qt_sql.scanner_knowledge.blackboard \
    packages/qt-sql/qt_sql/benchmarks/postgres_dsb_76

# Findings only (Layer 2)
python3 -m qt_sql.scanner_knowledge.findings \
    packages/qt-sql/qt_sql/benchmarks/postgres_dsb_76

# Flags
#   --force-findings   Re-extract even if scanner_findings.json exists
#   --prompt-only      Print findings prompt without calling LLM
```

## Data Flow

1. **plan_scanner.py** (parent module) toggles 22 planner flag combos per query, collects EXPLAIN plans + wall-clock timings → `plan_scanner/` directory
2. **blackboard.py** reads those files + explore data → `scanner_blackboard.jsonl`
3. **findings.py** feeds blackboard to LLM → `scanner_findings.json`
4. Findings feed into **engine profiles** and **regression warnings** used by analyst/worker prompts

## Relationship to Optimization Blackboard

This is the **scanner** knowledge pipeline (PG planner flag exploration). Separate from `build_blackboard.py` (optimization outcomes blackboard) which extracts learning from swarm worker results. They serve different purposes:

- **Scanner blackboard**: "What does toggling PG planner flags reveal about query behavior?"
- **Optimization blackboard**: "Which rewrite strategies worked/failed on which queries?"
