# ADO (Autonomous Data Optimization)

ADO is a parallel optimization loop that uses fast validation (sf5) to generate, evaluate, and learn from candidate rewrites. The loop is intentionally compact and stateful: each round feeds relevance-ordered knowledge from other candidates into the next attempt.

```
┌─────────────────────────────────────────────────────────────┐
│                   ADO RUNNER (Orchestrator)                 │
└─────────────────────────────────────────────────────────────┘
            │
            ▼
┌─────────────────────────────────────────────────────────────┐
│ 1) PREPARE (Plan the attempt)                               │
│    - build ContextBundle (plan + stats + heuristics)         │
│    - fetch Knowledge (FAISS examples + constraints)          │
│    - rank AttemptHistory by relevance (from last round)      │
│    - build Prompt (generated from DAG v2 + v3 wrapper)       │
└─────────────────────────────────────────────────────────────┘
            │
            ▼
┌─────────────────────────────────────────────────────────────┐
│ 2) GENERATE (Parallel)                                      │
│    - produce N candidates (diverse styles)                   │
│    - record provenance (examples, style, neighbors)          │
└─────────────────────────────────────────────────────────────┘
            │
            ▼
┌─────────────────────────────────────────────────────────────┐
│ 3) VALIDATE + PICK                                           │
│    - correctness + timing + plan deltas                      │
│    - pick best passing candidate                             │
└─────────────────────────────────────────────────────────────┘
            │
            ▼
┌─────────────────────────────────────────────────────────────┐
│ 4) LEARN + STORE                                              │
│    - aggregate wins/failures to update scores                │
│    - update AttemptHistory relevance for next attempt         │
│    - update retrieval index + curate gold queue               │
└─────────────────────────────────────────────────────────────┘

         ┌──────────────────────────────────────────┐
         │ If no winner: adjust strategy and loop    │
         │ (rotate examples, increase diversity,     │
         │ switch validation profile)                │
         └──────────────────────────────────────────┘
```

## Start Building

Planned components (stubs to implement):

- `ado/context.py`: build ContextBundle (plan + stats + heuristics)
- `ado/knowledge.py`: FAISS retrieval + constraint selection
- `ado/prompt.py`: prompt assembly (DAG v2 base + v3 wrapper)
- `ado/generate.py`: parallel candidate generation + provenance
- `ado/validate.py`: correctness + speed + plan deltas
- `ado/learn.py`: score updates + AttemptHistory ranking
- `ado/store.py`: write artifacts + update indexes + gold queue

## Defaults

- Engine: DuckDB
- Validation DB: sf5 (`/mnt/d/TPC-DS/tpcds_sf5.duckdb`)
- Candidates per round: 10
