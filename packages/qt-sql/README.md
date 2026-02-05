# qt-sql

SQL optimization and analysis product for QueryTorque.

## Architecture

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                              QT-SQL ARCHITECTURE                            │
└─────────────────────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────────────────────┐
│  INTERFACES                                                                 │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│  ┌──────────────┐      ┌──────────────┐      ┌──────────────┐            │
│  │   CLI Tool   │      │   FastAPI    │      │   React Web  │            │
│  │              │      │   Backend    │      │   Frontend   │            │
│  │  qt-sql      │      │   :8002      │      │   :5173      │            │
│  │  commands    │      │              │      │              │            │
│  └──────┬───────┘      └──────┬───────┘      └──────┬───────┘            │
│         │                     │                     │                      │
│         └─────────────────────┴─────────────────────┘                      │
│                                 │                                          │
└─────────────────────────────────┼──────────────────────────────────────────┘
                                  │
┌─────────────────────────────────┼──────────────────────────────────────────┐
│  CORE MODULES                   │                                          │
├─────────────────────────────────┼──────────────────────────────────────────┤
│                                 ▼                                          │
│  ┌────────────────────────────────────────────────────────────────────┐   │
│  │  ANALYZERS (Static Analysis)                                       │   │
│  ├────────────────────────────────────────────────────────────────────┤   │
│  │  • AST Detector (119 rules)                                        │   │
│  │  • Opportunity Detector (11 transforms from knowledge_base.py)     │   │
│  │  • Plan Analyzer (EXPLAIN parsing, cost attribution)               │   │
│  └────────────────────────────────────────────────────────────────────┘   │
│                                 │                                          │
│                                 ▼                                          │
│  ┌────────────────────────────────────────────────────────────────────┐   │
│  │  OPTIMIZATION PIPELINE (Adaptive Rewriter V5)                      │   │
│  ├────────────────────────────────────────────────────────────────────┤   │
│  │                                                                     │   │
│  │  1. Query Recommender (ML-guided example selection)                │   │
│  │     ↓                                                               │   │
│  │  2. DAG Builder (Parse SQL → QueryDag with contracts)              │   │
│  │     ↓                                                               │   │
│  │  3. DAG v2 Prompt Builder (Subgraph slicing, cost analysis)        │   │
│  │     ↓                                                               │   │
│  │  4. Parallel Worker Fan-out (5 workers)                            │   │
│  │     ├─ Worker 1: Examples 1-3  (DAG JSON, Top ML recs)            │   │
│  │     ├─ Worker 2: Examples 4-6  (DAG JSON)                          │   │
│  │     ├─ Worker 3: Examples 7-9  (DAG JSON)                          │   │
│  │     ├─ Worker 4: Examples 10-12 (DAG JSON)                         │   │
│  │     └─ Worker 5: No examples   (Full SQL, Explore mode)            │   │
│  │     ↓                                                               │   │
│  │  5. DAG Assembler (JSON → Full SQL)                                │   │
│  │     ↓                                                               │   │
│  │  6. Sample DB Validation (Tick/Cross: runs + same row count)       │   │
│  │     ↓                                                               │   │
│  │  7. Full DB Benchmark (5-run trimmed mean)                         │   │
│  │     ↓                                                               │   │
│  │  8. Winner Selection (First to meet target speedup)                │   │
│  │                                                                     │   │
│  └────────────────────────────────────────────────────────────────────┘   │
│                                 │                                          │
└─────────────────────────────────┼──────────────────────────────────────────┘
                                  │
┌─────────────────────────────────┼──────────────────────────────────────────┐
│  EXECUTION & VALIDATION         │                                          │
├─────────────────────────────────┼──────────────────────────────────────────┤
│                                 ▼                                          │
│  ┌──────────────────┐      ┌──────────────────┐                          │
│  │  DuckDB Executor │      │ Postgres Executor│                          │
│  ├──────────────────┤      ├──────────────────┤                          │
│  │  • Execute       │      │  • Execute       │                          │
│  │  • EXPLAIN       │      │  • EXPLAIN       │                          │
│  │  • Cost estimate │      │  • Cost estimate │                          │
│  └────────┬─────────┘      └────────┬─────────┘                          │
│           │                         │                                     │
│           └────────────┬────────────┘                                     │
│                        │                                                  │
│                        ▼                                                  │
│  ┌────────────────────────────────────────────────────────────────────┐  │
│  │  Query Benchmarker                                                  │  │
│  ├────────────────────────────────────────────────────────────────────┤  │
│  │  • 3-run pattern (1 warmup + 2 measured, avg)                      │  │
│  │  • 5-run trimmed mean (discard min/max, avg middle 3)              │  │
│  │  • Speedup calculation                                              │  │
│  └────────────────────────────────────────────────────────────────────┘  │
│                        │                                                  │
│                        ▼                                                  │
│  ┌────────────────────────────────────────────────────────────────────┐  │
│  │  Equivalence Checker                                                │  │
│  ├────────────────────────────────────────────────────────────────────┤  │
│  │  • Row count comparison                                             │  │
│  │  • Result checksum (MD5)                                            │  │
│  │  • Semantic validation                                              │  │
│  └────────────────────────────────────────────────────────────────────┘  │
│                                                                           │
└───────────────────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────────────────────┐
│  KNOWLEDGE BASE & ML                                                        │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│  ┌────────────────────────────────────────────────────────────────────┐   │
│  │  Knowledge Base (knowledge_base.py)                                │   │
│  ├────────────────────────────────────────────────────────────────────┤   │
│  │  11 Canonical Transforms:                                          │   │
│  │  • QT-OPT-001: or_to_union (2.98x verified)                        │   │
│  │  • QT-OPT-002: correlated_to_cte (2.81x verified)                  │   │
│  │  • QT-OPT-003: date_cte_isolate (2.67x verified)                   │   │
│  │  • QT-OPT-004: push_pred (2.71x verified)                          │   │
│  │  • QT-OPT-005: consolidate_scans (1.84x verified)                  │   │
│  │  • QT-OPT-006: multi_push_pred                                     │   │
│  │  • QT-OPT-007: materialize_cte                                     │   │
│  │  • QT-OPT-008: flatten_subq                                        │   │
│  │  • QT-OPT-009: reorder_join                                        │   │
│  │  • QT-OPT-010: inline_cte                                          │   │
│  │  • QT-OPT-011: remove_redundant                                    │   │
│  └────────────────────────────────────────────────────────────────────┘   │
│                                 │                                          │
│                                 ▼                                          │
│  ┌────────────────────────────────────────────────────────────────────┐   │
│  │  Gold Examples (examples/*.json) - 13 files                        │   │
│  ├────────────────────────────────────────────────────────────────────┤   │
│  │  12 Unique Transform IDs for ALLOWED_TRANSFORMS:                   │   │
│  │  pushdown, decorrelate, or_to_union, early_filter,                │   │
│  │  date_cte_isolate, materialize_cte, flatten_subquery,             │   │
│  │  reorder_join, multi_push_predicate, inline_cte,                  │   │
│  │  remove_redundant, semantic_rewrite                               │   │
│  └────────────────────────────────────────────────────────────────────┘   │
│                                 │                                          │
│                                 ▼                                          │
│  ┌────────────────────────────────────────────────────────────────────┐   │
│  │  Query Recommender (query_recommender.py)                          │   │
│  ├────────────────────────────────────────────────────────────────────┤   │
│  │  • Parses query_recommendations_report.md                          │   │
│  │  • Returns top N examples per query (ML confidence scores)         │   │
│  │  • Caching for fast lookups                                        │   │
│  │  • 73/99 TPC-DS queries have recommendations                       │   │
│  └────────────────────────────────────────────────────────────────────┘   │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────────────────────┐
│  EXTERNAL INTEGRATIONS                                                      │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│  ┌──────────────────┐      ┌──────────────────┐      ┌─────────────────┐  │
│  │  LLM Providers   │      │  Databases       │      │  Shared (Auth)  │  │
│  ├──────────────────┤      ├──────────────────┤      ├─────────────────┤  │
│  │  • DeepSeek      │      │  • DuckDB        │      │  • Auth0        │  │
│  │  • Anthropic     │      │  • PostgreSQL    │      │  • Stripe       │  │
│  │  • OpenAI        │      │  • MySQL         │      │  • qt_shared    │  │
│  │  • Groq          │      │  • SQL Server    │      │                 │  │
│  │  • Gemini        │      │                  │      │                 │  │
│  │  • Kimi          │      │                  │      │                 │  │
│  └──────────────────┘      └──────────────────┘      └─────────────────┘  │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────────────────────┐
│  DATA FLOW: OPTIMIZATION REQUEST                                           │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│  User Input (SQL Query + query_id)                                         │
│      ↓                                                                      │
│  Query Recommender → Load ML recs for query_id                             │
│      ↓                                                                      │
│  Pad to 12 examples with gold examples                                     │
│      ↓                                                                      │
│  Split into 4 batches of 3 examples each                                   │
│      ↓                                                                      │
│  ┌────────────────────────────────────────────────────────────────┐        │
│  │  Parallel Worker Execution (5 workers)                         │        │
│  ├────────────────────────────────────────────────────────────────┤        │
│  │  Worker 1-4: DAG JSON format with 3 examples each              │        │
│  │  Worker 5:   Full SQL format, no examples (explore mode)       │        │
│  └────────────────────────────────────────────────────────────────┘        │
│      ↓                                                                      │
│  LLM generates optimizations (5 candidates)                                │
│      ↓                                                                      │
│  ┌────────────────────────────────────────────────────────────────┐        │
│  │  Sample DB Validation (Parallel)                               │        │
│  ├────────────────────────────────────────────────────────────────┤        │
│  │  • Execute original + optimized                                 │        │
│  │  • Check: Runs without error + same row count                  │        │
│  │  • Filter: Keep only valid candidates                          │        │
│  └────────────────────────────────────────────────────────────────┘        │
│      ↓                                                                      │
│  ┌────────────────────────────────────────────────────────────────┐        │
│  │  Full DB Benchmark (Sequential)                                │        │
│  ├────────────────────────────────────────────────────────────────┤        │
│  │  For each valid candidate:                                     │        │
│  │    • Run 5 times (discard min/max, avg middle 3)               │        │
│  │    • Calculate speedup                                          │        │
│  │    • Stop at first winner >= target_speedup                    │        │
│  └────────────────────────────────────────────────────────────────┘        │
│      ↓                                                                      │
│  Return: Winner (optimized SQL + speedup + worker_id)                      │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
```

## Features

- **Static SQL Analysis**: 119 anti-pattern detection rules via AST Detector
- **ML-Guided Optimization**: Query-specific example recommendations (73% coverage on TPC-DS)
- **Adaptive Rewriter V5**: 5-worker parallel optimization with maximum diversity
- **DAG v2 Pipeline**: Query DAG with node contracts, usage tracking, cost attribution
- **Multi-Database Support**: DuckDB, PostgreSQL, MySQL, SQL Server
- **Robust Benchmarking**: 5-run trimmed mean for stable performance measurement
- **12 Transform Types**: Verified optimizations with proven speedups (up to 2.98x)

## CLI

```bash
qt-sql audit <file.sql>              # Static analysis (119 rules)
qt-sql optimize <file.sql>           # LLM-powered optimization (V5)
qt-sql validate <orig.sql> <opt.sql> # Validate optimization
```

## ADO Mode

ADO (Autonomous Data Optimization) is a separate mode that runs batches of TPC-DS queries in parallel, captures wins and failures, and emits a YAML "brain" summary for new GOLD examples and constraints. It uses the fast sf5 DuckDB for validation by default.

```bash
python3 scripts/ado.py \
  --sample-db /mnt/d/TPC-DS/tpcds_sf5.duckdb \
  --query-count 10 \
  --workers 10 \
  --examples-per-prompt 3 \
  --provider deepseek
```
