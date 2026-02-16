# QueryTorque V8 - Project Context

> **AI Assistant Reference** - SQL Optimization Platform

---

## Quick Reference

| What | Where |
|------|-------|
| **Benchmark Results** | `BENCHMARKS.md` |
| **Shared Infrastructure** | `packages/qt-shared/` |
| **SQL Product** | `packages/qt-sql/` |
| **Shared UI Components** | `packages/qt-ui-shared/` |
| **Leaderboards** | `research/leaderboards/` |
| **Research/Experiments** | `research/` |

---

## Architecture

```
QueryTorque_V8/
├── packages/
│   ├── qt-shared/              # Shared Python infrastructure
│   │   ├── qt_shared/
│   │   │   ├── auth/           # Auth0 middleware, UserContext
│   │   │   ├── billing/        # Stripe, tier features
│   │   │   ├── database/       # Models, connection, migrations
│   │   │   ├── llm/            # All LLM clients
│   │   │   └── config/         # Settings class
│   │   └── pyproject.toml
│   │
│   ├── qt-sql/                 # SQL Optimization Product
│   │   ├── qt_sql/
│   │   │   ├── analyzers/      # AST detector, 119 SQL rules
│   │   │   ├── execution/      # DuckDB, Postgres executors
│   │   │   ├── templates/      # sql_report.html.j2
│   │   ├── cli/                # qt-sql CLI
│   │   ├── api/                # FastAPI backend
│   │   └── web/                # React frontend (sql.querytorque.com)
│   │
│   └── qt-ui-shared/           # Shared React components
│       ├── src/
│       │   ├── components/     # ReportViewer, CodeEditor, DropZone
│       │   ├── contexts/       # AuthContext (Auth0)
│       │   └── theme/          # Design tokens
│       └── package.json
│
├── research/
│   └── benchmarks/             # TPC-DS benchmark results by provider
│       ├── deepseek/           # DeepSeek V3 results
│       ├── kimi/               # Kimi results
│       └── _template.md        # Template for new runs
│
├── alembic/                    # Shared database migrations
├── docker-compose.yml          # Orchestrates all services
├── BENCHMARKS.md               # Benchmark results dashboard
└── pyproject.toml              # Workspace root
```

---

## Key Decisions

| Decision | Choice | Rationale |
|----------|--------|-----------|
| Shared code | Internal `qt-shared` package | Single source of truth |
| Database | SQL DB | Unified user/org/billing |
| Auth | Auth0 tenant | Secure access |
| Web apps | subdomain | Independent deployment |

---

## CLI Commands

### qt-sql CLI
```bash
qt-sql audit <file.sql>              # Static analysis
qt-sql optimize <file.sql>           # LLM-powered optimization
qt-sql validate <orig.sql> <opt.sql> # Validate optimization
```

---

## Running Services

### Development

```bash
# Start all services
docker compose up

# Or individually:
# SQL API (port 8002)
cd packages/qt-sql && python -m uvicorn api.main:app --port 8002

# SQL Web (port 5173)
cd packages/qt-sql/web && npm run dev
```

---

## Environment Variables

```bash
# Database
QT_DATABASE_URL=postgresql://user:pass@localhost:5432/querytorque

# Auth0
QT_AUTH0_DOMAIN=your-tenant.auth0.com
QT_AUTH0_API_AUDIENCE=https://api.querytorque.com
QT_AUTH0_CLIENT_ID=xxx

# Stripe
QT_STRIPE_API_KEY=sk_xxx
QT_STRIPE_WEBHOOK_SECRET=whsec_xxx

# LLM Provider
QT_LLM_PROVIDER=groq  # or: anthropic, deepseek, openai, gemini-api
QT_GROQ_API_KEY=xxx

```

---

## Package Dependencies

```
qt-shared (standalone)
    ↑
    ├── qt-sql (depends on qt-shared)

qt-ui-shared (standalone React)
    ↑
    ├── qt-sql/web (depends on qt-ui-shared)
```

---

## Testing

```bash
# qt-shared tests
cd packages/qt-shared && pytest tests/ -v

# qt-sql tests
cd packages/qt-sql && pytest tests/ -v
```

---

## Leaderboards

**All leaderboard data lives in `research/leaderboards/`.** Never create leaderboard data outside this directory.

- `*.html` at root — self-contained viewable leaderboards (double-click to open)
- `data/` — JSON + CSV machine-readable data
- `prompts/` — prompt chain snapshots keyed by run name
- `scripts/` — build and snapshot scripts
- Naming convention: `YYYYMMDD_{benchmark}_{label}.html`

---

## Recording Results

**All benchmark and experiment results MUST be recorded.** Never run benchmarks without saving the results.

### Benchmark Results Location

```
research/experiments/dspy_runs/
├── {name}_{YYYYMMDD_HHMMSS}/    # Timestamped run folder
│   ├── results.json             # Full results (query, status, speedup, times)
│   ├── summary.txt              # Human-readable summary
│   └── q{N}/                    # Per-query artifacts (SQL, plans, logs)
```

### Naming Convention

Format: `{description}_{YYYYMMDD_HHMMSS}_{mode}/`

Examples:
- `all_20260201_205640/` - Full benchmark run
- `all_20260202_143844_dag_mcts/` - DAG + MCTS mode
- `kimi_dag_20260202_190306/` - Kimi model with DAG mode
- `failures_20260201_223223/` - Re-run of failed queries

### Required Files

1. **`results.json`** - Machine-readable results array:
```json
[
  {"query": "q1", "status": "success", "speedup": 2.90, "original_time": 0.5, "optimized_time": 0.17},
  {"query": "q2", "status": "validation_failed", "error": "row count mismatch"}
]
```

2. **`summary.txt`** - Human-readable summary with:
   - Date, model, mode, parameters
   - Success/failed/error counts
   - Top speedups list

### When to Record

- Running TPC-DS benchmarks (full or partial)
- Testing new optimization strategies
- Comparing LLM providers/models
- Validating bug fixes with before/after metrics

---

## Test Data

| Dataset | Location | Description |
|---------|----------|-------------|
| **TPC-DS SF100** | `D:\TPC-DS\` | TPC-DS benchmark at scale factor 100 |
| TPC-DS Queries | `D:\TPC-DS\queries_sf100\` | 99 original TPC-DS queries |
| DuckDB Converted | `D:\TPC-DS\queries_duckdb_converted\` | DuckDB-compatible queries |
| Postgres Converted | `D:\TPC-DS\queries_postgres_converted\` | PostgreSQL-compatible queries |
| DuckDB Database | `D:\TPC-DS\tpcds_sf100.duckdb` | ~28GB DuckDB database with SF100 data |

---

## Deployment Architecture

```
                    ┌─────────────────────────────────────────┐
                    │            Cloudflare / CDN              │
                    └─────────────────────────────────────────┘
                              │
              sql.querytorque.com
                      │
              ┌───────┴────────┐
              │   qt-sql Web   │
              │   (React SPA)  │
              └───────┬────────┘
                      │
              ┌───────┴────────┐
              │  qt-sql API    │
              │  :8002         │
              └───────┬────────┘
                      │
                      └─────────┬─────────────────┘
                                │
                      ┌─────────┴─────────┐
                      │  Shared Database  │
                      │   (PostgreSQL)    │
                      └───────────────────┘
```
