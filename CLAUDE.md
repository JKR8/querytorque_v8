# QueryTorque V8 - Project Context

> **AI Assistant Reference** - Modular architecture with 3 products

---

## Quick Reference

| What | Where |
|------|-------|
| **Benchmark Results** | `BENCHMARKS.md` |
| **Shared Infrastructure** | `packages/qt-shared/` |
| **Calcite Optimizer** | `packages/qt-calcite/` |
| **SQL Product** | `packages/qt-sql/` |
| **DAX Product** | `packages/qt-dax/` |
| **Shared UI Components** | `packages/qt-ui-shared/` |
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
│   ├── qt-calcite/             # Java Calcite Service
│   │   ├── src/main/java/      # Java source
│   │   ├── api/                # FastAPI wrapper
│   │   └── build.gradle.kts
│   │
│   ├── qt-sql/                 # SQL Optimization Product
│   │   ├── qt_sql/
│   │   │   ├── analyzers/      # AST detector, 119 SQL rules
│   │   │   ├── execution/      # DuckDB, Postgres executors
│   │   │   ├── templates/      # sql_report.html.j2
│   │   │   └── calcite_client.py
│   │   ├── cli/                # qt-sql CLI
│   │   ├── api/                # FastAPI backend
│   │   └── web/                # React frontend (sql.querytorque.com)
│   │
│   ├── qt-dax/                 # DAX/Power BI Product
│   │   ├── qt_dax/
│   │   │   ├── analyzers/      # VPAX, DAX + Model rules
│   │   │   ├── parsers/        # DAX parser
│   │   │   └── templates/      # dax_report.html.j2
│   │   ├── cli/                # qt-dax CLI
│   │   ├── api/                # FastAPI backend
│   │   └── web/                # React frontend (dax.querytorque.com)
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
| Database | Single shared DB | Unified user/org/billing |
| Auth | Single Auth0 tenant | SSO across both apps |
| Web apps | Separate subdomains | Independent deployments |
| qt-calcite | Optional service | qt-sql works without it |

---

## CLI Commands

### qt-sql CLI
```bash
qt-sql audit <file.sql>              # Static analysis
qt-sql audit <file.sql> --calcite    # Include Calcite optimization
qt-sql optimize <file.sql>           # LLM-powered optimization
qt-sql validate <orig.sql> <opt.sql> # Validate optimization
```

### qt-dax CLI
```bash
qt-dax audit <model.vpax>            # Analyze Power BI model
qt-dax optimize <model.vpax>         # LLM-powered DAX optimization
qt-dax connect                       # Connect to Power BI Desktop
```

---

## Running Services

### Development

```bash
# Start all services
docker compose up

# Or individually:
# Calcite (port 8001)
cd packages/qt-calcite && ./gradlew fatJar && python api/main.py

# SQL API (port 8002)
cd packages/qt-sql && python -m uvicorn api.main:app --port 8002

# DAX API (port 8003)
cd packages/qt-dax && python -m uvicorn api.main:app --port 8003

# SQL Web (port 5173)
cd packages/qt-sql/web && npm run dev

# DAX Web (port 5174)
cd packages/qt-dax/web && npm run dev
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

# Calcite
QTCALCITE_URL=http://localhost:8001
DEEPSEEK_API_KEY=xxx  # For Calcite's LLM optimization
```

---

## Package Dependencies

```
qt-shared (standalone)
    ↑
    ├── qt-sql (depends on qt-shared)
    │     └── qt-calcite (optional, HTTP)
    │
    └── qt-dax (depends on qt-shared)

qt-ui-shared (standalone React)
    ↑
    ├── qt-sql/web (depends on qt-ui-shared)
    │
    └── qt-dax/web (depends on qt-ui-shared)
```

---

## Testing

```bash
# qt-shared tests
cd packages/qt-shared && pytest tests/ -v

# qt-calcite tests
cd packages/qt-calcite && ./gradlew test

# qt-sql tests
cd packages/qt-sql && pytest tests/ -v

# qt-dax tests
cd packages/qt-dax && pytest tests/ -v
```

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
                              │                   │
              sql.querytorque.com        dax.querytorque.com
                      │                           │
              ┌───────┴────────┐         ┌───────┴────────┐
              │   qt-sql Web   │         │   qt-dax Web   │
              │   (React SPA)  │         │   (React SPA)  │
              └───────┬────────┘         └───────┬────────┘
                      │                           │
              ┌───────┴────────┐         ┌───────┴────────┐
              │  qt-sql API    │         │  qt-dax API    │
              │  :8002         │         │  :8003         │
              └───────┬────────┘         └───────┴────────┘
                      │                           │
                      └─────────┬─────────────────┘
                                │
                      ┌─────────┴─────────┐
                      │  Shared Database  │
                      │   (PostgreSQL)    │
                      └───────────────────┘
                                │
                      ┌─────────┴─────────┐
                      │   qt-calcite      │
                      │   :8001           │
                      └───────────────────┘
```
