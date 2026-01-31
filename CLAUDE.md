# QueryTorque V8 - Project Context

> **AI Assistant Reference** - Modular architecture with 3 products

---

## Quick Reference

| What | Where |
|------|-------|
| **Shared Infrastructure** | `packages/qt-shared/` |
| **Calcite Optimizer** | `packages/qt-calcite/` |
| **SQL Product** | `packages/qt-sql/` |
| **DAX Product** | `packages/qt-dax/` |
| **Shared UI Components** | `packages/qt-ui-shared/` |

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
├── alembic/                    # Shared database migrations
├── docker-compose.yml          # Orchestrates all services
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
