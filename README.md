# QueryTorque

## Mission

QueryTorque gathers **generalised intelligence on database engine optimizers** — what they handle well, where their gaps are, and which SQL rewrites exploit those gaps for measurable speedup. Each engine has a **master intelligence document** (`knowledge/{engine}.md`) that is the single source of truth, continuously refined by automated benchmarking, transform detection, and evidence-based distillation.

QueryTorque is a comprehensive query optimization platform. It analyzes SQL queries, identifies performance issues, and provides actionable recommendations with AI-powered explanations.

## Architecture

```
                                    QueryTorque Architecture

    +------------------+
    |    Qt-SQL UI     |
    |   (React/Vite)   |
    +--------+---------+
             |
             v
    +------------------+
    |   Qt-SQL API     |
    |   (FastAPI)      |
    |   Port: 8002     |
    +--------+---------+
             |
             |    +-------------+
             +--->|  Qt-Shared  |
                  | - Auth      |
                  | - Billing   |
                  | - Database  |
                  | - LLM       |
                  +------+------+
                         |
    +--------------------+--------------------+
    |                                         |
    v                                         v
+----------+                           +-------------+
|PostgreSQL|                           |  LLM APIs   |
|  (Data)  |                           | (Anthropic) |
|Port: 5432|                           +-------------+
+----------+

Package Structure:
==================
packages/
  qt-shared/       # Shared infrastructure (Python)
    qt_shared/
      auth/        # Auth0 integration, API keys
      billing/     # Stripe integration
      database/    # SQLAlchemy models, migrations
      llm/         # Multi-provider LLM client
      config/      # Settings management

  qt-sql/          # SQL optimization product (Python)
    qt_sql/
      analyzers/   # SQL analysis rules
      execution/   # Query execution engine
      templates/   # Report templates
    cli/           # Command-line interface
    api/           # FastAPI endpoints
    web/           # Web interface

  qt-ui-shared/    # Shared React components
    src/

Repository Layout:
==================
alembic/           # Database migrations
artifacts/         # Local artifacts (reports, lists, exports)
docs/              # Project documentation
landing_page/      # Marketing/landing page assets
packages/          # Product packages (see above)
research/          # Exploratory notes and experiments
runs/              # Local run outputs (test runs, retries, temp data)
scripts/           # Utility scripts for dev/test workflows
secrets/           # Local API keys and credentials (ignored by git)
tests/             # Repository-level tests and harnesses
```

## Quick Start

### Prerequisites

- Python 3.11+
- Docker and Docker Compose
- Node.js 18+ (for UI development)

### Development Setup

1. **Clone and setup environment**

```bash
git clone https://github.com/querytorque/querytorque.git
cd querytorque

# Copy environment configuration
cp .env.example .env
# Edit .env with your API keys and configuration
```

2. **Start infrastructure services**

```bash
# Start PostgreSQL
docker-compose up -d postgres

# Wait for services to be healthy
docker-compose ps
```

3. **Install Python dependencies**

```bash
# Create virtual environment
python -m venv .venv
source .venv/bin/activate  # On Windows: .venv\Scripts\activate

# Verify you're using the root venv
./scripts/check-venv.sh

# Install packages in development mode
pip install -e packages/qt-shared[dev,all-llm]
pip install -e packages/qt-sql[dev]
```

4. **Run database migrations**

```bash
alembic upgrade head
```

5. **Start development servers**

```bash
# Terminal 1: Qt-SQL API
cd packages/qt-sql
uvicorn qt_sql.api.main:app --reload --port 8002
```

### Docker Compose (Full Stack)

```bash
# Start all services
docker-compose up -d

# Start only specific services
docker-compose up -d postgres         # Application database
docker-compose up -d dsb-postgres     # Benchmarking database
docker-compose up -d qt-sql-api       # SQL API

# View logs
docker-compose logs -f

# View specific service logs
docker-compose logs -f dsb-postgres

# Stop all services
docker-compose down
```

### Service Endpoints

| Service            | URL                    | Description                          |
|--------------------|------------------------|--------------------------------------|
| Qt-Calcite         | http://localhost:8001  | SQL parsing service                  |
| Qt-SQL API         | http://localhost:8002  | SQL optimization API                 |
| PostgreSQL         | localhost:5432         | Application database (Docker)        |
| DSB PostgreSQL     | localhost:5433         | Benchmarking database (Docker)       |

#### Database Details

**Application PostgreSQL** (port 5432):
- User: `querytorque`
- Password: `querytorque_dev`
- Database: `querytorque`

**DSB PostgreSQL** (port 5433, for benchmarking):
- User: `jakc9`
- Password: `jakc9`
- Databases: `dsb_sf10` (SF10), `dsb_sf10_sample` (1% sample), `tpch10` (TPC-H SF10)

## CLI Commands

### Qt-SQL CLI

```bash
# Analyze a SQL file
qt-sql analyze query.sql

# Analyze with specific database dialect
qt-sql analyze query.sql --dialect postgresql

# Analyze with JSON output
qt-sql analyze query.sql --format json

# Analyze directory of SQL files
qt-sql analyze ./sql-scripts/ --recursive

# Generate optimization report
qt-sql report query.sql --output report.html

# Check against quality gate
qt-sql check query.sql --threshold 70

# Dry run with explain plan
qt-sql explain query.sql --connection postgres://...
```

## Testing

```bash
# Run all tests
pytest

# Run with coverage
pytest --cov=packages --cov-report=html

# Run specific package tests
pytest packages/qt-sql/tests/
pytest packages/qt-shared/tests/

# Run specific test file
pytest packages/qt-sql/tests/test_analyzers.py -v
```

## Linting and Formatting

```bash
# Lint all packages
ruff check packages

# Fix auto-fixable issues
ruff check packages --fix

# Format code
ruff format packages

# Type checking
mypy packages
```

## Database Migrations

```bash
# Create a new migration
alembic revision --autogenerate -m "Add new column"

# Apply migrations
alembic upgrade head

# Rollback one migration
alembic downgrade -1

# View migration history
alembic history

# View current revision
alembic current
```

## API Documentation

When running locally, API documentation is available at:

- Qt-SQL: http://localhost:8002/docs (Swagger UI)
- Qt-SQL: http://localhost:8002/redoc (ReDoc)

## Configuration

See `.env.example` for all available configuration options. Key settings:

| Variable | Description | Default |
|----------|-------------|---------|
| `QT_DB_HOST` | PostgreSQL host | localhost |
| `QT_DB_PASSWORD` | Database password | - |
| `AUTH0_DOMAIN` | Auth0 tenant domain | - |
| `ANTHROPIC_API_KEY` | Claude API key | - |
| `STRIPE_SECRET_KEY` | Stripe secret key | - |

## License

MIT License - see LICENSE file for details.
