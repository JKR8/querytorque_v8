# QueryTorque

QueryTorque is a comprehensive query optimization platform for SQL and DAX. It analyzes queries, identifies performance issues, and provides actionable recommendations with AI-powered explanations.

## Architecture

```
                                    QueryTorque Architecture

    +------------------+     +------------------+
    |    Qt-SQL UI     |     |    Qt-DAX UI     |
    |   (React/Vite)   |     |   (React/Vite)   |
    +--------+---------+     +--------+---------+
             |                        |
             v                        v
    +------------------+     +------------------+
    |   Qt-SQL API     |     |   Qt-DAX API     |
    |   (FastAPI)      |     |   (FastAPI)      |
    |   Port: 8002     |     |   Port: 8003     |
    +--------+---------+     +--------+---------+
             |                        |
             |    +-------------+     |
             +--->|  Qt-Shared  |<----+
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

  qt-dax/          # DAX optimization product (Python)
    qt_dax/
      analyzers/   # DAX analysis rules
      parsers/     # VPAX parsing
      connections/ # Power BI connections
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
pip install -e packages/qt-dax[dev]
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

# Terminal 2: Qt-DAX API
cd packages/qt-dax
uvicorn qt_dax.api.main:app --reload --port 8003
```

### Docker Compose (Full Stack)

```bash
# Start all services
docker-compose up -d

# View logs
docker-compose logs -f

# Stop all services
docker-compose down
```

### Service Endpoints

| Service     | URL                    | Description              |
|-------------|------------------------|--------------------------|
| Qt-Calcite  | http://localhost:8001  | SQL parsing service      |
| Qt-SQL API  | http://localhost:8002  | SQL optimization API     |
| Qt-DAX API  | http://localhost:8003  | DAX optimization API     |
| PostgreSQL  | localhost:5432         | Database                 |

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

### Qt-DAX CLI

```bash
# Analyze a VPAX file
qt-dax analyze model.vpax

# Analyze with detailed output
qt-dax analyze model.vpax --verbose

# Analyze with JSON output
qt-dax analyze model.vpax --format json

# Generate optimization report
qt-dax report model.vpax --output report.html

# Analyze specific measures only
qt-dax analyze model.vpax --measures "Total Sales,Profit Margin"

# Check against quality gate
qt-dax check model.vpax --threshold 70

# Connect to Power BI service
qt-dax connect --workspace "Sales Analytics" --dataset "Sales Model"
```

## Testing

```bash
# Run all tests
pytest

# Run with coverage
pytest --cov=packages --cov-report=html

# Run specific package tests
pytest packages/qt-sql/tests/
pytest packages/qt-dax/tests/
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
- Qt-DAX: http://localhost:8003/docs (Swagger UI)
- Qt-DAX: http://localhost:8003/redoc (ReDoc)

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
