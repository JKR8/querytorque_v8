# PostgreSQL Setup and Configuration

This document centralizes all PostgreSQL connection and setup information for QueryTorque V8.

## Two PostgreSQL Instances

QueryTorque uses two separate PostgreSQL instances:

### 1. Docker Compose PostgreSQL (Development/API)
**Primary database for the QueryTorque application services.**

- **Container Name**: `qt-postgres`
- **Image**: PostgreSQL 16
- **Port**: `5432`
- **Host**: `localhost` (Docker) or `postgres` (Docker Compose network)
- **Username**: `querytorque`
- **Password**: `querytorque_dev` (default, override with `QT_DB_PASSWORD` env var)
- **Database**: `querytorque`
- **Connection String**:
  ```
  postgresql://querytorque:querytorque_dev@localhost:5432/querytorque
  ```

**Configuration**: `docker-compose.yml` (lines 4-22)
- Persistent volume: `postgres_data:/var/lib/postgresql/data`
- Init script: `./scripts/init-db.sql`
- Health check: Enabled with 10s intervals

### 2. Docker DSB PostgreSQL (Benchmarking)
**Docker container for running DSB (Decision Support Benchmark) queries.**

- **Container Name**: `qt-dsb-postgres`
- **Image**: PostgreSQL 16
- **Port**: `5433` (mapped from container 5432)
- **Host**: `localhost` (Docker) or `dsb-postgres` (Docker Compose network)
- **Username**: `jakc9`
- **Password**: `jakc9`
- **Database**: `dsb_sf10`
- **Data Directory**: `/mnt/d/pgdata` (bind mount from host)
- **Connection String**:
  ```
  postgresql://jakc9:jakc9@localhost:5433/dsb_sf10
  ```

**Used by**:
- `research/ado/validate_dsb_pg.py` - DSB query validation (line 41)
- `research/scripts/dsb_collect_rewrites.py` - DSB rewrite collection

**Scale Factor**: SF10 (10GB dataset)
**Sample DB**: `dsb_sf10_sample` (1%, for testing)

---

## Quick Start

### Start Docker Compose PostgreSQL

```bash
# Start only PostgreSQL
docker-compose up -d postgres

# Wait for health check
docker-compose ps

# Verify connection
psql -h localhost -U querytorque -d querytorque
```

### Start Docker DSB PostgreSQL

```bash
# Start DSB PostgreSQL container only
docker-compose up -d dsb-postgres

# Wait for health check
docker-compose ps

# Check container logs
docker-compose logs -f dsb-postgres

# Stop DSB PostgreSQL
docker-compose stop dsb-postgres

# Remove container (keeps data volume)
docker-compose down dsb-postgres
```

### Access Local DSB PostgreSQL

```bash
# Connect to DSB database
psql -h 127.0.0.1 -p 5433 -U jakc9 -d dsb_sf10

# Query list
\dt           # Show tables
SELECT COUNT(*) FROM store_sales;  # Check data
```

### Environment Variables

For Docker Compose, override credentials:

```bash
export QT_DB_PASSWORD=your_secure_password
docker-compose up -d postgres
```

---

## Connection from Python

### Using psycopg2
```python
import psycopg2

# Docker Compose connection
conn = psycopg2.connect(
    host="localhost",
    port=5432,
    user="querytorque",
    password="querytorque_dev",
    database="querytorque"
)

# DSB connection
dsb_conn = psycopg2.connect(
    host="127.0.0.1",
    port=5433,
    user="jakc9",
    password="jakc9",
    database="dsb_sf10"
)
```

### Using SQLAlchemy
```python
from sqlalchemy import create_engine

# Docker Compose
engine = create_engine(
    "postgresql://querytorque:querytorque_dev@localhost:5432/querytorque"
)

# DSB
dsb_engine = create_engine(
    "postgresql://jakc9:jakc9@127.0.0.1:5433/dsb_sf10"
)
```

### QueryTorque Factory Pattern
```python
from qt_sql.execution.factory import create_executor_from_dsn

# Automatically detects PostgreSQL by DSN prefix
executor = create_executor_from_dsn(
    "postgresql://jakc9:jakc9@127.0.0.1:5433/dsb_sf10"
)
```

---

## Database Initialization

### Docker Compose Database
Initialized via `scripts/init-db.sql` on first startup.

Run migrations:
```bash
# From repository root
alembic upgrade head
```

### DSB Database
Manual setup required. Create database:

```bash
# Create database
createdb -h 127.0.0.1 -U jakc9 dsb_sf10

# Load DSB schema and data
psql -h 127.0.0.1 -U jakc9 -d dsb_sf10 < dsb_schema.sql
```

---

## Benchmarking & Validation

### Run DSB Query Validation
```bash
cd research/ado

# Validate optimizations on DSB SF10
python validate_dsb_pg.py --round round_01

# With specific query
python validate_dsb_pg.py --round round_01 --query query001_multi

# 5x trimmed mean validation
python validate_dsb_pg.py --round round_01 --runs 5
```

### Validation Rules (CRITICAL)
**Only 2 valid ways to validate query speedup:**
1. **3x runs**: Run 3 times, discard 1st (warmup), average last 2
2. **5x trimmed mean**: Run 5 times, remove min/max outliers, average remaining 3

**NEVER use single-run timing** - results are unreliable.

---

## Troubleshooting

### Connection Refused
```
Error: could not connect to server: Connection refused
```
**Solutions:**
- Docker Compose: `docker-compose up -d postgres` (start container)
- Local DSB: Ensure PostgreSQL service is running on port 5433
- Check firewall/networking: `netstat -an | grep 5433`

### Authentication Failed
```
Error: FATAL: password authentication failed
```
**Solutions:**
- Verify credentials in connection string
- Docker Compose: Check `QT_DB_PASSWORD` env var
- DSB: Default is `jakc9:jakc9`

### Port Already in Use
```
Error: bind() failed: Address already in use
```
**Solutions:**
- Find process: `lsof -i :5432` (Docker) or `lsof -i :5433` (DSB)
- Kill process: `kill -9 <PID>`
- Use different port: Update `docker-compose.yml` or PostgreSQL config

### Database Does Not Exist
```
Error: database "dsb_sf10" does not exist
```
**Solution:** Create database or restore from backup

---

## Performance Tuning

### Docker Compose PostgreSQL
Edit `docker-compose.yml` to add environment variables:

```yaml
postgres:
  environment:
    POSTGRES_INITDB_ARGS: "-c shared_buffers=256MB -c work_mem=16MB"
```

### Local DSB PostgreSQL
Edit PostgreSQL config (`postgresql.conf`):

```conf
shared_buffers = 256MB
work_mem = 16MB
random_page_cost = 1.1
effective_cache_size = 1GB
```

Then restart PostgreSQL.

---

## References

- **Docker Compose Config**: `docker-compose.yml`
- **Database Migrations**: `alembic/versions/`
- **Validation Script**: `research/ado/validate_dsb_pg.py`
- **Rewrite Collection**: `research/scripts/dsb_collect_rewrites.py`
- **PostgreSQL Executor**: `packages/qt-sql/qt_sql/execution/postgres_executor.py`

