"""QueryTorque SQL API.

FastAPI backend for the qt_sql optimization pipeline.

Routes:
    POST /api/sql/optimize  - Run 7-phase optimization pipeline
    POST /api/sql/validate  - Validate candidate SQL equivalence + timing
    GET  /health            - Health check

    Database session routes:
    POST   /api/database/connect/duckdb       - Upload fixture file
    POST   /api/database/connect/duckdb/quick - Connect via server path
    GET    /api/database/status/{session_id}   - Connection status
    DELETE /api/database/disconnect/{session_id} - Disconnect
    POST   /api/database/execute/{session_id}  - Execute SQL
    POST   /api/database/explain/{session_id}  - EXPLAIN plan
    GET    /api/database/schema/{session_id}   - Schema info
"""

import logging
import math
import os
import tempfile
import time
import uuid
from typing import Optional, Literal

from fastapi import FastAPI, HTTPException, UploadFile, File, Form, status
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field

from qt_shared.config import get_settings

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Create FastAPI app
app = FastAPI(
    title="QueryTorque SQL API",
    description="SQL optimization pipeline API — 7-phase LLM-powered query rewriting",
    version="2.0.0",
    docs_url="/api/docs",
    redoc_url="/api/redoc",
    openapi_url="/api/openapi.json",
)

# CORS configuration
_settings = get_settings()
_origins = [origin.strip() for origin in _settings.cors_origins.split(",")]

app.add_middleware(
    CORSMiddleware,
    allow_origins=_origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# ============================================
# Request/Response Models — Optimization
# ============================================

class OptimizeRequest(BaseModel):
    """Request body for pipeline-backed SQL optimization."""
    sql: str = Field(..., description="SQL query to optimize", min_length=1)
    dsn: str = Field(..., description="Database DSN (duckdb:///path.db or postgres://user:pass@host:port/db)")
    mode: Literal["swarm", "expert", "oneshot"] = Field(
        default="expert",
        description="Optimization mode: swarm (4-worker fan-out), expert (iterative), oneshot (single call)"
    )
    query_id: Optional[str] = Field(default=None, description="Query identifier for traceability")
    session_id: Optional[str] = Field(default=None, description="Database session ID — required for DuckDB uploaded fixtures")
    max_iterations: int = Field(default=3, ge=1, le=10, description="Max optimization iterations")
    target_speedup: float = Field(default=1.10, ge=1.0, description="Target speedup ratio to achieve")


class WorkerResultResponse(BaseModel):
    """Per-worker optimization result."""
    worker_id: int
    strategy: str = ""
    examples_used: list[str] = []
    optimized_sql: str = ""
    speedup: float = 1.0
    status: str = ""
    transforms: list[str] = []
    error_message: Optional[str] = None


class OptimizeResponse(BaseModel):
    """Response from the 7-phase optimization pipeline."""
    status: str = Field(..., description="WIN | IMPROVED | NEUTRAL | REGRESSION | ERROR")
    speedup: float = Field(default=1.0, description="Best speedup ratio achieved")
    speedup_type: str = Field(default="measured", description="measured | vs_timeout_ceiling | both_timeout")
    validation_confidence: str = Field(default="high", description="high | row_count_only | zero_row_unverified")
    optimized_sql: Optional[str] = Field(default=None, description="Best optimized SQL")
    original_sql: str = Field(default="", description="Original SQL echoed back")
    transforms: list[str] = Field(default_factory=list, description="Transforms applied in best candidate")
    workers: list[WorkerResultResponse] = Field(default_factory=list, description="Per-worker results (swarm mode)")
    query_id: str = Field(default="", description="Query identifier")
    error: Optional[str] = Field(default=None, description="Error message if status=ERROR")
    n_iterations: int = Field(default=0, description="Number of optimization iterations run")
    n_api_calls: int = Field(default=0, description="Number of LLM API calls made")


# ============================================
# Request/Response Models — Health
# ============================================

class HealthResponse(BaseModel):
    """Health check response."""
    status: str
    version: str
    llm_configured: bool
    llm_provider: Optional[str] = None


# ============================================
# Routes — Core Pipeline
# ============================================

@app.get("/health", response_model=HealthResponse, tags=["Health"])
@app.get("/api/health", response_model=HealthResponse, tags=["Health"], include_in_schema=False)
async def health_check():
    """Health check endpoint."""
    settings = get_settings()
    return HealthResponse(
        status="ok",
        version="2.0.0",
        llm_configured=settings.has_llm_provider,
        llm_provider=settings.llm_provider if settings.llm_provider else None,
    )


@app.post("/api/sql/optimize", response_model=OptimizeResponse, tags=["SQL"])
async def optimize_sql(request: OptimizeRequest):
    """Run the 7-phase optimization pipeline on a SQL query.

    Phases: Context Gathering -> Knowledge Retrieval -> Intelligence Handoff ->
    Prompt Generation -> LLM Inference -> Response Processing -> Validation & Benchmarking

    - **sql**: The SQL query to optimize
    - **dsn**: Database connection string (DuckDB or PostgreSQL)
    - **mode**: swarm (4 workers), expert (iterative), or oneshot (single call)
    - **query_id**: Optional identifier for traceability
    """
    try:
        from qt_sql.pipeline import Pipeline
        from qt_sql.schemas import OptimizationMode
    except ImportError as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Pipeline module not available: {e}"
        )

    mode_map = {
        "swarm": OptimizationMode.SWARM,
        "expert": OptimizationMode.EXPERT,
        "oneshot": OptimizationMode.ONESHOT,
    }
    mode = mode_map[request.mode]
    query_id = request.query_id or f"api_{uuid.uuid4().hex[:8]}"

    logger.info(
        "Optimizing SQL via pipeline",
        extra={
            "query_id": query_id,
            "mode": request.mode,
            "dsn_type": "postgres" if "postgres" in request.dsn else "duckdb",
            "sql_length": len(request.sql),
        }
    )

    # For DuckDB uploaded fixtures, export session data to a temp .duckdb file
    # so the pipeline can access the actual tables (not an empty :memory: DB)
    dsn = request.dsn
    tmp_duckdb_path = None
    if request.session_id and request.session_id in db_sessions:
        executor = db_sessions[request.session_id]
        if type(executor).__name__ == "DuckDBExecutor":
            try:
                import duckdb
                conn = executor._ensure_connected()
                tmp_duckdb = tempfile.NamedTemporaryFile(suffix='.duckdb', delete=False)
                tmp_duckdb_path = tmp_duckdb.name
                tmp_duckdb.close()
                conn.execute(f"ATTACH '{tmp_duckdb_path}' AS export_db")
                conn.execute("COPY FROM DATABASE memory TO export_db")
                conn.execute("DETACH export_db")
                dsn = tmp_duckdb_path  # Pipeline uses file path for DuckDB
            except Exception as e:
                logger.warning(f"Failed to export DuckDB session, using DSN as-is: {e}")

    try:
        pipeline = Pipeline(dsn=dsn)
        result = pipeline.run_optimization_session(
            query_id=query_id,
            sql=request.sql,
            mode=mode,
            max_iterations=request.max_iterations,
            target_speedup=request.target_speedup,
        )

        # Build per-worker responses if available
        workers = []
        if hasattr(result, 'iterations') and result.iterations:
            for iteration in result.iterations:
                if hasattr(iteration, 'workers'):
                    for w in iteration.workers:
                        workers.append(WorkerResultResponse(
                            worker_id=w.worker_id,
                            strategy=getattr(w, 'strategy', ''),
                            examples_used=getattr(w, 'examples_used', []),
                            optimized_sql=getattr(w, 'optimized_sql', ''),
                            speedup=getattr(w, 'speedup', 1.0),
                            status=getattr(w, 'status', ''),
                            transforms=getattr(w, 'transforms', []),
                            error_message=getattr(w, 'error_message', None),
                        ))

        return OptimizeResponse(
            status=result.status,
            speedup=result.best_speedup,
            optimized_sql=result.best_sql,
            original_sql=request.sql,
            transforms=getattr(result, 'best_transforms', []),
            workers=workers,
            query_id=query_id,
            n_iterations=getattr(result, 'n_iterations', 0),
            n_api_calls=getattr(result, 'n_api_calls', 0),
        )

    except Exception as e:
        logger.exception("Pipeline optimization failed")
        return OptimizeResponse(
            status="ERROR",
            original_sql=request.sql,
            query_id=query_id,
            error=str(e),
        )
    finally:
        # Clean up temp DuckDB export file
        if tmp_duckdb_path:
            try:
                os.unlink(tmp_duckdb_path)
            except Exception:
                pass


# ============================================
# Database Connection Endpoints
# ============================================

db_sessions: dict[str, object] = {}  # DuckDBExecutor or PostgresExecutor
db_fixture_paths: dict[str, str] = {}  # session_id -> temp fixture file path (DuckDB only)


class DatabaseConnectResponse(BaseModel):
    session_id: str
    connected: bool
    type: str
    details: Optional[str] = None
    error: Optional[str] = None


class DatabaseStatusResponse(BaseModel):
    connected: bool
    type: Optional[str] = None
    details: Optional[str] = None


class SchemaColumn(BaseModel):
    name: str
    type: str
    nullable: Optional[bool] = None


class SchemaResponse(BaseModel):
    session_id: str
    tables: dict[str, list[SchemaColumn]]
    error: Optional[str] = None


class ExecuteRequest(BaseModel):
    sql: str
    limit: int = Field(default=100, ge=1, le=10000)


class QueryResultResponse(BaseModel):
    columns: list[str]
    column_types: list[str]
    rows: list[list]
    row_count: int
    execution_time_ms: float
    truncated: bool = False
    error: Optional[str] = None


class ExplainRequest(BaseModel):
    sql: str
    analyze: bool = True


class ExecutionPlanResponse(BaseModel):
    success: bool
    plan_text: Optional[str] = None
    plan_json: Optional[dict] = None
    plan_tree: Optional[list] = None
    execution_time_ms: Optional[float] = None
    total_cost: Optional[float] = None
    bottleneck: Optional[dict] = None
    warnings: Optional[list[str]] = None
    error: Optional[str] = None


@app.post("/api/database/connect/duckdb", response_model=DatabaseConnectResponse, tags=["Database"])
async def connect_duckdb(fixture_file: UploadFile = File(...)):
    """Connect to DuckDB with a fixture file (SQL, CSV, or Parquet)."""
    from qt_sql.execution import DuckDBExecutor

    session_id = str(uuid.uuid4())[:12]

    try:
        content = await fixture_file.read()
        suffix = os.path.splitext(fixture_file.filename or "")[1] or ".sql"
        with tempfile.NamedTemporaryFile(mode='wb', suffix=suffix, delete=False) as tmp:
            tmp.write(content)
            tmp_path = tmp.name

        executor = DuckDBExecutor(":memory:")
        executor.connect()

        if suffix.lower() == ".sql":
            executor.execute_script(content.decode('utf-8'))
        elif suffix.lower() == ".csv":
            executor._ensure_connected().execute(
                f"CREATE TABLE data AS SELECT * FROM read_csv_auto('{tmp_path}')"
            )
        elif suffix.lower() in (".parquet", ".pq"):
            executor._ensure_connected().execute(
                f"CREATE TABLE data AS SELECT * FROM read_parquet('{tmp_path}')"
            )
        else:
            executor.execute_script(content.decode('utf-8'))

        db_sessions[session_id] = executor
        db_fixture_paths[session_id] = tmp_path  # Keep file for pipeline reuse
        schema = executor.get_schema_info(include_row_counts=False)
        table_count = len(schema.get("tables", []))

        return DatabaseConnectResponse(
            session_id=session_id, connected=True, type="duckdb",
            details=f"Loaded {table_count} table(s) from {fixture_file.filename}",
        )

    except Exception as e:
        logger.exception("DuckDB connection failed")
        return DatabaseConnectResponse(
            session_id=session_id, connected=False, type="duckdb", error=str(e),
        )


class PostgresConnectRequest(BaseModel):
    connection_string: str = Field(..., description="PostgreSQL connection string (postgres://user:pass@host:port/db)")


@app.post("/api/database/connect/postgres", response_model=DatabaseConnectResponse, tags=["Database"])
async def connect_postgres(request: PostgresConnectRequest):
    """Connect to PostgreSQL with a connection string."""
    from qt_sql.execution import PostgresExecutor
    from urllib.parse import urlparse

    session_id = str(uuid.uuid4())[:12]

    try:
        parsed = urlparse(request.connection_string)
        executor = PostgresExecutor(
            host=parsed.hostname or "localhost",
            port=parsed.port or 5432,
            database=parsed.path.lstrip("/") or "postgres",
            user=parsed.username or "postgres",
            password=parsed.password or "",
        )
        executor.connect()

        db_sessions[session_id] = executor
        schema = executor.get_schema_info(include_row_counts=False)
        table_count = len(schema.get("tables", []))

        return DatabaseConnectResponse(
            session_id=session_id, connected=True, type="postgres",
            details=f"Connected to {parsed.hostname}:{parsed.port}/{parsed.path.lstrip('/')} ({table_count} tables)",
        )

    except Exception as e:
        logger.exception("PostgreSQL connection failed")
        return DatabaseConnectResponse(
            session_id=session_id, connected=False, type="postgres", error=str(e),
        )


@app.post("/api/database/connect/duckdb/quick", response_model=DatabaseConnectResponse, tags=["Database"])
async def connect_duckdb_quick(fixture_path: str = Form(...)):
    """Connect to DuckDB with a fixture path on the server."""
    from qt_sql.execution import DuckDBExecutor

    session_id = str(uuid.uuid4())[:12]

    try:
        executor = DuckDBExecutor(":memory:")
        executor.connect()

        suffix = os.path.splitext(fixture_path)[1].lower()

        if suffix == ".sql":
            with open(fixture_path, 'r') as f:
                executor.execute_script(f.read())
        elif suffix == ".csv":
            executor._ensure_connected().execute(
                f"CREATE TABLE data AS SELECT * FROM read_csv_auto('{fixture_path}')"
            )
        elif suffix in (".parquet", ".pq"):
            executor._ensure_connected().execute(
                f"CREATE TABLE data AS SELECT * FROM read_parquet('{fixture_path}')"
            )
        elif suffix == ".duckdb":
            executor.close()
            executor = DuckDBExecutor(fixture_path, read_only=True)
            executor.connect()
        else:
            with open(fixture_path, 'r') as f:
                executor.execute_script(f.read())

        db_sessions[session_id] = executor
        schema = executor.get_schema_info(include_row_counts=False)
        table_count = len(schema.get("tables", []))

        return DatabaseConnectResponse(
            session_id=session_id, connected=True, type="duckdb",
            details=f"Connected to {fixture_path} ({table_count} tables)",
        )

    except Exception as e:
        logger.exception("DuckDB quick connection failed")
        return DatabaseConnectResponse(
            session_id=session_id, connected=False, type="duckdb", error=str(e),
        )


@app.get("/api/database/status/{session_id}", response_model=DatabaseStatusResponse, tags=["Database"])
async def get_database_status(session_id: str):
    """Get database connection status."""
    executor = db_sessions.get(session_id)
    if not executor:
        return DatabaseStatusResponse(connected=False)
    db_type = "postgres" if type(executor).__name__ == "PostgresExecutor" else "duckdb"
    return DatabaseStatusResponse(connected=True, type=db_type, details="Connected")


@app.delete("/api/database/disconnect/{session_id}", tags=["Database"])
async def disconnect_database(session_id: str):
    """Disconnect from database and clean up session."""
    executor = db_sessions.pop(session_id, None)
    if executor:
        try:
            executor.close()
        except Exception:
            pass
    # Clean up DuckDB fixture file
    fixture_path = db_fixture_paths.pop(session_id, None)
    if fixture_path:
        try:
            os.unlink(fixture_path)
        except Exception:
            pass
    return {"success": True}


@app.post("/api/database/execute/{session_id}", response_model=QueryResultResponse, tags=["Database"])
async def execute_query(session_id: str, request: ExecuteRequest):
    """Execute a SQL query and return results."""
    executor = db_sessions.get(session_id)
    if not executor:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Database session not found")

    try:
        start = time.time()
        sql = request.sql.strip().rstrip(';')
        has_limit = 'limit' in sql.lower().split()[-3:] if len(sql.split()) > 3 else False

        if not has_limit and request.limit:
            sql = f"SELECT * FROM ({sql}) AS _subq LIMIT {request.limit}"

        is_postgres = type(executor).__name__ == "PostgresExecutor"

        if is_postgres:
            # PostgresExecutor returns list[dict]
            raw_rows = executor.execute(sql)
            if raw_rows:
                columns = list(raw_rows[0].keys())
                column_types = [type(v).__name__ for v in raw_rows[0].values()]
                rows = [list(r.values()) for r in raw_rows]
            else:
                # Empty result — still fetch column metadata via LIMIT 0
                try:
                    meta_rows = executor.execute(f"SELECT * FROM ({request.sql.strip().rstrip(';')}) AS _meta LIMIT 0")
                    # meta_rows will be empty but we need column names from the executor's cursor
                    # Fall back to running with cursor directly
                    conn = executor._ensure_connected()
                    with conn.cursor() as cur:
                        cur.execute(f"SELECT * FROM ({request.sql.strip().rstrip(';')}) AS _meta LIMIT 0")
                        if cur.description:
                            columns = [desc[0] for desc in cur.description]
                            column_types = [str(desc[1]) for desc in cur.description]
                        else:
                            columns, column_types = [], []
                    conn.rollback()  # Don't leave transaction open
                except Exception:
                    columns, column_types = [], []
                rows = []
        else:
            # DuckDB path
            conn = executor._ensure_connected()
            result = conn.execute(sql)
            columns = [desc[0] for desc in result.description] if result.description else []
            column_types = [desc[1] for desc in result.description] if result.description else []
            rows = result.fetchall()

        execution_time = (time.time() - start) * 1000

        def convert_value(v):
            if v is None:
                return None
            if isinstance(v, (int, float, str, bool)):
                return v
            return str(v)

        serialized_rows = [[convert_value(v) for v in row] for row in rows]

        return QueryResultResponse(
            columns=columns, column_types=[str(t) for t in column_types],
            rows=serialized_rows, row_count=len(rows),
            execution_time_ms=round(execution_time, 2),
            truncated=len(rows) >= request.limit if request.limit else False,
        )

    except Exception as e:
        logger.exception("Query execution failed")
        return QueryResultResponse(
            columns=[], column_types=[], rows=[], row_count=0,
            execution_time_ms=0, error=str(e),
        )


@app.post("/api/database/explain/{session_id}", response_model=ExecutionPlanResponse, tags=["Database"])
async def explain_query(session_id: str, request: ExplainRequest):
    """Get execution plan for a SQL query."""
    from qt_sql.execution import DuckDBPlanParser
    from qt_sql.execution.postgres_plan_parser import PostgresPlanParser

    executor = db_sessions.get(session_id)
    if not executor:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Database session not found")

    try:
        plan_json = executor.explain(request.sql, analyze=request.analyze)
        if type(executor).__name__ == "PostgresExecutor":
            parser = PostgresPlanParser()
        else:
            parser = DuckDBPlanParser()
        analysis = parser.parse(plan_json)

        return ExecutionPlanResponse(
            success=True, plan_json=plan_json, plan_tree=analysis.plan_tree,
            execution_time_ms=analysis.execution_time_ms, total_cost=analysis.total_cost,
            bottleneck=analysis.bottleneck, warnings=analysis.warnings,
        )

    except Exception as e:
        logger.exception("Explain failed")
        return ExecutionPlanResponse(success=False, error=str(e))


class AuditResponse(BaseModel):
    success: bool
    plan_tree: Optional[list] = None
    bottleneck: Optional[dict] = None
    pathology_name: Optional[str] = None
    execution_time_ms: Optional[float] = None
    total_cost: Optional[float] = None
    warnings: list[str] = []
    error: Optional[str] = None


@app.post("/api/database/audit/{session_id}", response_model=AuditResponse, tags=["Database"])
async def audit_query(session_id: str, request: ExplainRequest):
    """Audit a SQL query — runs EXPLAIN ANALYZE, identifies bottleneck and pathology.

    This is the free tier: no LLM calls, just plan analysis.
    """
    from qt_sql.execution import DuckDBPlanParser
    from qt_sql.execution.postgres_plan_parser import PostgresPlanParser

    executor = db_sessions.get(session_id)
    if not executor:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Database session not found")

    try:
        plan_json = executor.explain(request.sql, analyze=request.analyze)
        if type(executor).__name__ == "PostgresExecutor":
            parser = PostgresPlanParser()
        else:
            parser = DuckDBPlanParser()
        analysis = parser.parse(plan_json)

        # Build human-readable pathology name from bottleneck
        pathology_name = None
        if analysis.bottleneck:
            op = analysis.bottleneck.get("operator", "Unknown")
            cost = analysis.bottleneck.get("cost_pct", 0)
            detail = analysis.bottleneck.get("details", "")
            suggestion = analysis.bottleneck.get("suggestion", "")

            if detail:
                pathology_name = f"{op} on {detail} — {cost:.0f}% of query cost"
            else:
                pathology_name = f"{op} — {cost:.0f}% of query cost"

            if suggestion:
                pathology_name += f". {suggestion}"

        return AuditResponse(
            success=True,
            plan_tree=analysis.plan_tree,
            bottleneck=analysis.bottleneck,
            pathology_name=pathology_name,
            execution_time_ms=analysis.execution_time_ms,
            total_cost=analysis.total_cost,
            warnings=analysis.warnings or [],
        )

    except Exception as e:
        logger.exception("Audit failed")
        return AuditResponse(success=False, error=str(e))


@app.get("/api/database/schema/{session_id}", response_model=SchemaResponse, tags=["Database"])
async def get_schema(session_id: str):
    """Get database schema (tables and columns)."""
    executor = db_sessions.get(session_id)
    if not executor:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Database session not found")

    try:
        schema = executor.get_schema_info(include_row_counts=True)
        tables_dict: dict[str, list[SchemaColumn]] = {}
        for table in schema.get("tables", []):
            table_name = table.get("name", "")
            columns = [
                SchemaColumn(name=col.get("name", ""), type=col.get("type", ""), nullable=col.get("nullable"))
                for col in table.get("columns", [])
            ]
            tables_dict[table_name] = columns

        return SchemaResponse(session_id=session_id, tables=tables_dict)

    except Exception as e:
        logger.exception("Schema fetch failed")
        return SchemaResponse(session_id=session_id, tables={}, error=str(e))


# ============================================
# SQL Validation Endpoints
# ============================================

class ValidateSQLRequest(BaseModel):
    """Request for SQL validation."""
    original_sql: str = Field(..., description="Original SQL query")
    optimized_sql: str = Field(..., description="Optimized SQL query")
    mode: Literal["sample", "full"] = Field(default="sample", description="Validation mode")
    schema_sql: Optional[str] = Field(default=None, description="Optional schema SQL for in-memory validation")
    session_id: Optional[str] = Field(default=None, description="Database session ID")
    limit_strategy: Literal["add_order", "remove_limit"] = Field(
        default="add_order", description="Strategy for LIMIT without ORDER BY"
    )


class ValueDifferenceResponse(BaseModel):
    """A value difference between queries."""
    row_index: int
    column: str
    original_value: Optional[str] = None
    optimized_value: Optional[str] = None


class ValidateSQLResponse(BaseModel):
    """Response from SQL validation."""
    status: Literal["pass", "fail", "warn", "error"]
    mode: str
    row_counts: dict[str, int]
    row_counts_match: bool
    timing: dict[str, float]
    speedup: float
    cost: dict[str, float]
    cost_reduction_pct: float
    values_match: bool
    checksum_match: Optional[bool] = None
    value_differences: list[ValueDifferenceResponse] = []
    limit_detected: bool = False
    limit_strategy_applied: Optional[str] = None
    errors: list[str] = []
    warnings: list[str] = []


@app.post("/api/sql/validate", response_model=ValidateSQLResponse, tags=["SQL"])
async def validate_sql(request: ValidateSQLRequest):
    """Validate that optimized SQL is equivalent to original.

    Compares row counts, checksums, and values. Measures timing with
    warmup + measurement pattern for accurate speedup comparison.
    """
    try:
        from qt_sql.validation import (
            SQLValidator,
            ValidationMode,
            ValidationStatus,
            LimitStrategy,
        )
    except ImportError as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Validation module not available: {e}"
        )

    database = ":memory:"
    validation_mode = ValidationMode.FULL if request.mode == "full" else ValidationMode.SAMPLE
    limit_strat = (
        LimitStrategy.REMOVE_LIMIT if request.limit_strategy == "remove_limit"
        else LimitStrategy.ADD_ORDER
    )

    try:
        with SQLValidator(
            database=database,
            mode=validation_mode,
            limit_strategy=limit_strat,
        ) as validator:
            result = validator.validate(
                request.original_sql,
                request.optimized_sql,
                request.schema_sql,
            )

        value_diffs = [
            ValueDifferenceResponse(
                row_index=d.row_index, column=d.column,
                original_value=str(d.original_value) if d.original_value is not None else None,
                optimized_value=str(d.optimized_value) if d.optimized_value is not None else None,
            )
            for d in result.value_differences[:10]
        ]

        def _safe_float(v: float, default: float = 0.0) -> float:
            """Replace inf/nan with a safe default for JSON serialization."""
            return default if (math.isinf(v) or math.isnan(v)) else v

        return ValidateSQLResponse(
            status=result.status.value,
            mode=result.mode.value,
            row_counts={"original": result.original_row_count, "optimized": result.optimized_row_count},
            row_counts_match=result.row_counts_match,
            timing={"original_ms": round(result.original_timing_ms, 2), "optimized_ms": round(result.optimized_timing_ms, 2)},
            speedup=round(_safe_float(result.speedup, 1.0), 2),
            cost={"original": round(_safe_float(result.original_cost), 2), "optimized": round(_safe_float(result.optimized_cost), 2)},
            cost_reduction_pct=round(_safe_float(result.cost_reduction_pct), 2),
            values_match=result.values_match,
            checksum_match=result.checksum_match,
            value_differences=value_diffs,
            limit_detected=result.limit_detected,
            limit_strategy_applied=result.limit_strategy_applied.value if result.limit_strategy_applied else None,
            errors=result.errors,
            warnings=result.warnings,
        )

    except Exception as e:
        logger.exception("SQL validation failed")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Validation failed: {str(e)}"
        )


# Factory function for testing
def create_app() -> FastAPI:
    """Factory function for creating the app."""
    return app


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8002)
