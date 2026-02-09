"""QueryTorque SQL API.

FastAPI backend for SQL analysis and optimization.

Routes:
    POST /api/sql/analyze  - Analyze SQL for anti-patterns
    POST /api/sql/optimize - Optimize SQL using LLM
    GET /health            - Health check
"""

import logging
from typing import Optional, Annotated, Literal

from fastapi import FastAPI, HTTPException, Depends, status
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field

from qt_shared.auth import OptionalUser, CurrentUser, UserContext
from qt_shared.config import get_settings, Settings

# V5 analyzer/renderer modules were removed in the ADO consolidation.
# These routes (/api/sql/analyze, /api/sql/optimize) need rewriting
# to use the new qt_sql pipeline. Guard imports so the module loads.
try:
    from qt_sql.analyzers.sql_antipattern_detector import SQLAntiPatternDetector
    from qt_sql.renderers import render_sql_report
    _HAS_V5_ANALYZER = True
except ImportError:
    SQLAntiPatternDetector = None  # type: ignore[assignment,misc]
    render_sql_report = None  # type: ignore[assignment]
    _HAS_V5_ANALYZER = False

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Create FastAPI app
app = FastAPI(
    title="QueryTorque SQL API",
    description="SQL analysis and optimization API",
    version="0.1.0",
    docs_url="/api/docs",
    redoc_url="/api/redoc",
    openapi_url="/api/openapi.json",
)


# CORS configuration - must be added at module level, not in startup event
_settings = get_settings()
_origins = [origin.strip() for origin in _settings.cors_origins.split(",")]

app.add_middleware(
    CORSMiddleware,
    allow_origins=_origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# Request/Response models
class AnalyzeRequest(BaseModel):
    """Request body for SQL analysis."""

    sql: str = Field(..., description="SQL query to analyze", min_length=1)
    dialect: str = Field(
        default="generic",
        description="SQL dialect (generic, snowflake, postgres, duckdb, tsql)"
    )
    include_structure: bool = Field(
        default=True,
        description="Include query structure analysis (CTEs, joins, tables)"
    )


class SQLIssueResponse(BaseModel):
    """A detected SQL issue."""

    rule_id: str
    name: str
    severity: str
    category: str
    penalty: int
    description: str
    location: Optional[str] = None
    match: Optional[str] = None
    explanation: str = ""
    suggestion: str = ""


class OpportunityResponse(BaseModel):
    """A detected optimization opportunity."""

    pattern_id: str
    pattern_name: str
    trigger: str
    rewrite_hint: str
    weight: int = 5  # 1-10 impact score
    weight_label: str = "Medium"  # Critical/High/Medium/Low/Minor


class SavingsEstimateResponse(BaseModel):
    """Estimated annual savings from optimization."""

    band_display: str  # e.g., "$500 - $2,000"
    low: float
    mid: float
    high: float
    total_weight: int
    opportunity_count: int


class AnalyzeResponse(BaseModel):
    """Response from SQL analysis."""

    score: int = Field(..., description="Overall quality score (0-100)")
    total_penalty: int = Field(..., description="Total penalty points")
    severity_counts: dict = Field(..., description="Issue counts by severity")
    issues: list[SQLIssueResponse] = Field(default_factory=list)
    opportunities: list[OpportunityResponse] = Field(
        default_factory=list,
        description="High-value optimization opportunities (fed to LLM optimizer)"
    )
    savings_estimate: Optional[SavingsEstimateResponse] = Field(
        default=None,
        description="Estimated annual savings if query runs daily"
    )
    query_structure: Optional[dict] = Field(
        default=None,
        description="Query structure information (CTEs, joins, etc.)"
    )
    html: str = Field(
        default="",
        description="HTML audit report"
    )
    original_sql: str = Field(
        default="",
        description="Original SQL query"
    )
    file_name: str = Field(
        default="query.sql",
        description="File name"
    )
    status: str = Field(
        default="pass",
        description="Overall status (pass, warn, fail, deny)"
    )


class OptimizeRequest(BaseModel):
    """Request body for SQL optimization."""

    sql: str = Field(..., description="SQL query to optimize", min_length=1)
    dialect: str = Field(default="generic", description="SQL dialect")
    provider: Optional[str] = Field(
        default=None,
        description="LLM provider override (anthropic, deepseek, openai, etc.)"
    )
    model: Optional[str] = Field(default=None, description="LLM model override")


class OptimizeResponse(BaseModel):
    """Response from SQL optimization."""

    original_sql: str
    optimized_sql: Optional[str] = None
    analysis: AnalyzeResponse
    llm_response: Optional[str] = None
    success: bool = True
    error: Optional[str] = None


class HealthResponse(BaseModel):
    """Health check response."""

    status: str
    version: str
    auth_enabled: bool
    llm_configured: bool
    mode: Literal['auto', 'manual'] = 'manual'
    llm_provider: Optional[str] = None


class ValidateManualRequest(BaseModel):
    """Request for manual validation."""
    original_sql: str = Field(..., description="Original SQL code")
    llm_response: str = Field(..., description="Raw LLM response (YAML or SQL)")
    dialect: str = Field(default="generic", description="SQL dialect")


class IssueDetailResponse(BaseModel):
    """Issue detail for validation response."""
    rule_id: Optional[str] = None
    severity: Optional[str] = None
    title: Optional[str] = None
    description: Optional[str] = None


class ValidateManualResponse(BaseModel):
    """Validation response matching frontend ValidationPreviewResponse."""
    session_id: str
    success: bool
    optimization_mode: str = "manual"
    syntax_status: str
    syntax_errors: list[str] = []
    schema_status: str = "skip"
    schema_violations: list[str] = []
    regression_status: str
    issues_fixed: list[IssueDetailResponse] = []
    new_issues: list[IssueDetailResponse] = []
    equivalence_status: str = "skip"
    original_code: str
    optimized_code: str
    diff_html: str = ""
    all_passed: bool
    errors: list[str] = []
    warnings: list[str] = []
    can_retry: bool = True
    retry_count: int = 0
    max_retries: int = 3
    llm_confidence: float = 0.0
    llm_explanation: str = ""


# Routes
@app.get("/health", response_model=HealthResponse, tags=["Health"])
async def health_check():
    """Health check endpoint.

    Returns service status and configuration information.
    """
    settings = get_settings()

    return HealthResponse(
        status="ok",
        version="0.1.0",
        auth_enabled=settings.auth_enabled,
        llm_configured=settings.has_llm_provider,
        mode='manual' if settings.is_manual_mode else 'auto',
        llm_provider=settings.llm_provider if settings.llm_provider else None,
    )


@app.post("/api/sql/analyze", response_model=AnalyzeResponse, tags=["SQL"])
async def analyze_sql(
    request: AnalyzeRequest,
):
    user = None  # Auth temporarily disabled for debugging
    """Analyze SQL for anti-patterns and issues.

    Performs static analysis on the provided SQL query and returns
    detected issues, severity levels, and improvement suggestions.

    - **sql**: The SQL query to analyze
    - **dialect**: SQL dialect for analysis (affects available rules)
    - **include_structure**: Whether to include query structure info
    """
    if not _HAS_V5_ANALYZER:
        raise HTTPException(
            status_code=status.HTTP_501_NOT_IMPLEMENTED,
            detail="Static analysis module not available (V5 analyzer was removed in consolidation)"
        )

    logger.info(
        "Analyzing SQL",
        extra={
            "user_id": user.user_id if user else "anonymous",
            "dialect": request.dialect,
            "sql_length": len(request.sql),
        }
    )

    try:
        # Run static analysis (includes AST-based opportunity detection)
        detector = SQLAntiPatternDetector(dialect=request.dialect)
        result = detector.analyze(request.sql, include_structure=request.include_structure)

        # Separate AST-detected optimization opportunities from regular issues
        # Opportunities have category="optimization_opportunity" and rule_id starts with "QT-OPT-"
        ast_opportunities = []
        regular_issues = []
        for issue in result.issues:
            if issue.category == "optimization_opportunity" and issue.rule_id.startswith("QT-OPT-"):
                ast_opportunities.append(issue)
            else:
                regular_issues.append(issue)

        # Convert AST opportunities to OpportunityResponse format
        from qt_sql.optimization.knowledge_base import get_transform, TransformID
        from qt_sql.optimization.cost_estimator import (
            estimate_savings, get_weight_description
        )

        opportunities = []
        for opp in ast_opportunities:
            # Look up the pattern in knowledge base for rewrite hints and weight
            transform = None
            for tid in TransformID:
                t = get_transform(tid)
                if t and t.code == opp.rule_id:
                    transform = t
                    break

            # Convert avg_speedup (0-1) to weight (1-10)
            # avg_speedup is normalized: (speedup - 1) * num_queries / 10
            # So 0.8 avg_speedup = high value, map to weight ~8
            weight = int(transform.avg_speedup * 10) + 1 if transform and transform.avg_speedup > 0 else 5
            weight = min(10, max(1, weight))  # Clamp to 1-10
            opportunities.append(OpportunityResponse(
                pattern_id=opp.rule_id,
                pattern_name=opp.name,
                trigger=opp.match or opp.description,
                rewrite_hint=transform.rewrite_hint if transform else opp.suggestion,
                weight=weight,
                weight_label=get_weight_description(weight),
            ))

        # Estimate savings (100x data scale, 24 runs/day, Snowflake pricing)
        savings = estimate_savings(
            opportunities=opportunities,
            query_complexity=result.query_structure,
            dialect=request.dialect,
        )
        savings_estimate = SavingsEstimateResponse(
            band_display=savings.band_display,
            low=savings.low,
            mid=savings.mid,
            high=savings.high,
            total_weight=savings.total_weight,
            opportunity_count=savings.opportunity_count,
        ) if opportunities else None

        # Convert regular issues to response format (excluding opportunities)
        issues = [
            SQLIssueResponse(
                rule_id=issue.rule_id,
                name=issue.name,
                severity=issue.severity,
                category=issue.category,
                penalty=issue.penalty,
                description=issue.description,
                location=issue.location,
                match=issue.match[:100] if issue.match and len(issue.match) > 100 else issue.match,
                explanation=issue.explanation,
                suggestion=issue.suggestion,
            )
            for issue in regular_issues
        ]

        # Determine status based on score
        score = result.final_score
        if score >= 90:
            status_str = "pass"
        elif score >= 70:
            status_str = "warn"
        elif score >= 50:
            status_str = "fail"
        else:
            status_str = "deny"

        # Generate HTML report
        try:
            html_report = render_sql_report(
                analysis_result=result,
                sql=request.sql,
                filename="query.sql",
                dialect=request.dialect,
                opportunities=opportunities,
                savings_estimate=savings.to_dict() if savings_estimate else None,
            )
        except Exception as e:
            logger.warning(f"Failed to render HTML report: {e}")
            html_report = ""

        response = AnalyzeResponse(
            score=result.final_score,
            total_penalty=result.total_penalty,
            severity_counts={
                "critical": result.critical_count,
                "high": result.high_count,
                "medium": result.medium_count,
                "low": result.low_count,
            },
            issues=issues,
            opportunities=opportunities,
            savings_estimate=savings_estimate,
            query_structure=result.query_structure,
            html=html_report,
            original_sql=request.sql,
            file_name="query.sql",
            status=status_str,
        )

        return response

    except Exception as e:
        logger.exception("SQL analysis failed")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Analysis failed: {str(e)}"
        )


@app.post("/api/sql/optimize", response_model=OptimizeResponse, tags=["SQL"])
async def optimize_sql(
    request: OptimizeRequest,
    user: Annotated[UserContext, Depends(CurrentUser)],
):
    """Optimize SQL using LLM-powered analysis.

    Analyzes the SQL query and uses an LLM to suggest optimizations
    based on detected anti-patterns and best practices.

    Requires authentication.

    - **sql**: The SQL query to optimize
    - **dialect**: SQL dialect for analysis
    - **provider**: Optional LLM provider override
    - **model**: Optional LLM model override
    """
    if not _HAS_V5_ANALYZER:
        raise HTTPException(
            status_code=status.HTTP_501_NOT_IMPLEMENTED,
            detail="Static analysis module not available (V5 analyzer was removed in consolidation)"
        )

    logger.info(
        "Optimizing SQL",
        extra={
            "user_id": user.user_id,
            "dialect": request.dialect,
            "sql_length": len(request.sql),
            "provider": request.provider,
        }
    )

    # Check tier permissions
    settings = get_settings()
    if user.tier == "free":
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="LLM optimization requires a paid tier"
        )

    try:
        # Run static analysis first
        detector = SQLAntiPatternDetector(dialect=request.dialect)
        analysis_result = detector.analyze(request.sql, include_structure=True)

        # Convert analysis to response format
        issues = [
            SQLIssueResponse(
                rule_id=issue.rule_id,
                name=issue.name,
                severity=issue.severity,
                category=issue.category,
                penalty=issue.penalty,
                description=issue.description,
                location=issue.location,
                match=issue.match[:100] if issue.match and len(issue.match) > 100 else issue.match,
                explanation=issue.explanation,
                suggestion=issue.suggestion,
            )
            for issue in analysis_result.issues
        ]

        analysis_response = AnalyzeResponse(
            score=analysis_result.final_score,
            total_penalty=analysis_result.total_penalty,
            severity_counts={
                "critical": analysis_result.critical_count,
                "high": analysis_result.high_count,
                "medium": analysis_result.medium_count,
                "low": analysis_result.low_count,
            },
            issues=issues,
            query_structure=analysis_result.query_structure,
        )

        response = OptimizeResponse(
            original_sql=request.sql,
            analysis=analysis_response,
        )

        # If no issues found, skip LLM
        if not analysis_result.issues:
            response.success = True
            response.optimized_sql = request.sql
            return response

        # Create LLM client and optimize
        try:
            from qt_shared.llm import create_llm_client

            llm_client = create_llm_client(
                provider=request.provider,
                model=request.model,
            )

            if llm_client is None:
                raise HTTPException(
                    status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                    detail="No LLM provider configured"
                )

            # Build optimization prompt
            issues_text = "\n".join(
                f"- {issue.name} ({issue.severity}): {issue.description}"
                for issue in analysis_result.issues
            )

            base_sql = request.sql

            prompt = f"""You are a SQL optimization expert. Optimize the following SQL query.

SQL Query:
```sql
{base_sql}
```

{"Detected issues:" + chr(10) + issues_text if issues_text else "No specific issues detected, but review for general optimization opportunities."}

Provide an optimized version with brief explanation. Format:
## Optimized SQL
```sql
<query>
```

## Changes
<brief list>
"""

            llm_response = llm_client.analyze(prompt)
            response.llm_response = llm_response

            # Extract optimized SQL from response
            import re
            sql_match = re.search(r"```sql\s*(.*?)\s*```", llm_response, re.DOTALL)
            if sql_match:
                response.optimized_sql = sql_match.group(1).strip()

            response.success = True

        except ImportError:
            raise HTTPException(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                detail="LLM client not available"
            )
        except Exception as e:
            logger.exception("LLM optimization failed")
            response.success = False
            response.error = str(e)

        return response

    except HTTPException:
        raise
    except Exception as e:
        logger.exception("SQL optimization failed")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Optimization failed: {str(e)}"
        )


import uuid
import re as regex_module


def _extract_optimized_sql(llm_response: str) -> Optional[str]:
    """Extract SQL from LLM response."""
    response = llm_response.strip()

    # Try markdown SQL code block
    sql_match = regex_module.search(r'```sql\s*(.*?)\s*```', response, regex_module.DOTALL | regex_module.IGNORECASE)
    if sql_match:
        return sql_match.group(1).strip()

    # Try any code block
    code_match = regex_module.search(r'```\s*(.*?)\s*```', response, regex_module.DOTALL)
    if code_match:
        potential_sql = code_match.group(1).strip()
        if regex_module.search(r'\b(SELECT|INSERT|UPDATE|DELETE|WITH)\b', potential_sql, regex_module.IGNORECASE):
            return potential_sql

    # Check if entire response is SQL
    if regex_module.search(r'^\s*(SELECT|WITH|INSERT|UPDATE|DELETE)\b', response, regex_module.IGNORECASE):
        return response

    return None


def _validate_syntax(sql: str, dialect: str) -> tuple[str, list[str]]:
    """Validate SQL syntax using sqlglot."""
    import sqlglot

    dialect_map = {
        "generic": None, "postgres": "postgres", "snowflake": "snowflake",
        "duckdb": "duckdb", "tsql": "tsql",
    }

    try:
        sqlglot.parse_one(sql, dialect=dialect_map.get(dialect))
        return "pass", []
    except sqlglot.errors.ParseError as e:
        return "fail", [str(e)]
    except Exception as e:
        return "fail", [f"Parse error: {str(e)}"]


@app.post("/api/optimize/manual/validate", response_model=ValidateManualResponse, tags=["Optimization"])
async def validate_manual_response(request: ValidateManualRequest):
    """Validate LLM response for manual optimization mode."""
    session_id = str(uuid.uuid4())[:8]

    # Extract SQL from response
    optimized_sql = _extract_optimized_sql(request.llm_response)

    if not optimized_sql:
        return ValidateManualResponse(
            session_id=session_id, success=False, syntax_status="fail",
            syntax_errors=["Could not extract SQL from LLM response"],
            regression_status="skip", original_code=request.original_sql,
            optimized_code="", all_passed=False, errors=["Failed to parse LLM response"]
        )

    # Syntax validation
    syntax_status, syntax_errors = _validate_syntax(optimized_sql, request.dialect)

    if syntax_status == "fail":
        return ValidateManualResponse(
            session_id=session_id, success=False, syntax_status=syntax_status,
            syntax_errors=syntax_errors, regression_status="skip",
            original_code=request.original_sql, optimized_code=optimized_sql,
            all_passed=False, errors=["Syntax validation failed"]
        )

    # Regression check
    if not _HAS_V5_ANALYZER:
        # Without the analyzer, skip regression detection — return syntax-only result
        return ValidateManualResponse(
            session_id=session_id, success=True, syntax_status="pass",
            regression_status="skip", original_code=request.original_sql,
            optimized_code=optimized_sql, all_passed=True,
            warnings=["Regression check unavailable (V5 analyzer removed)"],
        )

    detector = SQLAntiPatternDetector(dialect=request.dialect)
    original_result = detector.analyze(request.original_sql)
    optimized_result = detector.analyze(optimized_sql)

    original_rules = {i.rule_id for i in original_result.issues}
    optimized_rules = {i.rule_id for i in optimized_result.issues}

    fixed_rules = original_rules - optimized_rules
    new_rules = optimized_rules - original_rules

    issues_fixed = [
        IssueDetailResponse(rule_id=i.rule_id, severity=i.severity, title=i.name, description=i.description)
        for i in original_result.issues if i.rule_id in fixed_rules
    ]
    new_issues = [
        IssueDetailResponse(rule_id=i.rule_id, severity=i.severity, title=i.name, description=i.description)
        for i in optimized_result.issues if i.rule_id in new_rules
    ]

    regression_status = "pass"
    if any(i.severity in ("critical", "high") for i in new_issues):
        regression_status = "fail"
    elif new_issues:
        regression_status = "warn"

    all_passed = syntax_status == "pass" and regression_status in ("pass", "warn")

    # Extract explanation
    llm_explanation = ""
    expl_match = regex_module.search(r'(?:explanation|changes|what was changed)[:\s]*(.+?)(?=```|$)',
                           request.llm_response, regex_module.IGNORECASE | regex_module.DOTALL)
    if expl_match:
        llm_explanation = expl_match.group(1).strip()[:500]

    return ValidateManualResponse(
        session_id=session_id, success=all_passed, syntax_status=syntax_status,
        syntax_errors=syntax_errors, regression_status=regression_status,
        issues_fixed=issues_fixed, new_issues=new_issues,
        original_code=request.original_sql, optimized_code=optimized_sql,
        all_passed=all_passed, llm_explanation=llm_explanation
    )


# ============================================
# Database Connection Endpoints
# ============================================

import tempfile
import os
from fastapi import UploadFile, File, Form

# Session storage for database connections
db_sessions: dict[str, "DuckDBExecutor"] = {}


class DatabaseConnectResponse(BaseModel):
    """Response from database connection."""
    session_id: str
    connected: bool
    type: str
    details: Optional[str] = None
    error: Optional[str] = None


class DatabaseStatusResponse(BaseModel):
    """Database connection status."""
    connected: bool
    type: Optional[str] = None
    details: Optional[str] = None


class SchemaColumn(BaseModel):
    """Column definition."""
    name: str
    type: str
    nullable: Optional[bool] = None


class SchemaResponse(BaseModel):
    """Database schema response."""
    session_id: str
    tables: dict[str, list[SchemaColumn]]
    error: Optional[str] = None


class ExecuteRequest(BaseModel):
    """Request for SQL execution."""
    sql: str
    limit: int = Field(default=100, ge=1, le=10000)


class QueryResultResponse(BaseModel):
    """Query execution result."""
    columns: list[str]
    column_types: list[str]
    rows: list[list]
    row_count: int
    execution_time_ms: float
    truncated: bool = False
    error: Optional[str] = None


class ExplainRequest(BaseModel):
    """Request for execution plan."""
    sql: str
    analyze: bool = True


class ExecutionPlanResponse(BaseModel):
    """Execution plan response."""
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
    """Connect to DuckDB with a fixture file.

    Upload a SQL file containing CREATE TABLE and INSERT statements,
    or a CSV/Parquet file to create an in-memory database.
    """
    from qt_sql.execution import DuckDBExecutor

    session_id = str(uuid.uuid4())[:12]

    try:
        # Read uploaded file
        content = await fixture_file.read()

        # Create temp file for fixture
        suffix = os.path.splitext(fixture_file.filename or "")[1] or ".sql"
        with tempfile.NamedTemporaryFile(mode='wb', suffix=suffix, delete=False) as tmp:
            tmp.write(content)
            tmp_path = tmp.name

        try:
            # Create DuckDB executor
            executor = DuckDBExecutor(":memory:")
            executor.connect()

            # Load fixture based on file type
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
                # Try as SQL
                executor.execute_script(content.decode('utf-8'))

            # Store session
            db_sessions[session_id] = executor

            # Get table info
            schema = executor.get_schema_info(include_row_counts=False)
            table_count = len(schema.get("tables", []))

            return DatabaseConnectResponse(
                session_id=session_id,
                connected=True,
                type="duckdb",
                details=f"Loaded {table_count} table(s) from {fixture_file.filename}",
            )

        finally:
            # Clean up temp file
            os.unlink(tmp_path)

    except Exception as e:
        logger.exception("DuckDB connection failed")
        return DatabaseConnectResponse(
            session_id=session_id,
            connected=False,
            type="duckdb",
            error=str(e),
        )


@app.post("/api/database/connect/duckdb/quick", response_model=DatabaseConnectResponse, tags=["Database"])
async def connect_duckdb_quick(fixture_path: str = Form(...)):
    """Connect to DuckDB with a fixture path on the server.

    For development/testing when fixture files are on the server.
    """
    from qt_sql.execution import DuckDBExecutor

    session_id = str(uuid.uuid4())[:12]

    try:
        executor = DuckDBExecutor(":memory:")
        executor.connect()

        # Check file type and load
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
            # Connect to existing DuckDB file
            executor.close()
            executor = DuckDBExecutor(fixture_path, read_only=True)
            executor.connect()
        else:
            # Try as SQL
            with open(fixture_path, 'r') as f:
                executor.execute_script(f.read())

        db_sessions[session_id] = executor

        schema = executor.get_schema_info(include_row_counts=False)
        table_count = len(schema.get("tables", []))

        return DatabaseConnectResponse(
            session_id=session_id,
            connected=True,
            type="duckdb",
            details=f"Connected to {fixture_path} ({table_count} tables)",
        )

    except Exception as e:
        logger.exception("DuckDB quick connection failed")
        return DatabaseConnectResponse(
            session_id=session_id,
            connected=False,
            type="duckdb",
            error=str(e),
        )


@app.get("/api/database/status/{session_id}", response_model=DatabaseStatusResponse, tags=["Database"])
async def get_database_status(session_id: str):
    """Get database connection status."""
    executor = db_sessions.get(session_id)

    if not executor:
        return DatabaseStatusResponse(connected=False)

    return DatabaseStatusResponse(
        connected=True,
        type="duckdb",
        details="Connected",
    )


@app.delete("/api/database/disconnect/{session_id}", tags=["Database"])
async def disconnect_database(session_id: str):
    """Disconnect from database and clean up session."""
    executor = db_sessions.pop(session_id, None)

    if executor:
        try:
            executor.close()
        except Exception:
            pass

    return {"success": True}


@app.post("/api/database/execute/{session_id}", response_model=QueryResultResponse, tags=["Database"])
async def execute_query(session_id: str, request: ExecuteRequest):
    """Execute a SQL query and return results."""
    import time

    executor = db_sessions.get(session_id)
    if not executor:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Database session not found"
        )

    try:
        start = time.time()

        # Add LIMIT if not present (for safety)
        sql = request.sql.strip().rstrip(';')
        has_limit = 'limit' in sql.lower().split()[-3:] if len(sql.split()) > 3 else False

        if not has_limit and request.limit:
            sql = f"SELECT * FROM ({sql}) AS _subq LIMIT {request.limit}"

        conn = executor._ensure_connected()
        result = conn.execute(sql)

        columns = [desc[0] for desc in result.description] if result.description else []
        column_types = [desc[1] for desc in result.description] if result.description else []
        rows = result.fetchall()

        execution_time = (time.time() - start) * 1000

        # Convert rows to serializable format
        def convert_value(v):
            if v is None:
                return None
            if isinstance(v, (int, float, str, bool)):
                return v
            return str(v)

        serialized_rows = [[convert_value(v) for v in row] for row in rows]

        return QueryResultResponse(
            columns=columns,
            column_types=[str(t) for t in column_types],
            rows=serialized_rows,
            row_count=len(rows),
            execution_time_ms=round(execution_time, 2),
            truncated=len(rows) >= request.limit if request.limit else False,
        )

    except Exception as e:
        logger.exception("Query execution failed")
        return QueryResultResponse(
            columns=[],
            column_types=[],
            rows=[],
            row_count=0,
            execution_time_ms=0,
            error=str(e),
        )


@app.post("/api/database/explain/{session_id}", response_model=ExecutionPlanResponse, tags=["Database"])
async def explain_query(session_id: str, request: ExplainRequest):
    """Get execution plan for a SQL query."""
    from qt_sql.execution import DuckDBPlanParser

    executor = db_sessions.get(session_id)
    if not executor:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Database session not found"
        )

    try:
        plan_json = executor.explain(request.sql, analyze=request.analyze)

        # Parse plan
        parser = DuckDBPlanParser()
        analysis = parser.parse(plan_json)

        return ExecutionPlanResponse(
            success=True,
            plan_json=plan_json,
            plan_tree=analysis.plan_tree,
            execution_time_ms=analysis.execution_time_ms,
            total_cost=analysis.total_cost,
            bottleneck=analysis.bottleneck,
            warnings=analysis.warnings,
        )

    except Exception as e:
        logger.exception("Explain failed")
        return ExecutionPlanResponse(
            success=False,
            error=str(e),
        )


@app.get("/api/database/schema/{session_id}", response_model=SchemaResponse, tags=["Database"])
async def get_schema(session_id: str):
    """Get database schema (tables and columns)."""
    executor = db_sessions.get(session_id)
    if not executor:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Database session not found"
        )

    try:
        schema = executor.get_schema_info(include_row_counts=True)

        # Convert to response format
        tables_dict: dict[str, list[SchemaColumn]] = {}
        for table in schema.get("tables", []):
            table_name = table.get("name", "")
            columns = [
                SchemaColumn(
                    name=col.get("name", ""),
                    type=col.get("type", ""),
                    nullable=col.get("nullable"),
                )
                for col in table.get("columns", [])
            ]
            tables_dict[table_name] = columns

        return SchemaResponse(
            session_id=session_id,
            tables=tables_dict,
        )

    except Exception as e:
        logger.exception("Schema fetch failed")
        return SchemaResponse(
            session_id=session_id,
            tables={},
            error=str(e),
        )


# ============================================
# SQL Validation Endpoints
# ============================================

class ValidateSQLRequest(BaseModel):
    """Request for SQL validation."""
    original_sql: str = Field(..., description="Original SQL query")
    optimized_sql: str = Field(..., description="Optimized SQL query")
    mode: Literal["sample", "full"] = Field(
        default="sample",
        description="Validation mode: sample (signal) or full (confidence)"
    )
    schema_sql: Optional[str] = Field(
        default=None,
        description="Optional schema SQL for in-memory validation"
    )
    session_id: Optional[str] = Field(
        default=None,
        description="Database session ID (if using connected database)"
    )
    limit_strategy: Literal["add_order", "remove_limit"] = Field(
        default="add_order",
        description="Strategy for LIMIT without ORDER BY"
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

    # Row counts
    row_counts: dict[str, int]
    row_counts_match: bool

    # Timing
    timing: dict[str, float]
    speedup: float

    # Cost
    cost: dict[str, float]
    cost_reduction_pct: float

    # Values
    values_match: bool
    checksum_match: Optional[bool] = None
    value_differences: list[ValueDifferenceResponse] = []

    # Normalization
    limit_detected: bool = False
    limit_strategy_applied: Optional[str] = None

    # Messages
    errors: list[str] = []
    warnings: list[str] = []


@app.post("/api/sql/validate", response_model=ValidateSQLResponse, tags=["SQL"])
async def validate_sql(request: ValidateSQLRequest):
    """Validate that optimized SQL is equivalent to original.

    Uses the 1-1-2-2 benchmarking pattern for accurate timing comparison.

    **Modes:**
    - **sample**: Uses sample database for quick validation (gives signal)
    - **full**: Uses full database for thorough validation (gives confidence)

    Both modes compare:
    - Row counts (must match exactly)
    - Checksum comparison (fast)
    - Value-by-value comparison (if checksums differ)

    **LIMIT handling:**
    - Detects LIMIT without ORDER BY (non-deterministic results)
    - add_order: Injects ORDER BY 1, 2, 3... before LIMIT
    - remove_limit: Strips LIMIT clause entirely
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

    # Determine database to use
    database = ":memory:"
    if request.session_id and request.session_id in db_sessions:
        # Use the database from the session
        # For now, we'll use in-memory with schema since sessions are in-memory
        pass

    # Parse options
    validation_mode = ValidationMode.FULL if request.mode == "full" else ValidationMode.SAMPLE
    limit_strat = (
        LimitStrategy.REMOVE_LIMIT
        if request.limit_strategy == "remove_limit"
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

        # Convert value differences to response format
        value_diffs = [
            ValueDifferenceResponse(
                row_index=d.row_index,
                column=d.column,
                original_value=str(d.original_value) if d.original_value is not None else None,
                optimized_value=str(d.optimized_value) if d.optimized_value is not None else None,
            )
            for d in result.value_differences[:10]  # Limit to 10
        ]

        return ValidateSQLResponse(
            status=result.status.value,
            mode=result.mode.value,
            row_counts={
                "original": result.original_row_count,
                "optimized": result.optimized_row_count,
            },
            row_counts_match=result.row_counts_match,
            timing={
                "original_ms": round(result.original_timing_ms, 2),
                "optimized_ms": round(result.optimized_timing_ms, 2),
            },
            speedup=round(result.speedup, 2),
            cost={
                "original": round(result.original_cost, 2),
                "optimized": round(result.optimized_cost, 2),
            },
            cost_reduction_pct=round(result.cost_reduction_pct, 2),
            values_match=result.values_match,
            checksum_match=result.checksum_match,
            value_differences=value_diffs,
            limit_detected=result.limit_detected,
            limit_strategy_applied=(
                result.limit_strategy_applied.value
                if result.limit_strategy_applied
                else None
            ),
            errors=result.errors,
            warnings=result.warnings,
        )

    except Exception as e:
        logger.exception("SQL validation failed")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Validation failed: {str(e)}"
        )


# Include additional routers if needed
def create_app() -> FastAPI:
    """Factory function for creating the app (useful for testing)."""
    return app


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8002)
