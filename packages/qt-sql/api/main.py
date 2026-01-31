"""QueryTorque SQL API.

FastAPI backend for SQL analysis and optimization.

Routes:
    POST /api/sql/analyze  - Analyze SQL for anti-patterns
    POST /api/sql/optimize - Optimize SQL using LLM
    GET /health            - Health check
"""

import logging
from typing import Optional, Annotated

from fastapi import FastAPI, HTTPException, Depends, status
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field

from qt_shared.auth import OptionalUser, CurrentUser, UserContext
from qt_shared.config import get_settings, Settings

from qt_sql.analyzers.sql_antipattern_detector import SQLAntiPatternDetector
from qt_sql.calcite_client import get_calcite_client

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


# CORS configuration
@app.on_event("startup")
async def configure_cors():
    """Configure CORS based on settings."""
    settings = get_settings()
    origins = [origin.strip() for origin in settings.cors_origins.split(",")]

    app.add_middleware(
        CORSMiddleware,
        allow_origins=origins,
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
    include_calcite: bool = Field(
        default=False,
        description="Include Calcite optimization analysis"
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


class AnalyzeResponse(BaseModel):
    """Response from SQL analysis."""

    score: int = Field(..., description="Overall quality score (0-100)")
    total_penalty: int = Field(..., description="Total penalty points")
    severity_counts: dict = Field(..., description="Issue counts by severity")
    issues: list[SQLIssueResponse] = Field(default_factory=list)
    query_structure: Optional[dict] = Field(
        default=None,
        description="Query structure information (CTEs, joins, etc.)"
    )
    calcite: Optional[dict] = Field(
        default=None,
        description="Calcite optimization results if requested"
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
    use_calcite: bool = Field(
        default=True,
        description="Include Calcite optimization before LLM"
    )


class OptimizeResponse(BaseModel):
    """Response from SQL optimization."""

    original_sql: str
    optimized_sql: Optional[str] = None
    analysis: AnalyzeResponse
    llm_response: Optional[str] = None
    calcite_result: Optional[dict] = None
    success: bool = True
    error: Optional[str] = None


class HealthResponse(BaseModel):
    """Health check response."""

    status: str
    version: str
    auth_enabled: bool
    llm_configured: bool
    calcite_available: Optional[bool] = None


# Routes
@app.get("/health", response_model=HealthResponse, tags=["Health"])
async def health_check():
    """Health check endpoint.

    Returns service status and configuration information.
    """
    settings = get_settings()

    # Check Calcite availability
    calcite_available = None
    try:
        client = get_calcite_client()
        calcite_available = await client.is_available()
    except Exception:
        calcite_available = False

    return HealthResponse(
        status="ok",
        version="0.1.0",
        auth_enabled=settings.auth_enabled,
        llm_configured=settings.has_llm_provider,
        calcite_available=calcite_available,
    )


@app.post("/api/sql/analyze", response_model=AnalyzeResponse, tags=["SQL"])
async def analyze_sql(
    request: AnalyzeRequest,
    user: Annotated[Optional[UserContext], Depends(OptionalUser)],
):
    """Analyze SQL for anti-patterns and issues.

    Performs static analysis on the provided SQL query and returns
    detected issues, severity levels, and improvement suggestions.

    - **sql**: The SQL query to analyze
    - **dialect**: SQL dialect for analysis (affects available rules)
    - **include_structure**: Whether to include query structure info
    - **include_calcite**: Whether to include Calcite optimization
    """
    logger.info(
        "Analyzing SQL",
        extra={
            "user_id": user.user_id if user else "anonymous",
            "dialect": request.dialect,
            "sql_length": len(request.sql),
        }
    )

    try:
        # Run static analysis
        detector = SQLAntiPatternDetector(dialect=request.dialect)
        result = detector.analyze(request.sql, include_structure=request.include_structure)

        # Convert to response format
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
            for issue in result.issues
        ]

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
            query_structure=result.query_structure,
        )

        # Include Calcite analysis if requested
        if request.include_calcite:
            try:
                client = get_calcite_client()
                calcite_result = await client.optimize(request.sql, dry_run=True)

                response.calcite = {
                    "available": calcite_result.success or calcite_result.error != "Calcite service not available",
                    "query_changed": calcite_result.query_changed,
                    "rules_applied": calcite_result.rules_applied,
                    "optimized_sql": calcite_result.optimized_sql if calcite_result.query_changed else None,
                    "error": calcite_result.error if not calcite_result.success else None,
                }
            except Exception as e:
                response.calcite = {
                    "available": False,
                    "error": str(e),
                }

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
    - **use_calcite**: Whether to include Calcite optimization
    """
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

        # Run Calcite optimization if requested
        if request.use_calcite:
            try:
                client = get_calcite_client()
                calcite_result = await client.optimize(request.sql)

                response.calcite_result = {
                    "success": calcite_result.success,
                    "query_changed": calcite_result.query_changed,
                    "optimized_sql": calcite_result.optimized_sql,
                    "rules_applied": calcite_result.rules_applied,
                    "improvement_percent": calcite_result.improvement_percent,
                    "error": calcite_result.error if not calcite_result.success else None,
                }

                # Use Calcite-optimized SQL as base for LLM if it improved
                if calcite_result.query_changed and calcite_result.optimized_sql:
                    response.optimized_sql = calcite_result.optimized_sql
            except Exception as e:
                response.calcite_result = {"success": False, "error": str(e)}

        # If no issues found and no Calcite changes, skip LLM
        if not analysis_result.issues and not response.calcite_result:
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

            base_sql = response.optimized_sql or request.sql

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


# Include additional routers if needed
def create_app() -> FastAPI:
    """Factory function for creating the app (useful for testing)."""
    return app


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8002)
