"""QueryTorque DAX API.

FastAPI backend for DAX/Power BI analysis and optimization.

Routes:
    POST /api/dax/analyze   - Analyze VPAX file for anti-patterns
    POST /api/dax/optimize  - Optimize DAX measures using LLM
    POST /api/dax/diff      - Compare two VPAX files
    GET /health             - Health check
"""

import logging
import tempfile
from pathlib import Path
from typing import Optional, Annotated

from fastapi import FastAPI, HTTPException, Depends, status, UploadFile, File, Form
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse
from pydantic import BaseModel, Field

from qt_shared.auth import OptionalUser, CurrentUser, UserContext
from qt_shared.config import get_settings, Settings

from qt_dax.analyzers.vpax_analyzer import VPAXAnalyzer

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Create FastAPI app
app = FastAPI(
    title="QueryTorque DAX API",
    description="DAX/Power BI analysis and optimization API",
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
class DAXIssueResponse(BaseModel):
    """A detected DAX issue."""

    rule_id: str
    name: str
    severity: str
    category: str
    penalty: int
    description: str
    affected_object: Optional[str] = None
    explanation: str = ""
    suggestion: str = ""


class ModelStatsResponse(BaseModel):
    """Model statistics."""

    total_size_mb: float = 0
    table_count: int = 0
    column_count: int = 0
    measure_count: int = 0
    relationship_count: int = 0
    local_date_tables: int = 0


class AnalyzeResponse(BaseModel):
    """Response from DAX analysis."""

    torque_score: int = Field(..., description="Overall quality score (0-100)")
    total_penalty: int = Field(..., description="Total penalty points")
    quality_gate: str = Field(..., description="Quality gate status")
    severity_counts: dict = Field(..., description="Issue counts by severity")
    issues: list[DAXIssueResponse] = Field(default_factory=list)
    model_stats: Optional[ModelStatsResponse] = None
    optimization_context: Optional[dict] = None


class OptimizeMeasureRequest(BaseModel):
    """Request to optimize a specific measure."""

    measure_name: str = Field(..., description="Name of measure to optimize")
    measure_dax: str = Field(..., description="Current DAX expression")
    issues: list[str] = Field(default_factory=list, description="Known issues to address")


class OptimizeMeasureResponse(BaseModel):
    """Response from measure optimization."""

    measure_name: str
    original_dax: str
    optimized_dax: Optional[str] = None
    changes: list[str] = Field(default_factory=list)
    expected_improvement: str = ""
    success: bool = True
    error: Optional[str] = None


class OptimizeRequest(BaseModel):
    """Request body for batch optimization."""

    measures: list[OptimizeMeasureRequest] = Field(..., description="Measures to optimize")
    provider: Optional[str] = Field(
        default=None,
        description="LLM provider override (anthropic, deepseek, openai, etc.)"
    )
    model: Optional[str] = Field(default=None, description="LLM model override")


class OptimizeResponse(BaseModel):
    """Response from batch optimization."""

    results: list[OptimizeMeasureResponse] = Field(default_factory=list)
    success_count: int = 0
    failure_count: int = 0


class DiffSummaryResponse(BaseModel):
    """Diff summary response."""

    added: int = 0
    modified: int = 0
    removed: int = 0
    old_score: int = 0
    new_score: int = 0
    score_delta: int = 0


class ChangeResponse(BaseModel):
    """A single change entry."""

    change_type: str
    category: str
    object_name: str
    description: str
    severity: str = "info"


class DiffResponse(BaseModel):
    """Response from VPAX diff."""

    summary: DiffSummaryResponse
    changes: list[ChangeResponse] = Field(default_factory=list)


class HealthResponse(BaseModel):
    """Health check response."""

    status: str
    version: str
    auth_enabled: bool
    llm_configured: bool


def get_quality_gate(score: int) -> str:
    """Determine quality gate from score."""
    if score >= 90:
        return "peak_torque"
    elif score >= 70:
        return "power_band"
    elif score >= 50:
        return "stall_zone"
    else:
        return "redline"


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
    )


@app.post("/api/dax/analyze", response_model=AnalyzeResponse, tags=["DAX"])
async def analyze_vpax(
    file: UploadFile = File(..., description="VPAX file to analyze"),
    user: Annotated[Optional[UserContext], Depends(OptionalUser)] = None,
):
    """Analyze VPAX file for anti-patterns and issues.

    Upload a VPAX file (exported from DAX Studio or Tabular Editor)
    and receive a detailed analysis with detected issues and recommendations.

    - **file**: The VPAX file to analyze
    """
    logger.info(
        "Analyzing VPAX",
        extra={
            "user_id": user.user_id if user else "anonymous",
            "filename": file.filename,
        }
    )

    # Validate file type
    if not file.filename or not file.filename.lower().endswith(".vpax"):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="File must be a .vpax file"
        )

    try:
        # Save uploaded file to temp location
        with tempfile.NamedTemporaryFile(suffix=".vpax", delete=False) as tmp:
            content = await file.read()
            tmp.write(content)
            tmp_path = Path(tmp.name)

        try:
            # Run analysis
            analyzer = VPAXAnalyzer()
            result = analyzer.analyze(tmp_path)

            # Convert to response format
            issues = [
                DAXIssueResponse(
                    rule_id=issue.rule_id,
                    name=issue.name,
                    severity=issue.severity,
                    category=issue.category,
                    penalty=issue.penalty,
                    description=issue.description,
                    affected_object=issue.affected_object,
                    explanation=issue.explanation,
                    suggestion=issue.suggestion,
                )
                for issue in result.issues
            ]

            model_stats = None
            if result.model_stats:
                model_stats = ModelStatsResponse(**result.model_stats)

            return AnalyzeResponse(
                torque_score=result.torque_score,
                total_penalty=result.total_penalty,
                quality_gate=get_quality_gate(result.torque_score),
                severity_counts={
                    "critical": result.critical_count,
                    "high": result.high_count,
                    "medium": result.medium_count,
                    "low": result.low_count,
                },
                issues=issues,
                model_stats=model_stats,
                optimization_context=result.optimization_context,
            )

        finally:
            # Clean up temp file
            tmp_path.unlink(missing_ok=True)

    except Exception as e:
        logger.exception("VPAX analysis failed")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Analysis failed: {str(e)}"
        )


@app.post("/api/dax/analyze/report", response_class=HTMLResponse, tags=["DAX"])
async def analyze_vpax_report(
    file: UploadFile = File(..., description="VPAX file to analyze"),
    user: Annotated[Optional[UserContext], Depends(OptionalUser)] = None,
):
    """Analyze VPAX file and return HTML report.

    Upload a VPAX file and receive a formatted HTML report
    suitable for viewing in a browser or iframe.

    - **file**: The VPAX file to analyze
    """
    logger.info(
        "Generating VPAX report",
        extra={
            "user_id": user.user_id if user else "anonymous",
            "filename": file.filename,
        }
    )

    # Validate file type
    if not file.filename or not file.filename.lower().endswith(".vpax"):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="File must be a .vpax file"
        )

    try:
        # Save uploaded file to temp location
        with tempfile.NamedTemporaryFile(suffix=".vpax", delete=False) as tmp:
            content = await file.read()
            tmp.write(content)
            tmp_path = Path(tmp.name)

        try:
            # Run analysis
            analyzer = VPAXAnalyzer()
            result = analyzer.analyze(tmp_path)

            # Generate HTML report
            from qt_dax.renderers import DAXRenderer

            renderer = DAXRenderer()
            html = renderer.render(result)

            return HTMLResponse(content=html)

        finally:
            # Clean up temp file
            tmp_path.unlink(missing_ok=True)

    except ImportError:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Report renderer not available"
        )
    except Exception as e:
        logger.exception("VPAX report generation failed")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Report generation failed: {str(e)}"
        )


@app.post("/api/dax/optimize", response_model=OptimizeResponse, tags=["DAX"])
async def optimize_measures(
    request: OptimizeRequest,
    user: Annotated[UserContext, Depends(CurrentUser)],
):
    """Optimize DAX measures using LLM-powered analysis.

    Submit one or more measures with their current DAX and known issues,
    and receive optimized versions with explanations.

    Requires authentication.

    - **measures**: List of measures to optimize
    - **provider**: Optional LLM provider override
    - **model**: Optional LLM model override
    """
    logger.info(
        "Optimizing DAX measures",
        extra={
            "user_id": user.user_id,
            "measure_count": len(request.measures),
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
        # Create LLM client
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

        results = []
        success_count = 0
        failure_count = 0

        for measure_req in request.measures:
            try:
                issues_text = "\n".join(f"- {issue}" for issue in measure_req.issues)

                prompt = f"""You are a DAX optimization expert. Optimize the following DAX measure.

Measure: {measure_req.measure_name}

Original DAX:
```dax
{measure_req.measure_dax}
```

{"Known issues:" + chr(10) + issues_text if issues_text else "Review for optimization opportunities."}

Provide an optimized version with brief explanation. Format:
## Optimized DAX
```dax
<query>
```

## Changes
<brief list>

## Expected Improvement
<description>
"""

                response = llm_client.analyze(prompt)

                # Extract optimized DAX
                import re
                dax_match = re.search(r"```dax\s*(.*?)\s*```", response, re.DOTALL)
                optimized_dax = dax_match.group(1).strip() if dax_match else None

                # Extract changes
                changes = []
                changes_match = re.search(r"## Changes\s*(.*?)(?=##|$)", response, re.DOTALL)
                if changes_match:
                    changes_text = changes_match.group(1).strip()
                    changes = [line.strip("- ").strip() for line in changes_text.split("\n") if line.strip()]

                # Extract improvement
                improvement = ""
                improvement_match = re.search(r"## Expected Improvement\s*(.*?)(?=##|$)", response, re.DOTALL)
                if improvement_match:
                    improvement = improvement_match.group(1).strip()

                results.append(OptimizeMeasureResponse(
                    measure_name=measure_req.measure_name,
                    original_dax=measure_req.measure_dax,
                    optimized_dax=optimized_dax,
                    changes=changes,
                    expected_improvement=improvement,
                    success=True,
                ))
                success_count += 1

            except Exception as e:
                logger.warning(f"Failed to optimize measure {measure_req.measure_name}: {e}")
                results.append(OptimizeMeasureResponse(
                    measure_name=measure_req.measure_name,
                    original_dax=measure_req.measure_dax,
                    success=False,
                    error=str(e),
                ))
                failure_count += 1

        return OptimizeResponse(
            results=results,
            success_count=success_count,
            failure_count=failure_count,
        )

    except ImportError:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="LLM client not available"
        )
    except HTTPException:
        raise
    except Exception as e:
        logger.exception("DAX optimization failed")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Optimization failed: {str(e)}"
        )


@app.post("/api/dax/diff", response_model=DiffResponse, tags=["DAX"])
async def diff_vpax(
    file1: UploadFile = File(..., description="Base VPAX file"),
    file2: UploadFile = File(..., description="New VPAX file to compare"),
    user: Annotated[Optional[UserContext], Depends(OptionalUser)] = None,
):
    """Compare two VPAX files and return differences.

    Upload two VPAX files to see what changed between versions.

    - **file1**: The base/original VPAX file
    - **file2**: The new VPAX file to compare against base
    """
    logger.info(
        "Comparing VPAX files",
        extra={
            "user_id": user.user_id if user else "anonymous",
            "file1": file1.filename,
            "file2": file2.filename,
        }
    )

    # Validate file types
    for f in [file1, file2]:
        if not f.filename or not f.filename.lower().endswith(".vpax"):
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"File {f.filename} must be a .vpax file"
            )

    try:
        # Save uploaded files to temp locations
        with tempfile.NamedTemporaryFile(suffix=".vpax", delete=False) as tmp1:
            content1 = await file1.read()
            tmp1.write(content1)
            tmp_path1 = Path(tmp1.name)

        with tempfile.NamedTemporaryFile(suffix=".vpax", delete=False) as tmp2:
            content2 = await file2.read()
            tmp2.write(content2)
            tmp_path2 = Path(tmp2.name)

        try:
            # Run diff
            from qt_dax.analyzers.vpax_differ import VPAXDiffer

            differ = VPAXDiffer()
            result = differ.compare(tmp_path1, tmp_path2)

            # Convert to response format
            summary = DiffSummaryResponse(
                added=result.summary.added,
                modified=result.summary.modified,
                removed=result.summary.removed,
                old_score=result.summary.score_delta.old_score,
                new_score=result.summary.score_delta.new_score,
                score_delta=result.summary.score_delta.delta,
            )

            changes = [
                ChangeResponse(
                    change_type=change.change_type.value,
                    category=change.category.value,
                    object_name=change.object_name,
                    description=change.description,
                    severity=change.severity.value if hasattr(change, 'severity') else "info",
                )
                for change in result.changes
            ]

            return DiffResponse(summary=summary, changes=changes)

        finally:
            # Clean up temp files
            tmp_path1.unlink(missing_ok=True)
            tmp_path2.unlink(missing_ok=True)

    except ImportError:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="VPAX differ not available"
        )
    except Exception as e:
        logger.exception("VPAX diff failed")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Diff failed: {str(e)}"
        )


# Factory function
def create_app() -> FastAPI:
    """Factory function for creating the app (useful for testing)."""
    return app


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8003)
