"""Job submission and retrieval API routes.

POST /api/v1/jobs          — Submit a single optimization job
POST /api/v1/jobs/batch    — Submit multiple .sql files as batch
GET  /api/v1/jobs          — List jobs for current user/org
GET  /api/v1/jobs/{id}     — Get job status and result
GET  /api/v1/jobs/{id}/results — Download optimization results
"""

import hashlib
import logging
import uuid
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, UploadFile, File, status
from pydantic import BaseModel, Field
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from qt_shared.auth.middleware import CurrentUser, OptionalUser
from qt_shared.database.connection import get_async_session
from qt_shared.database.models import AnalysisJob, Credential
from qt_shared.vault import decrypt_dsn

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/api/v1/jobs", tags=["Jobs"])


# ── Request/Response Models ──────────────────────────────────────────

class JobSubmitRequest(BaseModel):
    sql: str = Field(..., min_length=1, description="SQL query to optimize")
    credential_id: Optional[str] = Field(default=None, description="Credential UUID for DB connection")
    dsn: Optional[str] = Field(default=None, description="Direct DSN (non-SaaS mode)")
    engine: str = Field(default="postgresql", description="Database engine")
    max_iterations: int = Field(default=3, ge=1, le=10)
    target_speedup: float = Field(default=1.10, ge=1.0)
    query_id: Optional[str] = Field(default=None)
    callback_url: Optional[str] = Field(default=None, description="URL to POST results when done")


class JobResponse(BaseModel):
    id: str
    status: str
    progress: int = 0
    job_type: str = "optimize"
    file_name: str = ""
    best_speedup: Optional[float] = None
    best_sql: Optional[str] = None
    outcome: Optional[str] = None
    result_json: Optional[dict] = None
    error: Optional[str] = None
    created_at: Optional[str] = None
    completed_at: Optional[str] = None


class JobListResponse(BaseModel):
    jobs: list[JobResponse]
    total: int


class JobResultResponse(BaseModel):
    job_id: str
    status: str
    original_sql: str
    optimized_sql: Optional[str] = None
    speedup: Optional[float] = None
    outcome: Optional[str] = None
    transforms: list[str] = []
    diff: Optional[str] = None


# ── Routes ──────────────────────────────────────────────────────────

@router.post("", response_model=JobResponse, status_code=201)
async def submit_job(
    request: JobSubmitRequest,
    user: CurrentUser,
    session: AsyncSession = Depends(get_async_session),
):
    """Submit a single SQL optimization job.

    Creates an AnalysisJob row and dispatches a Celery task.
    Returns immediately — poll GET /api/v1/jobs/{id} for results.
    """
    # Validate credential_id format up front (before any branching)
    cred_uuid = None
    if request.credential_id:
        try:
            cred_uuid = uuid.UUID(request.credential_id)
        except ValueError:
            raise HTTPException(status_code=422, detail="Invalid credential_id format")

    # Resolve DSN
    dsn = request.dsn
    if cred_uuid and not dsn:
        stmt = select(Credential).where(
            Credential.id == cred_uuid,
            Credential.org_id == user.org_id,
            Credential.is_active == True,
        )
        result = await session.execute(stmt)
        cred = result.scalar_one_or_none()
        if not cred:
            raise HTTPException(status_code=404, detail="Credential not found")
        dsn = decrypt_dsn(cred.encrypted_dsn)

    if not dsn:
        raise HTTPException(status_code=400, detail="Either credential_id or dsn is required")

    # Create job
    job = AnalysisJob(
        org_id=user.org_id,
        user_id=user.id,
        file_name=request.query_id or f"query_{uuid.uuid4().hex[:8]}",
        file_type="sql",
        file_hash=hashlib.sha256(request.sql.encode()).hexdigest(),
        input_sql=request.sql,
        credential_id=cred_uuid,
        job_type="optimize",
        status="pending",
        callback_url=request.callback_url,
    )
    session.add(job)
    await session.commit()
    await session.refresh(job)

    # Dispatch Celery task
    from qt_sql.tasks import optimize_query
    task = optimize_query.delay(
        job_id=str(job.id),
        sql=request.sql,
        dsn=dsn,
        engine=request.engine,
        max_iterations=request.max_iterations,
        target_speedup=request.target_speedup,
        org_id=str(user.org_id),
        callback_url=request.callback_url,
    )

    job.celery_task_id = task.id
    await session.commit()

    return JobResponse(
        id=str(job.id),
        status=job.status,
        progress=job.progress,
        job_type=job.job_type,
        file_name=job.file_name,
        created_at=job.created_at.isoformat() if job.created_at else None,
    )


@router.post("/batch", response_model=JobListResponse, status_code=201)
async def submit_batch(
    files: list[UploadFile] = File(...),
    credential_id: Optional[str] = None,
    engine: str = "postgresql",
    user: CurrentUser = Depends(),
    session: AsyncSession = Depends(get_async_session),
):
    """Submit multiple .sql files as a batch of optimization jobs."""
    from qt_shared.config import get_settings, get_tier_features

    settings = get_settings()
    tier_features = get_tier_features(getattr(user, "tier", "free"))
    max_files = tier_features.get("max_batch_files", settings.max_batch_files)

    if len(files) > max_files:
        raise HTTPException(
            status_code=400,
            detail=f"Maximum {max_files} files per batch (got {len(files)})",
        )

    # Resolve DSN once — batch requires a credential
    if not credential_id:
        raise HTTPException(status_code=400, detail="credential_id is required for batch submissions")

    try:
        cred_uuid = uuid.UUID(credential_id)
    except ValueError:
        raise HTTPException(status_code=422, detail="Invalid credential_id format")

    stmt = select(Credential).where(
        Credential.id == cred_uuid,
        Credential.org_id == user.org_id,
        Credential.is_active == True,
    )
    result = await session.execute(stmt)
    cred = result.scalar_one_or_none()
    if not cred:
        raise HTTPException(status_code=404, detail="Credential not found")
    dsn = decrypt_dsn(cred.encrypted_dsn)

    jobs = []
    for f in files:
        content = (await f.read()).decode("utf-8")
        job = AnalysisJob(
            org_id=user.org_id,
            user_id=user.id,
            file_name=f.filename or f"batch_{uuid.uuid4().hex[:8]}.sql",
            file_type="sql",
            file_hash=hashlib.sha256(content.encode()).hexdigest(),
            input_sql=content,
            credential_id=uuid.UUID(credential_id) if credential_id else None,
            job_type="optimize",
            status="pending",
        )
        session.add(job)
        await session.flush()

        from qt_sql.tasks import optimize_query
        task = optimize_query.delay(
            job_id=str(job.id),
            sql=content,
            dsn=dsn,
            engine=engine,
            org_id=str(user.org_id),
        )
        job.celery_task_id = task.id

        jobs.append(job)

    await session.commit()

    return JobListResponse(
        jobs=[
            JobResponse(
                id=str(j.id),
                status=j.status,
                progress=j.progress,
                job_type=j.job_type,
                file_name=j.file_name,
                created_at=j.created_at.isoformat() if j.created_at else None,
            )
            for j in jobs
        ],
        total=len(jobs),
    )


@router.get("", response_model=JobListResponse)
async def list_jobs(
    limit: int = 50,
    offset: int = 0,
    status_filter: Optional[str] = None,
    user: CurrentUser = Depends(),
    session: AsyncSession = Depends(get_async_session),
):
    """List jobs for the current user's organization."""
    stmt = (
        select(AnalysisJob)
        .where(AnalysisJob.org_id == user.org_id)
        .order_by(AnalysisJob.created_at.desc())
        .offset(offset)
        .limit(limit)
    )
    if status_filter:
        stmt = stmt.where(AnalysisJob.status == status_filter)

    result = await session.execute(stmt)
    jobs = result.scalars().all()

    return JobListResponse(
        jobs=[
            JobResponse(
                id=str(j.id),
                status=j.status,
                progress=j.progress,
                job_type=j.job_type,
                file_name=j.file_name,
                best_speedup=j.best_speedup,
                outcome=j.outcome,
                error=j.error,
                created_at=j.created_at.isoformat() if j.created_at else None,
                completed_at=j.completed_at.isoformat() if j.completed_at else None,
            )
            for j in jobs
        ],
        total=len(jobs),
    )


@router.get("/{job_id}", response_model=JobResponse)
async def get_job(
    job_id: str,
    user: CurrentUser = Depends(),
    session: AsyncSession = Depends(get_async_session),
):
    """Get job status and result."""
    try:
        job_uuid = uuid.UUID(job_id)
    except ValueError:
        raise HTTPException(status_code=422, detail="Invalid job ID format")

    stmt = select(AnalysisJob).where(
        AnalysisJob.id == job_uuid,
        AnalysisJob.org_id == user.org_id,
    )
    result = await session.execute(stmt)
    job = result.scalar_one_or_none()
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")

    return JobResponse(
        id=str(job.id),
        status=job.status,
        progress=job.progress,
        job_type=job.job_type,
        file_name=job.file_name,
        best_speedup=job.best_speedup,
        best_sql=job.best_sql,
        outcome=job.outcome,
        result_json=job.result_json,
        error=job.error,
        created_at=job.created_at.isoformat() if job.created_at else None,
        completed_at=job.completed_at.isoformat() if job.completed_at else None,
    )


@router.get("/{job_id}/results", response_model=JobResultResponse)
async def get_job_results(
    job_id: str,
    user: CurrentUser = Depends(),
    session: AsyncSession = Depends(get_async_session),
):
    """Download optimization results for a completed job."""
    try:
        job_uuid = uuid.UUID(job_id)
    except ValueError:
        raise HTTPException(status_code=422, detail="Invalid job ID format")

    stmt = select(AnalysisJob).where(
        AnalysisJob.id == job_uuid,
        AnalysisJob.org_id == user.org_id,
    )
    result = await session.execute(stmt)
    job = result.scalar_one_or_none()
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")

    if job.status not in ("completed", "failed"):
        raise HTTPException(status_code=409, detail=f"Job is still {job.status}")

    transforms = []
    if job.result_json and isinstance(job.result_json, dict):
        transforms = job.result_json.get("transforms", [])

    return JobResultResponse(
        job_id=str(job.id),
        status=job.status,
        original_sql=job.input_sql or "",
        optimized_sql=job.best_sql,
        speedup=job.best_speedup,
        outcome=job.outcome,
        transforms=transforms,
    )
