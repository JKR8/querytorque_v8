"""Fleet dashboard SaaS routes (Tier B).

POST /api/v1/fleet/survey       — Trigger fleet survey on customer DB
GET  /api/v1/fleet/surveys      — List surveys for current org
GET  /api/v1/fleet/surveys/{id} — Get survey status and triage results
"""

import logging
import uuid

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel, Field
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from qt_shared.auth.middleware import CurrentUser
from qt_shared.database.connection import get_async_session
from qt_shared.database.models import Credential, FleetSurvey
from qt_shared.vault import decrypt_dsn

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/api/v1/fleet", tags=["Fleet"])


class SurveyRequest(BaseModel):
    credential_id: str = Field(..., description="Credential UUID for DB connection")


class SurveyResponse(BaseModel):
    id: str
    status: str
    celery_task_id: str | None = None
    created_at: str | None = None


class SurveyDetailResponse(BaseModel):
    id: str
    status: str
    triage: dict | None = None
    results: dict | None = None
    created_at: str | None = None
    updated_at: str | None = None


class SurveyListResponse(BaseModel):
    surveys: list[SurveyResponse]


@router.post("/survey", response_model=SurveyResponse, status_code=201)
async def create_survey(
    request: SurveyRequest,
    user: CurrentUser,
    session: AsyncSession = Depends(get_async_session),
):
    """Trigger a fleet survey on a customer database.

    Dispatches a Celery task that connects to the DB, discovers slow queries,
    and produces triage results.
    """
    # Verify credential
    stmt = select(Credential).where(
        Credential.id == uuid.UUID(request.credential_id),
        Credential.org_id == user.org_id,
        Credential.is_active == True,
    )
    result = await session.execute(stmt)
    cred = result.scalar_one_or_none()
    if not cred:
        raise HTTPException(status_code=404, detail="Credential not found")

    dsn = decrypt_dsn(cred.encrypted_dsn)

    # Create survey record
    survey = FleetSurvey(
        org_id=user.org_id,
        credential_id=cred.id,
        status="pending",
    )
    session.add(survey)
    await session.commit()
    await session.refresh(survey)

    # Dispatch Celery task
    from qt_sql.tasks import fleet_survey as fleet_survey_task
    task = fleet_survey_task.delay(
        survey_id=str(survey.id),
        dsn=dsn,
        engine=cred.engine,
        org_id=str(user.org_id),
    )

    survey.celery_task_id = task.id
    survey.status = "processing"
    await session.commit()

    return SurveyResponse(
        id=str(survey.id),
        status=survey.status,
        celery_task_id=task.id,
        created_at=survey.created_at.isoformat() if survey.created_at else None,
    )


@router.get("/surveys", response_model=SurveyListResponse)
async def list_surveys(
    user: CurrentUser,
    session: AsyncSession = Depends(get_async_session),
):
    """List fleet surveys for the current organization."""
    stmt = (
        select(FleetSurvey)
        .where(FleetSurvey.org_id == user.org_id)
        .order_by(FleetSurvey.created_at.desc())
        .limit(50)
    )
    result = await session.execute(stmt)
    surveys = result.scalars().all()

    return SurveyListResponse(
        surveys=[
            SurveyResponse(
                id=str(s.id),
                status=s.status,
                celery_task_id=s.celery_task_id,
                created_at=s.created_at.isoformat() if s.created_at else None,
            )
            for s in surveys
        ]
    )


@router.get("/surveys/{survey_id}", response_model=SurveyDetailResponse)
async def get_survey(
    survey_id: str,
    user: CurrentUser,
    session: AsyncSession = Depends(get_async_session),
):
    """Get survey status and triage results."""
    stmt = select(FleetSurvey).where(
        FleetSurvey.id == uuid.UUID(survey_id),
        FleetSurvey.org_id == user.org_id,
    )
    result = await session.execute(stmt)
    survey = result.scalar_one_or_none()
    if not survey:
        raise HTTPException(status_code=404, detail="Survey not found")

    return SurveyDetailResponse(
        id=str(survey.id),
        status=survey.status,
        triage=survey.triage_json,
        results=survey.results_json,
        created_at=survey.created_at.isoformat() if survey.created_at else None,
        updated_at=survey.updated_at.isoformat() if survey.updated_at else None,
    )
