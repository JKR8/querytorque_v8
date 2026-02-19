"""Credential management routes (Tier A).

POST   /api/v1/credentials            — Store encrypted credential
GET    /api/v1/credentials            — List credentials (names only, no DSNs)
POST   /api/v1/credentials/{id}/test  — Test credential connectivity
DELETE /api/v1/credentials/{id}       — Delete credential
"""

import logging
import uuid

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel, Field
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from qt_shared.auth.middleware import CurrentUser
from qt_shared.database.connection import get_async_session
from qt_shared.database.models import Credential
from qt_shared.vault import encrypt_dsn, decrypt_dsn, mask_dsn

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/api/v1/credentials", tags=["Credentials"])


class CredentialCreateRequest(BaseModel):
    name: str = Field(..., min_length=1, max_length=255)
    dsn: str = Field(..., min_length=1, description="Database connection string")
    engine: str = Field(default="postgresql", description="postgresql, duckdb, or snowflake")


class CredentialResponse(BaseModel):
    id: str
    name: str
    engine: str
    is_active: bool
    last_tested_at: str | None = None
    created_at: str | None = None


class CredentialListResponse(BaseModel):
    credentials: list[CredentialResponse]


class ConnectivityTestResponse(BaseModel):
    success: bool
    message: str
    details: str | None = None


@router.post("", response_model=CredentialResponse, status_code=201)
async def create_credential(
    request: CredentialCreateRequest,
    user: CurrentUser,
    session: AsyncSession = Depends(get_async_session),
):
    """Store an encrypted database credential."""
    encrypted = encrypt_dsn(request.dsn)
    masked = mask_dsn(request.dsn)
    logger.info("Storing credential '%s' for org %s (%s)", request.name, user.org_id, masked)

    cred = Credential(
        org_id=user.org_id,
        name=request.name,
        engine=request.engine,
        encrypted_dsn=encrypted,
    )
    session.add(cred)
    await session.commit()
    await session.refresh(cred)

    return CredentialResponse(
        id=str(cred.id),
        name=cred.name,
        engine=cred.engine,
        is_active=cred.is_active,
        created_at=cred.created_at.isoformat() if cred.created_at else None,
    )


@router.get("", response_model=CredentialListResponse)
async def list_credentials(
    user: CurrentUser,
    session: AsyncSession = Depends(get_async_session),
):
    """List credentials for the current organization (names only, no DSNs)."""
    stmt = (
        select(Credential)
        .where(Credential.org_id == user.org_id)
        .order_by(Credential.created_at.desc())
    )
    result = await session.execute(stmt)
    creds = result.scalars().all()

    return CredentialListResponse(
        credentials=[
            CredentialResponse(
                id=str(c.id),
                name=c.name,
                engine=c.engine,
                is_active=c.is_active,
                last_tested_at=c.last_tested_at.isoformat() if c.last_tested_at else None,
                created_at=c.created_at.isoformat() if c.created_at else None,
            )
            for c in creds
        ]
    )


@router.post("/{credential_id}/test", response_model=ConnectivityTestResponse)
async def test_credential(
    credential_id: str,
    user: CurrentUser,
    session: AsyncSession = Depends(get_async_session),
):
    """Test database connectivity for a stored credential."""
    stmt = select(Credential).where(
        Credential.id == uuid.UUID(credential_id),
        Credential.org_id == user.org_id,
    )
    result = await session.execute(stmt)
    cred = result.scalar_one_or_none()
    if not cred:
        raise HTTPException(status_code=404, detail="Credential not found")

    dsn = decrypt_dsn(cred.encrypted_dsn)

    try:
        from qt_sql.execution.factory import create_executor_from_dsn

        with create_executor_from_dsn(dsn) as executor:
            schema = executor.get_schema_info(include_row_counts=False)
            table_count = len(schema.get("tables", []))

        from datetime import datetime
        cred.last_tested_at = datetime.utcnow()
        await session.commit()

        return ConnectivityTestResponse(
            success=True,
            message=f"Connected successfully ({table_count} tables)",
            details=f"Engine: {cred.engine}",
        )

    except Exception as e:
        return ConnectivityTestResponse(
            success=False,
            message=f"Connection failed: {str(e)}",
        )


@router.delete("/{credential_id}")
async def delete_credential(
    credential_id: str,
    user: CurrentUser,
    session: AsyncSession = Depends(get_async_session),
):
    """Delete a stored credential."""
    stmt = select(Credential).where(
        Credential.id == uuid.UUID(credential_id),
        Credential.org_id == user.org_id,
    )
    result = await session.execute(stmt)
    cred = result.scalar_one_or_none()
    if not cred:
        raise HTTPException(status_code=404, detail="Credential not found")

    await session.delete(cred)
    await session.commit()

    return {"success": True, "message": "Credential deleted"}
