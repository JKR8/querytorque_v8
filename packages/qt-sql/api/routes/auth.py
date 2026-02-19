"""Authentication and API key management routes.

POST /api/v1/auth/callback     — Auth0 callback (create/get user)
POST /api/v1/auth/api-keys     — Generate API key for current user
DELETE /api/v1/auth/api-keys   — Revoke API key
GET  /api/v1/auth/me           — Get current user info
"""

import logging
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel
from sqlalchemy.ext.asyncio import AsyncSession

from qt_shared.auth.middleware import CurrentUser
from qt_shared.database.connection import get_async_session

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/api/v1/auth", tags=["Auth"])


class UserInfoResponse(BaseModel):
    id: str
    email: str
    org_id: str
    role: str
    tier: str = "free"


class APIKeyResponse(BaseModel):
    api_key: str
    prefix: str
    message: str = "Store this key securely — it will not be shown again."


class CallbackRequest(BaseModel):
    auth0_id: str
    email: str
    name: Optional[str] = None


class CallbackResponse(BaseModel):
    user_id: str
    org_id: str
    is_new: bool


@router.get("/me", response_model=UserInfoResponse)
async def get_current_user_info(user: CurrentUser):
    """Get current authenticated user info."""
    return UserInfoResponse(
        id=str(user.id),
        email=user.email,
        org_id=str(user.org_id),
        role=user.role,
        tier=getattr(user, "tier", "free"),
    )


@router.post("/callback", response_model=CallbackResponse)
async def auth_callback(
    request: CallbackRequest,
    session: AsyncSession = Depends(get_async_session),
):
    """Auth0 callback — get or create user and org on first login."""
    from qt_shared.auth.service import AuthService

    auth_service = AuthService(session)
    user, is_new = await auth_service.get_or_create_user(
        auth0_id=request.auth0_id,
        email=request.email,
        name=request.name,
    )

    return CallbackResponse(
        user_id=str(user.id),
        org_id=str(user.org_id),
        is_new=is_new,
    )


@router.post("/api-keys", response_model=APIKeyResponse)
async def generate_api_key(
    user: CurrentUser,
    session: AsyncSession = Depends(get_async_session),
):
    """Generate a new API key for the current user.

    The full key is returned only once. Store it securely.
    """
    from qt_shared.auth.service import AuthService

    auth_service = AuthService(session)
    full_key = await auth_service.generate_user_api_key(user.id)

    return APIKeyResponse(
        api_key=full_key,
        prefix=full_key[:16],
    )


@router.delete("/api-keys")
async def revoke_api_key(
    user: CurrentUser,
    session: AsyncSession = Depends(get_async_session),
):
    """Revoke the current user's API key."""
    from qt_shared.auth.service import AuthService

    auth_service = AuthService(session)
    await auth_service.revoke_user_api_key(user.id)

    return {"success": True, "message": "API key revoked"}
