"""Authentication middleware for QueryTorque API."""

import hashlib
import secrets
from typing import Optional, Annotated
from datetime import datetime

from fastapi import Depends, HTTPException, status, Request, Security
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials, APIKeyHeader
from jose import jwt, JWTError
import httpx

from ..config import get_settings
from .context import UserContext


# Security schemes
bearer_scheme = HTTPBearer(auto_error=False)
api_key_header = APIKeyHeader(name="X-API-Key", auto_error=False)


# JWKS cache
_jwks_cache: dict = {}
_jwks_cache_time: Optional[datetime] = None
JWKS_CACHE_TTL = 3600  # 1 hour


async def get_jwks(domain: str) -> dict:
    """Fetch and cache JWKS from Auth0."""
    global _jwks_cache, _jwks_cache_time

    now = datetime.utcnow()
    if _jwks_cache_time and (now - _jwks_cache_time).total_seconds() < JWKS_CACHE_TTL:
        return _jwks_cache

    jwks_url = f"https://{domain}/.well-known/jwks.json"
    async with httpx.AsyncClient() as client:
        response = await client.get(jwks_url, timeout=10)
        response.raise_for_status()
        _jwks_cache = response.json()
        _jwks_cache_time = now
        return _jwks_cache


def get_signing_key(token: str, jwks: dict) -> Optional[dict]:
    """Get the signing key for a JWT from JWKS."""
    try:
        unverified_header = jwt.get_unverified_header(token)
    except JWTError:
        return None

    kid = unverified_header.get("kid")
    if not kid:
        return None

    for key in jwks.get("keys", []):
        if key.get("kid") == kid:
            return key

    return None


async def verify_jwt_token(token: str) -> Optional[UserContext]:
    """Verify a JWT token and return user context."""
    settings = get_settings()

    if not settings.auth0_domain:
        return None

    try:
        # Get JWKS
        jwks = await get_jwks(settings.auth0_domain)
        signing_key = get_signing_key(token, jwks)

        if not signing_key:
            return None

        # Verify the token
        payload = jwt.decode(
            token,
            signing_key,
            algorithms=[settings.auth0_algorithms],
            audience=settings.auth0_api_audience,
            issuer=f"https://{settings.auth0_domain}/",
        )

        # Extract user info
        user_id = payload.get("sub", "")
        email = payload.get("email", payload.get(f"{settings.auth0_api_audience}/email", ""))

        # Extract custom claims (org_id, role, tier)
        org_id = payload.get(f"{settings.auth0_api_audience}/org_id", "")
        role = payload.get(f"{settings.auth0_api_audience}/role", "member")
        tier = payload.get(f"{settings.auth0_api_audience}/tier", "free")

        if not user_id:
            return None

        return UserContext(
            user_id=user_id,
            org_id=org_id,
            email=email,
            role=role,
            tier=tier,
            auth_method="jwt",
        )

    except JWTError:
        return None
    except httpx.HTTPError:
        return None


async def verify_api_key(api_key: str) -> Optional[UserContext]:
    """Verify an API key and return user context."""
    from ..database import get_session_context, User, Organization

    if not api_key or len(api_key) < 20:
        return None

    # Extract prefix (first 16 chars for better collision resistance)
    prefix = api_key[:16]

    # Hash the full key
    key_hash = hashlib.sha256(api_key.encode()).hexdigest()

    try:
        async with get_session_context() as session:
            from sqlalchemy import select

            # Find user by API key prefix and verify hash
            result = await session.execute(
                select(User, Organization)
                .join(Organization, User.org_id == Organization.id)
                .where(User.api_key_prefix == prefix)
                .where(User.api_key_hash == key_hash)
                .where(User.is_active == True)
            )
            row = result.first()

            if not row:
                return None

            user, org = row

            return UserContext(
                user_id=str(user.id),
                org_id=str(org.id),
                email=user.email,
                role=user.role,
                tier=org.tier,
                auth_method="api_key",
            )

    except Exception:
        return None


def generate_api_key() -> tuple[str, str, str]:
    """Generate a new API key.

    SECURITY: Uses 32 bytes of cryptographically secure random data.
    Prefix is 16 characters for better collision resistance.

    Returns:
        Tuple of (full_key, prefix, hash)
    """
    full_key = f"qt_{secrets.token_urlsafe(32)}"
    prefix = full_key[:16]
    key_hash = hashlib.sha256(full_key.encode()).hexdigest()

    return full_key, prefix, key_hash


async def get_current_user(
    request: Request,
    bearer_credentials: Annotated[
        Optional[HTTPAuthorizationCredentials], Security(bearer_scheme)
    ] = None,
    api_key: Annotated[Optional[str], Security(api_key_header)] = None,
) -> Optional[UserContext]:
    """Get current user from JWT or API key.

    This is a non-raising dependency - returns None if not authenticated.
    """
    # Try JWT first
    if bearer_credentials:
        user = await verify_jwt_token(bearer_credentials.credentials)
        if user:
            return user

    # Try API key
    if api_key:
        user = await verify_api_key(api_key)
        if user:
            return user

    return None


async def require_auth(
    user: Annotated[Optional[UserContext], Depends(get_current_user)],
) -> UserContext:
    """Require authentication - raises 401 if not authenticated."""
    settings = get_settings()

    # Skip auth check if not enabled
    if not settings.auth_enabled:
        return UserContext(
            user_id="anonymous",
            org_id="default",
            email="anonymous@local",
            role="admin",
            tier="enterprise",
            auth_method="disabled",
        )

    if not user:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Not authenticated",
            headers={"WWW-Authenticate": "Bearer"},
        )

    return user


def require_role(*roles: str):
    """Decorator to require specific roles."""
    async def role_checker(
        user: Annotated[UserContext, Depends(require_auth)],
    ) -> UserContext:
        if user.role not in roles:
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail=f"Role '{user.role}' not authorized. Required: {', '.join(roles)}",
            )
        return user
    return role_checker


# Type aliases for FastAPI dependencies
OptionalUser = Annotated[Optional[UserContext], Depends(get_current_user)]
CurrentUser = Annotated[UserContext, Depends(require_auth)]
AdminUser = Annotated[UserContext, Depends(require_role("admin", "owner"))]
