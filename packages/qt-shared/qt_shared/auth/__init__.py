"""Authentication module for QueryTorque shared infrastructure."""

from .context import UserContext
from .middleware import (
    bearer_scheme,
    api_key_header,
    get_jwks,
    get_signing_key,
    verify_jwt_token,
    verify_api_key,
    generate_api_key,
    get_current_user,
    require_auth,
    require_role,
    OptionalUser,
    CurrentUser,
    AdminUser,
)
from .service import AuthService

__all__ = [
    # Context
    "UserContext",
    # Middleware
    "bearer_scheme",
    "api_key_header",
    "get_jwks",
    "get_signing_key",
    "verify_jwt_token",
    "verify_api_key",
    "generate_api_key",
    "get_current_user",
    "require_auth",
    "require_role",
    "OptionalUser",
    "CurrentUser",
    "AdminUser",
    # Service
    "AuthService",
]
