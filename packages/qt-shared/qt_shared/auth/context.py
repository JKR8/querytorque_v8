"""User context model for authentication."""

from pydantic import BaseModel


class UserContext(BaseModel):
    """User context from authentication.

    This represents the authenticated user's identity and permissions.
    Used across all QueryTorque products.
    """

    user_id: str
    org_id: str
    email: str
    role: str = "member"
    tier: str = "free"
    auth_method: str = "jwt"  # "jwt", "api_key", "disabled", "database"

    class Config:
        frozen = True
