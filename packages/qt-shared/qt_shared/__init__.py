"""QueryTorque Shared Infrastructure.

This package provides shared components for QueryTorque products:
- auth: Auth0 middleware and UserContext
- billing: Stripe integration and tier features
- database: SQLAlchemy models and connection management
- llm: LLM client implementations (DeepSeek, Groq, Gemini, OpenAI, etc.)
- config: Shared settings management
"""

__version__ = "0.1.0"

from .config.settings import Settings, get_settings, TIER_FEATURES
from .auth.context import UserContext
from .database.models import Base

__all__ = [
    "Settings",
    "get_settings",
    "TIER_FEATURES",
    "UserContext",
    "Base",
]
