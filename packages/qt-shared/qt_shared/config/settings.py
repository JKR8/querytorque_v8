"""Shared application configuration for QueryTorque products."""

from functools import lru_cache
from typing import Optional
from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    """Shared application settings loaded from environment.

    Product-specific settings should be defined in their respective packages.
    """

    # Database
    database_url: str = ""
    db_host: str = "localhost"
    db_port: str = "5432"
    db_name: str = "querytorque"
    db_user: str = "querytorque"
    db_password: str = ""
    db_echo: bool = False
    serverless: bool = False

    # Redis (for rate limiting)
    redis_url: str = "memory://"

    # Auth0
    auth0_domain: str = ""
    auth0_api_audience: str = ""
    auth0_client_id: str = ""
    auth0_client_secret: str = ""
    auth0_algorithms: str = "RS256"

    # Stripe
    stripe_api_key: str = ""
    stripe_webhook_secret: str = ""
    stripe_price_free: str = ""
    stripe_price_workspace_pro: str = ""
    stripe_price_capacity: str = ""
    stripe_price_enterprise: str = ""

    # Feature flags
    require_auth: bool = False

    # CORS
    cors_origins: str = "http://localhost:5173"

    # File limits
    max_file_size_mb: int = 50
    max_batch_files: int = 20

    # LLM Configuration (shared across products)
    deepseek_api_key: str = ""
    gemini_api_key: str = ""
    groq_api_key: str = ""
    openai_api_key: str = ""
    openrouter_api_key: str = ""
    llm_provider: str = ""
    llm_model: str = ""
    manual_mode: bool = False

    # Pattern selector (local Ollama or any OpenAI-compatible endpoint)
    pattern_selector_url: str = ""
    pattern_selector_model: str = "qwen2.5-coder:14b-instruct-q4_K_M"
    pattern_selector_api_key: str = ""

    # Snowflake connection
    snowflake_account: str = ""
    snowflake_user: str = ""
    snowflake_password: str = ""
    snowflake_warehouse: str = ""
    snowflake_database: str = ""
    snowflake_schema: str = ""

    class Config:
        env_prefix = "QT_"
        env_file = ".env"

    @property
    def auth_enabled(self) -> bool:
        """Check if authentication is enabled."""
        return self.require_auth and bool(self.auth0_domain)

    @property
    def saas_mode(self) -> bool:
        """Check if running in full SaaS mode (auth + billing configured)."""
        return self.auth_enabled and bool(self.stripe_api_key)

    @property
    def has_database(self) -> bool:
        """Check if database is configured."""
        return bool(self.database_url) or bool(self.db_password)

    @property
    def is_manual_mode(self) -> bool:
        """Check if running in manual mode (no LLM API available)."""
        if self.manual_mode:
            return True
        return not self.has_llm_provider

    @property
    def has_llm_provider(self) -> bool:
        """Check if an LLM provider is configured."""
        return bool(self.llm_provider)

    def get_llm_provider_config(self) -> tuple[str, str, str]:
        """Get the active LLM provider, model, and API key.

        Returns:
            Tuple of (provider, model, api_key).
        """
        provider = self.llm_provider
        model = self.llm_model

        if provider == "deepseek":
            return provider, model or "deepseek-reasoner", self.deepseek_api_key
        elif provider == "gemini-api":
            return provider, model or "gemini-3-flash-preview", self.gemini_api_key
        elif provider == "gemini-cli":
            return provider, model, ""
        elif provider == "groq":
            return provider, model or "llama-3.3-70b-versatile", self.groq_api_key
        elif provider == "openai":
            return provider, model or "gpt-4o", self.openai_api_key
        elif provider == "openrouter":
            return provider, model or "moonshotai/kimi-k2.5", self.openrouter_api_key
        return "", "", ""


# Feature tier configuration
TIER_FEATURES = {
    "free": {
        "analyses_per_day": 1,
        "llm_fixes": False,
        "api_access": False,
        "max_file_size_mb": 1,
        "workspaces": 1,
    },
    "workspace_pro": {
        "analyses_per_day": -1,
        "llm_fixes": True,
        "api_access": True,
        "api_calls_per_month": 1000,
        "max_file_size_mb": 10,
        "workspaces": 1,
    },
    "capacity": {
        "analyses_per_day": -1,
        "llm_fixes": True,
        "api_access": True,
        "api_calls_per_month": 10000,
        "max_file_size_mb": 50,
        "workspaces": 5,
    },
    "enterprise": {
        "analyses_per_day": -1,
        "llm_fixes": True,
        "api_access": True,
        "api_calls_per_month": -1,
        "max_file_size_mb": 200,
        "workspaces": -1,
        "sso": True,
    },
}


@lru_cache
def get_settings() -> Settings:
    """Get cached settings instance."""
    return Settings()


def get_tier_features(tier: str) -> dict:
    """Get feature configuration for a tier."""
    return TIER_FEATURES.get(tier, TIER_FEATURES["free"])
