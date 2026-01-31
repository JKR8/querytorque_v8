"""Tier feature configuration for QueryTorque products."""

# Feature tier configuration shared across products
TIER_FEATURES = {
    "free": {
        "analyses_per_day": 1,
        "llm_fixes": False,
        "api_access": False,
        "max_file_size_mb": 1,
        "workspaces": 1,
    },
    "workspace_pro": {
        "analyses_per_day": -1,  # unlimited
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
        "api_calls_per_month": -1,  # unlimited
        "max_file_size_mb": 200,
        "workspaces": -1,  # unlimited
        "sso": True,
    },
}


def get_tier_features(tier: str) -> dict:
    """Get feature configuration for a tier."""
    return TIER_FEATURES.get(tier, TIER_FEATURES["free"])
