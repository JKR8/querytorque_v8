"""Tests for configuration module."""

import os
import pytest
from qt_shared.config import Settings, get_tier_features, TIER_FEATURES


def test_settings_defaults():
    """Test that settings have sensible defaults."""
    settings = Settings()
    assert settings.db_host == "localhost"
    assert settings.db_port == "5432"
    assert settings.db_name == "querytorque"
    assert settings.require_auth is False


def test_tier_features():
    """Test tier feature configuration."""
    free = get_tier_features("free")
    assert free["analyses_per_day"] == 1
    assert free["llm_fixes"] is False

    enterprise = get_tier_features("enterprise")
    assert enterprise["analyses_per_day"] == -1  # unlimited
    assert enterprise["llm_fixes"] is True
    assert enterprise["sso"] is True


def test_unknown_tier_defaults_to_free():
    """Test that unknown tiers default to free features."""
    unknown = get_tier_features("unknown_tier")
    free = get_tier_features("free")
    assert unknown == free


def test_auth_enabled_property():
    """Test auth_enabled property."""
    settings = Settings()
    assert settings.auth_enabled is False

    # Would be True if both require_auth and auth0_domain are set
    settings.require_auth = True
    settings.auth0_domain = ""
    assert settings.auth_enabled is False


def test_has_llm_provider_property():
    """Test has_llm_provider property."""
    settings = Settings()
    assert settings.has_llm_provider is False

    settings.llm_provider = "groq"
    assert settings.has_llm_provider is True
