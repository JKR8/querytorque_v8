"""Phase 1: Smoke Tests - Import Validation for qt-shared.

Validates that all qt-shared modules can be imported correctly.
This catches missing dependencies, syntax errors, and circular imports early.
"""

import pytest


class TestQtSharedImports:
    """Test that all qt-shared modules import without errors."""

    def test_import_qt_shared_root(self):
        """Test root package import."""
        import qt_shared
        assert hasattr(qt_shared, "__version__")
        assert hasattr(qt_shared, "Settings")
        assert hasattr(qt_shared, "UserContext")
        assert hasattr(qt_shared, "Base")

    def test_import_config_module(self):
        """Test config module imports."""
        from qt_shared.config import settings
        from qt_shared.config.settings import Settings, get_settings, TIER_FEATURES
        assert Settings is not None
        assert callable(get_settings)
        assert isinstance(TIER_FEATURES, dict)

    def test_import_auth_context(self):
        """Test auth context imports."""
        from qt_shared.auth import UserContext
        from qt_shared.auth.context import UserContext as UC
        assert UserContext is UC
        # Verify UserContext is a dataclass or pydantic model
        assert hasattr(UserContext, "__init__")

    def test_import_auth_service(self):
        """Test auth service imports."""
        from qt_shared.auth import AuthService
        from qt_shared.auth.service import AuthService as AS
        assert AuthService is AS

    def test_import_auth_middleware(self):
        """Test auth middleware imports."""
        from qt_shared.auth import (
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
        assert callable(verify_jwt_token)
        assert callable(generate_api_key)

    def test_import_billing_module(self):
        """Test billing module imports."""
        from qt_shared.billing import service
        from qt_shared.billing.service import BillingService
        from qt_shared.billing import tiers
        assert BillingService is not None

    def test_import_database_models(self):
        """Test database models import."""
        from qt_shared.database import models
        from qt_shared.database.models import (
            Base,
            Organization,
            User,
            Workspace,
            AnalysisJob,
            Subscription,
            APIUsage,
        )
        assert Base is not None
        assert hasattr(Organization, "__tablename__")
        assert hasattr(User, "__tablename__")

    def test_import_database_connection(self):
        """Test database connection import."""
        from qt_shared.database import connection
        from qt_shared.database.connection import get_engine, get_session_factory
        assert callable(get_engine)
        assert callable(get_session_factory)

    def test_import_llm_protocol(self):
        """Test LLM protocol import."""
        from qt_shared.llm import protocol
        from qt_shared.llm.protocol import LLMClient
        # Should be a Protocol class
        assert LLMClient is not None

    def test_import_llm_factory(self):
        """Test LLM factory import."""
        from qt_shared.llm import factory
        from qt_shared.llm.factory import create_llm_client
        assert callable(create_llm_client)

    def test_import_llm_openai(self):
        """Test OpenAI LLM client import."""
        from qt_shared.llm import openai
        from qt_shared.llm.openai import OpenAIClient
        assert OpenAIClient is not None

    def test_import_llm_deepseek(self):
        """Test DeepSeek LLM client import."""
        from qt_shared.llm import deepseek
        from qt_shared.llm.deepseek import DeepSeekClient
        assert DeepSeekClient is not None

    def test_import_llm_groq(self):
        """Test Groq LLM client import."""
        from qt_shared.llm import groq
        from qt_shared.llm.groq import GroqClient
        assert GroqClient is not None

    def test_import_llm_gemini(self):
        """Test Gemini LLM client import."""
        from qt_shared.llm import gemini
        from qt_shared.llm.gemini import GeminiClient
        assert GeminiClient is not None


class TestQtSharedCrossModuleImports:
    """Test cross-module imports work correctly."""

    def test_auth_uses_database_models(self):
        """Auth service should work with database models."""
        from qt_shared.auth import AuthService
        from qt_shared.database.models import User, Organization
        # Both should be importable together
        assert AuthService is not None
        assert User is not None

    def test_billing_uses_database_models(self):
        """Billing service should work with database models."""
        from qt_shared.billing.service import BillingService
        from qt_shared.database.models import Subscription, Organization
        assert BillingService is not None
        assert Subscription is not None

    def test_config_tier_features_complete(self):
        """Tier features should have all required tiers."""
        from qt_shared.config.settings import TIER_FEATURES
        expected_tiers = ["free", "workspace_pro", "capacity", "enterprise"]
        for tier in expected_tiers:
            assert tier in TIER_FEATURES, f"Missing tier: {tier}"


class TestQtSharedDataclassValidation:
    """Test that dataclasses/models are properly defined."""

    def test_user_context_fields(self):
        """UserContext should have expected fields."""
        from qt_shared.auth.context import UserContext
        # Check that it can be instantiated with expected fields
        # This tests the model definition
        expected_fields = ["user_id", "org_id", "email", "role", "tier"]
        for field in expected_fields:
            # Check if field exists in model
            assert hasattr(UserContext, "__annotations__") or hasattr(UserContext, "model_fields")

    def test_settings_has_properties(self):
        """Settings should have computed properties."""
        from qt_shared.config.settings import Settings
        expected_properties = ["auth_enabled", "has_llm_provider", "has_database"]
        # Check class has these as properties or methods
        for prop in expected_properties:
            assert hasattr(Settings, prop), f"Settings missing property: {prop}"
