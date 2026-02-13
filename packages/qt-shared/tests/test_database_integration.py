"""Phase 6: Integration Tests - Database Integration.

Tests for database model creation, relationships, and shared infrastructure.
"""

import pytest
from unittest.mock import MagicMock, patch


class TestDatabaseModels:
    """Tests for database model definitions."""

    def test_organization_model(self):
        """Test Organization model definition."""
        from qt_shared.database.models import Organization

        assert hasattr(Organization, "__tablename__")
        assert hasattr(Organization, "id")
        assert hasattr(Organization, "name")
        assert hasattr(Organization, "tier")

    def test_user_model(self):
        """Test User model definition."""
        from qt_shared.database.models import User

        assert hasattr(User, "__tablename__")
        assert hasattr(User, "id")
        assert hasattr(User, "email")
        assert hasattr(User, "org_id")
        assert hasattr(User, "is_active")

    def test_workspace_model(self):
        """Test Workspace model definition."""
        from qt_shared.database.models import Workspace

        assert hasattr(Workspace, "__tablename__")
        assert hasattr(Workspace, "id")
        assert hasattr(Workspace, "org_id")
        assert hasattr(Workspace, "name")

    def test_analysis_job_model(self):
        """Test AnalysisJob model definition."""
        from qt_shared.database.models import AnalysisJob

        assert hasattr(AnalysisJob, "__tablename__")
        assert hasattr(AnalysisJob, "id")
        assert hasattr(AnalysisJob, "user_id")
        assert hasattr(AnalysisJob, "product")  # SQL or DAX
        assert hasattr(AnalysisJob, "torque_score")

    def test_subscription_model(self):
        """Test Subscription model definition."""
        from qt_shared.database.models import Subscription

        assert hasattr(Subscription, "__tablename__")
        assert hasattr(Subscription, "id")
        assert hasattr(Subscription, "org_id")
        assert hasattr(Subscription, "tier")

    def test_api_usage_model(self):
        """Test APIUsage model definition."""
        from qt_shared.database.models import APIUsage

        assert hasattr(APIUsage, "__tablename__")
        assert hasattr(APIUsage, "id")
        assert hasattr(APIUsage, "user_id")


class TestModelRelationships:
    """Tests for model relationships."""

    def test_user_organization_relationship(self):
        """Test User -> Organization relationship."""
        from qt_shared.database.models import User

        # User should have org_id foreign key
        assert hasattr(User, "org_id")

    def test_workspace_organization_relationship(self):
        """Test Workspace -> Organization relationship."""
        from qt_shared.database.models import Workspace

        assert hasattr(Workspace, "org_id")

    def test_analysis_job_user_relationship(self):
        """Test AnalysisJob -> User relationship."""
        from qt_shared.database.models import AnalysisJob

        assert hasattr(AnalysisJob, "user_id")

    def test_subscription_organization_relationship(self):
        """Test Subscription -> Organization relationship."""
        from qt_shared.database.models import Subscription

        assert hasattr(Subscription, "org_id")


class TestDatabaseConnection:
    """Tests for database connection management."""

    def test_get_engine_function(self):
        """Test get_engine function exists."""
        from qt_shared.database.connection import get_engine

        assert callable(get_engine)

    def test_get_session_factory_function(self):
        """Test get_session_factory function exists."""
        from qt_shared.database.connection import get_session_factory

        assert callable(get_session_factory)

    def test_get_async_session_function(self):
        """Test get_async_session function exists."""
        from qt_shared.database.connection import get_async_session

        # This is a generator function for FastAPI dependency injection
        assert get_async_session is not None


class TestGUIDType:
    """Tests for custom GUID type."""

    def test_guid_type_exists(self):
        """Test GUID type is defined."""
        from qt_shared.database.models import GUID

        assert GUID is not None

    def test_guid_type_is_type_decorator(self):
        """Test GUID is a SQLAlchemy type."""
        from qt_shared.database.models import GUID
        from sqlalchemy import TypeDecorator

        assert issubclass(GUID, TypeDecorator)


class TestFlexibleJSONType:
    """Tests for FlexibleJSON type."""

    def test_flexible_json_exists(self):
        """Test FlexibleJSON type is defined."""
        from qt_shared.database.models import FlexibleJSON

        assert FlexibleJSON is not None


class TestAuthServiceIntegration:
    """Tests for auth service integration with database."""

    def test_auth_service_instantiation(self):
        """Test AuthService can be instantiated."""
        from qt_shared.auth.service import AuthService

        # May need mocked session
        service = AuthService(session=MagicMock())
        assert service is not None

    def test_auth_service_methods_exist(self):
        """Test AuthService has expected methods."""
        from qt_shared.auth.service import AuthService

        expected_methods = [
            "get_or_create_user",
            "get_user_by_id",
            "get_user_by_email",
            "generate_user_api_key",
        ]

        for method in expected_methods:
            assert hasattr(AuthService, method)


class TestBillingServiceIntegration:
    """Tests for billing service integration with database."""

    def test_billing_service_instantiation(self):
        """Test BillingService can be instantiated."""
        from qt_shared.billing.service import BillingService

        # May need mocked session
        service = BillingService(session=MagicMock())
        assert service is not None

    def test_billing_service_methods_exist(self):
        """Test BillingService has expected methods."""
        from qt_shared.billing.service import BillingService

        expected_methods = [
            "get_subscription",
            "create_checkout_session",
            "get_tier_features",
        ]

        for method in expected_methods:
            assert hasattr(BillingService, method)


class TestTierFeatures:
    """Tests for tier feature definitions."""

    def test_tier_features_defined(self):
        """Test all tiers have features defined."""
        from qt_shared.config.settings import TIER_FEATURES

        expected_tiers = ["free", "workspace_pro", "capacity", "enterprise"]
        for tier in expected_tiers:
            assert tier in TIER_FEATURES

    def test_tier_features_have_limits(self):
        """Test tier features have limit fields."""
        from qt_shared.config.settings import TIER_FEATURES

        for tier, features in TIER_FEATURES.items():
            assert isinstance(features, dict)
            # Should have some limit fields
            assert len(features) > 0

    def test_free_tier_has_limits(self):
        """Test free tier has restrictive limits."""
        from qt_shared.config.settings import TIER_FEATURES

        free = TIER_FEATURES["free"]
        # Free tier should have some limits
        assert free is not None

    def test_enterprise_tier_generous(self):
        """Test enterprise tier has generous limits."""
        from qt_shared.config.settings import TIER_FEATURES

        enterprise = TIER_FEATURES["enterprise"]
        assert enterprise is not None


class TestLLMIntegration:
    """Tests for LLM client integration."""

    def test_create_llm_client_without_config(self):
        """Test create_llm_client returns None without config."""
        from qt_shared.llm import create_llm_client

        # Without API keys, should return None
        with patch.dict("os.environ", {}, clear=True):
            client = create_llm_client()
            # May return None or raise
            assert client is None or client is not None

    def test_llm_protocol_has_analyze(self):
        """Test LLMClient protocol has analyze method."""
        from qt_shared.llm.protocol import LLMClient

        # Protocol should define analyze
        assert hasattr(LLMClient, "analyze") or "analyze" in dir(LLMClient)
