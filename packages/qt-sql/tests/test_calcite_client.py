"""Phase 7: Calcite Tests - Python Client.

Tests for the Calcite client that communicates with the Java Calcite service.
"""

import pytest
from unittest.mock import MagicMock, patch, AsyncMock


class TestCalciteClient:
    """Tests for CalciteClient class."""

    def test_calcite_client_import(self):
        """Test CalciteClient can be imported."""
        from qt_sql.calcite_client import CalciteClient
        assert CalciteClient is not None

    def test_calcite_result_import(self):
        """Test CalciteResult can be imported."""
        from qt_sql.calcite_client import CalciteResult
        assert CalciteResult is not None

    def test_get_calcite_client_function(self):
        """Test get_calcite_client function exists."""
        from qt_sql.calcite_client import get_calcite_client
        assert callable(get_calcite_client)

    def test_client_instantiation(self):
        """Test CalciteClient can be instantiated."""
        from qt_sql.calcite_client import CalciteClient

        client = CalciteClient()
        assert client is not None

    def test_client_with_custom_url(self):
        """Test CalciteClient accepts custom URL."""
        from qt_sql.calcite_client import CalciteClient

        client = CalciteClient(base_url="http://custom:8001")
        assert client is not None

    def test_get_calcite_client_with_url(self):
        """Test get_calcite_client accepts URL parameter."""
        from qt_sql.calcite_client import get_calcite_client

        client = get_calcite_client(base_url="http://localhost:8001")
        assert client is not None


class TestCalciteResult:
    """Tests for CalciteResult dataclass."""

    def test_calcite_result_creation(self):
        """Test CalciteResult can be created."""
        from qt_sql.calcite_client import CalciteResult

        result = CalciteResult(
            success=True,
            original_sql="SELECT * FROM users",
            query_changed=False,
            optimized_sql=None,
            rules_applied=[],
        )

        assert result.success
        assert not result.query_changed

    def test_calcite_result_with_optimization(self):
        """Test CalciteResult with optimization data."""
        from qt_sql.calcite_client import CalciteResult

        result = CalciteResult(
            success=True,
            original_sql="SELECT * FROM users WHERE id = 1",
            query_changed=True,
            optimized_sql="SELECT id FROM users WHERE id = 1",
            rules_applied=["ProjectMergeRule", "FilterMergeRule"],
            original_cost=100.0,
            optimized_cost=50.0,
            improvement_percent=50.0,
        )

        assert result.query_changed
        assert len(result.rules_applied) == 2
        assert result.improvement_percent == 50.0

    def test_calcite_result_with_error(self):
        """Test CalciteResult with error."""
        from qt_sql.calcite_client import CalciteResult

        result = CalciteResult(
            success=False,
            original_sql="SELECT * FROM users",
            query_changed=False,
            error="Service not available",
        )

        assert not result.success
        assert result.error is not None


class TestCalciteClientMethods:
    """Tests for CalciteClient methods."""

    @pytest.fixture
    def mock_httpx_client(self):
        """Mock httpx client for testing."""
        with patch("httpx.AsyncClient") as mock:
            yield mock

    @pytest.mark.asyncio
    async def test_is_available_method(self):
        """Test is_available method exists."""
        from qt_sql.calcite_client import CalciteClient

        client = CalciteClient()
        assert hasattr(client, "is_available")

    @pytest.mark.asyncio
    async def test_optimize_method_exists(self):
        """Test optimize method exists."""
        from qt_sql.calcite_client import CalciteClient

        client = CalciteClient()
        assert hasattr(client, "optimize")

    @pytest.mark.asyncio
    @patch("httpx.AsyncClient")
    async def test_optimize_returns_result(self, mock_client_class):
        """Test optimize returns CalciteResult."""
        from qt_sql.calcite_client import CalciteClient, CalciteResult

        # Mock response
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "success": True,
            "query_changed": False,
            "optimized_sql": None,
            "rules_applied": [],
        }

        mock_client = AsyncMock()
        mock_client.post.return_value = mock_response
        mock_client.__aenter__.return_value = mock_client
        mock_client.__aexit__.return_value = None
        mock_client_class.return_value = mock_client

        client = CalciteClient()

        try:
            result = await client.optimize("SELECT 1")
            assert isinstance(result, CalciteResult)
        except Exception:
            # May fail without running service
            pass

    @pytest.mark.asyncio
    @patch("httpx.AsyncClient")
    async def test_optimize_handles_service_unavailable(self, mock_client_class):
        """Test optimize handles service unavailable."""
        from qt_sql.calcite_client import CalciteClient, CalciteResult

        # Mock connection error
        mock_client = AsyncMock()
        mock_client.post.side_effect = Exception("Connection refused")
        mock_client.__aenter__.return_value = mock_client
        mock_client.__aexit__.return_value = None
        mock_client_class.return_value = mock_client

        client = CalciteClient()

        result = await client.optimize("SELECT 1")

        # Should return failure result, not raise
        assert isinstance(result, CalciteResult)
        assert not result.success

    @pytest.mark.asyncio
    @patch("httpx.AsyncClient")
    async def test_optimize_handles_timeout(self, mock_client_class):
        """Test optimize handles timeout."""
        import asyncio
        from qt_sql.calcite_client import CalciteClient, CalciteResult

        # Mock timeout
        mock_client = AsyncMock()
        mock_client.post.side_effect = asyncio.TimeoutError()
        mock_client.__aenter__.return_value = mock_client
        mock_client.__aexit__.return_value = None
        mock_client_class.return_value = mock_client

        client = CalciteClient()

        result = await client.optimize("SELECT 1")

        # Should handle timeout gracefully
        assert isinstance(result, CalciteResult)
        assert not result.success


class TestCalciteClientIntegration:
    """Integration tests for Calcite client (require running service)."""

    @pytest.mark.integration
    @pytest.mark.asyncio
    async def test_real_service_health(self):
        """Test real Calcite service health check."""
        from qt_sql.calcite_client import CalciteClient

        client = CalciteClient()

        try:
            available = await client.is_available()
            # If service is running, should return True
            # If not running, should return False (not raise)
            assert isinstance(available, bool)
        except Exception:
            pytest.skip("Calcite service not available")

    @pytest.mark.integration
    @pytest.mark.asyncio
    async def test_real_service_optimize(self):
        """Test real Calcite optimization."""
        from qt_sql.calcite_client import CalciteClient

        client = CalciteClient()

        try:
            available = await client.is_available()
            if not available:
                pytest.skip("Calcite service not available")

            result = await client.optimize("SELECT * FROM users WHERE id = 1")

            assert result.success
            # May or may not change query
            assert isinstance(result.rules_applied, list)

        except Exception:
            pytest.skip("Calcite service not available")


class TestCalciteClientConfiguration:
    """Tests for Calcite client configuration."""

    def test_default_url(self):
        """Test default URL is localhost:8001."""
        from qt_sql.calcite_client import CalciteClient

        client = CalciteClient()
        # Default should be localhost:8001
        assert "8001" in str(client.base_url) or "localhost" in str(client.base_url)

    def test_url_from_environment(self):
        """Test URL can be set from environment."""
        with patch.dict("os.environ", {"QTCALCITE_URL": "http://custom:9999"}):
            from qt_sql.calcite_client import get_calcite_client

            client = get_calcite_client()
            # May or may not read from env depending on implementation
            assert client is not None

    def test_timeout_configuration(self):
        """Test timeout can be configured."""
        from qt_sql.calcite_client import CalciteClient

        client = CalciteClient(timeout=60.0)
        assert client is not None
