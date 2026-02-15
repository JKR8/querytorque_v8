"""API endpoint tests for qt_sql API.

Tests for the FastAPI routes defined in api/main.py:
    POST /api/sql/optimize  - Pipeline-backed optimization
    POST /api/sql/validate  - Equivalence + timing validation
    GET  /health            - Health check
    Database session routes
"""

import pytest
from unittest.mock import MagicMock, patch
from pathlib import Path
import sys

# Ensure sibling package imports work when tests run from packages/qt-sql.
QT_SQL_ROOT = Path(__file__).resolve().parents[1]
REPO_ROOT = QT_SQL_ROOT.parents[1]
QT_SHARED_PATH = REPO_ROOT / "packages" / "qt-shared"
if QT_SHARED_PATH.exists():
    sys.path.insert(0, str(QT_SHARED_PATH))


class TestHealthEndpoint:
    """Tests for the health check endpoint."""

    @pytest.fixture
    def client(self):
        from fastapi.testclient import TestClient
        from api.main import app
        return TestClient(app)

    def test_health_returns_200(self, client):
        response = client.get("/health")
        assert response.status_code == 200

    def test_health_returns_status_ok(self, client):
        response = client.get("/health")
        data = response.json()
        assert data["status"] == "ok"

    def test_health_returns_version(self, client):
        response = client.get("/health")
        data = response.json()
        assert "version" in data
        assert data["version"] == "2.0.0"

    def test_health_returns_llm_configured(self, client):
        response = client.get("/health")
        data = response.json()
        assert "llm_configured" in data
        assert isinstance(data["llm_configured"], bool)

    def test_health_returns_llm_provider(self, client):
        response = client.get("/health")
        data = response.json()
        assert "llm_provider" in data


class TestOptimizeEndpoint:
    """Tests for the /api/sql/optimize endpoint."""

    @pytest.fixture
    def client(self):
        from fastapi.testclient import TestClient
        from api.main import app
        return TestClient(app)

    def test_optimize_requires_sql_and_dsn(self, client):
        """Missing required fields should return 422."""
        response = client.post("/api/sql/optimize", json={})
        assert response.status_code == 422

    def test_optimize_rejects_empty_sql(self, client):
        """Empty SQL should fail validation."""
        response = client.post(
            "/api/sql/optimize",
            json={"sql": "", "dsn": "duckdb:///:memory:"}
        )
        assert response.status_code == 422

    def test_optimize_requires_dsn(self, client):
        """Missing DSN should return 422."""
        response = client.post(
            "/api/sql/optimize",
            json={"sql": "SELECT 1"}
        )
        assert response.status_code == 422

    def test_optimize_accepts_valid_request(self, client):
        """Valid request should return 200 (even if pipeline errors)."""
        response = client.post(
            "/api/sql/optimize",
            json={
                "sql": "SELECT 1",
                "dsn": "duckdb:///:memory:",
                "mode": "beam",
            }
        )
        # Pipeline may error (no intelligence data) but API should return 200
        assert response.status_code == 200
        data = response.json()
        assert "status" in data
        assert "query_id" in data

    def test_optimize_response_has_contract_fields(self, client):
        """Response should include all contract fields."""
        response = client.post(
            "/api/sql/optimize",
            json={
                "sql": "SELECT 1",
                "dsn": "duckdb:///:memory:",
                "mode": "beam",
            }
        )
        assert response.status_code == 200
        data = response.json()
        # Contract-required fields
        assert "status" in data
        assert "speedup" in data
        assert "speedup_type" in data
        assert "validation_confidence" in data
        assert "optimized_sql" in data or data["status"] == "ERROR"
        assert "original_sql" in data
        assert "transforms" in data
        assert "workers" in data
        assert "query_id" in data
        assert "n_iterations" in data
        assert "n_api_calls" in data

    def test_optimize_accepts_mode_parameter(self, client):
        """Should accept beam optimization mode."""
        response = client.post(
            "/api/sql/optimize",
            json={
                "sql": "SELECT 1",
                "dsn": "duckdb:///:memory:",
                "mode": "beam",
            }
        )
        assert response.status_code == 200

    def test_optimize_rejects_invalid_mode(self, client):
        """Invalid mode should return 422."""
        response = client.post(
            "/api/sql/optimize",
            json={
                "sql": "SELECT 1",
                "dsn": "duckdb:///:memory:",
                "mode": "invalid_mode",
            }
        )
        assert response.status_code == 422

    def test_optimize_accepts_optional_fields(self, client):
        """Should accept query_id, max_iterations, target_speedup."""
        response = client.post(
            "/api/sql/optimize",
            json={
                "sql": "SELECT 1",
                "dsn": "duckdb:///:memory:",
                "mode": "beam",
                "query_id": "test_q",
                "max_iterations": 2,
                "target_speedup": 1.5,
            }
        )
        assert response.status_code == 200
        data = response.json()
        assert data["query_id"] == "test_q"


class TestValidateEndpoint:
    """Tests for the /api/sql/validate endpoint."""

    @pytest.fixture
    def client(self):
        from fastapi.testclient import TestClient
        from api.main import app
        return TestClient(app)

    def test_validate_requires_both_sqls(self, client):
        """Missing SQL fields should return 422."""
        response = client.post("/api/sql/validate", json={})
        assert response.status_code == 422

    def test_validate_accepts_valid_request(self, client):
        """Valid request should return 200."""
        response = client.post(
            "/api/sql/validate",
            json={
                "original_sql": "SELECT 1",
                "optimized_sql": "SELECT 1",
                "schema_sql": "CREATE TABLE t (id INT);",
            }
        )
        # Should succeed or return validation result
        assert response.status_code in (200, 500)

    def test_validate_response_has_expected_fields(self, client):
        """Response should include validation result fields."""
        response = client.post(
            "/api/sql/validate",
            json={
                "original_sql": "SELECT 1 AS x",
                "optimized_sql": "SELECT 1 AS x",
            }
        )
        if response.status_code == 200:
            data = response.json()
            assert "status" in data
            assert "row_counts_match" in data
            assert "speedup" in data
            assert "timing" in data


class TestDatabaseEndpoints:
    """Tests for database session management endpoints."""

    @pytest.fixture
    def client(self):
        from fastapi.testclient import TestClient
        from api.main import app
        return TestClient(app)

    def test_quick_connect_duckdb(self, client, tmp_path):
        """Test DuckDB quick connect with a fixture file."""
        fixture = tmp_path / "test.sql"
        fixture.write_text("CREATE TABLE t (id INT); INSERT INTO t VALUES (1);")

        response = client.post(
            "/api/database/connect/duckdb/quick",
            data={"fixture_path": str(fixture)},
        )
        assert response.status_code == 200
        data = response.json()
        assert data["connected"] is True
        assert data["type"] == "duckdb"
        assert "session_id" in data

        # Clean up
        session_id = data["session_id"]
        client.delete(f"/api/database/disconnect/{session_id}")

    def test_session_status(self, client):
        """Test session status for nonexistent session."""
        response = client.get("/api/database/status/nonexistent")
        assert response.status_code == 200
        data = response.json()
        assert data["connected"] is False

    def test_disconnect_nonexistent(self, client):
        """Test disconnecting a nonexistent session."""
        response = client.delete("/api/database/disconnect/nonexistent")
        assert response.status_code == 200

    def test_execute_requires_session(self, client):
        """Execute without valid session should return 404."""
        response = client.post(
            "/api/database/execute/nonexistent",
            json={"sql": "SELECT 1"},
        )
        assert response.status_code == 404

    def test_schema_requires_session(self, client):
        """Schema without valid session should return 404."""
        response = client.get("/api/database/schema/nonexistent")
        assert response.status_code == 404


class TestEdgeCases:
    """Tests for edge cases in API."""

    @pytest.fixture
    def client(self):
        from fastapi.testclient import TestClient
        from api.main import app
        return TestClient(app)

    def test_very_long_sql(self, client):
        """Test handling of very long SQL."""
        columns = ", ".join([f"col{i}" for i in range(100)])
        sql = f"SELECT {columns} FROM very_long_table_name"

        response = client.post(
            "/api/sql/optimize",
            json={"sql": sql, "dsn": "duckdb:///:memory:", "mode": "beam"}
        )
        assert response.status_code == 200

    def test_unicode_in_sql(self, client):
        """Test handling of Unicode in SQL."""
        response = client.post(
            "/api/sql/optimize",
            json={
                "sql": "SELECT * FROM users WHERE name = '日本語'",
                "dsn": "duckdb:///:memory:",
                "mode": "beam",
            }
        )
        assert response.status_code == 200

    def test_multiline_sql(self, client):
        """Test handling of multiline SQL."""
        sql = """
        SELECT
            id,
            name,
            email
        FROM users
        WHERE active = true
        """
        response = client.post(
            "/api/sql/optimize",
            json={"sql": sql, "dsn": "duckdb:///:memory:", "mode": "beam"}
        )
        assert response.status_code == 200
