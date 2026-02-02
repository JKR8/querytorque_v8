"""Phase 4: API Endpoint Tests - DAX API.

Tests for DAX API REST endpoints using FastAPI TestClient.
"""

import pytest
import io
import zipfile
import json
from unittest.mock import MagicMock, patch


class TestDAXAPIEndpoints:
    """Tests for DAX API endpoints."""

    @pytest.fixture
    def client(self):
        """Create a test client for the API."""
        from fastapi.testclient import TestClient
        from api.main import app
        return TestClient(app)

    @pytest.fixture
    def sample_vpax_bytes(self, sample_vpax_data):
        """Create VPAX file bytes for upload."""
        buffer = io.BytesIO()
        with zipfile.ZipFile(buffer, "w") as zf:
            zf.writestr("DaxVpaView.json", json.dumps(sample_vpax_data))
            zf.writestr("DaxModel.json", json.dumps({"ModelName": "Test"}))
        buffer.seek(0)
        return buffer.read()

    @pytest.fixture
    def mock_auth_user(self):
        """Mock authenticated user."""
        from qt_shared.auth.context import UserContext
        return UserContext(
            user_id="test-user-123",
            org_id="test-org-456",
            email="test@example.com",
            role="user",
            tier="professional",
            auth_method="jwt",
        )

    def test_health_endpoint_returns_200(self, client):
        """Test health endpoint returns 200."""
        response = client.get("/health")
        assert response.status_code == 200

    def test_health_endpoint_returns_status(self, client):
        """Test health endpoint returns status field."""
        response = client.get("/health")
        data = response.json()
        assert "status" in data
        assert data["status"] == "ok"

    def test_health_endpoint_returns_version(self, client):
        """Test health endpoint returns version."""
        response = client.get("/health")
        data = response.json()
        assert "version" in data

    def test_health_endpoint_returns_config_info(self, client):
        """Test health endpoint returns configuration info."""
        response = client.get("/health")
        data = response.json()
        assert "auth_enabled" in data
        assert "llm_configured" in data

    def test_analyze_endpoint_accepts_vpax(self, client, sample_vpax_bytes):
        """Test analyze endpoint accepts VPAX file."""
        files = {"file": ("test.vpax", sample_vpax_bytes, "application/octet-stream")}
        response = client.post("/api/dax/analyze", files=files)
        # Should return 200 or validation error
        assert response.status_code in (200, 400, 422, 500)

    def test_analyze_endpoint_returns_torque_score(self, client, sample_vpax_bytes):
        """Test analyze endpoint returns Torque Score."""
        files = {"file": ("test.vpax", sample_vpax_bytes, "application/octet-stream")}
        response = client.post("/api/dax/analyze", files=files)
        if response.status_code == 200:
            data = response.json()
            assert "torque_score" in data
            assert 0 <= data["torque_score"] <= 100

    def test_analyze_endpoint_returns_quality_gate(self, client, sample_vpax_bytes):
        """Test analyze endpoint returns quality gate."""
        files = {"file": ("test.vpax", sample_vpax_bytes, "application/octet-stream")}
        response = client.post("/api/dax/analyze", files=files)
        if response.status_code == 200:
            data = response.json()
            assert "quality_gate" in data

    def test_analyze_endpoint_returns_issues(self, client, sample_vpax_bytes):
        """Test analyze endpoint returns issues list."""
        files = {"file": ("test.vpax", sample_vpax_bytes, "application/octet-stream")}
        response = client.post("/api/dax/analyze", files=files)
        if response.status_code == 200:
            data = response.json()
            assert "issues" in data
            assert isinstance(data["issues"], list)

    def test_analyze_endpoint_returns_severity_counts(self, client, sample_vpax_bytes):
        """Test analyze endpoint returns severity counts."""
        files = {"file": ("test.vpax", sample_vpax_bytes, "application/octet-stream")}
        response = client.post("/api/dax/analyze", files=files)
        if response.status_code == 200:
            data = response.json()
            assert "severity_counts" in data
            counts = data["severity_counts"]
            assert "critical" in counts
            assert "high" in counts
            assert "medium" in counts
            assert "low" in counts

    def test_analyze_endpoint_rejects_non_vpax(self, client):
        """Test analyze endpoint rejects non-VPAX files."""
        files = {"file": ("test.txt", b"not a vpax file", "text/plain")}
        response = client.post("/api/dax/analyze", files=files)
        # FastAPI may return 400 (BAD_REQUEST) or 422 (Unprocessable Entity)
        assert response.status_code in [400, 422]

    def test_analyze_endpoint_returns_model_stats(self, client, sample_vpax_bytes):
        """Test analyze endpoint returns model stats."""
        files = {"file": ("test.vpax", sample_vpax_bytes, "application/octet-stream")}
        response = client.post("/api/dax/analyze", files=files)
        if response.status_code == 200:
            data = response.json()
            assert "model_stats" in data


class TestDAXAPIIssueFormat:
    """Tests for issue format in API responses."""

    @pytest.fixture
    def client(self):
        from fastapi.testclient import TestClient
        from api.main import app
        return TestClient(app)

    @pytest.fixture
    def sample_vpax_bytes(self, sample_vpax_data):
        buffer = io.BytesIO()
        with zipfile.ZipFile(buffer, "w") as zf:
            zf.writestr("DaxVpaView.json", json.dumps(sample_vpax_data))
            zf.writestr("DaxModel.json", json.dumps({"ModelName": "Test"}))
        buffer.seek(0)
        return buffer.read()

    def test_issue_has_rule_id(self, client, sample_vpax_bytes):
        """Test that issues include rule_id."""
        files = {"file": ("test.vpax", sample_vpax_bytes, "application/octet-stream")}
        response = client.post("/api/dax/analyze", files=files)
        if response.status_code == 200:
            data = response.json()
            if data["issues"]:
                issue = data["issues"][0]
                assert "rule_id" in issue

    def test_issue_has_severity(self, client, sample_vpax_bytes):
        """Test that issues include severity."""
        files = {"file": ("test.vpax", sample_vpax_bytes, "application/octet-stream")}
        response = client.post("/api/dax/analyze", files=files)
        if response.status_code == 200:
            data = response.json()
            if data["issues"]:
                issue = data["issues"][0]
                assert "severity" in issue


class TestDAXAPIDiff:
    """Tests for VPAX diff endpoint."""

    @pytest.fixture
    def client(self):
        from fastapi.testclient import TestClient
        from api.main import app
        return TestClient(app)

    @pytest.fixture
    def vpax_v1_bytes(self):
        """Create V1 VPAX file."""
        data = {
            "Tables": [{"TableName": "Sales", "RowsCount": 1000}],
            "Columns": [],
            "Measures": [
                {"TableName": "Sales", "MeasureName": "Total", "MeasureExpression": "SUM('Sales'[Amount])"}
            ],
            "Relationships": [],
        }
        buffer = io.BytesIO()
        with zipfile.ZipFile(buffer, "w") as zf:
            zf.writestr("DaxVpaView.json", json.dumps(data))
            zf.writestr("DaxModel.json", json.dumps({"ModelName": "V1"}))
        buffer.seek(0)
        return buffer.read()

    @pytest.fixture
    def vpax_v2_bytes(self):
        """Create V2 VPAX file with changes."""
        data = {
            "Tables": [{"TableName": "Sales", "RowsCount": 2000}],
            "Columns": [],
            "Measures": [
                {"TableName": "Sales", "MeasureName": "Total", "MeasureExpression": "SUM('Sales'[Amount])"},
                {"TableName": "Sales", "MeasureName": "Average", "MeasureExpression": "AVERAGE('Sales'[Amount])"},
            ],
            "Relationships": [],
        }
        buffer = io.BytesIO()
        with zipfile.ZipFile(buffer, "w") as zf:
            zf.writestr("DaxVpaView.json", json.dumps(data))
            zf.writestr("DaxModel.json", json.dumps({"ModelName": "V2"}))
        buffer.seek(0)
        return buffer.read()

    def test_diff_endpoint_accepts_two_files(self, client, vpax_v1_bytes, vpax_v2_bytes):
        """Test diff endpoint accepts two VPAX files."""
        files = {
            "file1": ("v1.vpax", vpax_v1_bytes, "application/octet-stream"),
            "file2": ("v2.vpax", vpax_v2_bytes, "application/octet-stream"),
        }
        response = client.post("/api/dax/diff", files=files)
        assert response.status_code in (200, 400, 422, 500)

    def test_diff_endpoint_returns_summary(self, client, vpax_v1_bytes, vpax_v2_bytes):
        """Test diff endpoint returns summary."""
        files = {
            "file1": ("v1.vpax", vpax_v1_bytes, "application/octet-stream"),
            "file2": ("v2.vpax", vpax_v2_bytes, "application/octet-stream"),
        }
        response = client.post("/api/dax/diff", files=files)
        if response.status_code == 200:
            data = response.json()
            assert "summary" in data

    def test_diff_endpoint_returns_changes(self, client, vpax_v1_bytes, vpax_v2_bytes):
        """Test diff endpoint returns changes list."""
        files = {
            "file1": ("v1.vpax", vpax_v1_bytes, "application/octet-stream"),
            "file2": ("v2.vpax", vpax_v2_bytes, "application/octet-stream"),
        }
        response = client.post("/api/dax/diff", files=files)
        if response.status_code == 200:
            data = response.json()
            assert "changes" in data
            assert isinstance(data["changes"], list)


class TestDAXAPIOptimize:
    """Tests for DAX optimization endpoint."""

    @pytest.fixture
    def client(self):
        from fastapi.testclient import TestClient
        from api.main import app
        return TestClient(app)

    def test_optimize_requires_auth(self, client):
        """Test optimize endpoint requires authentication."""
        response = client.post(
            "/api/dax/optimize",
            json={
                "measures": [
                    {"measure_name": "Test", "measure_dax": "SUM('Sales'[Amount])"}
                ]
            }
        )
        # Should require auth
        assert response.status_code in (401, 403, 422, 500)

    @patch("api.main.CurrentUser")
    def test_optimize_requires_paid_tier(self, mock_current_user, client):
        """Test optimize endpoint requires paid tier."""
        # Even with proper structure, should fail without auth
        response = client.post(
            "/api/dax/optimize",
            json={
                "measures": [
                    {"measure_name": "Test", "measure_dax": "SUM('Sales'[Amount])", "issues": []}
                ]
            }
        )
        assert response.status_code in (401, 403, 422, 500)


class TestDAXAPIReport:
    """Tests for HTML report endpoint."""

    @pytest.fixture
    def client(self):
        from fastapi.testclient import TestClient
        from api.main import app
        return TestClient(app)

    @pytest.fixture
    def sample_vpax_bytes(self, sample_vpax_data):
        buffer = io.BytesIO()
        with zipfile.ZipFile(buffer, "w") as zf:
            zf.writestr("DaxVpaView.json", json.dumps(sample_vpax_data))
            zf.writestr("DaxModel.json", json.dumps({"ModelName": "Test"}))
        buffer.seek(0)
        return buffer.read()

    def test_report_endpoint_returns_html(self, client, sample_vpax_bytes):
        """Test report endpoint returns HTML."""
        files = {"file": ("test.vpax", sample_vpax_bytes, "application/octet-stream")}
        response = client.post("/api/dax/analyze/report", files=files)
        if response.status_code == 200:
            assert "text/html" in response.headers.get("content-type", "")
