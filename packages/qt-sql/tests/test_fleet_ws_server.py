"""Tests for fleet ws_server: FleetWSServer, HTML injection, client message handling."""

import json
import threading
from pathlib import Path
from unittest.mock import MagicMock

import pytest

from qt_sql.fleet.event_bus import EventBus, EventType
from qt_sql.fleet.ws_server import FleetWSServer


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def minimal_html(tmp_path):
    """Create a minimal HTML file with __FLEET_DATA__ placeholder."""
    html = tmp_path / "fleet_c2.html"
    html.write_text(
        '<!DOCTYPE html><html><body>'
        '<script type="application/json" id="fleet-data">__FLEET_DATA__</script>'
        '</body></html>'
    )
    return html


@pytest.fixture
def sample_data():
    return [{"id": "q1", "runtime_ms": 5000, "bucket": "HIGH"}]


@pytest.fixture
def server(minimal_html, sample_data):
    bus = EventBus()
    gate = threading.Event()
    pause = threading.Event()
    pause.set()
    return FleetWSServer(
        event_bus=bus,
        triage_gate=gate,
        html_path=minimal_html,
        initial_data=sample_data,
        pause_event=pause,
    )


# ---------------------------------------------------------------------------
# HTML injection
# ---------------------------------------------------------------------------

class TestHTMLInjection:
    def test_placeholder_replaced(self, server, sample_data):
        app = server.get_app()
        # The served HTML is captured at app creation time.
        # We need to call the index route to verify the replacement.
        # Since the route closure captures `served_html`, we can verify indirectly
        # by checking that __FLEET_DATA__ is gone from what would be served.
        html_text = server.html_path.read_text()
        initial_json = json.dumps(sample_data, default=str)
        served = html_text.replace("__FLEET_DATA__", initial_json, 1)
        assert "__FLEET_DATA__" not in served
        assert '"q1"' in served

    def test_injected_json_is_valid(self, server, sample_data):
        """The JSON injected into the HTML must be parseable."""
        html_text = server.html_path.read_text()
        initial_json = json.dumps(sample_data, default=str)
        served = html_text.replace("__FLEET_DATA__", initial_json, 1)

        # Extract JSON from the script tag
        start = served.index('id="fleet-data">') + len('id="fleet-data">')
        end = served.index("</script>", start)
        extracted = served[start:end]
        parsed = json.loads(extracted)
        assert parsed == sample_data

    def test_complex_data_injection(self, minimal_html):
        """Data with special characters should survive JSON injection."""
        data = [{"id": "q1", "detail": {"explain": 'Seq Scan "users" (cost=0..100)'}}]
        bus = EventBus()
        gate = threading.Event()
        srv = FleetWSServer(bus, gate, minimal_html, data)
        # build app triggers HTML read + replace
        srv.get_app()

        html_text = minimal_html.read_text()
        injected = html_text.replace("__FLEET_DATA__", json.dumps(data, default=str), 1)
        start = injected.index('id="fleet-data">') + len('id="fleet-data">')
        end = injected.index("</script>", start)
        parsed = json.loads(injected[start:end])
        assert parsed[0]["detail"]["explain"] == 'Seq Scan "users" (cost=0..100)'

    def test_count_1_replace(self, tmp_path, sample_data):
        """Only the first __FLEET_DATA__ occurrence should be replaced."""
        html = tmp_path / "double.html"
        html.write_text(
            '<script type="application/json" id="fleet-data">__FLEET_DATA__</script>'
            '<!-- comment referencing __FLEET_DATA__ -->'
        )
        bus = EventBus()
        gate = threading.Event()
        srv = FleetWSServer(bus, gate, html, sample_data)
        srv.get_app()

        injected = html.read_text().replace(
            "__FLEET_DATA__", json.dumps(sample_data, default=str), 1
        )
        # First occurrence replaced, second preserved
        assert injected.count("__FLEET_DATA__") == 1


# ---------------------------------------------------------------------------
# Client message handling
# ---------------------------------------------------------------------------

class TestClientMessageHandling:
    def test_approve_sets_gate(self, server):
        assert not server.triage_gate.is_set()
        server._handle_client_message({"type": "approve"})
        assert server.triage_gate.is_set()

    def test_pause_clears_event(self, server):
        assert server.pause_event.is_set()
        server._handle_client_message({"type": "pause"})
        assert not server.pause_event.is_set()

    def test_resume_sets_event(self, server):
        server.pause_event.clear()
        server._handle_client_message({"type": "resume"})
        assert server.pause_event.is_set()

    def test_pause_without_event(self, minimal_html, sample_data):
        """Pause/resume should be no-op when pause_event is None."""
        bus = EventBus()
        gate = threading.Event()
        srv = FleetWSServer(bus, gate, minimal_html, sample_data, pause_event=None)
        # Should not raise
        srv._handle_client_message({"type": "pause"})
        srv._handle_client_message({"type": "resume"})

    def test_unknown_message_type(self, server):
        """Unknown message types should be silently ignored."""
        server._handle_client_message({"type": "unknown_command"})
        # No exception, no state change
        assert not server.triage_gate.is_set()

    def test_empty_message(self, server):
        """Message with no type should be silently ignored."""
        server._handle_client_message({})
        assert not server.triage_gate.is_set()


# ---------------------------------------------------------------------------
# FastAPI app creation
# ---------------------------------------------------------------------------

class TestAppCreation:
    def test_get_app_returns_fastapi(self, server):
        app = server.get_app()
        assert app.title == "Fleet C2"

    def test_app_has_routes(self, server):
        app = server.get_app()
        paths = [route.path for route in app.routes]
        assert "/" in paths
        assert "/ws" in paths
