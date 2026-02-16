"""Minimal test server for e2e Playwright tests.

Starts FleetWSServer on port 8766 with fixture data.
Exits cleanly on SIGTERM or after 120s timeout.
"""

import json
import signal
import sys
import threading
from pathlib import Path

# Add project packages to path
root = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(root / "packages" / "qt-shared"))
sys.path.insert(0, str(root / "packages" / "qt-sql"))

from qt_sql.fleet.event_bus import EventBus
from qt_sql.fleet.ws_server import FleetWSServer

PORT = 8766

FIXTURE_DATA = [
    {
        "query_id": "query_1",
        "runtime_ms": 12400,
        "bucket": "HIGH",
        "rounds": 5,
        "overlap": 0.72,
        "q_error": 3.4,
        "q_error_severity": "S2",
        "est_annual_cost": 4520.0,
        "annualised_savings": 0,
        "confidence": 2,
        "transforms": ["decorrelate", "date_cte_isolate"],
        "family": "B",
        "status": "PENDING",
        "speedup": None,
        "sql": "SELECT * FROM store_sales WHERE ss_sold_date_sk IN (SELECT d_date_sk FROM date_dim WHERE d_year = 2000)",
    },
    {
        "query_id": "query_42",
        "runtime_ms": 850,
        "bucket": "LOW",
        "rounds": 1,
        "overlap": 0.35,
        "q_error": 1.2,
        "q_error_severity": "S4",
        "est_annual_cost": 310.0,
        "annualised_savings": 0,
        "confidence": 1,
        "transforms": ["early_filter"],
        "family": "A",
        "status": "PENDING",
        "speedup": None,
        "sql": "SELECT i_item_id, SUM(ss_ext_sales_price) FROM store_sales JOIN item ON ss_item_sk = i_item_sk GROUP BY i_item_id",
    },
]


def main():
    bus = EventBus()
    gate = threading.Event()
    pause = threading.Event()
    pause.set()

    html_path = root / "packages" / "qt-sql" / "qt_sql" / "dashboard" / "fleet_c2.html"

    server = FleetWSServer(
        event_bus=bus,
        triage_gate=gate,
        html_path=html_path,
        initial_data=FIXTURE_DATA,
        pause_event=pause,
    )
    app = server.get_app()

    import uvicorn

    # Graceful shutdown
    def _shutdown(sig, frame):
        sys.exit(0)

    signal.signal(signal.SIGTERM, _shutdown)
    signal.signal(signal.SIGINT, _shutdown)

    print(f"E2E test server on http://127.0.0.1:{PORT}", flush=True)
    uvicorn.run(app, host="127.0.0.1", port=PORT, log_level="warning")


if __name__ == "__main__":
    main()
