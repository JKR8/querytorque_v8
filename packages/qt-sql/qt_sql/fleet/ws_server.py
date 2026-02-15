"""WebSocket server for Fleet C2 live dashboard."""

from __future__ import annotations

import asyncio
import json
import logging
import threading
from pathlib import Path
from typing import Any, Dict, List, Optional

from .event_bus import EventBus, EventType

logger = logging.getLogger(__name__)


class FleetWSServer:
    """Serves fleet_c2.html with live data injection + WebSocket for real-time updates."""

    def __init__(
        self,
        event_bus: EventBus,
        triage_gate: threading.Event,
        html_path: Path,
        initial_data: List[Dict[str, Any]],
        pause_event: Optional[threading.Event] = None,
    ) -> None:
        self.event_bus = event_bus
        self.triage_gate = triage_gate
        self.pause_event = pause_event
        self.html_path = html_path
        self.initial_data = initial_data
        self._clients: List[Any] = []  # WebSocket connections

    def get_app(self):
        """Build and return a FastAPI app."""
        from fastapi import FastAPI, WebSocket, WebSocketDisconnect
        from fastapi.responses import HTMLResponse

        app = FastAPI(title="Fleet C2")

        html_template = self.html_path.read_text()
        # Inject real data into the <script type="application/json"> tag.
        # The placeholder lives inside an HTML text node (not JS), so raw JSON is safe.
        # Use replace() with count=1 to only replace the first occurrence.
        initial_json = json.dumps(self.initial_data, default=str)
        served_html = html_template.replace("__FLEET_DATA__", initial_json, 1)

        @app.get("/", response_class=HTMLResponse)
        async def index():
            return served_html

        @app.websocket("/ws")
        async def websocket_endpoint(ws: WebSocket):
            await ws.accept()
            self._clients.append(ws)
            logger.info("Fleet C2: WebSocket client connected")

            # Send initial triage data
            await ws.send_json({
                "type": "triage_data",
                "data": {"queries": self.initial_data},
            })

            try:
                while True:
                    msg = await ws.receive_json()
                    self._handle_client_message(msg)
            except WebSocketDisconnect:
                self._clients.remove(ws)
                logger.info("Fleet C2: WebSocket client disconnected")
            except Exception:
                if ws in self._clients:
                    self._clients.remove(ws)

        @app.on_event("startup")
        async def start_broadcast():
            asyncio.create_task(self._broadcast_loop())

        return app

    def _handle_client_message(self, msg: Dict[str, Any]) -> None:
        """Process browser commands."""
        msg_type = msg.get("type", "")

        if msg_type == "approve":
            logger.info("Fleet C2: Triage approved by browser")
            self.triage_gate.set()

        elif msg_type == "pause":
            if self.pause_event:
                self.pause_event.clear()
                logger.info("Fleet C2: Paused by browser")

        elif msg_type == "resume":
            if self.pause_event:
                self.pause_event.set()
                logger.info("Fleet C2: Resumed by browser")

    async def _broadcast_loop(self) -> None:
        """Async loop consuming EventBus and broadcasting to all WebSocket clients."""
        while True:
            events = self.event_bus.drain(max_events=20)
            if not events:
                await asyncio.sleep(0.2)
                continue

            for event in events:
                payload = event.to_json()
                dead: List[Any] = []
                for ws in self._clients:
                    try:
                        await ws.send_text(payload)
                    except Exception:
                        dead.append(ws)
                for ws in dead:
                    if ws in self._clients:
                        self._clients.remove(ws)


def run_server_in_thread(
    event_bus: EventBus,
    triage_gate: threading.Event,
    html_path: Path,
    initial_data: List[Dict[str, Any]],
    pause_event: Optional[threading.Event] = None,
    port: int = 8765,
) -> threading.Thread:
    """Launch the WebSocket server as a daemon thread. Returns the thread."""
    import uvicorn

    server = FleetWSServer(
        event_bus=event_bus,
        triage_gate=triage_gate,
        html_path=html_path,
        initial_data=initial_data,
        pause_event=pause_event,
    )
    app = server.get_app()

    def _run():
        uvicorn.run(app, host="127.0.0.1", port=port, log_level="warning")

    thread = threading.Thread(target=_run, daemon=True, name="fleet-c2-ws")
    thread.start()
    return thread
