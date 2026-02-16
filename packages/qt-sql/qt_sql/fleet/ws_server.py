"""WebSocket server for Fleet C2 live dashboard."""

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
        benchmark_dir: Optional[Path] = None,
    ) -> None:
        self.event_bus = event_bus
        self.triage_gate = triage_gate
        self.pause_event = pause_event
        self.html_path = html_path
        self.initial_data = initial_data
        self.benchmark_dir = benchmark_dir
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

        help_path = self.html_path.parent / "fleet_c2_help.html"
        help_html = help_path.read_text() if help_path.exists() else "<h1>Help not found</h1>"

        @app.get("/", response_class=HTMLResponse)
        async def index():
            return served_html

        @app.get("/help", response_class=HTMLResponse)
        async def help_page():
            return help_html

        @app.websocket("/ws")
        async def websocket_endpoint(websocket: WebSocket):
            await websocket.accept()
            self._clients.append(websocket)
            logger.info("Fleet C2: WebSocket client connected")

            # Send initial triage data
            await websocket.send_json({
                "type": "triage_data",
                "data": {"queries": self.initial_data},
            })

            try:
                while True:
                    msg = await websocket.receive_json()
                    self._handle_client_message(msg)
            except WebSocketDisconnect:
                self._clients.remove(websocket)
                logger.info("Fleet C2: WebSocket client disconnected")
            except Exception:
                if websocket in self._clients:
                    self._clients.remove(websocket)

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

        elif msg_type in ("editor_beam", "editor_strike"):
            query_id = msg.get("query_id", "")
            sql = msg.get("sql", "")
            strategy = msg.get("strategy", "")
            max_iters = msg.get("max_iterations", 1 if msg_type == "editor_strike" else 3)
            logger.info("Fleet C2: %s on %s (%d iters, strategy=%s)", msg_type, query_id, max_iters, strategy)
            self._run_editor_session(query_id, sql, max_iters, strategy=strategy)

    def _run_editor_session(
        self, query_id: str, sql: str, max_iterations: int, strategy: str = ""
    ) -> None:
        """Spawn a daemon thread to run single-query optimization for the editor."""
        def _worker():
            try:
                from ..pipeline import Pipeline
                from ..sessions.beam_session import BeamSession

                if not self.benchmark_dir:
                    raise RuntimeError("No benchmark_dir configured for editor sessions")
                pipeline = Pipeline(self.benchmark_dir)
                pipeline.config.tiered_patch_enabled = True
                pipeline.config.benchmark_dsn = pipeline.config.db_path_or_dsn

                session = BeamSession(
                    pipeline=pipeline,
                    query_id=query_id,
                    original_sql=sql,
                    target_speedup=2.0,
                    max_iterations=max_iterations,
                    patch=True,
                )

                def _on_phase(phase: str, iteration: int) -> None:
                    if phase != "explain":
                        return
                    # "explain" fires after benchmark completes;
                    # session._current_patches has speedup/status populated
                    patches_data = []
                    for p in getattr(session, '_current_patches', []):
                        patches_data.append({
                            "worker_id": getattr(p, 'patch_id', ''),
                            "speedup": getattr(p, 'speedup', None),
                            "status": getattr(p, 'status', 'PENDING'),
                            "transform": getattr(p, 'transform', ''),
                            "sql": getattr(p, 'output_sql', ''),
                        })
                    if patches_data:
                        self.event_bus.emit(
                            EventType.EDITOR_ITERATION,
                            query_id=query_id,
                            iteration=iteration + 1,
                            patches=patches_data,
                        )

                session.on_phase_change = _on_phase
                result = session.run()

                self.event_bus.emit(
                    EventType.EDITOR_COMPLETE,
                    query_id=query_id,
                    status=result.status,
                    best_sql=result.best_sql,
                    best_speedup=result.best_speedup,
                )
            except Exception as exc:
                logger.error("Fleet C2: editor session failed: %s", exc)
                self.event_bus.emit(
                    EventType.EDITOR_COMPLETE,
                    query_id=query_id,
                    status="ERROR",
                    best_sql="",
                    best_speedup=0.0,
                    error=str(exc),
                )

        t = threading.Thread(target=_worker, daemon=True, name=f"editor-{query_id}")
        t.start()

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
    benchmark_dir: Optional[Path] = None,
) -> threading.Thread:
    """Launch the WebSocket server as a daemon thread. Returns the thread."""
    import uvicorn

    server = FleetWSServer(
        event_bus=event_bus,
        triage_gate=triage_gate,
        html_path=html_path,
        initial_data=initial_data,
        pause_event=pause_event,
        benchmark_dir=benchmark_dir,
    )
    app = server.get_app()

    def _run():
        uvicorn.run(app, host="127.0.0.1", port=port, log_level="warning")

    thread = threading.Thread(target=_run, daemon=True, name="fleet-c2-ws")
    thread.start()
    return thread
