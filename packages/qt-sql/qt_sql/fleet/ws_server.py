"""WebSocket server for Fleet C2 live dashboard."""

import asyncio
import json
import logging
import os
import re
import threading
import time
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
        run_context: Optional[Dict[str, Any]] = None,
    ) -> None:
        self.event_bus = event_bus
        self.triage_gate = triage_gate
        self.pause_event = pause_event
        self.html_path = html_path
        self.initial_data = initial_data
        self.benchmark_dir = benchmark_dir
        self.run_context: Dict[str, Any] = dict(run_context or {})
        self.runtime_config: Dict[str, Any] = {}
        self._clients: List[Any] = []  # WebSocket connections

    @staticmethod
    def _cfg_flag_enabled(value: Any, default: bool = False) -> bool:
        if value is None:
            return default
        if isinstance(value, bool):
            return value
        return str(value).strip().lower() in {"1", "true", "yes", "on"}

    def _persist_runtime_config(self) -> None:
        """Persist UI runtime config for orchestrator consumption."""
        if not self.benchmark_dir:
            return
        try:
            path = self.benchmark_dir / ".fleet_runtime_config.json"
            path.write_text(json.dumps(self.runtime_config, indent=2))
        except Exception as exc:
            logger.warning("Fleet C2: failed to persist runtime config: %s", exc)

    @staticmethod
    def _mask_dsn(dsn: str) -> str:
        if not dsn:
            return ""
        masked = str(dsn)
        # Common key=value form (password=secret)
        masked = re.sub(r"(password=)([^\s;]+)", r"\1***", masked, flags=re.IGNORECASE)
        # URI user:pass@host form
        masked = re.sub(r"://([^:/\s]+):([^@/\s]+)@", r"://\1:***@", masked)
        return masked

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
            return HTMLResponse(
                content=served_html,
                headers={
                    "Cache-Control": "no-store, no-cache, must-revalidate, max-age=0",
                    "Pragma": "no-cache",
                    "Expires": "0",
                },
            )

        @app.get("/help", response_class=HTMLResponse)
        async def help_page():
            return help_html

        @app.websocket("/ws")
        async def websocket_endpoint(websocket: WebSocket):
            await websocket.accept()
            self._clients.append(websocket)
            logger.info("Fleet C2: WebSocket client connected")
            self.event_bus.emit(
                EventType.EVENT_LOG,
                scope="fleet",
                target="Fleet",
                msg="Dashboard websocket client connected.",
                level="system",
            )

            # Send initial triage data
            await websocket.send_json({
                "type": "triage_data",
                "data": {"queries": self.initial_data},
            })
            if self.run_context:
                await websocket.send_json({
                    "type": "run_context",
                    "data": dict(self.run_context),
                })
            self.event_bus.emit(
                EventType.EVENT_LOG,
                scope="fleet",
                target="Fleet",
                msg=(
                    "Sent triage_data payload "
                    f"({len(self.initial_data)} queries)"
                    + (
                        f" for {self.run_context.get('benchmark_name', '')}"
                        if self.run_context
                        else ""
                    )
                    + "."
                ),
                level="system",
            )

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
            plan = msg.get("plan", {}) or {}
            excluded = plan.get("excluded", []) if isinstance(plan, dict) else []
            overrides = plan.get("overrides", []) if isinstance(plan, dict) else []
            if isinstance(plan, dict) and isinstance(plan.get("config"), dict):
                self.runtime_config = dict(plan["config"])
                self._persist_runtime_config()
            use_history = self._cfg_flag_enabled(
                self.runtime_config.get("use_blackboard_history"),
                default=True,
            )
            logger.info("Fleet C2: Triage approved by browser")
            self.triage_gate.set()
            self.event_bus.emit(
                EventType.EVENT_LOG,
                scope="fleet",
                target="Fleet",
                msg=(
                    "Triage approved; execution dispatcher released "
                    f"(excluded={len(excluded)}, overrides={len(overrides)}, "
                    f"config={'yes' if bool(self.runtime_config) else 'no'}, "
                    f"history={'on' if use_history else 'off'})."
                ),
                level="system",
            )

        elif msg_type == "config_update":
            cfg = msg.get("config", {})
            if isinstance(cfg, dict):
                self.runtime_config = dict(cfg)
                self._persist_runtime_config()
            source_mode = self.runtime_config.get("source_mode", "local")
            bench_dir = self.runtime_config.get("benchmark_dir") or ""
            db_dsn = self.runtime_config.get("db_dsn") or ""
            use_history = self._cfg_flag_enabled(
                self.runtime_config.get("use_blackboard_history"),
                default=True,
            )
            logger.info(
                "Fleet C2: config_update received (mode=%s, benchmark_dir=%s, history=%s)",
                source_mode,
                bench_dir,
                "on" if use_history else "off",
            )
            self.event_bus.emit(
                EventType.EVENT_LOG,
                scope="fleet",
                target="Config",
                msg=(
                    "Config updated: "
                    f"mode={source_mode}, benchmark_dir={bench_dir or 'n/a'}, "
                    f"db={'set' if bool(db_dsn) else 'unset'}, "
                    f"history={'on' if use_history else 'off'}"
                ),
                level="system",
            )

        elif msg_type == "test_connection":
            target = str(msg.get("target", "") or "").strip().lower()
            cfg = msg.get("config", {})
            effective_cfg = dict(self.runtime_config)
            if isinstance(cfg, dict):
                effective_cfg.update(cfg)
            if target not in {"db", "llm"}:
                self.event_bus.emit(
                    EventType.EVENT_LOG,
                    scope="fleet",
                    target="Config",
                    msg=f"Ignoring unknown connection test target: {target or 'n/a'}",
                    level="warn",
                )
                return
            self.event_bus.emit(
                EventType.EVENT_LOG,
                scope="fleet",
                target="Config",
                msg=f"Connection test requested: {target.upper()}",
                level="system",
            )
            self._run_connection_test(target=target, config=effective_cfg)

        elif msg_type == "pause":
            if self.pause_event:
                self.pause_event.clear()
                logger.info("Fleet C2: Paused by browser")
                self.event_bus.emit(
                    EventType.EVENT_LOG,
                    scope="fleet",
                    target="Fleet",
                    msg="Paused by user.",
                    level="system",
                )

        elif msg_type == "resume":
            if self.pause_event:
                self.pause_event.set()
                logger.info("Fleet C2: Resumed by browser")
                self.event_bus.emit(
                    EventType.EVENT_LOG,
                    scope="fleet",
                    target="Fleet",
                    msg="Resumed by user.",
                    level="system",
                )

        elif msg_type in ("editor_beam", "editor_strike"):
            query_id = msg.get("query_id", "")
            sql = msg.get("sql", "")
            strategy = msg.get("strategy", "")
            run_type = "strike" if msg_type == "editor_strike" else "beam"
            max_iters = msg.get("max_iterations", 3)
            if run_type == "strike":
                max_iters = 1
            logger.info("Fleet C2: %s on %s (%d iters, strategy=%s)", msg_type, query_id, max_iters, strategy)
            self.event_bus.emit(
                EventType.EVENT_LOG,
                scope="editor",
                target=query_id or "Editor",
                msg=(
                    f"Request accepted: {run_type.upper()} "
                    f"(max_iterations={max_iters}{', strategy=' + strategy if strategy else ''}, "
                    f"sql_chars={len(sql)})"
                ),
                level="system",
            )
            self._run_editor_session(
                query_id,
                sql,
                max_iters,
                strategy=strategy,
                run_type=run_type,
            )

    def _run_connection_test(self, target: str, config: Dict[str, Any]) -> None:
        """Run DB/LLM connectivity test in background to avoid blocking websocket loop."""

        def _worker() -> None:
            started = time.time()
            ok = False
            message = ""
            details: Dict[str, Any] = {}
            try:
                if target == "db":
                    ok, message, details = self._test_db_connection(config)
                elif target == "llm":
                    ok, message, details = self._test_llm_connection(config)
                else:
                    raise ValueError(f"Unsupported test target: {target}")
            except Exception as exc:
                ok = False
                message = str(exc)
            elapsed_ms = int((time.time() - started) * 1000)
            level = "system" if ok else "error"
            status = "passed" if ok else "failed"

            self.event_bus.emit(
                EventType.EVENT_LOG,
                scope="fleet",
                target="Config",
                msg=f"{target.upper()} connectivity test {status} ({elapsed_ms}ms): {message}",
                level=level,
            )
            self.event_bus.emit(
                EventType.CONNECTION_TEST_RESULT,
                target=target,
                ok=ok,
                message=message,
                elapsed_ms=elapsed_ms,
                details=details,
            )

        t = threading.Thread(target=_worker, daemon=True, name=f"conn-test-{target}")
        t.start()

    def _test_db_connection(self, config: Dict[str, Any]) -> tuple[bool, str, Dict[str, Any]]:
        """Attempt a real DB connection and run SELECT 1."""
        from ..execution.factory import create_executor_from_dsn

        dsn = str(config.get("db_dsn", "") or "").strip()
        if not dsn:
            return False, "Database DSN is empty. Configure DB DSN/Path first.", {"dsn": ""}

        masked_dsn = self._mask_dsn(dsn)
        executor = None
        try:
            executor = create_executor_from_dsn(dsn)
            executor.connect()
            rows = executor.execute("SELECT 1 AS qt_ok")
            probe_value = None
            if rows and isinstance(rows[0], dict):
                probe_value = rows[0].get("qt_ok")
                if probe_value is None:
                    # Some engines uppercase keys (e.g., Snowflake)
                    probe_value = rows[0].get("QT_OK")
                    if probe_value is None and rows[0]:
                        probe_value = next(iter(rows[0].values()))
            details = {
                "dsn": masked_dsn,
                "rows": len(rows) if isinstance(rows, list) else 0,
                "probe_value": probe_value,
            }
            return True, "Connected and executed SELECT 1 successfully.", details
        except Exception as exc:
            return False, str(exc), {"dsn": masked_dsn}
        finally:
            if executor is not None:
                try:
                    executor.close()
                except Exception:
                    pass

    def _test_llm_connection(self, config: Dict[str, Any]) -> tuple[bool, str, Dict[str, Any]]:
        """Attempt a real LLM probe call using configured provider/model."""
        from qt_shared.config import get_settings
        from qt_shared.llm import create_llm_client

        settings = get_settings()
        provider = str(config.get("llm_provider", "") or settings.llm_provider or "").strip()
        model = str(config.get("llm_model", "") or settings.llm_model or "").strip()
        api_key = str(config.get("llm_api_key", "") or "").strip() or None

        if not provider:
            return (
                False,
                "LLM provider is not configured. Set QT_LLM_PROVIDER or provide llm_provider.",
                {"provider": "", "model": model},
            )
        client = create_llm_client(
            provider=provider,
            model=model or None,
            api_key=api_key,
        )
        if client is None:
            return False, "Failed to initialize LLM client.", {"provider": provider, "model": model}

        # Small probe to validate credentials + network path with minimal token usage.
        probe_prompt = "Reply with exactly: OK"
        response = (client.analyze(probe_prompt) or "").strip()
        if not response:
            return False, "LLM responded with empty output.", {"provider": provider, "model": model}

        snippet = response.replace("\n", " ").strip()
        if len(snippet) > 80:
            snippet = snippet[:80] + "..."
        return True, "LLM probe request succeeded.", {
            "provider": provider,
            "model": model or getattr(client, "model", ""),
            "response_preview": snippet,
            "manual_mode": bool(getattr(settings, "manual_mode", False)),
            "env_has_key": any(
                bool(os.getenv(k))
                for k in (
                    "QT_DEEPSEEK_API_KEY",
                    "QT_OPENAI_API_KEY",
                    "QT_GROQ_API_KEY",
                    "QT_GEMINI_API_KEY",
                    "QT_OPENROUTER_API_KEY",
                )
            ),
        }

    def _run_editor_session(
        self,
        query_id: str,
        sql: str,
        max_iterations: int,
        strategy: str = "",
        run_type: str = "beam",
    ) -> None:
        """Spawn a daemon thread to run single-query optimization for the editor."""
        def _worker():
            try:
                from ..pipeline import Pipeline
                from ..sessions.beam_session import BeamSession

                self.event_bus.emit(
                    EventType.EVENT_LOG,
                    scope="editor",
                    target=query_id or "Editor",
                    msg=(
                        "Initializing optimization session "
                        f"(mode={run_type}, strategy={strategy or 'auto'})..."
                    ),
                    level="system",
                )

                effective_benchmark_dir = self.benchmark_dir
                cfg_bench = str(self.runtime_config.get("benchmark_dir", "") or "").strip()
                if cfg_bench:
                    candidate = Path(cfg_bench).expanduser()
                    if candidate.exists():
                        effective_benchmark_dir = candidate
                    else:
                        self.event_bus.emit(
                            EventType.EVENT_LOG,
                            scope="editor",
                            target=query_id or "Editor",
                            msg=f"Configured benchmark_dir not found: {candidate}",
                            level="warn",
                        )

                if not effective_benchmark_dir:
                    raise RuntimeError("No benchmark_dir configured for editor sessions")
                pipeline = Pipeline(effective_benchmark_dir)

                cfg_db = str(self.runtime_config.get("db_dsn", "") or "").strip()
                if cfg_db:
                    pipeline.config.db_path_or_dsn = cfg_db
                    pipeline.config.benchmark_dsn = cfg_db
                else:
                    pipeline.config.benchmark_dsn = pipeline.config.db_path_or_dsn

                cfg_policy = str(self.runtime_config.get("explain_policy", "") or "").strip()
                if cfg_policy:
                    setattr(pipeline.config, "explain_policy", cfg_policy)

                self.event_bus.emit(
                    EventType.EVENT_LOG,
                    scope="editor",
                    target=query_id or "Editor",
                    msg=(
                        "Runtime config applied: "
                        f"benchmark_dir={effective_benchmark_dir}, "
                        f"db={'set' if bool(cfg_db) else 'default'}, "
                        f"policy={cfg_policy or 'default'}"
                    ),
                    level="system",
                )

                session = BeamSession(
                    pipeline=pipeline,
                    query_id=query_id,
                    original_sql=sql,
                    target_speedup=2.0,
                    max_iterations=max_iterations,
                    patch=True,
                )

                def _on_phase(phase: str, iteration: int) -> None:
                    phase_label = {
                        "analyst": "Analyst dispatcher",
                        "workers": "Worker fan-out",
                        "strike_prepare": "Strike prompt assembly",
                        "strike_worker": "Strike worker single-call",
                        "benchmark": "Semantic + benchmark validation",
                        "snipe": "Sniper refinement",
                    }.get(phase, phase)
                    self.event_bus.emit(
                        EventType.EVENT_LOG,
                        scope="editor",
                        target=query_id or "Editor",
                        msg=f"Phase: {phase_label} (iteration {iteration + 1})",
                        level="system",
                    )

                session.on_phase_change = _on_phase
                if run_type == "strike":
                    self.event_bus.emit(
                        EventType.EVENT_LOG,
                        scope="editor",
                        target=query_id or "Editor",
                        msg=(
                            "Strike execution mode: single transform, single worker call "
                            "(dispatcher/sniper bypassed)."
                        ),
                        level="system",
                    )
                    result = session.run_editor_strike(transform_id=strategy)
                else:
                    result = session.run()

                iterations = getattr(result, "iterations", []) or []
                for it in iterations:
                    patches_data = []
                    for p in it.get("patches", []):
                        patches_data.append({
                            "worker_id": p.get("patch_id", ""),
                            "speedup": p.get("speedup", None),
                            "status": p.get("status", "PENDING"),
                            "transform": p.get("transform", ""),
                            "sql": p.get("output_sql", ""),
                        })
                    if patches_data:
                        self.event_bus.emit(
                            EventType.EDITOR_ITERATION,
                            query_id=query_id,
                            iteration=int(it.get("iteration", 0)) + 1,
                            patches=patches_data,
                        )

                self.event_bus.emit(
                    EventType.EVENT_LOG,
                    scope="editor",
                    target=query_id or "Editor",
                    msg=f"Complete: {result.status} ({result.best_speedup:.2f}x best)",
                    level="system",
                )
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
                    EventType.EVENT_LOG,
                    scope="editor",
                    target=query_id or "Editor",
                    msg=f"Error: {exc}",
                    level="error",
                )
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
                try:
                    payload = event.to_json()
                except Exception:
                    logger.exception("Fleet C2: failed to serialize event: %r", event)
                    continue
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
    run_context: Optional[Dict[str, Any]] = None,
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
        run_context=run_context,
    )
    app = server.get_app()

    def _run():
        uvicorn.run(app, host="127.0.0.1", port=port, log_level="warning")

    thread = threading.Thread(target=_run, daemon=True, name="fleet-c2-ws")
    thread.start()
    return thread
