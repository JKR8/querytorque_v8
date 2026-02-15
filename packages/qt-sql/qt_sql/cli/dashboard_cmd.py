"""qt dashboard — serve a live HTML dashboard for fleet + swarm results."""

from __future__ import annotations

import json
import logging
import re
import webbrowser
from datetime import datetime
from http.server import HTTPServer, SimpleHTTPRequestHandler
from pathlib import Path
from typing import Any, Dict, List, Optional

import click

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Log parsing
# ---------------------------------------------------------------------------

# Patterns to extract structured events from run_*.log
_LOG_PATTERNS = [
    # PARSE: done (0.1s)
    (r"\[(\S+)\] PARSE: done \(([0-9.]+)s\)", "PARSE",
     lambda m: {"duration_s": float(m.group(2))}),

    # Data ready — 6 examples, 25 constraints, EXPLAIN=yes, GlobalKnowledge=yes (2.0s)
    (r"\[(\S+)\] Data ready .+ \(([0-9.]+)s\)", "DATA",
     lambda m: {"duration_s": float(m.group(2))}),

    # ANALYST: done (17835 chars, 5m32s)
    (r"\[(\S+)\] ANALYST: done \((\d+) chars, (\d+)m(\d+)s\)", "ANALYST",
     lambda m: {"duration_s": int(m.group(3)) * 60 + int(m.group(4)),
                "details": f"{m.group(2)} chars"}),

    # Worker strategy assignment:  W1: explicit_joins
    (r"\[(\S+)\]\s+W(\d): (\S+)", "_WORKER_STRATEGY",
     lambda m: {"worker_id": int(m.group(2)), "strategy": m.group(3)}),

    # GENERATE: W1 (explicit_joins) ready (1m30s)
    (r"\[(\S+)\] GENERATE: W(\d) \((\S+)\) ready \((\d+)m(\d+)s\)", "_GEN_WORKER",
     lambda m: {"worker_id": int(m.group(2)), "strategy": m.group(3),
                "duration_s": int(m.group(4)) * 60 + int(m.group(5))}),

    # GENERATE: all 4 workers complete (2m22s)
    (r"\[(\S+)\] GENERATE: all \d+ workers complete \((\d+)m(\d+)s\)", "GENERATE",
     lambda m: {"duration_s": int(m.group(2)) * 60 + int(m.group(3))}),

    # Race: warmup run (original)
    (r"Race: warmup run", "_RACE_WARMUP", lambda m: {}),

    # Race: original too fast — falling back to standard validation
    (r"Race: original too fast \((\d+)ms", "_RACE_FALLBACK",
     lambda m: {"baseline_ms": int(m.group(1))}),

    # Baseline (PG): 1479.9ms (1 rows)
    (r"Baseline \((\w+)\): ([0-9.]+)ms \((\d+) rows\)", "_BASELINE",
     lambda m: {"engine": m.group(1), "baseline_ms": float(m.group(2)),
                "rows": int(m.group(3))}),

    # VALIDATE: complete (48.5s)
    (r"\[(\S+)\] VALIDATE: complete \(([0-9.]+)s\)", "VALIDATE",
     lambda m: {"duration_s": float(m.group(2))}),

    # VALIDATE: WIN 1.51x (4.4s)
    (r"\[(\S+)\] VALIDATE: WIN ([0-9.]+)x \(([0-9.]+)s\)", "VALIDATE",
     lambda m: {"duration_s": float(m.group(3)),
                "details": f"WIN {m.group(2)}x"}),

    # Worker result:  W1 (explicit_joins): NEUTRAL 1.01x
    (r"\[(\S+)\]\s+W(\d) \((\S+)\)(?:\s+\[EXPLORE\])?: (\w+) ([0-9.]+)x", "_VALIDATE_RESULT",
     lambda m: {"worker_id": int(m.group(2)), "strategy": m.group(3),
                "status": m.group(4), "speedup": float(m.group(5))}),

    # EXPLAIN candidates: 4 collected (10.8s)
    (r"\[(\S+)\] EXPLAIN candidates: (\d+) collected \(([0-9.]+)s\)", "EXPLAIN",
     lambda m: {"duration_s": float(m.group(3)),
                "details": f"{m.group(2)} candidates"}),

    # FAN-OUT: complete (8m57s) | total 8m57s
    (r"\[(\S+)\] FAN-OUT: complete \((\d+)m(\d+)s\)", "_FANOUT_DONE",
     lambda m: {"duration_s": int(m.group(2)) * 60 + int(m.group(3))}),

    # RETRY WORKER: generated (3m33s)
    (r"\[(\S+)\] RETRY WORKER: generated \((\d+)m(\d+)s\)", "RETRY",
     lambda m: {"duration_s": int(m.group(2)) * 60 + int(m.group(3))}),

    # SNIPE 1: complete (3m37s) | total 12m35s
    (r"\[(\S+)\] SNIPE (\d+): complete \((\d+)m(\d+)s\) \| total (\d+)m(\d+)s", "_SNIPE_DONE",
     lambda m: {"iteration": int(m.group(2)),
                "duration_s": int(m.group(3)) * 60 + int(m.group(4)),
                "total_s": int(m.group(5)) * 60 + int(m.group(6))}),
]


def parse_run_log(log_path: Path) -> Dict[str, Any]:
    """Parse a run_*.log file into structured timeline events.

    Returns dict with:
      - events: list of {event, duration_s, details} for timeline rendering
      - gen_workers: per-worker generate timing
      - validation_mode: 'race' or 'sequential'
      - baseline_ms: original query timing
      - total_duration_s: total session duration
    """
    text = log_path.read_text(errors="replace")
    lines = text.splitlines()

    events: List[Dict[str, Any]] = []
    gen_workers: List[Dict[str, Any]] = []
    validation_mode = "sequential"
    baseline_ms: Optional[float] = None
    total_duration_s: Optional[float] = None

    for line in lines:
        for pattern, event_name, extractor in _LOG_PATTERNS:
            m = re.search(pattern, line)
            if not m:
                continue

            data = extractor(m)

            if event_name.startswith("_"):
                # Internal events — not shown in timeline but used for metadata
                if event_name == "_GEN_WORKER":
                    gen_workers.append(data)
                elif event_name == "_RACE_WARMUP":
                    validation_mode = "race"
                elif event_name == "_RACE_FALLBACK":
                    validation_mode = "sequential"
                    if data.get("baseline_ms"):
                        baseline_ms = data["baseline_ms"]
                elif event_name == "_BASELINE":
                    baseline_ms = data.get("baseline_ms")
                elif event_name == "_SNIPE_DONE":
                    total_duration_s = data.get("total_s")
                elif event_name == "_FANOUT_DONE" and total_duration_s is None:
                    total_duration_s = data.get("duration_s")
            else:
                ev = {"event": event_name, "duration_s": data.get("duration_s", 0)}
                if data.get("details"):
                    ev["details"] = data["details"]
                # Attach worker sub-bars to GENERATE event
                if event_name == "GENERATE" and gen_workers:
                    ev["workers"] = list(gen_workers)
                events.append(ev)
            break  # Only match first pattern per line

    return {
        "events": events,
        "gen_workers": gen_workers,
        "validation_mode": validation_mode,
        "baseline_ms": baseline_ms,
        "total_duration_s": total_duration_s,
    }


# ---------------------------------------------------------------------------
# Data collection
# ---------------------------------------------------------------------------

def _find_latest_log(session_dir: Path) -> Optional[Path]:
    """Find the most recent run_*.log in a session directory."""
    logs = sorted(session_dir.glob("run_*.log"))
    return logs[-1] if logs else None


def _load_json(path: Path) -> Optional[dict]:
    """Load JSON file, returning None on any error."""
    try:
        return json.loads(path.read_text())
    except (FileNotFoundError, json.JSONDecodeError, OSError):
        return None


def collect_session(session_dir: Path) -> Optional[Dict[str, Any]]:
    """Collect all data for a single swarm session.

    Works for both completed sessions (with session.json) and in-progress
    sessions that only have iteration dirs / logs so far.
    """
    session_json = _load_json(session_dir / "session.json")

    # Even without session.json, show the session if we have any data
    has_iterations = any(session_dir.glob("iteration_*"))
    has_log = any(session_dir.glob("run_*.log"))
    if not session_json and not has_iterations and not has_log:
        return None

    if session_json:
        result: Dict[str, Any] = {
            "query_id": session_json.get("query_id", session_dir.name),
            "mode": session_json.get("mode", "swarm"),
            "target_speedup": session_json.get("target_speedup"),
            "max_iterations": session_json.get("max_iterations"),
            "n_iterations": session_json.get("n_iterations", 0),
            "best_speedup": session_json.get("best_speedup", 0),
            "best_worker_id": session_json.get("best_worker_id"),
            "best_strategy": session_json.get("best_strategy", ""),
            "total_workers": session_json.get("total_workers", 0),
            "n_api_calls": session_json.get("n_api_calls", 0),
        }
    else:
        result: Dict[str, Any] = {
            "query_id": session_dir.name,
            "mode": "swarm",
            "target_speedup": None,
            "max_iterations": None,
            "n_iterations": 0,
            "best_speedup": 0,
            "best_worker_id": None,
            "best_strategy": "",
            "total_workers": 0,
            "n_api_calls": 0,
            "in_progress": True,
        }

    # Collect worker results from all iterations
    workers: List[Dict[str, Any]] = []
    for iter_dir in sorted(session_dir.glob("iteration_*")):
        for worker_dir in sorted(iter_dir.glob("worker_*")):
            wr = _load_json(worker_dir / "result.json")
            if wr:
                workers.append({
                    "worker_id": wr.get("worker_id"),
                    "strategy": wr.get("strategy", ""),
                    "speedup": wr.get("speedup", 0),
                    "status": wr.get("status", "?"),
                    "transforms": wr.get("transforms", []),
                    "examples_used": wr.get("examples_used", []),
                    "hint": wr.get("hint", ""),
                    "error_message": wr.get("error_message"),
                    "error_messages": wr.get("error_messages", []),
                    "error_category": wr.get("error_category"),
                    "optimized_sql": wr.get("optimized_sql", ""),
                    "exploratory": wr.get("exploratory", False),
                })
    result["workers"] = workers

    # For in-progress sessions, derive best_speedup from worker results
    if result.get("in_progress") and workers:
        passing = [w for w in workers if w["status"] in ("WIN", "IMPROVED", "pass")]
        if passing:
            best = max(passing, key=lambda w: w["speedup"])
            result["best_speedup"] = best["speedup"]
            result["best_worker_id"] = best["worker_id"]
            result["best_strategy"] = best["strategy"]
        result["total_workers"] = len(workers)

    # Parse timeline from latest log
    log_path = _find_latest_log(session_dir)
    if log_path:
        log_data = parse_run_log(log_path)
        result["timeline"] = log_data["events"]
        result["validation_mode"] = log_data["validation_mode"]
        result["baseline_ms"] = log_data["baseline_ms"]
        result["total_duration_s"] = log_data["total_duration_s"]
    else:
        result["timeline"] = []
        result["validation_mode"] = None
        result["baseline_ms"] = None
        result["total_duration_s"] = None

    return result


def collect_dashboard_data(benchmark_dir: Path) -> Dict[str, Any]:
    """Collect all dashboard data using the WorkloadProfile pipeline."""
    import dataclasses
    from ..dashboard.collector import collect_workload_profile

    config = _load_json(benchmark_dir / "config.json") or {}
    engine = config.get("engine", "unknown")

    # New forensic profile pipeline
    profile = collect_workload_profile(benchmark_dir, engine)

    # Legacy fleet data (still used by fleet table rendering)
    fleet_queries, fleet_runs = _collect_fleet_data(benchmark_dir, engine)

    # Collect swarm sessions (backward compat)
    sessions_dir = benchmark_dir / "swarm_sessions"
    sessions: List[Dict[str, Any]] = []
    latest_ts: Optional[str] = None

    if sessions_dir.exists():
        for sd in sorted(sessions_dir.iterdir()):
            if not sd.is_dir():
                continue
            session = collect_session(sd)
            if session:
                sessions.append(session)
                log = _find_latest_log(sd)
                if log:
                    ts = log.stem.replace("run_", "")
                    if latest_ts is None or ts > latest_ts:
                        latest_ts = ts

    # Format latest timestamp
    latest_run = ""
    if latest_ts:
        try:
            dt = datetime.strptime(latest_ts[:15], "%Y%m%d_%H%M%S")
            latest_run = dt.strftime("%Y-%m-%d %H:%M:%S")
        except ValueError:
            latest_run = latest_ts

    # Auto-detect mode: fleet if we have fleet queries, swarm otherwise
    mode = "fleet" if fleet_queries else "swarm"

    return {
        "benchmark_name": benchmark_dir.name,
        "engine": engine,
        "mode": mode,
        "profile": dataclasses.asdict(profile),
        "fleet_queries": fleet_queries,
        "fleet_runs": fleet_runs,
        "sessions": sessions,
        "latest_run": latest_run,
    }


# ---------------------------------------------------------------------------
# Fleet data collection
# ---------------------------------------------------------------------------

# Triage constants (mirror fleet/orchestrator.py)
_RUNTIME_THRESHOLDS = [(100, "SKIP"), (1_000, "LOW"), (10_000, "MEDIUM"), (float("inf"), "HIGH")]
_RUNTIME_WEIGHTS = {"SKIP": 0, "LOW": 1, "MEDIUM": 3, "HIGH": 5}
_MAX_ITERS_BASE = {"SKIP": 0, "LOW": 1, "MEDIUM": 2, "HIGH": 3}


def _bucket_runtime(runtime_ms: float) -> str:
    if runtime_ms < 0:
        return "MEDIUM"
    for threshold, bucket in _RUNTIME_THRESHOLDS:
        if runtime_ms < threshold:
            return bucket
    return "HIGH"


def _collect_fleet_data(
    benchmark_dir: Path,
    engine: str,
) -> tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    """Run live AST detection + load fleet run results."""
    queries_dir = benchmark_dir / "queries"
    if not queries_dir.exists():
        return [], []

    # Load query SQL files
    query_files = sorted(queries_dir.glob("*.sql"))
    if not query_files:
        return [], []

    # Run AST detection (instant, no LLM)
    transforms_catalog = None
    try:
        from ..detection import detect_transforms, load_transforms
        transforms_catalog = load_transforms()
    except Exception as e:
        logger.warning(f"Detection unavailable: {e}")

    engine_name = {"postgresql": "postgresql", "postgres": "postgresql"}.get(engine, engine)
    dialect = "postgres" if engine in ("postgresql", "postgres") else engine

    # Load cached classifications if available
    classifications: Dict = {}
    cls_path = benchmark_dir / "classifications.json"
    if cls_path.exists():
        try:
            classifications = json.loads(cls_path.read_text())
        except Exception:
            pass

    # Build per-query fleet data
    fleet_queries: List[Dict[str, Any]] = []

    for sql_path in query_files:
        qid = sql_path.stem
        sql = sql_path.read_text().strip()

        # EXPLAIN timing
        runtime_ms = _load_explain_timing(benchmark_dir, qid, engine)
        bucket = _bucket_runtime(runtime_ms)

        # AST detection
        ast_matches = []
        tractability = 0
        structural_bonus = 0.0
        top_transform = ""

        if transforms_catalog and sql:
            try:
                matched = detect_transforms(
                    sql, transforms_catalog,
                    engine=engine_name, dialect=dialect,
                )
                ast_matches = [
                    {"id": m.id, "overlap": round(m.overlap_ratio, 3),
                     "gap": getattr(m, "gap", "")}
                    for m in matched[:5]
                    if m.overlap_ratio >= 0.25
                ]
                tractability = sum(1 for m in matched if m.overlap_ratio >= 0.6)
                if matched:
                    structural_bonus = matched[0].overlap_ratio
                    top_transform = matched[0].id
            except Exception as e:
                logger.debug(f"[{qid}] Detection error: {e}")

        # Priority score
        weight = _RUNTIME_WEIGHTS.get(bucket, 0)
        priority_score = weight * (1.0 + tractability + structural_bonus)

        # Max iterations
        base_iters = _MAX_ITERS_BASE.get(bucket, 0)
        if bucket == "HIGH" and tractability >= 2:
            max_iters = 5
        elif bucket == "MEDIUM" and tractability >= 2:
            max_iters = 3
        else:
            max_iters = base_iters

        # Classification data
        cls_data = classifications.get(qid, {})
        llm_matches = cls_data.get("llm_matches", [])

        fleet_queries.append({
            "query_id": qid,
            "runtime_ms": runtime_ms,
            "bucket": bucket,
            "tractability": tractability,
            "structural_bonus": round(structural_bonus, 3),
            "top_transform": top_transform,
            "priority_score": round(priority_score, 1),
            "max_iterations": max_iters,
            "ast_matches": ast_matches,
            "llm_matches": llm_matches,
            "status": None,   # populated from latest run
            "speedup": None,  # populated from latest run
        })

    # Sort by priority (descending)
    fleet_queries.sort(key=lambda q: -q["priority_score"])

    # Load fleet run results
    fleet_runs = _load_fleet_runs(benchmark_dir, fleet_queries)

    return fleet_queries, fleet_runs


def _load_explain_timing(benchmark_dir: Path, query_id: str, engine: str) -> float:
    """Extract execution time from cached EXPLAIN JSON."""
    search_paths = [
        benchmark_dir / "explains" / f"{query_id}.json",
        benchmark_dir / "explains" / "sf10" / f"{query_id}.json",
        benchmark_dir / "explains" / "sf5" / f"{query_id}.json",
    ]

    for path in search_paths:
        if not path.exists():
            continue
        try:
            data = json.loads(path.read_text())

            # DuckDB format
            if "execution_time_ms" in data:
                val = data["execution_time_ms"]
                if val and val > 0:
                    return float(val)

            # PostgreSQL format
            plan_json = data.get("plan_json")
            if isinstance(plan_json, list) and plan_json:
                exec_time = plan_json[0].get("Execution Time")
                if exec_time is not None:
                    return float(exec_time)

            # DuckDB plan_json dict
            if isinstance(plan_json, dict):
                latency = plan_json.get("latency")
                if latency and latency > 0:
                    return float(latency) * 1000

            # Snowflake: compilationTime + executionTime fields
            comp_time = data.get("compilationTime")
            exec_time = data.get("executionTime")
            if exec_time is not None:
                return float(exec_time)

        except Exception:
            pass

    return -1.0


def _load_fleet_runs(
    benchmark_dir: Path,
    fleet_queries: List[Dict[str, Any]],
) -> List[Dict[str, Any]]:
    """Load results from runs/ directories and merge into fleet_queries."""
    runs_dir = benchmark_dir / "runs"
    if not runs_dir.exists():
        return []

    fleet_runs: List[Dict[str, Any]] = []
    latest_results: Dict[str, Dict] = {}  # latest result per query_id

    for run_dir in sorted(runs_dir.iterdir()):
        if not run_dir.is_dir():
            continue

        summary = _load_json(run_dir / "summary.json")
        if not summary:
            continue

        run_id = run_dir.name
        results = summary.get("results", [])

        # Parse timestamp from dir name
        timestamp = ""
        ts_match = re.search(r"(\d{8}_\d{6})", run_id)
        if ts_match:
            try:
                dt = datetime.strptime(ts_match.group(1), "%Y%m%d_%H%M%S")
                timestamp = dt.strftime("%Y-%m-%d %H:%M:%S")
            except ValueError:
                timestamp = ts_match.group(1)

        fleet_runs.append({
            "run_id": run_id,
            "timestamp": timestamp,
            "mode": summary.get("mode", "unknown"),
            "completed": summary.get("completed", 0),
            "total": summary.get("total", 0),
            "elapsed_seconds": summary.get("elapsed_seconds"),
            "results": results,
        })

        # Track latest result per query
        for r in results:
            qid = r.get("query_id", "")
            latest_results[qid] = r

    # Merge latest run results into fleet_queries
    for q in fleet_queries:
        r = latest_results.get(q["query_id"])
        if r:
            q["status"] = r.get("status")
            q["speedup"] = r.get("speedup")

    return fleet_runs


# ---------------------------------------------------------------------------
# HTTP server
# ---------------------------------------------------------------------------

class _DashboardHandler(SimpleHTTPRequestHandler):
    """Serves the dashboard HTML — re-collects data on every refresh."""

    template: str = ""
    benchmark_dir: Optional[Path] = None

    def do_GET(self):
        data = collect_dashboard_data(self.benchmark_dir)
        data_json = json.dumps(data, default=str)
        html = self.template.replace("__DASHBOARD_DATA__", data_json)

        self.send_response(200)
        self.send_header("Content-Type", "text/html; charset=utf-8")
        self.send_header("Cache-Control", "no-cache")
        self.end_headers()
        self.wfile.write(html.encode("utf-8"))

    def log_message(self, format, *args):
        pass


def serve_dashboard(benchmark_dir: Path, port: int = 8765) -> None:
    """Serve live dashboard — each browser refresh re-reads all session data."""
    template_path = Path(__file__).resolve().parent.parent / "dashboard" / "index.html"
    if not template_path.exists():
        raise click.ClickException(f"Dashboard template not found: {template_path}")

    # Quick initial check
    sessions_dir = benchmark_dir / "swarm_sessions"
    if not sessions_dir.exists():
        sessions_dir.mkdir(parents=True)
    click.echo(f"Serving dashboard for {benchmark_dir.name} (live refresh on F5)")

    _DashboardHandler.template = template_path.read_text()
    _DashboardHandler.benchmark_dir = benchmark_dir
    server = HTTPServer(("127.0.0.1", port), _DashboardHandler)

    url = f"http://127.0.0.1:{port}"
    click.echo(f"Dashboard: {url}")
    click.echo("Press Ctrl+C to stop.")

    webbrowser.open(url)

    try:
        server.serve_forever()
    except KeyboardInterrupt:
        click.echo("\nStopped.")
    finally:
        server.server_close()
