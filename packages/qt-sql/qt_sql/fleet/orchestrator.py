"""Fleet orchestrator: survey → triage → parallel execute → compile scorecard."""

from __future__ import annotations

import json
import logging
import re
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Set, TYPE_CHECKING

if TYPE_CHECKING:
    from ..pipeline import Pipeline
    from .dashboard import FleetDashboard
from .event_bus import EventType

logger = logging.getLogger(__name__)

# ── Triage Constants ───────────────────────────────────────────────────────

RUNTIME_THRESHOLDS = [
    (100, "SKIP"),
    (1_000, "LOW"),
    (10_000, "MEDIUM"),
    (float("inf"), "HIGH"),
]

RUNTIME_WEIGHTS = {"SKIP": 0, "LOW": 1, "MEDIUM": 3, "HIGH": 5}

MAX_ITERS_BASE = {"SKIP": 0, "LOW": 1, "MEDIUM": 2, "HIGH": 3}
PRIOR_HISTORY_SPEEDUP_BONUS_CAP = 2.0


# ── Data Classes ───────────────────────────────────────────────────────────


@dataclass
class SurveyResult:
    """Per-query survey data from Phase 0."""

    query_id: str
    runtime_ms: float
    matched_transforms: list = field(default_factory=list)
    tractability: int = 0  # count of high-overlap matches (>= 0.6)
    structural_bonus: float = 0.0
    explain_text: str = ""  # truncated EXPLAIN plan for dashboard
    actual_rows: int = 0  # rows returned by query
    timing_source: str = ""  # "explain_analyze" | "leaderboard" | "unknown"


@dataclass
class TriageResult:
    """Per-query triage decision from Phase 1."""

    query_id: str
    sql: str
    bucket: str  # SKIP | LOW | MEDIUM | HIGH
    priority_score: float
    max_iterations: int
    survey: SurveyResult
    seed_sql: str = ""
    prior_best_speedup: Optional[float] = None
    prior_best_sql: str = ""
    prior_source: str = ""
    prior_reference: str = ""


@dataclass
class PriorOptimization:
    """Best known historical optimization artifact for a query."""

    query_id: str
    best_speedup: float
    best_sql: str
    source: str
    reference: str
    updated_at: float


# ── Orchestrator ───────────────────────────────────────────────────────────


class FleetOrchestrator:
    """4-phase fleet orchestrator: survey → triage → execute → compile."""

    def __init__(
        self,
        pipeline: Optional["Pipeline"] = None,
        benchmark_dir: Optional[Path] = None,
        dashboard: Optional["FleetDashboard"] = None,
        event_bus: Optional[Any] = None,
        triage_gate: Optional[threading.Event] = None,
        pause_event: Optional[threading.Event] = None,
    ) -> None:
        self.pipeline = pipeline
        self.benchmark_dir = benchmark_dir or (Path(pipeline.benchmark_dir) if pipeline else None)
        self.dashboard = dashboard
        self.event_bus = event_bus
        self.triage_gate = triage_gate
        self.pause_event = pause_event
        # Global benchmark slot pool — each individual cloud compute connection
        # acquires a slot. Default 8 from config.
        bench_cfg = {}
        try:
            cfg_path = benchmark_dir / "config.json"
            if cfg_path.exists():
                bench_cfg = json.loads(cfg_path.read_text())
        except Exception:
            pass
        n_bench_slots = max(1, int(bench_cfg.get("benchmark_slots", 8) or 8))
        self.benchmark_sem = threading.Semaphore(n_bench_slots)
        self._prior_history_cache: Optional[Dict[str, PriorOptimization]] = None

    def _emit(self, event_type: EventType | str, **data) -> None:
        """Emit event to EventBus if attached."""
        if self.event_bus:
            normalized = event_type
            if isinstance(event_type, str):
                try:
                    normalized = EventType(event_type)
                except ValueError:
                    logger.warning("Fleet: unknown event type %r", event_type)
            self.event_bus.emit(normalized, **data)

    def _runtime_config_path(self) -> Path:
        return self.benchmark_dir / ".fleet_runtime_config.json"

    @staticmethod
    def _canonical_query_id(query_id: str) -> str:
        qid = str(query_id or "").strip().lower()
        if not qid:
            return ""
        if qid.startswith("query_") or qid.startswith("query"):
            return qid
        if re.match(r"^q\d", qid):
            return f"query_{qid[1:]}"
        return qid

    @staticmethod
    def _compact_query_id(query_id: str) -> str:
        qid = str(query_id or "").strip().lower()
        if qid.startswith("query_"):
            return f"q{qid[len('query_'):]}"
        return qid

    @staticmethod
    def _parse_bool_flag(value: Any, default: bool = False) -> bool:
        if value is None:
            return default
        if isinstance(value, bool):
            return value
        return str(value).strip().lower() in {"1", "true", "yes", "on"}

    def _load_runtime_config(self) -> Dict[str, Any]:
        path = self._runtime_config_path()
        if not path.exists():
            return {}
        try:
            cfg = json.loads(path.read_text())
            return cfg if isinstance(cfg, dict) else {}
        except Exception as e:
            logger.warning("Fleet: failed to read runtime config: %s", e)
            return {}

    def _use_blackboard_history(self, cfg: Optional[Dict[str, Any]] = None) -> bool:
        active_cfg = cfg if cfg is not None else self._load_runtime_config()
        return self._parse_bool_flag(
            active_cfg.get("use_blackboard_history"),
            default=True,
        )

    @staticmethod
    def _session_query_id_from_dirname(dirname: str) -> str:
        # Expected patterns include query_88_YYYYMMDD_HHMMSS and q88_YYYYMMDD_HHMMSS.
        return re.sub(r"_\d{8}_\d{6}$", "", dirname)

    @staticmethod
    def _safe_read_json(path: Path) -> Optional[Dict[str, Any]]:
        try:
            data = json.loads(path.read_text())
            return data if isinstance(data, dict) else None
        except Exception:
            return None

    def _relative_ref(self, path: Path) -> str:
        try:
            return str(path.relative_to(self.benchmark_dir))
        except Exception:
            return str(path)

    def _read_sql_file(self, path: Path) -> str:
        try:
            return path.read_text().strip()
        except Exception:
            return ""

    def _worker_sql_path(self, query_dir: Path, worker_id: Any) -> Optional[Path]:
        try:
            wid = int(worker_id)
        except Exception:
            return None
        candidates = [
            query_dir / f"worker_{wid}_sql.sql",
            query_dir / f"worker_{wid:02d}_sql.sql",
        ]
        for candidate in candidates:
            if candidate.exists():
                return candidate
        return None

    def _record_prior_candidate(
        self,
        history: Dict[str, PriorOptimization],
        query_id: str,
        speedup: Any,
        sql: str,
        source: str,
        reference: str,
        updated_at: float,
    ) -> None:
        if not query_id:
            return
        qid = self._canonical_query_id(query_id)
        if not qid:
            return
        try:
            score = float(speedup)
        except Exception:
            return
        if score <= 0:
            return
        normalized_sql = (sql or "").strip()
        if not normalized_sql:
            return

        candidate = PriorOptimization(
            query_id=qid,
            best_speedup=score,
            best_sql=normalized_sql,
            source=source,
            reference=reference,
            updated_at=updated_at,
        )
        current = history.get(qid)
        if current is None:
            history[qid] = candidate
            return
        if candidate.best_speedup > current.best_speedup:
            history[qid] = candidate
            return
        if (
            abs(candidate.best_speedup - current.best_speedup) < 1e-9
            and candidate.updated_at > current.updated_at
        ):
            history[qid] = candidate

    def _load_prior_optimizations(self) -> Dict[str, PriorOptimization]:
        if self._prior_history_cache is not None:
            return self._prior_history_cache

        history: Dict[str, PriorOptimization] = {}

        # Beam session history (iter0_result.txt has best_speedup + best_sql).
        beam_root = self.benchmark_dir / "beam_sessions"
        if beam_root.exists():
            for result_path in beam_root.glob("*/iter0_result.txt"):
                payload = self._safe_read_json(result_path)
                if not payload:
                    continue
                session_query_id = self._session_query_id_from_dirname(result_path.parent.name)
                speedup = payload.get("best_speedup")
                best_sql = str(payload.get("best_sql", "") or "")
                self._record_prior_candidate(
                    history=history,
                    query_id=session_query_id,
                    speedup=speedup,
                    sql=best_sql,
                    source="beam_sessions",
                    reference=self._relative_ref(result_path),
                    updated_at=result_path.stat().st_mtime,
                )

        # Swarm/blackboard history.
        for batch_dir in self.benchmark_dir.glob("swarm_batch_*"):
            if not batch_dir.is_dir():
                continue
            for query_dir in batch_dir.glob("query_*"):
                if not query_dir.is_dir():
                    continue
                query_id = query_dir.name

                # Prefer benchmark summaries for speedup, map worker -> SQL file.
                for bench_path in query_dir.glob("benchmark_iter*.json"):
                    payload = self._safe_read_json(bench_path)
                    if not payload:
                        continue
                    speedup = payload.get("best_speedup")
                    worker_id = payload.get("best_worker_id")
                    sql_path = self._worker_sql_path(query_dir, worker_id)
                    if not sql_path:
                        for fallback_name in ("final_worker_sql.sql", "snipe_worker_sql.sql"):
                            candidate = query_dir / fallback_name
                            if candidate.exists():
                                sql_path = candidate
                                break
                    if not sql_path:
                        continue
                    sql_text = self._read_sql_file(sql_path)
                    self._record_prior_candidate(
                        history=history,
                        query_id=query_id,
                        speedup=speedup,
                        sql=sql_text,
                        source=f"swarm_batch:{batch_dir.name}",
                        reference=self._relative_ref(sql_path),
                        updated_at=bench_path.stat().st_mtime,
                    )

                # Blackboard metadata fallback for runs missing benchmark_iter artifacts.
                bb_raw = batch_dir / "blackboard" / "raw" / query_id
                if bb_raw.exists():
                    for worker_json in bb_raw.glob("worker_*.json"):
                        payload = self._safe_read_json(worker_json)
                        if not payload:
                            continue
                        speedup = payload.get("speedup")
                        sql_path = self._worker_sql_path(
                            query_dir,
                            payload.get("worker_id"),
                        )
                        if not sql_path:
                            continue
                        sql_text = self._read_sql_file(sql_path)
                        self._record_prior_candidate(
                            history=history,
                            query_id=query_id,
                            speedup=speedup,
                            sql=sql_text,
                            source=f"blackboard:{batch_dir.name}",
                            reference=self._relative_ref(worker_json),
                            updated_at=worker_json.stat().st_mtime,
                        )

        self._prior_history_cache = history
        logger.info("Fleet: loaded %d prior optimization records", len(history))
        return history

    def _apply_runtime_config(self) -> None:
        cfg = self._load_runtime_config()
        if not cfg:
            return

        db_dsn = str(cfg.get("db_dsn", "") or "").strip()
        explain_policy = str(cfg.get("explain_policy", "") or "").strip()
        source_mode = str(cfg.get("source_mode", "local") or "local").strip()

        if db_dsn:
            self.pipeline.config.db_path_or_dsn = db_dsn
            self.pipeline.config.benchmark_dsn = db_dsn
        if explain_policy:
            setattr(self.pipeline.config, "explain_policy", explain_policy)

        logger.info(
            "Fleet: runtime config applied (mode=%s, db=%s, explain_policy=%s)",
            source_mode,
            "set" if db_dsn else "default",
            explain_policy or "default",
        )
        self._emit(
            EventType.EVENT_LOG,
            scope="fleet",
            target="Config",
            msg=(
                "Runtime config applied for execution: "
                f"mode={source_mode}, "
                f"db={'set' if db_dsn else 'default'}, "
                f"policy={explain_policy or 'default'}"
            ),
            level="system",
        )

    def wait_for_triage_approval(self, timeout: float = 3600) -> bool:
        """Block until the triage gate is set (browser clicks Approve). Returns True if approved."""
        if not self.triage_gate:
            return True
        return self.triage_gate.wait(timeout=timeout)

    # ── Phase 0: Survey ────────────────────────────────────────────────

    def survey_from_dsn(
        self,
        dsn: str,
        engine: str = "postgresql",
        limit: int = 50,
    ) -> List["TriageResult"]:
        """Survey a customer database for slow queries without a benchmark_dir.

        Connects to the live database and discovers slow queries from
        pg_stat_statements (PostgreSQL) or system views. Creates an
        ephemeral Pipeline and runs survey + triage.

        Args:
            dsn: Database connection string
            engine: Database engine
            limit: Max queries to discover

        Returns:
            List of TriageResult sorted by priority
        """
        from ..pipeline import Pipeline

        # Create pipeline from DSN if not already set
        if self.pipeline is None:
            self.pipeline = Pipeline.from_dsn(dsn=dsn, engine=engine)
            self.benchmark_dir = self.pipeline.benchmark_dir

        # Discover slow queries from pg_stat_statements
        queries: Dict[str, str] = {}
        try:
            from ..execution.factory import create_executor_from_dsn

            with create_executor_from_dsn(dsn) as executor:
                if engine in ("postgresql", "postgres"):
                    rows = executor.execute(
                        "SELECT queryid::text, query, mean_exec_time "
                        "FROM pg_stat_statements "
                        "WHERE mean_exec_time > 100 "
                        "ORDER BY mean_exec_time DESC "
                        f"LIMIT {limit}"
                    )
                    for row in rows:
                        qid = f"q_{row['queryid']}"
                        queries[qid] = row["query"]
                else:
                    logger.warning("survey_from_dsn: unsupported engine %s", engine)
        except Exception as e:
            logger.error("Failed to discover queries from %s: %s", engine, e)

        if not queries:
            return []

        # Run standard survey + triage
        query_ids = list(queries.keys())
        survey_results = self.survey(query_ids, queries)
        triage_results = self.triage(query_ids, queries, survey_results)

        return triage_results

    def survey(
        self,
        query_ids: List[str],
        queries: Dict[str, str],
        on_progress: Optional[Callable[[str, int, int], None]] = None,
    ) -> Dict[str, SurveyResult]:
        """Collect baselines + run structural detection per query.

        Runs EXPLAIN ANALYZE via the pipeline to get real execution times,
        row counts, and plan text. Falls back to cached/leaderboard data.

        Args:
            on_progress: Optional callback(query_id, completed, total) for UI.
        """
        from ..detection import detect_transforms, load_transforms

        transforms_catalog = load_transforms()
        engine = self.pipeline.config.engine
        engine_name = {
            "postgresql": "postgresql",
            "postgres": "postgresql",
        }.get(engine, engine)
        dialect = "postgres" if engine in ("postgresql", "postgres") else engine

        results: Dict[str, SurveyResult] = {}
        total = len(query_ids)

        for i, qid in enumerate(query_ids):
            sql = queries.get(qid, "")

            # Collect baseline: EXPLAIN ANALYZE (cached first, run if needed)
            runtime_ms = -1.0
            explain_text = ""
            actual_rows = 0
            timing_source = "unknown"

            explain_data = self._get_explain_data(qid, sql)
            if explain_data:
                # Extract timing
                ems = explain_data.get("execution_time_ms")
                if ems and ems > 0:
                    runtime_ms = float(ems)
                    timing_source = "explain_analyze"
                else:
                    # plan_json latency fallback (DuckDB)
                    pj = explain_data.get("plan_json")
                    if isinstance(pj, dict):
                        lat = pj.get("latency")
                        if lat and lat > 0:
                            runtime_ms = float(lat) * 1000
                            timing_source = "explain_analyze"

                # Extract plan text (truncated for dashboard)
                pt = explain_data.get("plan_text", "")
                if pt:
                    lines = pt.split("\n")
                    explain_text = "\n".join(lines[:80])

                # Extract row count
                ar = explain_data.get("actual_rows")
                if ar:
                    actual_rows = int(ar)

            # Leaderboard fallback for timing
            if runtime_ms <= 0:
                lb_ms = self._leaderboard_timings.get(qid)
                if lb_ms is not None:
                    runtime_ms = lb_ms
                    timing_source = "leaderboard"

            # Structural detection
            matched = []
            tractability = 0
            structural_bonus = 0.0
            if sql:
                try:
                    matched = detect_transforms(
                        sql, transforms_catalog,
                        engine=engine_name, dialect=dialect,
                    )
                    tractability = sum(
                        1 for m in matched if m.overlap_ratio >= 0.6
                    )
                    if matched:
                        structural_bonus = matched[0].overlap_ratio
                except Exception as e:
                    logger.warning(f"[{qid}] Detection failed: {e}")

            results[qid] = SurveyResult(
                query_id=qid,
                runtime_ms=runtime_ms,
                matched_transforms=matched[:5],
                tractability=tractability,
                structural_bonus=structural_bonus,
                explain_text=explain_text,
                actual_rows=actual_rows,
                timing_source=timing_source,
            )

            if on_progress:
                on_progress(qid, i + 1, total)

        return results

    def _get_explain_data(self, query_id: str, sql: str) -> Optional[Dict[str, Any]]:
        """Get EXPLAIN ANALYZE result via pipeline (cached first, run if needed).

        Returns dict with execution_time_ms, plan_text, plan_json, actual_rows.
        Returns None if no DB connection or query fails.
        """
        if not sql:
            return None
        try:
            # Survey/triage is the pre-flight stage allowed to collect missing EXPLAINs.
            return self.pipeline._get_explain(
                query_id,
                sql,
                collect_if_missing=True,
            )
        except Exception as e:
            logger.warning(f"[{query_id}] EXPLAIN collection failed: {e}")
            return None

    @property
    def _leaderboard_timings(self) -> Dict[str, float]:
        """Lazy-load leaderboard.json timings (cached)."""
        if not hasattr(self, "_lb_cache"):
            self._lb_cache: Dict[str, float] = {}
            lb_path = self.benchmark_dir / "leaderboard.json"
            if lb_path.exists():
                try:
                    lb = json.loads(lb_path.read_text())
                    for q in lb.get("queries", []):
                        qid = q.get("query_id", "")
                        ms = q.get("original_ms")
                        if qid and ms is not None and ms > 0:
                            # Normalize: leaderboard uses "q88", survey uses "query_88"
                            self._lb_cache[qid] = float(ms)
                            # Also store with query_ prefix
                            if qid.startswith("q") and not qid.startswith("query_"):
                                num = qid[1:]
                                self._lb_cache[f"query_{num}"] = float(ms)
                except Exception as e:
                    logger.warning(f"Failed to load leaderboard timings: {e}")
        return self._lb_cache

    # ── Phase 1: Triage ────────────────────────────────────────────────

    def triage(
        self,
        surveys: Dict[str, SurveyResult],
        queries: Dict[str, str],
    ) -> List[TriageResult]:
        """Score and sort queries by optimization potential."""
        cfg = self._load_runtime_config()
        use_history = self._use_blackboard_history(cfg)
        history = self._load_prior_optimizations() if use_history else {}

        results: List[TriageResult] = []

        for qid, sv in surveys.items():
            bucket = self._bucket_runtime(sv.runtime_ms)
            base_sql = queries.get(qid, "")
            prior = history.get(self._canonical_query_id(qid))
            prior_speedup = prior.best_speedup if prior else None
            prior_sql = prior.best_sql if prior else ""
            priority = self._compute_priority(
                sv,
                bucket,
                prior_best_speedup=prior_speedup,
            )
            max_iters = self._compute_max_iterations(bucket, sv.tractability)

            results.append(TriageResult(
                query_id=qid,
                sql=base_sql,
                bucket=bucket,
                priority_score=priority,
                max_iterations=max_iters,
                survey=sv,
                seed_sql=prior_sql or base_sql,
                prior_best_speedup=prior_speedup,
                prior_best_sql=prior_sql,
                prior_source=prior.source if prior else "",
                prior_reference=prior.reference if prior else "",
            ))

        # Sort descending by priority (HIGH first)
        results.sort(key=lambda t: -t.priority_score)
        return results

    @staticmethod
    def _bucket_runtime(runtime_ms: float) -> str:
        if runtime_ms < 0:
            return "MEDIUM"  # unknown runtime — worth attempting
        for threshold, bucket in RUNTIME_THRESHOLDS:
            if runtime_ms < threshold:
                return bucket
        return "HIGH"

    @staticmethod
    def _compute_priority(
        sv: SurveyResult,
        bucket: str,
        prior_best_speedup: Optional[float] = None,
    ) -> float:
        weight = RUNTIME_WEIGHTS[bucket]
        base_priority = weight * (1.0 + sv.tractability + sv.structural_bonus)
        if prior_best_speedup is None or prior_best_speedup <= 1.0:
            return base_priority
        bonus = min(
            max(prior_best_speedup - 1.0, 0.0),
            PRIOR_HISTORY_SPEEDUP_BONUS_CAP,
        )
        return base_priority * (1.0 + bonus)

    @staticmethod
    def _compute_max_iterations(bucket: str, tractability: int) -> int:
        base = MAX_ITERS_BASE[bucket]
        if bucket == "HIGH" and tractability >= 2:
            return 5
        if bucket == "MEDIUM" and tractability >= 2:
            return 3
        return base

    # ── Phase 2: Execute ───────────────────────────────────────────────

    def execute(
        self,
        triaged: List[TriageResult],
        completed_ids: Set[str],
        out: Path,
        checkpoint_path: Path,
    ) -> List[Dict]:
        """Parallel LLM + serial benchmark execution."""
        from ..schemas import OptimizationMode

        self._apply_runtime_config()
        runtime_cfg = self._load_runtime_config()
        use_history = self._use_blackboard_history(runtime_cfg)
        history = self._load_prior_optimizations() if use_history else {}
        self._emit(
            EventType.EVENT_LOG,
            scope="fleet",
            target="Triage",
            msg=(
                "Prior optimization seeding "
                f"{'enabled' if use_history else 'disabled'}."
            ),
            level="system",
        )

        # Filter out SKIP and already completed queries
        work_items = [
            t for t in triaged
            if t.bucket != "SKIP" and t.query_id not in completed_ids
        ]

        if not work_items:
            logger.info("Fleet: no queries to execute")
            return []

        # Initialize dashboard with all queries
        if self.dashboard:
            for t in triaged:
                detail = ""
                if t.survey.matched_transforms:
                    detail = t.survey.matched_transforms[0].id
                self.dashboard.init_query(
                    t.query_id,
                    bucket=t.bucket,
                    detail=f"{t.survey.runtime_ms:.0f}ms {detail}".strip(),
                )

        results: List[Dict] = []

        def _run_one(triage_item: TriageResult) -> tuple:
            qid = triage_item.query_id
            seed_sql = triage_item.sql
            seed_source = "original"
            seed_speedup = None
            seed_ref = ""

            if use_history:
                if triage_item.prior_best_sql:
                    seed_sql = triage_item.prior_best_sql
                    seed_source = triage_item.prior_source or "history"
                    seed_speedup = triage_item.prior_best_speedup
                    seed_ref = triage_item.prior_reference
                else:
                    prior = history.get(self._canonical_query_id(qid))
                    if prior and prior.best_sql:
                        seed_sql = prior.best_sql
                        seed_source = prior.source
                        seed_speedup = prior.best_speedup
                        seed_ref = prior.reference

            if seed_source != "original":
                speed_txt = (
                    f", prior={seed_speedup:.2f}x"
                    if isinstance(seed_speedup, (int, float))
                    else ""
                )
                ref_txt = f", ref={seed_ref}" if seed_ref else ""
                self._emit(
                    EventType.EVENT_LOG,
                    scope="fleet",
                    target=qid,
                    msg=f"Seed SQL: {seed_source}{speed_txt}{ref_txt}",
                    level="system",
                )

            # Update dashboard
            if self.dashboard:
                self.dashboard.set_query_status(
                    qid, "RUNNING", phase="starting",
                    detail=f"iter 1/{triage_item.max_iterations}",
                )

            # Set up phase callback for dashboard
            def _on_phase(phase: str, iteration: int) -> None:
                if self.dashboard:
                    self.dashboard.set_query_status(
                        qid, "RUNNING", phase=phase,
                        detail=f"iter {iteration + 1}/{triage_item.max_iterations}",
                    )
                self._emit(
                    "query_update",
                    query_id=qid, status="RUNNING", phase=phase,
                    iteration=iteration + 1,
                    max_iterations=triage_item.max_iterations,
                )
                self._emit(
                    EventType.EVENT_LOG,
                    scope="fleet",
                    target=qid,
                    msg=f"Phase: {phase} (iter {iteration + 1}/{triage_item.max_iterations})",
                    level="system",
                )

            # Check pause gate before starting
            if self.pause_event:
                self.pause_event.wait()

            result = self.pipeline.run_optimization_session(
                query_id=qid,
                sql=seed_sql or triage_item.sql,
                max_iterations=triage_item.max_iterations,
                target_speedup=10.0,  # intentional: fleet always aims for 10x
                mode=OptimizationMode.BEAM,
                patch=True,
                benchmark_sem=self.benchmark_sem,
                on_phase_change=_on_phase,
            )

            return qid, result

        with ThreadPoolExecutor(max_workers=len(work_items)) as pool:
            futures = {
                pool.submit(_run_one, item): item.query_id
                for item in work_items
            }

            for future in as_completed(futures):
                qid = futures[future]
                try:
                    qid, result = future.result()
                    speedup = getattr(result, "best_speedup", None) or getattr(result, "speedup", None)
                    status_str = getattr(result, "status", "?")
                    n_api_calls = int(getattr(result, "n_api_calls", 0) or 0)
                    beam_cost = float(getattr(result, "beam_cost_usd", 0.0) or 0.0)

                    # Save result
                    self._save_query_result(
                        result, qid, out, checkpoint_path, completed_ids,
                    )
                    results.append({
                        "query_id": qid,
                        "status": str(status_str),
                        "speedup": speedup,
                    })

                    # Update dashboard
                    if self.dashboard:
                        self.dashboard.set_query_status(
                            qid, status_str, phase="done",
                            speedup=speedup,
                        )
                    self._emit(
                        "query_complete",
                        query_id=qid, status=str(status_str),
                        speedup=speedup,
                        n_api_calls=n_api_calls,
                        beam_cost_usd=beam_cost,
                        beam_token_totals=getattr(result, "beam_token_totals", {}) or {},
                        completed=len(results), total=len(work_items),
                    )
                    speed_txt = f" {speedup:.2f}x" if isinstance(speedup, (int, float)) else ""
                    calls_txt = f", calls={n_api_calls}" if n_api_calls > 0 else ""
                    cost_txt = f", cost=${beam_cost:.4f}" if beam_cost > 0 else ""
                    self._emit(
                        EventType.EVENT_LOG,
                        scope="fleet",
                        target=qid,
                        msg=f"Completed: {status_str}{speed_txt}{calls_txt}{cost_txt}",
                        level=str(status_str).lower(),
                    )

                    logger.info(
                        f"Fleet [{len(results)}/{len(work_items)}] "
                        f"{qid}: {status_str} {speedup or '?'}x"
                    )

                except Exception as e:
                    results.append({
                        "query_id": qid,
                        "status": "ERROR",
                        "speedup": None,
                        "error": str(e),
                    })
                    if self.dashboard:
                        self.dashboard.set_query_status(
                            qid, "ERROR", phase="error",
                            detail=str(e)[:40],
                        )
                    self._emit(
                        "query_complete",
                        query_id=qid, status="ERROR",
                        error=str(e)[:100],
                        n_api_calls=0,
                        beam_cost_usd=0.0,
                        completed=len(results), total=len(work_items),
                    )
                    self._emit(
                        EventType.EVENT_LOG,
                        scope="fleet",
                        target=qid,
                        msg=f"Error: {e}",
                        level="error",
                    )
                    logger.error(f"Fleet {qid}: ERROR {e}")

        return results

    def _save_query_result(
        self,
        result: Any,
        qid: str,
        out: Path,
        checkpoint_path: Path,
        completed_ids: Set[str],
    ) -> None:
        """Save per-query result and update checkpoint."""
        query_out = out / qid
        query_out.mkdir(exist_ok=True)
        (query_out / "result.json").write_text(
            json.dumps(
                result.__dict__ if hasattr(result, "__dict__") else str(result),
                indent=2, default=str,
            )
        )

        completed_ids.add(qid)
        checkpoint_path.write_text(json.dumps({
            "completed": sorted(completed_ids),
            "last_updated": datetime.now().isoformat(),
        }, indent=2))

    # ── Phase 3: Compile ───────────────────────────────────────────────

    def compile(
        self,
        results: List[Dict],
        triaged: List[TriageResult],
    ) -> str:
        """Aggregate results into scorecard markdown."""
        # Build lookup
        result_map = {r["query_id"]: r for r in results}

        # Categorize
        wins = []
        improved = []
        neutral = []
        regression = []
        errors = []
        skipped = []

        for t in triaged:
            if t.bucket == "SKIP":
                skipped.append(t)
                continue

            r = result_map.get(t.query_id)
            if not r:
                continue

            status = r.get("status", "")
            entry = {**r, "bucket": t.bucket, "runtime_ms": t.survey.runtime_ms}

            if status == "WIN":
                wins.append(entry)
            elif status == "IMPROVED":
                improved.append(entry)
            elif status == "NEUTRAL":
                neutral.append(entry)
            elif status == "REGRESSION":
                regression.append(entry)
            else:
                errors.append(entry)

        total_executed = len(results)
        total_queries = len(triaged)

        lines = [
            "# Fleet Scorecard",
            "",
            f"**Benchmark:** {self.benchmark_dir.name}",
            f"**Engine:** {self.pipeline.config.engine}",
            f"**Timestamp:** {datetime.now().isoformat()}",
            f"**Benchmark Slots:** {self.benchmark_sem._value}",
            "",
            "## Summary",
            "",
            "| Category | Count | % |",
            "|----------|------:|--:|",
            f"| WIN (>=1.1x) | {len(wins)} | {self._pct(len(wins), total_executed)} |",
            f"| IMPROVED (>=1.05x) | {len(improved)} | {self._pct(len(improved), total_executed)} |",
            f"| NEUTRAL | {len(neutral)} | {self._pct(len(neutral), total_executed)} |",
            f"| REGRESSION | {len(regression)} | {self._pct(len(regression), total_executed)} |",
            f"| ERROR | {len(errors)} | {self._pct(len(errors), total_executed)} |",
            f"| SKIPPED (<100ms) | {len(skipped)} | {self._pct(len(skipped), total_queries)} |",
            "",
        ]

        if wins:
            lines.append("## Top Winners")
            lines.append("")
            lines.append("| Query | Bucket | Speedup | Baseline (ms) |")
            lines.append("|-------|--------|--------:|--------------:|")
            for w in sorted(wins, key=lambda x: -(x.get("speedup") or 0)):
                lines.append(
                    f"| {w['query_id']} | {w['bucket']} "
                    f"| **{w.get('speedup', 0):.2f}x** "
                    f"| {w.get('runtime_ms', 0):.0f} |"
                )
            lines.append("")

        if regression:
            lines.append("## Regressions")
            lines.append("")
            lines.append("| Query | Bucket | Speedup | Baseline (ms) |")
            lines.append("|-------|--------|--------:|--------------:|")
            for r in sorted(regression, key=lambda x: x.get("speedup", 0)):
                lines.append(
                    f"| {r['query_id']} | {r['bucket']} "
                    f"| {r.get('speedup', 0):.2f}x "
                    f"| {r.get('runtime_ms', 0):.0f} |"
                )
            lines.append("")

        # Triage distribution
        lines.append("## Triage Distribution")
        lines.append("")
        lines.append("| Bucket | Count | Avg Runtime (ms) |")
        lines.append("|--------|------:|-----------------:|")
        for bucket in ("HIGH", "MEDIUM", "LOW", "SKIP"):
            bucket_items = [t for t in triaged if t.bucket == bucket]
            if bucket_items:
                avg_rt = sum(t.survey.runtime_ms for t in bucket_items) / len(bucket_items)
                lines.append(
                    f"| {bucket} | {len(bucket_items)} | {avg_rt:.0f} |"
                )
        lines.append("")

        return "\n".join(lines) + "\n"

    @staticmethod
    def _pct(n: int, total: int) -> str:
        if total == 0:
            return "0%"
        return f"{100 * n // total}%"
