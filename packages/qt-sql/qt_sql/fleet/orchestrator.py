"""Fleet orchestrator: survey → triage → parallel execute → compile scorecard."""

from __future__ import annotations

import json
import logging
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


# ── Orchestrator ───────────────────────────────────────────────────────────


class FleetOrchestrator:
    """4-phase fleet orchestrator: survey → triage → execute → compile."""

    def __init__(
        self,
        pipeline: "Pipeline",
        benchmark_dir: Path,
        concurrency: int = 4,
        dashboard: Optional["FleetDashboard"] = None,
        event_bus: Optional[Any] = None,
        triage_gate: Optional[threading.Event] = None,
        pause_event: Optional[threading.Event] = None,
    ) -> None:
        self.pipeline = pipeline
        self.benchmark_dir = benchmark_dir
        self.concurrency = concurrency
        self.dashboard = dashboard
        self.event_bus = event_bus
        self.triage_gate = triage_gate
        self.pause_event = pause_event
        self.benchmark_lock = threading.Lock()

    def _emit(self, event_type, **data) -> None:
        """Emit event to EventBus if attached."""
        if self.event_bus:
            self.event_bus.emit(event_type, **data)

    def wait_for_triage_approval(self, timeout: float = 3600) -> bool:
        """Block until the triage gate is set (browser clicks Approve). Returns True if approved."""
        if not self.triage_gate:
            return True
        return self.triage_gate.wait(timeout=timeout)

    # ── Phase 0: Survey ────────────────────────────────────────────────

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
            return self.pipeline._get_explain(query_id, sql)
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
        results: List[TriageResult] = []

        for qid, sv in surveys.items():
            bucket = self._bucket_runtime(sv.runtime_ms)
            priority = self._compute_priority(sv, bucket)
            max_iters = self._compute_max_iterations(bucket, sv.tractability)

            results.append(TriageResult(
                query_id=qid,
                sql=queries.get(qid, ""),
                bucket=bucket,
                priority_score=priority,
                max_iterations=max_iters,
                survey=sv,
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
    def _compute_priority(sv: SurveyResult, bucket: str) -> float:
        weight = RUNTIME_WEIGHTS[bucket]
        return weight * (1.0 + sv.tractability + sv.structural_bonus)

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

            # Check pause gate before starting
            if self.pause_event:
                self.pause_event.wait()

            result = self.pipeline.run_optimization_session(
                query_id=qid,
                sql=triage_item.sql,
                max_iterations=triage_item.max_iterations,
                target_speedup=10.0,  # intentional: fleet always aims for 10x
                mode=OptimizationMode.BEAM,
                patch=True,
                benchmark_lock=self.benchmark_lock,
                on_phase_change=_on_phase,
            )

            return qid, result

        with ThreadPoolExecutor(max_workers=self.concurrency) as pool:
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
                        completed=len(results), total=len(work_items),
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
                        completed=len(results), total=len(work_items),
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
            f"**Concurrency:** {self.concurrency}",
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
