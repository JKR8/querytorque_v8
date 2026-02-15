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
    ) -> None:
        self.pipeline = pipeline
        self.benchmark_dir = benchmark_dir
        self.concurrency = concurrency
        self.dashboard = dashboard
        self.benchmark_lock = threading.Lock()

    # ── Phase 0: Survey ────────────────────────────────────────────────

    def survey(
        self,
        query_ids: List[str],
        queries: Dict[str, str],
    ) -> Dict[str, SurveyResult]:
        """Read EXPLAIN timings + run structural detection per query."""
        from ..detection import detect_transforms, load_transforms

        transforms_catalog = load_transforms()
        engine = self.pipeline.config.engine
        engine_name = {
            "postgresql": "postgresql",
            "postgres": "postgresql",
        }.get(engine, engine)
        dialect = "postgres" if engine in ("postgresql", "postgres") else engine

        results: Dict[str, SurveyResult] = {}

        for qid in query_ids:
            runtime_ms = self._load_explain_timing(qid)
            sql = queries.get(qid, "")

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
                    # Tractability = count of transforms with >= 0.6 overlap
                    tractability = sum(
                        1 for m in matched if m.overlap_ratio >= 0.6
                    )
                    # Structural bonus: top match overlap (0.0-1.0)
                    if matched:
                        structural_bonus = matched[0].overlap_ratio
                except Exception as e:
                    logger.warning(f"[{qid}] Detection failed: {e}")

            results[qid] = SurveyResult(
                query_id=qid,
                runtime_ms=runtime_ms,
                matched_transforms=matched[:5],  # top 5
                tractability=tractability,
                structural_bonus=structural_bonus,
            )

        return results

    def _load_explain_timing(self, query_id: str) -> float:
        """Extract execution time from cached EXPLAIN JSON.

        Searches: explains/{query_id}.json (flat) → explains/sf10/ → explains/sf5/
        DuckDB: top-level "execution_time_ms" field
        PostgreSQL: plan_json[0]["Execution Time"] (ms)
        """
        search_paths = [
            self.benchmark_dir / "explains" / f"{query_id}.json",
            self.benchmark_dir / "explains" / "sf10" / f"{query_id}.json",
            self.benchmark_dir / "explains" / "sf5" / f"{query_id}.json",
        ]

        for path in search_paths:
            if not path.exists():
                continue
            try:
                data = json.loads(path.read_text())

                # DuckDB format: top-level execution_time_ms
                if "execution_time_ms" in data:
                    val = data["execution_time_ms"]
                    if val and val > 0:
                        return float(val)

                # PostgreSQL format: plan_json[0]["Execution Time"]
                plan_json = data.get("plan_json")
                if isinstance(plan_json, list) and plan_json:
                    exec_time = plan_json[0].get("Execution Time")
                    if exec_time is not None:
                        return float(exec_time)

                # DuckDB plan_json dict → latency field
                if isinstance(plan_json, dict):
                    latency = plan_json.get("latency")
                    if latency and latency > 0:
                        return float(latency) * 1000  # s → ms

            except Exception as e:
                logger.warning(f"[{query_id}] Failed to read EXPLAIN timing: {e}")

        return 0.0  # unknown — will bucket as SKIP

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

            result = self.pipeline.run_optimization_session(
                query_id=qid,
                sql=triage_item.sql,
                max_iterations=triage_item.max_iterations,
                target_speedup=10.0,
                mode=OptimizationMode.ONESHOT,
                patch=True,
                benchmark_lock=self.benchmark_lock,
            )

            # Attach phase callback to the session (for future iterations)
            # Note: run_optimization_session creates the session internally,
            # so we can't easily attach before run(). The dashboard still
            # gets updated at start/end of each query.

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
