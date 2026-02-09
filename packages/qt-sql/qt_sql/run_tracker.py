"""Run tracker — manages runs/ directory and run_meta.json.

Creates timestamped run directories under benchmarks/<name>/runs/
and tracks metadata (git SHA, model, cost, duration) for full traceability.

Usage:
    from qt_sql.run_tracker import RunTracker

    tracker = RunTracker(benchmark_dir, notes="R-Bot comparison")
    run_dir = tracker.start()
    # ... run queries, save artifacts to run_dir ...
    tracker.finish(queries_attempted=76, queries_improved=42, total_api_calls=312)
"""

from __future__ import annotations

import json
import logging
import time
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

from .schemas import BenchmarkConfig, RunMeta

logger = logging.getLogger(__name__)


class RunTracker:
    """Manages a single benchmark run within the standard runs/ directory."""

    def __init__(
        self,
        benchmark_dir: str | Path,
        notes: str = "",
        provider: Optional[str] = None,
        model: Optional[str] = None,
    ):
        self.benchmark_dir = Path(benchmark_dir)
        self.notes = notes
        self._start_time: Optional[float] = None
        self._run_meta: Optional[RunMeta] = None
        self._run_dir: Optional[Path] = None

        # Load config
        config_path = self.benchmark_dir / "config.json"
        self._config_snapshot: Dict[str, Any] = {}
        if config_path.exists():
            self._config_snapshot = json.loads(config_path.read_text())

        # Resolve provider/model from settings if not given
        self._provider = provider or ""
        self._model = model or ""
        if not self._provider or not self._model:
            try:
                from qt_shared.config import get_settings
                settings = get_settings()
                self._provider = self._provider or settings.llm_provider
                self._model = self._model or settings.llm_model
            except Exception:
                pass

    @property
    def run_dir(self) -> Optional[Path]:
        return self._run_dir

    @property
    def run_meta(self) -> Optional[RunMeta]:
        return self._run_meta

    def start(self) -> Path:
        """Start a new run. Creates runs/run_YYYYMMDD_HHMMSS/ and writes initial run_meta.json."""
        self._start_time = time.time()
        run_id = RunMeta.generate_run_id()

        runs_dir = self.benchmark_dir / "runs"
        self._run_dir = runs_dir / run_id
        self._run_dir.mkdir(parents=True, exist_ok=True)

        git_info = RunMeta.capture_git_info()

        config = self._config_snapshot
        validation_method = config.get("validation_method", "3-run")

        self._run_meta = RunMeta(
            run_id=run_id,
            started_at=datetime.now().isoformat(),
            git_sha=git_info.get("git_sha", ""),
            git_branch=git_info.get("git_branch", ""),
            git_dirty=git_info.get("git_dirty", False),
            model=self._model,
            provider=self._provider,
            config_snapshot=config,
            workers=config.get("workers_state_0", 4),
            validation_method=validation_method,
            notes=self.notes,
        )

        self._run_meta.save(self._run_dir / "run_meta.json")
        logger.info(f"Run started: {self._run_dir}")
        return self._run_dir

    def finish(
        self,
        queries_attempted: int = 0,
        queries_improved: int = 0,
        total_api_calls: int = 0,
        estimated_cost_usd: float = 0.0,
    ) -> RunMeta:
        """Finalize the run. Updates run_meta.json with final stats."""
        if self._run_meta is None or self._run_dir is None:
            raise RuntimeError("Cannot finish a run that was not started")

        self._run_meta.finished_at = datetime.now().isoformat()
        self._run_meta.duration_seconds = round(
            time.time() - (self._start_time or time.time()), 1
        )
        self._run_meta.queries_attempted = queries_attempted
        self._run_meta.queries_improved = queries_improved
        self._run_meta.total_api_calls = total_api_calls
        self._run_meta.estimated_cost_usd = estimated_cost_usd

        self._run_meta.save(self._run_dir / "run_meta.json")
        logger.info(
            f"Run finished: {self._run_dir.name} — "
            f"{queries_improved}/{queries_attempted} improved, "
            f"{total_api_calls} API calls, "
            f"${estimated_cost_usd:.2f}"
        )
        return self._run_meta

    def save_run_leaderboard(self, results: List[Dict[str, Any]]) -> Path:
        """Save this run's leaderboard.json (per-run, not the benchmark-level one)."""
        if self._run_dir is None:
            raise RuntimeError("Cannot save leaderboard for a run that was not started")

        lb_path = self._run_dir / "leaderboard.json"
        lb_data = {
            "run_id": self._run_meta.run_id if self._run_meta else "",
            "queries": results,
        }
        lb_path.write_text(json.dumps(lb_data, indent=2))
        return lb_path

    @staticmethod
    def list_runs(benchmark_dir: str | Path) -> List[Path]:
        """List all run directories for a benchmark, sorted by name (chronological)."""
        runs_dir = Path(benchmark_dir) / "runs"
        if not runs_dir.exists():
            return []
        return sorted(
            [d for d in runs_dir.iterdir() if d.is_dir() and d.name.startswith("run_")],
            key=lambda d: d.name,
        )

    @staticmethod
    def load_run_meta(run_dir: str | Path) -> Optional[RunMeta]:
        """Load run_meta.json from a run directory."""
        meta_path = Path(run_dir) / "run_meta.json"
        if meta_path.exists():
            return RunMeta.from_file(meta_path)
        return None
