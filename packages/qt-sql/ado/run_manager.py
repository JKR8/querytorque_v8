"""Named run management — replaces anonymous state_N directories.

Runs live under benchmarks/<name>/runs/<run_name>/ with a run.yaml config,
per-query results, blackboard entries, and a run leaderboard.

Structure:
    runs/<run_name>/
    ├── run.yaml                   # RunConfig
    ├── results/<query_id>/
    │   └── worker_<N>/
    │       ├── prompt.txt
    │       ├── response.txt
    │       ├── optimized.sql
    │       └── validation.json
    ├── blackboard/
    │   ├── raw/<query_id>/worker_<N>.json
    │   └── collated.json
    ├── leaderboard.json
    └── summary.json
"""

from __future__ import annotations

import json
import logging
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

from .schemas import RunConfig

logger = logging.getLogger(__name__)

# Optional YAML
try:
    import yaml
    HAS_YAML = True
except ImportError:
    HAS_YAML = False


class RunManager:
    """Create and manage named optimization runs."""

    def __init__(self, benchmark_dir: Path):
        self.benchmark_dir = Path(benchmark_dir)
        self.runs_dir = self.benchmark_dir / "runs"

    def create_run(
        self,
        name: str,
        mode: str = "swarm",
        n_workers: int = 4,
        parent_run: Optional[str] = None,
        query_filter: Optional[List[str]] = None,
        target_speedup: float = 2.0,
        max_iterations: int = 3,
    ) -> Path:
        """Create a new named run directory with config.

        Args:
            name: Run name (e.g., "discovery_20260208")
            mode: Optimization mode (standard | expert | swarm)
            n_workers: Number of workers
            parent_run: Name of parent run for refinements
            query_filter: Optional query ID filter list
            target_speedup: Target speedup threshold
            max_iterations: Max optimization iterations

        Returns:
            Path to the created run directory.
        """
        run_dir = self.runs_dir / name
        if run_dir.exists():
            logger.warning(f"Run '{name}' already exists, reusing directory")
        else:
            run_dir.mkdir(parents=True, exist_ok=True)

        # Create subdirectories
        for subdir in ("results", "blackboard/raw"):
            (run_dir / subdir).mkdir(parents=True, exist_ok=True)

        # Write run config
        config = RunConfig(
            name=name,
            created=datetime.now().isoformat(),
            mode=mode,
            n_workers=n_workers,
            parent_run=parent_run,
            query_filter=query_filter,
            target_speedup=target_speedup,
            max_iterations=max_iterations,
        )

        config_data = config.to_dict()
        config_path = run_dir / "run.yaml"
        if HAS_YAML:
            config_path.write_text(
                yaml.dump(config_data, default_flow_style=False, sort_keys=False)
            )
        else:
            (run_dir / "run.json").write_text(json.dumps(config_data, indent=2))

        logger.info(f"Created run: {run_dir}")
        return run_dir

    def load_run(self, name: str) -> RunConfig:
        """Load run config by name."""
        run_dir = self.runs_dir / name

        # Try YAML first, then JSON
        for filename in ("run.yaml", "run.yml", "run.json"):
            path = run_dir / filename
            if path.exists():
                text = path.read_text()
                if filename.endswith((".yaml", ".yml")) and HAS_YAML:
                    data = yaml.safe_load(text) or {}
                else:
                    data = json.loads(text)
                return RunConfig.from_dict(data)

        raise FileNotFoundError(f"Run config not found in {run_dir}")

    def list_runs(self) -> List[Dict[str, Any]]:
        """List all runs with summary info."""
        if not self.runs_dir.exists():
            return []

        runs = []
        for run_dir in sorted(self.runs_dir.iterdir()):
            if not run_dir.is_dir():
                continue
            try:
                config = self.load_run(run_dir.name)
                summary_path = run_dir / "summary.json"
                summary = {}
                if summary_path.exists():
                    summary = json.loads(summary_path.read_text())

                runs.append({
                    "name": config.name,
                    "created": config.created,
                    "mode": config.mode,
                    "n_workers": config.n_workers,
                    "parent_run": config.parent_run,
                    **summary,
                })
            except Exception:
                runs.append({"name": run_dir.name, "error": "failed to load"})

        return runs

    def get_run_dir(self, name: str) -> Path:
        """Get path to a run directory."""
        return self.runs_dir / name

    def save_worker_result(
        self,
        run_name: str,
        query_id: str,
        worker_id: int,
        prompt: str = "",
        response: str = "",
        optimized_sql: str = "",
        validation: Optional[Dict[str, Any]] = None,
    ) -> Path:
        """Save a worker's results to the run directory.

        Args:
            run_name: Name of the run
            query_id: Query identifier
            worker_id: Worker number
            prompt: LLM prompt text
            response: LLM response text
            optimized_sql: Optimized SQL output
            validation: Validation result dict

        Returns:
            Path to worker result directory.
        """
        worker_dir = (
            self.runs_dir / run_name / "results" / query_id
            / f"worker_{worker_id:02d}"
        )
        worker_dir.mkdir(parents=True, exist_ok=True)

        if prompt:
            (worker_dir / "prompt.txt").write_text(prompt)
        if response:
            (worker_dir / "response.txt").write_text(response)
        if optimized_sql:
            (worker_dir / "optimized.sql").write_text(optimized_sql)
        if validation:
            (worker_dir / "validation.json").write_text(
                json.dumps(validation, indent=2)
            )

        return worker_dir

    def save_best(
        self,
        run_name: str,
        query_id: str,
        best_worker_id: int,
        best_data: Dict[str, Any],
    ) -> Path:
        """Save best worker selection for a query."""
        query_dir = self.runs_dir / run_name / "results" / query_id
        query_dir.mkdir(parents=True, exist_ok=True)
        path = query_dir / "best.json"
        best_data["best_worker_id"] = best_worker_id
        path.write_text(json.dumps(best_data, indent=2))
        return path

    def save_run_leaderboard(
        self,
        run_name: str,
        results: List[Dict[str, Any]],
    ) -> Path:
        """Save the run's leaderboard."""
        run_dir = self.runs_dir / run_name
        lb_path = run_dir / "leaderboard.json"
        lb_path.write_text(json.dumps(results, indent=2))
        return lb_path

    def save_run_summary(
        self,
        run_name: str,
        summary: Dict[str, Any],
    ) -> Path:
        """Save run summary statistics."""
        run_dir = self.runs_dir / run_name
        path = run_dir / "summary.json"
        path.write_text(json.dumps(summary, indent=2))
        return path
