"""ADO runner — delegates to the 5-phase DAG Pipeline.

Usage:
    from ado.runner import ADORunner, ADOConfig

    config = ADOConfig(
        benchmark_dir="ado/benchmarks/duckdb_tpcds",
        provider="anthropic",
        model="claude-sonnet-4-5-20250929",
    )
    runner = ADORunner(config)
    result = runner.run_query("query_1", sql)
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional

from .pipeline import Pipeline
from .schemas import OptimizationMode, ValidationStatus, PipelineResult, SessionResult

logger = logging.getLogger(__name__)


@dataclass
class ADOConfig:
    """Configuration for ADO runner."""
    benchmark_dir: str                          # Required: path to benchmark directory
    provider: Optional[str] = None
    model: Optional[str] = None
    analyze_fn: Optional[Callable[[str], str]] = None
    candidates_per_round: int = 5


@dataclass
class ADOResult:
    """Complete result from ADO optimization."""
    query_id: str
    status: str
    speedup: float
    optimized_sql: str
    original_sql: str
    transforms: List[str]
    nodes_rewritten: List[str] = field(default_factory=list)
    attempts: int = 0
    all_validations: List[Dict[str, Any]] = field(default_factory=list)


class ADORunner:
    """Run the ADO optimization pipeline.

    Thin wrapper around Pipeline that provides a simplified interface.
    All optimization flows through the 5-phase DAG pipeline:
    1. Parse → 2. FAISS → 3. Rewrite → 4. Syntax Gate → 5. Validate
    """

    def __init__(self, config: ADOConfig):
        self.config = config
        self.pipeline = Pipeline(
            benchmark_dir=config.benchmark_dir,
            provider=config.provider,
            model=config.model,
            analyze_fn=config.analyze_fn,
        )

    def run_query(
        self,
        query_id: str,
        sql: str,
        n_workers: int = 0,
    ) -> ADOResult:
        """Run optimization on a single query.

        Args:
            query_id: Query identifier (e.g., 'query_1')
            sql: The SQL query to optimize
            n_workers: Number of parallel workers (0 = use config default)
        """
        workers = n_workers or self.config.candidates_per_round
        result = self.pipeline.run_query(
            query_id=query_id,
            sql=sql,
            n_workers=workers,
        )
        return self._to_ado_result(result)

    def run_batch(
        self,
        state_num: int,
        n_workers: int = 0,
        query_ids: Optional[List[str]] = None,
    ) -> List[ADOResult]:
        """Run a full state batch.

        Args:
            state_num: State number (0 = discovery, 1+ = refinement)
            n_workers: Worker count override (0 = use config default)
            query_ids: Optional subset of queries to run
        """
        results = self.pipeline.run_state(
            state_num=state_num,
            n_workers=n_workers or None,
            query_ids=query_ids,
        )
        return [self._to_ado_result(r) for r in results]

    def run_queries(
        self,
        queries: Dict[str, str],
        progress_callback: Optional[Callable[[str, ADOResult], None]] = None,
    ) -> List[ADOResult]:
        """Run optimization on multiple queries.

        Args:
            queries: Dict of {query_id: sql}
            progress_callback: Optional callback(query_id, result) after each query
        """
        results = []
        for query_id, sql in queries.items():
            logger.info(f"Processing {query_id}")
            result = self.run_query(query_id, sql)
            results.append(result)
            if progress_callback:
                progress_callback(query_id, result)
        return results

    def run_analyst(
        self,
        query_id: str,
        sql: str,
        max_iterations: int = 3,
        target_speedup: float = 2.0,
        n_workers: int = 3,
        mode: OptimizationMode = OptimizationMode.EXPERT,
    ) -> ADOResult:
        """Run optimization on a single query in the specified mode.

        Modes:
        - STANDARD: Fast, no analyst, single iteration
        - EXPERT: Iterative with analyst failure analysis (default, legacy behavior)
        - SWARM: Multi-worker fan-out with snipe refinement

        Args:
            query_id: Query identifier (e.g., 'query_88')
            sql: Original SQL query
            max_iterations: Max optimization rounds
            target_speedup: Stop early when this speedup is reached
            n_workers: Parallel workers per iteration
            mode: Optimization mode (standard, expert, swarm)

        Returns:
            ADOResult with the best result
        """
        session_result = self.pipeline.run_optimization_session(
            query_id=query_id,
            sql=sql,
            max_iterations=max_iterations,
            target_speedup=target_speedup,
            n_workers=n_workers,
            mode=mode,
        )
        return self._session_result_to_ado_result(session_result)

    @staticmethod
    def _session_result_to_ado_result(sr: SessionResult) -> ADOResult:
        """Convert SessionResult to ADOResult."""
        return ADOResult(
            query_id=sr.query_id,
            status=sr.status,
            speedup=sr.best_speedup,
            optimized_sql=sr.best_sql,
            original_sql=sr.original_sql,
            transforms=sr.best_transforms,
            attempts=sr.n_iterations,
            all_validations=[
                {
                    "iteration": it.get("iteration", i),
                    "status": it.get("status", ""),
                    "speedup": it.get("speedup", 0.0) if isinstance(it.get("speedup"), (int, float)) else it.get("best_speedup", 0.0),
                    "transforms": it.get("transforms", it.get("best_transforms", [])),
                }
                for i, it in enumerate(sr.iterations)
                if isinstance(it, dict)
            ],
        )

    def promote(self, state_num: int) -> str:
        """Promote winners from state_N → state_N+1."""
        return self.pipeline.promote(state_num)

    def close(self) -> None:
        """Close resources (no-op, Pipeline has no persistent connections)."""
        pass

    @staticmethod
    def _to_ado_result(result: PipelineResult) -> ADOResult:
        return ADOResult(
            query_id=result.query_id,
            status=result.status,
            speedup=result.speedup,
            optimized_sql=result.optimized_sql,
            original_sql=result.original_sql,
            transforms=result.transforms_applied,
            nodes_rewritten=result.nodes_rewritten,
            attempts=1,
        )
