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
from .schemas import ValidationStatus, PipelineResult

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
    1. Parse → 2. Annotate → 3. Rewrite → 4. Reassemble → 5. Validate
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
