"""Expert optimization session — iterative with analyst failure analysis.

Wraps the existing AnalystSession behavior:
1. Always optimize from ORIGINAL SQL (no compounding)
2. Run analyst for structural guidance each iteration
3. Generate N candidates and validate
4. If speedup < target → LLM failure analysis
5. History includes all previous failure analyses
6. Stop when target reached or max iterations exhausted

This is the DEFAULT mode and matches prior behavior exactly.
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

from .base_session import OptimizationSession
from ..schemas import SessionResult

if TYPE_CHECKING:
    from ..pipeline import Pipeline

logger = logging.getLogger(__name__)


class ExpertSession(OptimizationSession):
    """Current analyst mode: iterative with failure analysis.

    Delegates to the existing AnalystSession for full backwards compatibility.
    """

    def run(self) -> SessionResult:
        """Run iterative analyst optimization."""
        from ..analyst_session import AnalystSession

        logger.info(
            f"[{self.query_id}] ExpertSession: "
            f"max {self.max_iterations} iterations, "
            f"target {self.target_speedup:.1f}x, "
            f"{self.n_workers} workers"
        )

        session = AnalystSession(
            pipeline=self.pipeline,
            query_id=self.query_id,
            original_sql=self.original_sql,
            max_iterations=self.max_iterations,
            target_speedup=self.target_speedup,
            n_workers=self.n_workers,
        )
        best_iteration = session.run()
        session.save_session()

        # Convert AnalystSession result to SessionResult
        best = session._best_iteration()
        if best is None:
            return SessionResult(
                query_id=self.query_id,
                mode="expert",
                best_speedup=0.0,
                best_sql=self.original_sql,
                original_sql=self.original_sql,
                best_transforms=[],
                status="ERROR",
                iterations=[
                    {
                        "iteration": it.iteration,
                        "status": it.status,
                        "speedup": it.speedup,
                        "transforms": it.transforms,
                        "examples_used": it.examples_used,
                        "failure_analysis": it.failure_analysis,
                    }
                    for it in session.iterations
                ],
                n_iterations=len(session.iterations),
                n_api_calls=self._count_api_calls(session),
            )

        return SessionResult(
            query_id=self.query_id,
            mode="expert",
            best_speedup=best.speedup,
            best_sql=best.optimized_sql,
            original_sql=self.original_sql,
            best_transforms=best.transforms,
            status=best.status,
            iterations=[
                {
                    "iteration": it.iteration,
                    "status": it.status,
                    "speedup": it.speedup,
                    "transforms": it.transforms,
                    "examples_used": it.examples_used,
                    "failure_analysis": it.failure_analysis,
                }
                for it in session.iterations
            ],
            n_iterations=len(session.iterations),
            n_api_calls=self._count_api_calls(session),
        )

    @staticmethod
    def _count_api_calls(session) -> int:
        """Count total API calls made during the session.

        Per iteration: 1 analyst + n_workers generation + (0 or 1 failure analysis)
        """
        total = 0
        for it in session.iterations:
            total += 1  # analyst call
            total += session.n_workers  # generation calls
            if it.failure_analysis:
                total += 1  # failure analysis call
        return total
