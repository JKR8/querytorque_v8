"""Base class for all optimization sessions."""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Any, List, Optional, TYPE_CHECKING

from ..schemas import SessionResult

if TYPE_CHECKING:
    from ..pipeline import Pipeline
    from ..orchestrator import Orchestrator

logger = logging.getLogger(__name__)


class OptimizationSession:
    """Base class for Oneshot and Swarm optimization sessions.

    Subclasses implement run() with mode-specific logic.
    All sessions share the same Pipeline for logical-tree parsing, example retrieval,
    candidate generation, and validation.
    """

    def __init__(
        self,
        pipeline: "Pipeline",
        query_id: str,
        original_sql: str,
        target_speedup: float = 2.0,
        max_iterations: int = 3,
        n_workers: int = 3,
        orchestrator: Optional["Orchestrator"] = None,
    ):
        self.pipeline = pipeline
        self.query_id = query_id
        self.original_sql = original_sql
        self.target_speedup = target_speedup
        self.max_iterations = max_iterations
        self.n_workers = n_workers
        self.orchestrator = orchestrator

        # Derived config
        self.dialect = (
            self.pipeline.config.engine
            if self.pipeline.config.engine != "postgresql"
            else "postgres"
        )
        self.engine = (
            "postgres"
            if self.pipeline.config.engine in ("postgresql", "postgres")
            else self.pipeline.config.engine
        )

    def run(self) -> SessionResult:
        """Execute the optimization session. Override in subclasses."""
        raise NotImplementedError

    def _classify_speedup(self, speedup: float) -> str:
        """Classify speedup into status category."""
        if speedup >= 1.10:
            return "WIN"
        elif speedup >= 1.05:
            return "IMPROVED"
        elif speedup >= 0.95:
            return "NEUTRAL"
        else:
            return "REGRESSION"
