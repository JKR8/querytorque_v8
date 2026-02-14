"""QueryTorque SQL — production SQL optimization engine.

Pipeline:
1. Parse:     SQL → logical tree (deterministic)
2. Retrieve:  Tag-based example matching (engine-specific)
3. Rewrite:   Full-query prompt with logical-tree topology (N parallel workers)
4. Validate:  Syntax check (deterministic)
5. Validate:  Timing + correctness (3-run or 5-run)

Optimization Modes:
- ONESHOT: 1 LLM call per iteration, analyst produces SQL directly
- SWARM:   Multi-worker fan-out with snipe refinement (default)

Usage:
    from qt_sql.pipeline import Pipeline
    from qt_sql.schemas import OptimizationMode
    p = Pipeline("qt_sql/benchmarks/duckdb_tpcds")

    # Oneshot mode (cheapest, 1 API call per iteration):
    result = p.run_optimization_session("query_1", sql, mode=OptimizationMode.ONESHOT)

    # Swarm mode (default, 4-worker fan-out + snipe):
    result = p.run_optimization_session("query_88", sql, mode=OptimizationMode.SWARM)
"""

from .pipeline import Pipeline
from .schemas import (
    BenchmarkConfig,
    OptimizationMode,
    PipelineResult,
    PromotionAnalysis,
    SessionResult,
    ValidationStatus,
    ValidationResult,
    WorkerResult,
)
from .sessions import (
    OptimizationSession,
    OneshotSession,
    SwarmSession,
)

__all__ = [
    "Pipeline",
    "BenchmarkConfig",
    "OptimizationMode",
    "OptimizationSession",
    "OneshotSession",
    "SwarmSession",
    "PipelineResult",
    "PromotionAnalysis",
    "SessionResult",
    "ValidationStatus",
    "ValidationResult",
    "WorkerResult",
]
