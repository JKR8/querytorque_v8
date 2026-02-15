"""QueryTorque SQL — production SQL optimization engine.

Pipeline:
1. Parse:     SQL → logical tree (deterministic)
2. Retrieve:  Tag-based example matching (engine-specific)
3. Rewrite:   Full-query prompt with logical-tree topology (N parallel workers)
4. Validate:  Syntax check (deterministic)
5. Validate:  Timing + correctness (3-run or 5-run)

Optimization Modes:
- BEAM:  Automated search: analyst → N workers → validate → snipe (default)
- SWARM: Multi-worker fan-out with coach refinement (legacy)

Usage:
    from qt_sql.pipeline import Pipeline
    from qt_sql.schemas import OptimizationMode
    p = Pipeline("qt_sql/benchmarks/duckdb_tpcds")

    # Beam mode (default, analyst → N workers → validate → snipe):
    result = p.run_optimization_session("query_88", sql, mode=OptimizationMode.BEAM)
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
    BeamSession,
    OneshotSession,
    SwarmSession,
)

__all__ = [
    "Pipeline",
    "BenchmarkConfig",
    "OptimizationMode",
    "OptimizationSession",
    "BeamSession",
    "OneshotSession",
    "SwarmSession",
    "PipelineResult",
    "PromotionAnalysis",
    "SessionResult",
    "ValidationStatus",
    "ValidationResult",
    "WorkerResult",
]
