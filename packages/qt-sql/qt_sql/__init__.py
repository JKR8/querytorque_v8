"""QueryTorque SQL — production SQL optimization engine.

Pipeline:
1. Parse:     SQL → logical tree (deterministic)
2. Retrieve:  Tag-based example matching (engine-specific)
3. Rewrite:   Full-query prompt with logical-tree topology (N parallel workers)
4. Validate:  Syntax check (deterministic)
5. Validate:  Timing + correctness (3-run or 5-run)

Usage:
    from qt_sql.pipeline import Pipeline
    p = Pipeline("qt_sql/benchmarks/duckdb_tpcds")
    result = p.run_optimization_session("query_88", sql)
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
)

__all__ = [
    "Pipeline",
    "BenchmarkConfig",
    "OptimizationMode",
    "OptimizationSession",
    "BeamSession",
    "PipelineResult",
    "PromotionAnalysis",
    "SessionResult",
    "ValidationStatus",
    "ValidationResult",
    "WorkerResult",
]
