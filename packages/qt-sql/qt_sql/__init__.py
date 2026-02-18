"""QueryTorque SQL — production SQL optimization engine.

Pipeline:
1. Parse:     SQL → logical tree (deterministic)
2. Retrieve:  Tag-based example matching (engine-specific)
3. Rewrite:   Full-query prompt with logical-tree topology (N parallel workers)
4. Validate:  Syntax check (deterministic)
5. Validate:  Timing + correctness (3-run or 5-run)

Usage:
    from qt_sql.pipeline import Pipeline
    from qt_sql.sessions.wave_runner import WaveRunner
    p = Pipeline("qt_sql/benchmarks/duckdb_tpcds")
    runner = WaveRunner(pipeline=p, bench_dir=p.benchmark_dir)
"""

from .pipeline import Pipeline
from .schemas import (
    BenchmarkConfig,
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
    "OptimizationSession",
    "BeamSession",
    "PipelineResult",
    "PromotionAnalysis",
    "SessionResult",
    "ValidationStatus",
    "ValidationResult",
    "WorkerResult",
]
