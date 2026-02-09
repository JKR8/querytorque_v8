"""QueryTorque SQL — production SQL optimization engine.

Pipeline:
1. Parse:     SQL → DAG (deterministic)
2. Retrieve:  Tag-based example matching (engine-specific)
3. Rewrite:   Full-query prompt with DAG topology (N parallel workers)
4. Validate:  Syntax check (deterministic)
5. Validate:  Timing + correctness (3-run or 5-run)

Optimization Modes:
- STANDARD: Fast, no analyst, single iteration
- EXPERT:   Iterative with analyst failure analysis (default)
- SWARM:    Multi-worker fan-out with snipe refinement

Usage:
    from qt_sql.pipeline import Pipeline
    from qt_sql.schemas import OptimizationMode
    p = Pipeline("qt_sql/benchmarks/duckdb_tpcds")

    # Standard mode (fast, no analyst):
    result = p.run_optimization_session("query_1", sql, mode=OptimizationMode.STANDARD)

    # Expert mode (default, iterative with failure analysis):
    result = p.run_optimization_session("query_1", sql, mode=OptimizationMode.EXPERT)

    # Swarm mode (4-worker fan-out + snipe):
    result = p.run_optimization_session("query_88", sql, mode=OptimizationMode.SWARM)

    # Or via ADORunner wrapper:
    from qt_sql import ADORunner, ADOConfig, OptimizationMode
    runner = ADORunner(ADOConfig(benchmark_dir="qt_sql/benchmarks/duckdb_tpcds"))
    result = runner.run_analyst("query_88", sql, mode=OptimizationMode.SWARM)
"""

from .pipeline import Pipeline
from .runner import ADORunner, ADOConfig, ADOResult
from .analyst_session import AnalystSession, AnalystIteration
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
    StandardSession,
    ExpertSession,
    SwarmSession,
)

__all__ = [
    "Pipeline",
    "ADORunner",
    "ADOConfig",
    "ADOResult",
    "AnalystSession",
    "AnalystIteration",
    "BenchmarkConfig",
    "OptimizationMode",
    "OptimizationSession",
    "StandardSession",
    "ExpertSession",
    "SwarmSession",
    "PipelineResult",
    "PromotionAnalysis",
    "SessionResult",
    "ValidationStatus",
    "ValidationResult",
    "WorkerResult",
]
