"""ADO (Autonomous Data Optimization) — production SQL optimization engine.

Pipeline:
1. Parse:     SQL → DAG (deterministic)
2. Retrieve:  FAISS example matching (engine-specific)
3. Rewrite:   Full-query prompt with DAG topology (N parallel workers)
4. Validate:  Syntax check (deterministic)
5. Validate:  Timing + correctness (3-run or 5-run)

Usage:
    from ado.pipeline import Pipeline
    p = Pipeline("ado/benchmarks/duckdb_tpcds")
    result = p.run_query("query_1", sql)

    # Or via ADORunner wrapper:
    from ado import ADORunner, ADOConfig
    config = ADOConfig(benchmark_dir="ado/benchmarks/duckdb_tpcds")
    runner = ADORunner(config)
    result = runner.run_query("query_1", sql)

    # Deep-dive analyst mode (iterative single-query optimization):
    from ado import AnalystSession
    session = p.run_analyst_session("query_88", sql, max_iterations=5)
"""

from .pipeline import Pipeline
from .runner import ADORunner, ADOConfig, ADOResult
from .analyst_session import AnalystSession, AnalystIteration
from .schemas import (
    BenchmarkConfig,
    PipelineResult,
    PromotionAnalysis,
    ValidationStatus,
    ValidationResult,
)

__all__ = [
    "Pipeline",
    "ADORunner",
    "ADOConfig",
    "ADOResult",
    "AnalystSession",
    "AnalystIteration",
    "BenchmarkConfig",
    "PipelineResult",
    "PromotionAnalysis",
    "ValidationStatus",
    "ValidationResult",
]
