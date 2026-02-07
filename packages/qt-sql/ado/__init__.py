"""ADO (Autonomous Data Optimization) — production SQL optimization engine.

5-phase DAG pipeline:
1. Parse:     SQL → DAG (deterministic)
2. Annotate:  DAG → {node: pattern} (1 LLM call)
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
"""

from .pipeline import Pipeline
from .runner import ADORunner, ADOConfig, ADOResult
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
    "BenchmarkConfig",
    "PipelineResult",
    "PromotionAnalysis",
    "ValidationStatus",
    "ValidationResult",
]
