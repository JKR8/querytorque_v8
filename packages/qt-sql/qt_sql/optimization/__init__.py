"""SQL optimization utilities."""

from .payload_builder import (
    build_optimization_payload_v2,
    PayloadV2Result,
    get_duckdb_engine_info,
    payload_v2_to_markdown,
)
from .plan_analyzer import (
    analyze_plan_for_optimization,
    build_optimization_prompt,
    apply_patches,
    parse_llm_response,
    OptimizationContext,
    DataFlow,
    TableScan,
    JoinInfo,
    CTEFlow,
    SQLPatch,
    PatchResult,
)
from .schemas import (
    OPTIMIZATION_PATCH_SCHEMA,
    OPTIMIZATION_SQL_SCHEMA,
)
from .block_map import (
    generate_block_map,
    format_block_map,
    build_full_prompt,
    BlockMapResult,
    Block,
    Clause,
)
from .iterative_optimizer import (
    test_optimization,
    format_test_feedback,
    run_optimization_loop,
    apply_operations,
    parse_response,
    TestResult,
)

# DSPy-based optimizer (optional, requires dspy-ai)
try:
    from .dspy_optimizer import (
        optimize_query,
        configure_lm,
        SQLOptimizationPipeline,
        SQLOptimizer,
        OptimizationValidator,
        speedup_metric,
    )
    DSPY_AVAILABLE = True
except ImportError:
    DSPY_AVAILABLE = False
    optimize_query = None
    configure_lm = None
    SQLOptimizationPipeline = None
    SQLOptimizer = None
    OptimizationValidator = None
    speedup_metric = None

__all__ = [
    # Legacy v2 payload builder
    "build_optimization_payload_v2",
    "PayloadV2Result",
    "get_duckdb_engine_info",
    "payload_v2_to_markdown",
    # Lightweight analyzer (patches)
    "analyze_plan_for_optimization",
    "build_optimization_prompt",
    "apply_patches",
    "parse_llm_response",
    "OptimizationContext",
    "DataFlow",
    "TableScan",
    "JoinInfo",
    "CTEFlow",
    "SQLPatch",
    "PatchResult",
    # Block Map (structured operations)
    "generate_block_map",
    "format_block_map",
    "build_full_prompt",
    "BlockMapResult",
    "Block",
    "Clause",
    # Iterative optimizer
    "test_optimization",
    "format_test_feedback",
    "run_optimization_loop",
    "apply_operations",
    "parse_response",
    "TestResult",
    # Schemas for structured output
    "OPTIMIZATION_PATCH_SCHEMA",
    "OPTIMIZATION_SQL_SCHEMA",
    # DSPy optimizer
    "DSPY_AVAILABLE",
    "optimize_query",
    "configure_lm",
    "SQLOptimizationPipeline",
    "SQLOptimizer",
    "OptimizationValidator",
    "speedup_metric",
]
