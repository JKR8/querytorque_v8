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
from .sql_dag import (
    SQLDag,
    DagNode,
    DagEdge,
    build_dag_prompt,
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
        # DAG-based optimizer
        DagOptimizationPipeline,
        DagOptimizationResult,
        SQLDagOptimizer,
        optimize_with_dag,
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
    DagOptimizationPipeline = None
    DagOptimizationResult = None
    SQLDagOptimizer = None
    optimize_with_dag = None

# MCTS-based optimizer
try:
    from .mcts import (
        MCTSSQLOptimizer,
        MCTSOptimizationResult,
        MCTSNode,
        MCTSTree,
        TRANSFORMATION_LIBRARY,
        TransformationType,
        apply_transformation,
        compute_reward,
        RewardConfig,
    )
    MCTS_AVAILABLE = True
except ImportError:
    MCTS_AVAILABLE = False
    MCTSSQLOptimizer = None
    MCTSOptimizationResult = None
    MCTSNode = None
    MCTSTree = None
    TRANSFORMATION_LIBRARY = None
    TransformationType = None
    apply_transformation = None
    compute_reward = None
    RewardConfig = None

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
    # SQL DAG (proper graph structure)
    "SQLDag",
    "DagNode",
    "DagEdge",
    "build_dag_prompt",
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
    # DSPy optimizer (legacy - full SQL output)
    "DSPY_AVAILABLE",
    "optimize_query",
    "configure_lm",
    "SQLOptimizationPipeline",
    "SQLOptimizer",
    "OptimizationValidator",
    "speedup_metric",
    # DSPy DAG optimizer (node-level rewrites)
    "DagOptimizationPipeline",
    "DagOptimizationResult",
    "SQLDagOptimizer",
    "optimize_with_dag",
    # MCTS optimizer
    "MCTS_AVAILABLE",
    "MCTSSQLOptimizer",
    "MCTSOptimizationResult",
    "MCTSNode",
    "MCTSTree",
    "TRANSFORMATION_LIBRARY",
    "TransformationType",
    "apply_transformation",
    "compute_reward",
    "RewardConfig",
]
