"""SQL optimization utilities.

Main entry point for LLM-powered optimization (Dag v2 + JSON v5):
    from qt_sql.optimization import optimize_v5_json
    result = optimize_v5_json(sql, db_path)

MCTS module removed; JSON v5 is the only optimization path.
"""

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
# sql_dag.py deprecated - use dag_v2.py instead
from .dag_v2 import (
    DagV2Pipeline,
    DagNode,
)
from .iterative_optimizer import (
    test_optimization,
    format_test_feedback,
    run_optimization_loop,
    apply_operations,
    parse_response,
    TestResult,
)

from .adaptive_rewriter_v5 import (
    optimize_v5_json,
    optimize_v5_json_queue,
)

__all__ = [
    # ============================================================
    # PRIMARY: Dag v2 + JSON v5 (recommended for LLM optimization)
    # ============================================================
    "optimize_v5_json",
    "optimize_v5_json_queue",
    # ============================================================
    # UTILITIES
    # ============================================================
    # Payload builder
    "build_optimization_payload_v2",
    "PayloadV2Result",
    "get_duckdb_engine_info",
    "payload_v2_to_markdown",
    # Plan analyzer
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
    # Block Map
    "generate_block_map",
    "format_block_map",
    "build_full_prompt",
    "BlockMapResult",
    "Block",
    "Clause",
    # DAG V2 (replaces sql_dag)
    "DagV2Pipeline",
    "DagNode",
    # Iterative optimizer
    "test_optimization",
    "format_test_feedback",
    "run_optimization_loop",
    "apply_operations",
    "parse_response",
    "TestResult",
    # Schemas
    "OPTIMIZATION_PATCH_SCHEMA",
    "OPTIMIZATION_SQL_SCHEMA",
]
