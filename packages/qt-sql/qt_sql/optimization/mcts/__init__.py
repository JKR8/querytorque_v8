"""MCTS-based SQL optimizer.

This module implements a Monte Carlo Tree Search (MCTS) approach to SQL optimization.
Instead of random LLM rewrites, it systematically explores transformation sequences
using focused transformation strategies.

Key components:
- MCTSNode: Tree node representing a query state
- MCTSTree: Tree operations (select, expand, backpropagate)
- TransformationLibrary: Focused LLM prompts for each transformation type
- MCTSSQLOptimizer: Main optimizer orchestrating MCTS + LLM + validation

Example usage:
    from qt_sql.optimization.mcts import MCTSSQLOptimizer

    optimizer = MCTSSQLOptimizer(
        database="tpcds_sf100.duckdb",
        provider="deepseek",
    )

    result = optimizer.optimize(
        query=original_sql,
        max_iterations=30,
    )

    if result.valid:
        print(f"Speedup: {result.speedup:.2f}x")
        print(result.optimized_sql)
"""

from .node import MCTSNode
from .tree import MCTSTree
from .transforms import (
    TRANSFORMATION_LIBRARY,
    TransformationType,
    apply_transformation,
)
from .reward import compute_reward, RewardConfig
from .optimizer import MCTSSQLOptimizer, MCTSOptimizationResult

__all__ = [
    # Node
    "MCTSNode",
    # Tree
    "MCTSTree",
    # Transforms
    "TRANSFORMATION_LIBRARY",
    "TransformationType",
    "apply_transformation",
    # Reward
    "compute_reward",
    "RewardConfig",
    # Optimizer
    "MCTSSQLOptimizer",
    "MCTSOptimizationResult",
]
