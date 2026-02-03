"""MCTS-based SQL optimizer.

This module implements a Monte Carlo Tree Search (MCTS) approach to SQL optimization.
It supports two transform selection strategies for A/B comparison:

## Selection Strategies

### 1. ORIGINAL: Random Selection (RANDOM_CONFIG)
    - Picks transforms randomly from untried candidates
    - No prioritization based on query patterns
    - Baseline for comparison

### 2. NEW: PUCT Selection (PUCT_CONFIG) - Default
    - Uses knowledge base weights to prioritize high-value transforms
    - Detects optimization opportunities (OR conditions, correlated subqueries, etc.)
    - Applies PUCT formula: Q + c*P*sqrt(N)/(1+n)
    - Progressive widening limits early exploration

## Quick Start - Comparison

    from qt_sql.optimization.mcts import (
        MCTSSQLOptimizer,
        RANDOM_CONFIG,   # Original random selection
        PUCT_CONFIG,     # New PUCT selection (default)
    )

    # ORIGINAL: Random selection (for baseline comparison)
    optimizer_random = MCTSSQLOptimizer(
        database="tpcds.duckdb",
        provider="deepseek",
        prior_config=RANDOM_CONFIG,
    )

    # NEW: PUCT selection (default behavior)
    optimizer_puct = MCTSSQLOptimizer(
        database="tpcds.duckdb",
        provider="deepseek",
        prior_config=PUCT_CONFIG,
    )

    # Or just use defaults (PUCT is default)
    optimizer_default = MCTSSQLOptimizer(
        database="tpcds.duckdb",
        provider="deepseek",
    )

    # Run and compare
    result_random = optimizer_random.optimize(query, max_iterations=30)
    result_puct = optimizer_puct.optimize(query, max_iterations=30)

    print(f"Random: {result_random.speedup:.2f}x in {result_random.iterations} iters")
    print(f"PUCT:   {result_puct.speedup:.2f}x in {result_puct.iterations} iters")

## Pre-defined Configs

    RANDOM_CONFIG    - Original random selection (use_puct=False)
    PUCT_CONFIG      - PUCT with KB weights + opportunity detection (default)
    PUCT_LLM_CONFIG  - PUCT with LLM ranking enabled

## Architecture

See PUCT_ARCHITECTURE.md for detailed design documentation.
"""

from .node import MCTSNode
from .tree import MCTSTree
from .transforms import (
    TRANSFORMATION_LIBRARY,
    TransformationType,
    apply_transformation,
)
from .reward import compute_reward, RewardConfig
from .priors import (
    PriorConfig,
    TransformPrior,
    compute_contextual_priors,
    PUCT_CONFIG,
    RANDOM_CONFIG,
    PUCT_LLM_CONFIG,
)
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
    # Priors (PUCT)
    "PriorConfig",
    "TransformPrior",
    "compute_contextual_priors",
    "PUCT_CONFIG",
    "RANDOM_CONFIG",
    "PUCT_LLM_CONFIG",
    # Optimizer
    "MCTSSQLOptimizer",
    "MCTSOptimizationResult",
]
