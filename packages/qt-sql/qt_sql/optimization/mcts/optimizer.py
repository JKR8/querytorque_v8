"""MCTS SQL Optimizer - Main optimizer class.

Orchestrates MCTS tree search with LLM transformations and SQL validation.

Usage:
    optimizer = MCTSSQLOptimizer(
        database="tpcds.duckdb",
        provider="deepseek",
    )

    result = optimizer.optimize(
        query=original_sql,
        max_iterations=30,
    )

    if result.valid:
        print(f"Speedup: {result.speedup:.2f}x")
"""

from __future__ import annotations

import logging
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Optional

from .node import MCTSNode
from .tree import MCTSTree
from .transforms import get_all_transform_ids, get_transform_description
from .reward import RewardConfig

logger = logging.getLogger(__name__)


@dataclass
class MCTSOptimizationResult:
    """Result from MCTS optimization.

    Attributes:
        original_sql: The original query.
        optimized_sql: The best optimized query found.
        valid: Whether the optimization passed validation.
        speedup: Performance speedup (original_time / optimized_time).
        method: Description of optimization method used.
        transforms_applied: List of transformations applied.
        iterations: Number of MCTS iterations run.
        tree_stats: Statistics about the MCTS tree.
        validation_result: Raw validation result for best node.
        elapsed_time: Total optimization time in seconds.
        detailed_log: Full log of all attempts and selections (optional).
        attempt_summary: Summary by transform type (optional).
    """

    original_sql: str
    optimized_sql: str
    valid: bool
    speedup: float
    method: str
    transforms_applied: list[str] = field(default_factory=list)
    iterations: int = 0
    tree_stats: dict = field(default_factory=dict)
    validation_result: Optional[Any] = None
    elapsed_time: float = 0.0
    detailed_log: Optional[dict] = None
    attempt_summary: Optional[dict] = None

    def to_dict(self) -> dict:
        """Convert to dictionary for serialization."""
        result = {
            "valid": self.valid,
            "speedup": round(self.speedup, 2),
            "method": self.method,
            "transforms_applied": self.transforms_applied,
            "iterations": self.iterations,
            "tree_stats": self.tree_stats,
            "elapsed_time": round(self.elapsed_time, 2),
        }
        if self.attempt_summary:
            result["attempt_summary"] = self.attempt_summary
        return result


class MCTSSQLOptimizer:
    """MCTS-based SQL optimizer with LLM transformations.

    Combines Monte Carlo Tree Search with focused LLM transformations
    and SQL validation to systematically explore optimization space.

    Features:
    - Focused transformations (one change at a time)
    - UCT-based exploration/exploitation balance
    - Validation-driven reward
    - Composable transformation sequences

    Attributes:
        database: Path to DuckDB database for validation.
        llm_client: LLM client for transformations.
        validator: SQL validator instance.
        reward_config: Reward function configuration.
        c: UCT exploration constant.
        max_depth: Maximum transformation chain depth.
    """

    def __init__(
        self,
        database: str,
        provider: Optional[str] = None,
        model: Optional[str] = None,
        c: float = 1.414,
        max_depth: int = 5,
        reward_config: Optional[RewardConfig] = None,
        transform_ids: Optional[list[str]] = None,
    ):
        """Initialize MCTS SQL optimizer.

        Args:
            database: Path to DuckDB database.
            provider: LLM provider name (deepseek, anthropic, etc.).
            model: LLM model name.
            c: UCT exploration constant.
            max_depth: Maximum depth of transformation chains.
            reward_config: Reward function configuration.
            transform_ids: Specific transforms to use (None = all).
        """
        self.database = database
        self.c = c
        self.max_depth = max_depth
        self.reward_config = reward_config or RewardConfig()
        self.transform_ids = transform_ids or get_all_transform_ids()

        # Lazy initialization
        self._llm_client = None
        self._validator = None
        self._provider = provider
        self._model = model

    def _get_llm_client(self):
        """Get or create LLM client."""
        if self._llm_client is None:
            from qt_shared.llm import create_llm_client

            self._llm_client = create_llm_client(
                provider=self._provider,
                model=self._model,
            )

            if self._llm_client is None:
                raise ValueError(
                    "No LLM provider configured. "
                    "Set QT_LLM_PROVIDER and API key environment variables."
                )

        return self._llm_client

    def _get_validator(self):
        """Get or create SQL validator."""
        if self._validator is None:
            from qt_sql.validation import SQLValidator, ValidationMode

            self._validator = SQLValidator(
                database=self.database,
                mode=ValidationMode.SAMPLE,
            )

        return self._validator

    def optimize(
        self,
        query: str,
        max_iterations: int = 30,
        early_stop_speedup: float = 3.0,
        convergence_patience: int = 10,
    ) -> MCTSOptimizationResult:
        """Optimize a SQL query using MCTS.

        Args:
            query: SQL query to optimize.
            max_iterations: Maximum number of MCTS iterations.
            early_stop_speedup: Stop early if this speedup achieved.
            convergence_patience: Stop if best unchanged for this many iterations.

        Returns:
            MCTSOptimizationResult with best optimization found.
        """
        start_time = time.time()

        logger.info(f"Starting MCTS optimization (max_iterations={max_iterations})")

        # Initialize tree
        tree = MCTSTree(
            original_sql=query,
            llm_client=self._get_llm_client(),
            validator=self._get_validator(),
            reward_config=self.reward_config,
            c=self.c,
            max_depth=self.max_depth,
            transform_ids=self.transform_ids,
        )

        # Track best result for convergence detection
        best_speedup = 1.0
        best_node = tree.root
        iterations_without_improvement = 0

        # Main MCTS loop
        for i in range(max_iterations):
            expanded, reward = tree.iterate()

            if expanded is not None and expanded.is_valid:
                speedup = expanded.speedup

                if speedup > best_speedup:
                    best_speedup = speedup
                    best_node = expanded
                    iterations_without_improvement = 0

                    logger.info(
                        f"Iteration {i+1}: New best speedup {speedup:.2f}x "
                        f"(transforms: {expanded.applied_transforms})"
                    )

                    # Early termination on great result
                    if speedup >= early_stop_speedup:
                        logger.info(f"Early stop: achieved {speedup:.2f}x speedup")
                        break
                else:
                    iterations_without_improvement += 1
            else:
                iterations_without_improvement += 1

            # Convergence check
            if iterations_without_improvement >= convergence_patience:
                logger.info(
                    f"Convergence: no improvement for {convergence_patience} iterations"
                )
                break

            # Log progress periodically
            if (i + 1) % 5 == 0:
                logger.debug(
                    f"Progress: {i+1}/{max_iterations} iterations, "
                    f"best speedup={best_speedup:.2f}x, "
                    f"tree size={tree.get_tree_size()}"
                )

        # Get final best node
        final_best = tree.get_best_speedup_node()
        if final_best.speedup > best_speedup:
            best_node = final_best

        elapsed_time = time.time() - start_time

        # Build result
        if best_node == tree.root:
            # No improvement found
            method = "original"
            valid = True
            speedup = 1.0
            optimized_sql = query
            transforms_applied = []
        else:
            method = f"mcts:{','.join(best_node.applied_transforms)}"
            valid = best_node.is_valid
            speedup = best_node.speedup
            optimized_sql = best_node.query_sql
            transforms_applied = best_node.applied_transforms

        result = MCTSOptimizationResult(
            original_sql=query,
            optimized_sql=optimized_sql,
            valid=valid,
            speedup=speedup,
            method=method,
            transforms_applied=transforms_applied,
            iterations=tree.total_iterations,
            tree_stats=tree.get_stats(),
            validation_result=best_node.validation_result,
            elapsed_time=elapsed_time,
            detailed_log=tree.get_detailed_log(),
            attempt_summary=tree.get_attempt_summary(),
        )

        logger.info(
            f"MCTS complete: {tree.total_iterations} iterations, "
            f"speedup={speedup:.2f}x, elapsed={elapsed_time:.1f}s"
        )

        return result

    def optimize_parallel(
        self,
        query: str,
        max_iterations: int = 30,
        num_parallel: int = 4,
        early_stop_speedup: float = 3.0,
        convergence_patience: int = 10,
    ) -> MCTSOptimizationResult:
        """Optimize a SQL query using MCTS with parallel LLM calls.

        Makes parallel LLM API calls for multiple transforms, then validates
        each result sequentially (to avoid DB contention affecting timing).

        Args:
            query: SQL query to optimize.
            max_iterations: Maximum number of MCTS iterations.
            num_parallel: Number of parallel LLM calls per iteration.
            early_stop_speedup: Stop early if this speedup achieved.
            convergence_patience: Stop if best unchanged for this many iterations.

        Returns:
            MCTSOptimizationResult with best optimization found.
        """
        start_time = time.time()

        logger.info(
            f"Starting parallel MCTS optimization "
            f"(max_iterations={max_iterations}, parallel={num_parallel})"
        )

        # Initialize tree
        tree = MCTSTree(
            original_sql=query,
            llm_client=self._get_llm_client(),
            validator=self._get_validator(),
            reward_config=self.reward_config,
            c=self.c,
            max_depth=self.max_depth,
            transform_ids=self.transform_ids,
        )

        # Track best result for convergence detection
        best_speedup = 1.0
        best_node = tree.root
        iterations_without_improvement = 0
        total_nodes_expanded = 0

        # Main MCTS loop with parallel expansion
        iteration = 0
        while iteration < max_iterations:
            # Parallel iteration: expands multiple nodes, validates sequentially
            results = tree.iterate_parallel(num_parallel=num_parallel)

            if not results:
                iterations_without_improvement += 1
                iteration += 1
                continue

            # Process results
            for expanded, reward in results:
                total_nodes_expanded += 1

                if expanded is not None and expanded.is_valid:
                    speedup = expanded.speedup

                    if speedup > best_speedup:
                        best_speedup = speedup
                        best_node = expanded
                        iterations_without_improvement = 0

                        logger.info(
                            f"Iteration {iteration+1}: New best speedup {speedup:.2f}x "
                            f"(transforms: {expanded.applied_transforms})"
                        )

                        # Early termination on great result
                        if speedup >= early_stop_speedup:
                            logger.info(f"Early stop: achieved {speedup:.2f}x speedup")
                            break
                    else:
                        iterations_without_improvement += 1
                else:
                    iterations_without_improvement += 1

            # Check for early stop
            if best_speedup >= early_stop_speedup:
                break

            iteration += 1

            # Convergence check (based on nodes, not just iterations)
            if iterations_without_improvement >= convergence_patience:
                logger.info(
                    f"Convergence: no improvement for {convergence_patience} nodes"
                )
                break

            # Log progress periodically
            if iteration % 3 == 0:
                logger.debug(
                    f"Progress: {iteration}/{max_iterations} iterations, "
                    f"{total_nodes_expanded} nodes, "
                    f"best speedup={best_speedup:.2f}x"
                )

        # Get final best node
        final_best = tree.get_best_speedup_node()
        if final_best.speedup > best_speedup:
            best_node = final_best

        elapsed_time = time.time() - start_time

        # Build result
        if best_node == tree.root:
            method = "original"
            valid = True
            speedup = 1.0
            optimized_sql = query
            transforms_applied = []
        else:
            method = f"mcts_parallel:{','.join(best_node.applied_transforms)}"
            valid = best_node.is_valid
            speedup = best_node.speedup
            optimized_sql = best_node.query_sql
            transforms_applied = best_node.applied_transforms

        result = MCTSOptimizationResult(
            original_sql=query,
            optimized_sql=optimized_sql,
            valid=valid,
            speedup=speedup,
            method=method,
            transforms_applied=transforms_applied,
            iterations=tree.total_iterations,
            tree_stats=tree.get_stats(),
            validation_result=best_node.validation_result,
            elapsed_time=elapsed_time,
            detailed_log=tree.get_detailed_log(),
            attempt_summary=tree.get_attempt_summary(),
        )

        logger.info(
            f"Parallel MCTS complete: {iteration} iterations, "
            f"{total_nodes_expanded} nodes, "
            f"speedup={speedup:.2f}x, elapsed={elapsed_time:.1f}s"
        )

        return result

    def optimize_with_quick_fallback(
        self,
        query: str,
        max_iterations: int = 30,
        quick_speedup_threshold: float = 1.1,
    ) -> MCTSOptimizationResult:
        """Optimize with quick 1-shot attempt first, then MCTS if needed.

        Tries a single direct optimization first. If that achieves
        acceptable speedup, returns immediately. Otherwise escalates
        to full MCTS search.

        Args:
            query: SQL query to optimize.
            max_iterations: Maximum MCTS iterations if needed.
            quick_speedup_threshold: Speedup threshold for quick pass.

        Returns:
            MCTSOptimizationResult.
        """
        # Try quick optimization first
        quick_result = self._quick_optimize(query)

        if quick_result is not None and quick_result.valid:
            if quick_result.speedup >= quick_speedup_threshold:
                logger.info(
                    f"Quick optimization succeeded: {quick_result.speedup:.2f}x"
                )
                return quick_result

        # Escalate to MCTS
        logger.info("Quick optimization insufficient, escalating to MCTS")
        return self.optimize(query, max_iterations=max_iterations)

    def _quick_optimize(self, query: str) -> Optional[MCTSOptimizationResult]:
        """Try a single direct optimization without full MCTS.

        Applies each transform once in order, keeping first valid improvement.

        Args:
            query: SQL query to optimize.

        Returns:
            MCTSOptimizationResult if improvement found, None otherwise.
        """
        from .transforms import apply_transformation

        start_time = time.time()
        validator = self._get_validator()
        llm_client = self._get_llm_client()

        for transform_id in self.transform_ids:
            new_sql, error = apply_transformation(
                query=query,
                transform_id=transform_id,
                llm_client=llm_client,
            )

            if error or new_sql is None or new_sql == query:
                continue

            # Validate
            try:
                result = validator.validate(query, new_sql)

                status = getattr(result, "status", None)
                if hasattr(status, "value"):
                    status_str = status.value
                else:
                    status_str = str(status).lower() if status else ""

                if status_str == "pass":
                    speedup = getattr(result, "speedup", 1.0)

                    return MCTSOptimizationResult(
                        original_sql=query,
                        optimized_sql=new_sql,
                        valid=True,
                        speedup=speedup,
                        method=f"quick:{transform_id}",
                        transforms_applied=[transform_id],
                        iterations=1,
                        validation_result=result,
                        elapsed_time=time.time() - start_time,
                    )
            except Exception as e:
                logger.debug(f"Quick validation failed for {transform_id}: {e}")
                continue

        return None

    def close(self):
        """Close validator connection."""
        if self._validator is not None:
            self._validator.close()
            self._validator = None

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()


def optimize_sql_file(
    input_path: str,
    output_path: Optional[str] = None,
    database: str = ":memory:",
    provider: Optional[str] = None,
    model: Optional[str] = None,
    max_iterations: int = 30,
) -> MCTSOptimizationResult:
    """Convenience function to optimize a SQL file.

    Args:
        input_path: Path to input SQL file.
        output_path: Path to write optimized SQL (optional).
        database: Path to DuckDB database.
        provider: LLM provider.
        model: LLM model.
        max_iterations: Maximum MCTS iterations.

    Returns:
        MCTSOptimizationResult.
    """
    # Read input
    input_sql = Path(input_path).read_text(encoding="utf-8")

    # Optimize
    with MCTSSQLOptimizer(
        database=database,
        provider=provider,
        model=model,
    ) as optimizer:
        result = optimizer.optimize(input_sql, max_iterations=max_iterations)

    # Write output if requested
    if output_path and result.valid:
        Path(output_path).write_text(result.optimized_sql, encoding="utf-8")

    return result
