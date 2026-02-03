"""MCTS tree operations: selection, expansion, simulation, backpropagation."""

from __future__ import annotations

import logging
import random
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from typing import Optional, Callable, Any

from .node import MCTSNode
from .transforms import get_all_transform_ids, apply_transformation, apply_dag_transformation
from .reward import compute_reward, RewardConfig

logger = logging.getLogger(__name__)


@dataclass
class TransformAttempt:
    """Record of a single transformation attempt for detailed logging."""

    iteration: int
    parent_path: list[str]  # Transforms applied to reach parent
    transform_id: str
    timestamp: float
    duration_ms: int = 0

    # LLM results
    llm_success: bool = False
    llm_error: Optional[str] = None
    output_sql: Optional[str] = None
    sql_changed: bool = False

    # Validation results
    validated: bool = False
    validation_status: Optional[str] = None
    validation_error: Optional[str] = None
    speedup: float = 1.0
    original_rows: int = 0
    optimized_rows: int = 0

    # UCT context
    uct_score: float = 0.0
    parent_visits: int = 0

    # Reward
    reward: float = 0.0

    def to_dict(self, include_full_sql: bool = False) -> dict:
        """Convert to dictionary for JSON serialization.

        Args:
            include_full_sql: If True, include full output_sql. Otherwise truncate to 200 chars.
        """
        result = {
            "iteration": self.iteration,
            "parent_path": self.parent_path,
            "transform_id": self.transform_id,
            "duration_ms": self.duration_ms,
            "llm_success": self.llm_success,
            "llm_error": self.llm_error,
            "sql_changed": self.sql_changed,
            "validated": self.validated,
            "validation_status": self.validation_status,
            "validation_error": self.validation_error,
            "speedup": round(self.speedup, 4),
            "original_rows": self.original_rows,
            "optimized_rows": self.optimized_rows,
            "uct_score": round(self.uct_score, 4),
            "parent_visits": self.parent_visits,
            "reward": round(self.reward, 4),
        }

        if include_full_sql:
            result["output_sql"] = self.output_sql
        else:
            result["output_sql_preview"] = (
                (self.output_sql[:200] + "...") if self.output_sql and len(self.output_sql) > 200
                else self.output_sql
            )

        return result


@dataclass
class SelectionStep:
    """Record of a UCT selection decision."""

    node_path: list[str]
    candidates: list[dict]  # [{transform_id, uct_score, visits, avg_reward}]
    selected: str
    reason: str  # "uct_best", "untried", "terminal"

    def to_dict(self) -> dict:
        return {
            "node_path": self.node_path,
            "candidates": self.candidates,
            "selected": self.selected,
            "reason": self.reason,
        }


class MCTSTree:
    """Monte Carlo Tree Search operations for SQL optimization.

    Implements the four phases of MCTS:
    1. Selection: Walk tree picking best UCT child until unexpanded node
    2. Expansion: Pick untried transform, call LLM, create child node
    3. Simulation: Evaluate node quality via validation
    4. Backpropagation: Update visit counts and rewards up to root

    Attributes:
        root: Root node (original query).
        llm_client: LLM client for applying transformations.
        validator: SQL validator for checking equivalence and timing.
        original_sql: The original query being optimized.
        reward_config: Configuration for reward computation.
        c: UCT exploration constant.
        max_depth: Maximum tree depth.
    """

    def __init__(
        self,
        original_sql: str,
        llm_client: Any,
        validator: Any,
        reward_config: Optional[RewardConfig] = None,
        c: float = 1.414,
        max_depth: int = 5,
        transform_ids: Optional[list[str]] = None,
        use_dag_mode: bool = True,
    ):
        """Initialize MCTS tree.

        Args:
            original_sql: The original SQL query to optimize.
            llm_client: LLM client with analyze() method.
            validator: SQLValidator instance.
            reward_config: Configuration for reward computation.
            c: UCT exploration constant. Higher = more exploration.
            max_depth: Maximum depth of transformations to chain.
            transform_ids: List of transformation IDs to use.
                          If None, uses all available transforms.
            use_dag_mode: Use DAG-based node patching (default True).
        """
        self.original_sql = original_sql
        self.llm_client = llm_client
        self.validator = validator
        self.reward_config = reward_config or RewardConfig()
        self.c = c
        self.max_depth = max_depth
        self.use_dag_mode = use_dag_mode

        # Initialize transform IDs
        if transform_ids is None:
            transform_ids = get_all_transform_ids()
        self.transform_ids = transform_ids

        # Create root node
        self.root = MCTSNode(
            query_sql=original_sql,
            applied_transforms=[],
            remaining_transforms=list(transform_ids),
            depth=0,
        )

        # Statistics
        self.total_iterations = 0
        self.successful_expansions = 0
        self.failed_expansions = 0
        self.validation_calls = 0

        # Detailed logging
        self.transform_attempts: list[TransformAttempt] = []
        self.selection_steps: list[SelectionStep] = []

    def select(self, node: Optional[MCTSNode] = None, log_selection: bool = True) -> MCTSNode:
        """Select a node for expansion using UCT.

        Walk tree from root picking best UCT child at each step
        until we find a node that:
        - Has untried transforms, OR
        - Is terminal (no remaining transforms)

        Args:
            node: Starting node. If None, starts from root.
            log_selection: Whether to log selection decisions.

        Returns:
            The selected node for expansion.
        """
        if node is None:
            node = self.root

        while True:
            # If node has untried transforms, select it for expansion
            untried = node.get_untried_transforms()
            if untried:
                if log_selection:
                    self.selection_steps.append(SelectionStep(
                        node_path=node.applied_transforms,
                        candidates=[{"transform_id": t, "status": "untried"} for t in untried],
                        selected=untried[0] if untried else "",
                        reason="untried_available",
                    ))
                return node

            # If no children (terminal or all failed), return this node
            if not node.children:
                if log_selection:
                    self.selection_steps.append(SelectionStep(
                        node_path=node.applied_transforms,
                        candidates=[],
                        selected="",
                        reason="terminal_no_children",
                    ))
                return node

            # If at max depth, return this node
            if node.depth >= self.max_depth:
                if log_selection:
                    self.selection_steps.append(SelectionStep(
                        node_path=node.applied_transforms,
                        candidates=[],
                        selected="",
                        reason="max_depth_reached",
                    ))
                return node

            # Select best child by UCT score
            best_child = None
            best_score = float("-inf")
            candidates = []

            for child in node.children.values():
                score = child.uct_score(node.visit_count, self.c)
                transform_id = child.applied_transforms[-1] if child.applied_transforms else "?"
                candidates.append({
                    "transform_id": transform_id,
                    "uct_score": round(score, 4) if score != float("inf") else "inf",
                    "visits": child.visit_count,
                    "avg_reward": round(child.avg_reward, 4),
                    "is_valid": child.is_valid,
                    "speedup": round(child.speedup, 4),
                })
                if score > best_score:
                    best_score = score
                    best_child = child

            if best_child is None:
                if log_selection:
                    self.selection_steps.append(SelectionStep(
                        node_path=node.applied_transforms,
                        candidates=candidates,
                        selected="",
                        reason="no_viable_children",
                    ))
                return node

            if log_selection:
                selected_transform = best_child.applied_transforms[-1] if best_child.applied_transforms else "?"
                self.selection_steps.append(SelectionStep(
                    node_path=node.applied_transforms,
                    candidates=candidates,
                    selected=selected_transform,
                    reason="uct_best",
                ))

            node = best_child

    def expand(self, node: MCTSNode) -> Optional[MCTSNode]:
        """Expand a node by applying an untried transformation.

        Args:
            node: Node to expand.

        Returns:
            New child node if expansion successful, None if failed.
        """
        untried = node.get_untried_transforms()
        if not untried:
            return None

        # Don't expand beyond max depth
        if node.depth >= self.max_depth:
            return None

        # Pick transform to try (could use heuristics here)
        transform_id = self._select_transform(untried, node)

        # Create attempt record
        attempt = TransformAttempt(
            iteration=self.total_iterations,
            parent_path=node.applied_transforms.copy(),
            transform_id=transform_id,
            timestamp=time.time(),
            parent_visits=node.visit_count,
        )

        # Apply transformation via LLM (use DAG mode if enabled)
        start_time = time.perf_counter()
        if self.use_dag_mode:
            new_sql, error = apply_dag_transformation(
                query=node.query_sql,
                transform_id=transform_id,
                llm_client=self.llm_client,
            )
        else:
            new_sql, error = apply_transformation(
                query=node.query_sql,
                transform_id=transform_id,
                llm_client=self.llm_client,
            )
        attempt.duration_ms = int((time.perf_counter() - start_time) * 1000)

        if error or new_sql is None:
            # Mark this transform as tried but failed
            attempt.llm_success = False
            attempt.llm_error = error
            self.transform_attempts.append(attempt)

            child = node.add_child(
                transform_id=transform_id,
                new_sql=node.query_sql,  # Keep original SQL
                remaining=[],  # No further transforms from failed node
            )
            child.transform_error = error
            self.failed_expansions += 1
            logger.debug(f"Transform {transform_id} failed: {error}")
            return None

        # Record successful LLM call
        attempt.llm_success = True
        attempt.output_sql = new_sql
        attempt.sql_changed = new_sql.strip() != node.query_sql.strip()

        # Create child node with transformed SQL
        child = node.add_child(
            transform_id=transform_id,
            new_sql=new_sql,
        )
        child._attempt = attempt  # Link attempt for later update
        self.successful_expansions += 1
        logger.debug(f"Transform {transform_id} applied successfully")

        return child

    def expand_parallel(
        self,
        node: MCTSNode,
        num_transforms: int = 4,
    ) -> list[MCTSNode]:
        """Expand a node by applying multiple transformations in parallel.

        Makes parallel LLM API calls but returns nodes for sequential validation.

        Args:
            node: Node to expand.
            num_transforms: Max number of transforms to try in parallel.

        Returns:
            List of successfully created child nodes (not yet validated).
        """
        untried = node.get_untried_transforms()
        if not untried:
            return []

        # Don't expand beyond max depth
        if node.depth >= self.max_depth:
            return []

        # Pick transforms to try (up to num_transforms)
        transforms_to_try = untried[:num_transforms]

        # Apply transformations in parallel
        results: list[tuple[str, Optional[str], Optional[str], int]] = []

        def apply_single(transform_id: str) -> tuple[str, Optional[str], Optional[str], int]:
            """Apply a single transformation, return (id, sql, error, duration_ms)."""
            start_time = time.perf_counter()
            if self.use_dag_mode:
                new_sql, error = apply_dag_transformation(
                    query=node.query_sql,
                    transform_id=transform_id,
                    llm_client=self.llm_client,
                )
            else:
                new_sql, error = apply_transformation(
                    query=node.query_sql,
                    transform_id=transform_id,
                    llm_client=self.llm_client,
                )
            duration_ms = int((time.perf_counter() - start_time) * 1000)
            return (transform_id, new_sql, error, duration_ms)

        with ThreadPoolExecutor(max_workers=num_transforms) as executor:
            futures = {
                executor.submit(apply_single, tid): tid
                for tid in transforms_to_try
            }

            for future in as_completed(futures):
                try:
                    result = future.result()
                    results.append(result)
                except Exception as e:
                    tid = futures[future]
                    results.append((tid, None, str(e), 0))

        # Create child nodes from results
        children = []
        for transform_id, new_sql, error, duration_ms in results:
            # Create attempt record
            attempt = TransformAttempt(
                iteration=self.total_iterations,
                parent_path=node.applied_transforms.copy(),
                transform_id=transform_id,
                timestamp=time.time(),
                duration_ms=duration_ms,
                parent_visits=node.visit_count,
            )

            if error or new_sql is None:
                # Mark as tried but failed
                attempt.llm_success = False
                attempt.llm_error = error
                self.transform_attempts.append(attempt)

                child = node.add_child(
                    transform_id=transform_id,
                    new_sql=node.query_sql,
                    remaining=[],
                )
                child.transform_error = error
                self.failed_expansions += 1
                logger.debug(f"Transform {transform_id} failed: {error}")
            else:
                # Success - create child for validation
                attempt.llm_success = True
                attempt.output_sql = new_sql
                attempt.sql_changed = new_sql.strip() != node.query_sql.strip()

                child = node.add_child(
                    transform_id=transform_id,
                    new_sql=new_sql,
                )
                child._attempt = attempt  # Link for later update
                self.successful_expansions += 1
                logger.debug(f"Transform {transform_id} applied successfully")
                children.append(child)

        return children

    def iterate_parallel(
        self,
        num_parallel: int = 4,
    ) -> list[tuple[MCTSNode, float]]:
        """Run one MCTS iteration with parallel LLM calls.

        Expands multiple transforms in parallel, then validates sequentially.

        Args:
            num_parallel: Number of parallel LLM calls.

        Returns:
            List of (node, reward) tuples for successfully expanded nodes.
        """
        self.total_iterations += 1

        # 1. Selection
        selected = self.select()

        # 2. Parallel Expansion (LLM calls)
        children = self.expand_parallel(selected, num_transforms=num_parallel)

        if not children:
            # All expansions failed
            self.backpropagate(selected, 0.0)
            return []

        # 3. Sequential Simulation (validation) - DB access must be sequential
        results = []
        for child in children:
            reward = self.simulate(child)
            # 4. Backpropagation
            self.backpropagate(child, reward)
            results.append((child, reward))

        return results

    def simulate(self, node: MCTSNode) -> float:
        """Simulate (evaluate) a node by validating the query.

        Runs validation to check:
        - Semantic equivalence (row counts, checksums)
        - Performance (speedup)

        Args:
            node: Node to evaluate.

        Returns:
            Reward value for this node.
        """
        if node.transform_error:
            # Failed transformation gets zero reward
            return 0.0

        if node.validation_result is not None:
            # Already validated, compute reward from cached result
            return compute_reward(node.validation_result, self.reward_config)

        # Get linked attempt if available
        attempt = getattr(node, '_attempt', None)

        try:
            # Run validation
            result = self.validator.validate(
                self.original_sql,
                node.query_sql,
            )
            node.validation_result = result
            self.validation_calls += 1

            # Compute reward
            reward = compute_reward(result, self.reward_config)

            # Update attempt with validation results
            if attempt:
                attempt.validated = True
                attempt.validation_status = str(getattr(result, 'status', 'unknown'))
                attempt.speedup = getattr(result, 'speedup', 1.0)
                attempt.reward = reward
                # Try to get row counts if available
                if hasattr(result, 'original_rows'):
                    attempt.original_rows = result.original_rows
                if hasattr(result, 'optimized_rows'):
                    attempt.optimized_rows = result.optimized_rows
                self.transform_attempts.append(attempt)

            logger.debug(
                f"Validation: status={getattr(result, 'status', 'unknown')}, "
                f"speedup={getattr(result, 'speedup', 1.0):.2f}x, "
                f"reward={reward:.3f}"
            )

            return reward

        except Exception as e:
            # Update attempt with error
            if attempt:
                attempt.validated = True
                attempt.validation_status = "error"
                attempt.validation_error = str(e)
                attempt.reward = 0.0
                self.transform_attempts.append(attempt)

            logger.warning(f"Validation failed: {e}")
            return 0.0

    def backpropagate(self, node: MCTSNode, reward: float) -> None:
        """Backpropagate reward up to root.

        Updates visit counts and total rewards for all nodes
        from the given node up to the root.

        Args:
            node: Starting node.
            reward: Reward to propagate.
        """
        current = node
        while current is not None:
            current.visit_count += 1
            current.total_reward += reward
            current = current.parent

    def iterate(self) -> tuple[Optional[MCTSNode], float]:
        """Run one MCTS iteration.

        Returns:
            Tuple of (expanded_node, reward).
            Node may be None if expansion failed.
        """
        self.total_iterations += 1

        # 1. Selection
        selected = self.select()

        # 2. Expansion
        expanded = self.expand(selected)

        if expanded is None:
            # Expansion failed, backpropagate zero reward from selected node
            self.backpropagate(selected, 0.0)
            return None, 0.0

        # 3. Simulation
        reward = self.simulate(expanded)

        # 4. Backpropagation
        self.backpropagate(expanded, reward)

        return expanded, reward

    def _select_transform(self, candidates: list[str], node: MCTSNode) -> str:
        """Select which transform to try next.

        Currently uses random selection, but could incorporate:
        - Transform success rates
        - Query-specific heuristics
        - Learning from past runs

        Args:
            candidates: List of untried transform IDs.
            node: Current node.

        Returns:
            Transform ID to try.
        """
        # Simple random selection for now
        return random.choice(candidates)

    def get_best_node(self) -> MCTSNode:
        """Get the best node found so far.

        Uses most-visited valid leaf as the best result,
        which is statistically more robust than highest reward.

        Returns:
            Best node (may be root if no valid transformations found).
        """
        best = self.root
        best_visits = 0

        def visit(node: MCTSNode):
            nonlocal best, best_visits

            # Only consider nodes that passed validation
            if node.is_valid and node.visit_count > best_visits:
                best = node
                best_visits = node.visit_count

            for child in node.children.values():
                visit(child)

        visit(self.root)
        return best

    def get_highest_reward_node(self) -> MCTSNode:
        """Get node with highest average reward.

        Alternative to get_best_node for more aggressive optimization.

        Returns:
            Node with highest avg_reward.
        """
        best = self.root
        best_reward = self.root.avg_reward

        def visit(node: MCTSNode):
            nonlocal best, best_reward

            if node.is_valid and node.avg_reward > best_reward:
                best = node
                best_reward = node.avg_reward

            for child in node.children.values():
                visit(child)

        visit(self.root)
        return best

    def get_best_speedup_node(self) -> MCTSNode:
        """Get node with highest speedup.

        Returns:
            Node with highest speedup value.
        """
        best = self.root
        best_speedup = 1.0

        def visit(node: MCTSNode):
            nonlocal best, best_speedup

            if node.is_valid and node.speedup > best_speedup:
                best = node
                best_speedup = node.speedup

            for child in node.children.values():
                visit(child)

        visit(self.root)
        return best

    def get_tree_size(self) -> int:
        """Get total number of nodes in tree."""
        count = 0

        def visit(node: MCTSNode):
            nonlocal count
            count += 1
            for child in node.children.values():
                visit(child)

        visit(self.root)
        return count

    def get_stats(self) -> dict:
        """Get tree statistics."""
        return {
            "total_iterations": self.total_iterations,
            "tree_size": self.get_tree_size(),
            "successful_expansions": self.successful_expansions,
            "failed_expansions": self.failed_expansions,
            "validation_calls": self.validation_calls,
            "root_visits": self.root.visit_count,
            "total_attempts": len(self.transform_attempts),
        }

    def get_all_attempts(self, include_full_sql: bool = False) -> list[dict]:
        """Get all transformation attempts as dicts for JSON export.

        Args:
            include_full_sql: If True, include full SQL in each attempt.
        """
        return [a.to_dict(include_full_sql=include_full_sql) for a in self.transform_attempts]

    def get_selection_log(self) -> list[dict]:
        """Get all selection decisions as dicts for JSON export."""
        return [s.to_dict() for s in self.selection_steps]

    def get_detailed_log(self, include_full_sql: bool = False) -> dict:
        """Get comprehensive log of all MCTS activity.

        Args:
            include_full_sql: If True, include full SQL in attempts.

        Returns dict with:
        - stats: Summary statistics
        - attempts: All transform attempts with LLM/validation results
        - selections: All UCT selection decisions
        - tree: Serialized tree structure
        """
        return {
            "stats": self.get_stats(),
            "attempts": self.get_all_attempts(include_full_sql=include_full_sql),
            "selections": self.get_selection_log(),
            "tree": self._serialize_tree(),
        }

    def _serialize_tree(self, node: Optional[MCTSNode] = None, max_depth: int = 10) -> dict:
        """Serialize tree to nested dict for JSON export."""
        if node is None:
            node = self.root

        result = {
            "transforms": node.applied_transforms,
            "depth": node.depth,
            "visits": node.visit_count,
            "avg_reward": round(node.avg_reward, 4),
            "is_valid": node.is_valid,
            "speedup": round(node.speedup, 4),
            "error": node.transform_error,
        }

        if node.depth < max_depth and node.children:
            result["children"] = {
                tid: self._serialize_tree(child, max_depth)
                for tid, child in node.children.items()
            }

        return result

    def get_attempt_summary(self) -> dict:
        """Get summary of attempts by transform type and outcome."""
        summary = {}
        for attempt in self.transform_attempts:
            tid = attempt.transform_id
            if tid not in summary:
                summary[tid] = {
                    "total": 0,
                    "llm_success": 0,
                    "llm_failed": 0,
                    "validation_pass": 0,
                    "validation_fail": 0,
                    "validation_error": 0,
                    "avg_speedup": 0.0,
                    "max_speedup": 0.0,
                    "speedups": [],
                }
            s = summary[tid]
            s["total"] += 1

            if attempt.llm_success:
                s["llm_success"] += 1
                if attempt.validated:
                    if "pass" in str(attempt.validation_status).lower():
                        s["validation_pass"] += 1
                        s["speedups"].append(attempt.speedup)
                        s["max_speedup"] = max(s["max_speedup"], attempt.speedup)
                    elif attempt.validation_error:
                        s["validation_error"] += 1
                    else:
                        s["validation_fail"] += 1
            else:
                s["llm_failed"] += 1

        # Compute averages
        for tid, s in summary.items():
            if s["speedups"]:
                s["avg_speedup"] = round(sum(s["speedups"]) / len(s["speedups"]), 4)
            del s["speedups"]  # Don't include raw list

        return summary

    def print_tree(self, max_depth: int = 3) -> str:
        """Generate ASCII representation of tree for debugging."""
        lines = []

        def visit(node: MCTSNode, prefix: str = "", is_last: bool = True):
            # Node info
            transform = node.applied_transforms[-1] if node.applied_transforms else "root"
            status = "✓" if node.is_valid else "✗" if node.validation_result else "?"
            info = f"{transform} [{status}] v={node.visit_count} r={node.avg_reward:.2f}"

            # Tree branch
            connector = "└── " if is_last else "├── "
            lines.append(prefix + connector + info)

            # Children
            if node.depth < max_depth:
                children = list(node.children.values())
                for i, child in enumerate(children):
                    is_last_child = i == len(children) - 1
                    new_prefix = prefix + ("    " if is_last else "│   ")
                    visit(child, new_prefix, is_last_child)

        visit(self.root, "", True)
        return "\n".join(lines)
