"""MCTS Node dataclass for representing query states in the search tree."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Optional, Any
import math


@dataclass
class MCTSNode:
    """A node in the MCTS tree representing a query state.

    Each node contains:
    - The current SQL query state
    - The path of transformations applied to reach this state
    - Which transformations haven't been tried yet
    - Statistics for UCT selection (visits, rewards)
    - Validation result from execution

    Attributes:
        query_sql: The SQL query at this node.
        applied_transforms: List of transformation IDs applied from root to here.
        remaining_transforms: Transformations not yet tried at this node.
        visit_count: Number of times this node has been visited.
        total_reward: Sum of rewards from all simulations through this node.
        children: Dict mapping transformation ID to child node.
        parent: Parent node (None for root).
        validation_result: Result from validating this query against original.
        transform_error: Error message if transformation failed.
        depth: Depth in tree (root = 0).
    """

    query_sql: str
    applied_transforms: list[str] = field(default_factory=list)
    remaining_transforms: list[str] = field(default_factory=list)
    visit_count: int = 0
    total_reward: float = 0.0
    children: dict[str, "MCTSNode"] = field(default_factory=dict)
    parent: Optional["MCTSNode"] = None
    validation_result: Optional[Any] = None  # ValidationResult from validator
    transform_error: Optional[str] = None
    depth: int = 0

    @property
    def avg_reward(self) -> float:
        """Average reward across all visits."""
        if self.visit_count == 0:
            return 0.0
        return self.total_reward / self.visit_count

    @property
    def is_terminal(self) -> bool:
        """Check if this node has no more transformations to try."""
        return len(self.remaining_transforms) == 0

    @property
    def is_fully_expanded(self) -> bool:
        """Check if all remaining transforms have been tried."""
        return len(self.children) >= len(self.remaining_transforms)

    @property
    def is_valid(self) -> bool:
        """Check if the query at this node passed validation."""
        if self.validation_result is None:
            return False
        # ValidationResult has status attribute with value "pass", "fail", etc.
        return getattr(self.validation_result, "status", None) == "pass" or \
               str(getattr(self.validation_result, "status", "")).lower() == "pass"

    @property
    def speedup(self) -> float:
        """Get speedup from validation result, or 1.0 if not validated."""
        if self.validation_result is None:
            return 1.0
        return getattr(self.validation_result, "speedup", 1.0)

    def uct_score(self, parent_visits: int, c: float = 1.414) -> float:
        """Calculate UCT score for this node.

        UCT = avg_reward + C * sqrt(ln(parent_visits) / visits)

        The UCT formula balances:
        - Exploitation (avg_reward): prefer nodes with high average reward
        - Exploration (sqrt term): prefer less-visited nodes

        Args:
            parent_visits: Number of visits to parent node.
            c: Exploration constant. Higher = more exploration.
               Default is sqrt(2) ≈ 1.414, standard starting point.

        Returns:
            UCT score for this node.
        """
        if self.visit_count == 0:
            # Unvisited nodes get infinite score to ensure exploration
            return float("inf")

        exploitation = self.avg_reward
        exploration = c * math.sqrt(math.log(parent_visits) / self.visit_count)
        return exploitation + exploration

    def puct_score(self, parent_visits: int, prior: float, c_puct: float = 2.0) -> float:
        """Calculate PUCT score for this node.

        PUCT = Q(s,a) + c * P(s,a) * sqrt(N(s)) / (1 + N(s,a))

        The PUCT formula (from AlphaGo/AlphaZero) balances:
        - Exploitation (Q): average reward of this action
        - Exploration: weighted by prior probability and inverse visit count

        Unlike UCT, PUCT uses a prior probability P(s,a) to guide initial
        exploration toward more promising transforms.

        Args:
            parent_visits: Number of visits to parent node N(s).
            prior: Prior probability P(s,a) for this action (0.0 to 1.0).
            c_puct: PUCT exploration constant. Higher = more exploration.
                    Default 2.0 is typical for tree search problems.

        Returns:
            PUCT score for this node.
        """
        if self.visit_count == 0:
            # Unvisited: pure exploration term (infinite if prior > 0)
            return c_puct * prior * math.sqrt(parent_visits + 1)

        exploitation = self.avg_reward
        exploration = c_puct * prior * math.sqrt(parent_visits) / (1 + self.visit_count)
        return exploitation + exploration

    def get_untried_transforms(self) -> list[str]:
        """Get transforms that haven't been tried yet at this node."""
        tried = set(self.children.keys())
        return [t for t in self.remaining_transforms if t not in tried]

    def add_child(
        self,
        transform_id: str,
        new_sql: str,
        remaining: Optional[list[str]] = None,
    ) -> "MCTSNode":
        """Create and add a child node.

        Args:
            transform_id: The transformation that was applied.
            new_sql: The resulting SQL query.
            remaining: Remaining transforms for child. If None, uses
                       parent's remaining minus the applied transform.

        Returns:
            The new child node.
        """
        if remaining is None:
            # Remove the applied transform from remaining
            remaining = [t for t in self.remaining_transforms if t != transform_id]

        child = MCTSNode(
            query_sql=new_sql,
            applied_transforms=self.applied_transforms + [transform_id],
            remaining_transforms=remaining,
            parent=self,
            depth=self.depth + 1,
        )
        self.children[transform_id] = child
        return child

    def path_to_root(self) -> list["MCTSNode"]:
        """Get the path from this node to the root."""
        path = [self]
        node = self
        while node.parent is not None:
            path.append(node.parent)
            node = node.parent
        return list(reversed(path))

    def __repr__(self) -> str:
        """String representation for debugging."""
        return (
            f"MCTSNode(depth={self.depth}, "
            f"transforms={self.applied_transforms}, "
            f"visits={self.visit_count}, "
            f"avg_reward={self.avg_reward:.3f}, "
            f"children={len(self.children)})"
        )

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary for serialization/debugging."""
        return {
            "depth": self.depth,
            "applied_transforms": self.applied_transforms,
            "remaining_transforms": self.remaining_transforms,
            "visit_count": self.visit_count,
            "total_reward": self.total_reward,
            "avg_reward": self.avg_reward,
            "is_valid": self.is_valid,
            "speedup": self.speedup,
            "children": list(self.children.keys()),
            "query_preview": self.query_sql[:100] + "..." if len(self.query_sql) > 100 else self.query_sql,
        }
