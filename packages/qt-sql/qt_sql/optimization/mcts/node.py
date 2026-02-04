"""MCTS node definition for Hybrid MCTS SQL optimizer."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Optional
import math


@dataclass
class MCTSNode:
    """A node in the MCTS tree representing a SQL state."""

    query_sql: str
    state_hash: str
    prior: float = 0.0
    parent: Optional["MCTSNode"] = None
    transform: Optional[str] = None
    depth: int = 0
    visit_count: int = 0
    value_sum: float = 0.0
    children: dict[str, "MCTSNode"] = field(default_factory=dict)
    expanded: bool = False

    @property
    def avg_reward(self) -> float:
        if self.visit_count == 0:
            return 0.0
        return self.value_sum / self.visit_count

    def add_child(self, *, transform: str, sql: str, prior: float, state_hash: str) -> "MCTSNode":
        child = MCTSNode(
            query_sql=sql,
            state_hash=state_hash,
            prior=prior,
            parent=self,
            transform=transform,
            depth=self.depth + 1,
        )
        self.children[transform] = child
        return child

    def puct_score(self, parent_visits: int, c_puct: float, fpu: float) -> float:
        """PUCT score with First Play Urgency for unvisited nodes."""
        if parent_visits < 1:
            parent_visits = 1

        exploitation = self.avg_reward if self.visit_count > 0 else fpu
        exploration = c_puct * self.prior * math.sqrt(parent_visits) / (1 + self.visit_count)
        return exploitation + exploration
