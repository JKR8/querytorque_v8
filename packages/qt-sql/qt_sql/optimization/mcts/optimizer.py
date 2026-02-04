"""Hybrid MCTS SQL Optimizer (Mini-HEP + trimmed-mean benchmark)."""

from __future__ import annotations

import time
from dataclasses import dataclass, field
from typing import Optional

from .benchmark import BenchmarkRunner
from .policy import PolicyNetwork, PolicyConfig
from .tree import MCTSTree, MCTSConfig
from .transforms import get_all_transform_ids


@dataclass
class MCTSOptimizationResult:
    original_sql: str
    optimized_sql: str
    valid: bool
    speedup: float
    method: str
    transforms_applied: list[str] = field(default_factory=list)
    iterations: int = 0
    elapsed_time: float = 0.0
    baseline_latency_s: float = 0.0
    tree_stats: dict = field(default_factory=dict)
    detailed_log: Optional[dict] = None
    attempt_summary: Optional[dict] = None

    def to_dict(self) -> dict:
        return {
            "valid": self.valid,
            "speedup": round(self.speedup, 4),
            "method": self.method,
            "transforms_applied": self.transforms_applied,
            "iterations": self.iterations,
            "elapsed_time": round(self.elapsed_time, 2),
            "baseline_latency_s": round(self.baseline_latency_s, 4),
            "tree_stats": self.tree_stats,
        }


class MCTSSQLOptimizer:
    """Hybrid MCTS optimizer following the Mini-HEP spec."""

    def __init__(
        self,
        *,
        database: str,
        policy_config: Optional[PolicyConfig] = None,
        mcts_config: Optional[MCTSConfig] = None,
        transform_ids: Optional[list[str]] = None,
        **_ignored: object,
    ):
        self.database = database
        self.policy_config = policy_config or PolicyConfig()
        self.mcts_config = mcts_config or MCTSConfig()
        self.transform_ids = transform_ids or get_all_transform_ids()

    def __enter__(self) -> "MCTSSQLOptimizer":
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        return None

    def optimize(
        self,
        query: str,
        max_iterations: int = 30,
        **_ignored: object,
    ) -> MCTSOptimizationResult:
        start = time.time()

        policy = PolicyNetwork(self.policy_config)
        benchmark = BenchmarkRunner(self.database)

        tree = MCTSTree(
            original_sql=query,
            policy=policy,
            benchmark=benchmark,
            config=self.mcts_config,
            transform_ids=self.transform_ids,
        )

        try:
            for _ in range(max_iterations):
                tree.iterate()
        finally:
            tree.close()

        best = tree.get_best_node()
        elapsed = time.time() - start

        if best is tree.root:
            optimized_sql = query
            transforms = []
            speedup = 1.0
            method = "original"
        else:
            optimized_sql = best.query_sql
            transforms = []
            node = best
            while node.parent is not None:
                if node.transform:
                    transforms.append(node.transform)
                node = node.parent
            transforms = list(reversed(transforms))
            speedup = best.avg_reward if best.visit_count > 0 else 1.0
            method = "mcts:" + ",".join(transforms)

        return MCTSOptimizationResult(
            original_sql=query,
            optimized_sql=optimized_sql,
            valid=True,
            speedup=speedup,
            method=method,
            transforms_applied=transforms,
            iterations=tree.total_iterations,
            elapsed_time=elapsed,
            baseline_latency_s=tree.baseline_latency_s,
            tree_stats=tree.get_stats(),
        )

    def close(self) -> None:
        return None
