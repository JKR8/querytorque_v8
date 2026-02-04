"""MCTS tree implementation using PUCT and deterministic transforms."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Optional
import re

from .node import MCTSNode
from .policy import PolicyNetwork
from .benchmark import BenchmarkRunner
from .transforms import apply_transform, get_all_transform_ids
from qt_sql.validation import SQLValidator
from qt_sql.validation.schemas import ValidationMode, ValidationStatus


@dataclass
class MCTSConfig:
    c_puct: float = 1.0
    fpu: float = 1.5
    max_depth: int = 5
    validate: bool = True
    validation_mode: ValidationMode = ValidationMode.SAMPLE
    validation_sample_pct: float = 1.0
    transposition_min_score: float = 0.01


@dataclass
class TranspositionStats:
    visits: int = 0
    value_sum: float = 0.0

    @property
    def avg_reward(self) -> float:
        if self.visits == 0:
            return 0.0
        return self.value_sum / self.visits


class MCTSTree:
    """Hybrid MCTS tree: deterministic transforms + trimmed mean reward."""

    def __init__(
        self,
        *,
        original_sql: str,
        policy: PolicyNetwork,
        benchmark: BenchmarkRunner,
        config: Optional[MCTSConfig] = None,
        transform_ids: Optional[list[str]] = None,
        validator: Optional[SQLValidator] = None,
    ):
        self.original_sql = original_sql
        self.policy = policy
        self.benchmark = benchmark
        self.config = config or MCTSConfig()
        self.transform_ids = transform_ids or get_all_transform_ids()
        self.validator = validator
        self._transform_cache: dict[tuple[str, str], Optional[str]] = {}
        self._validation_cache: dict[str, bool] = {}
        self._hash_cache: dict[str, str] = {}
        self._transposition: dict[str, dict[str, TranspositionStats]] = {}

        root_hash = self._get_state_hash(original_sql)
        self.root = MCTSNode(query_sql=original_sql, state_hash=root_hash)
        self.total_iterations = 0

        if self.config.validate and self.validator is None:
            self.validator = SQLValidator(
                database=self.benchmark.database,
                mode=self.config.validation_mode,
                sample_pct=self.config.validation_sample_pct,
            )

        baseline = self.benchmark.run_query_robust(original_sql)
        self.baseline_latency_s = baseline.latency_s

    def _get_state_hash(self, sql: str) -> str:
        cached = self._hash_cache.get(sql)
        if cached is not None:
            return cached

        normalized = sql.strip()
        normalized = re.sub(r"\s+", " ", normalized)
        normalized = normalized.lower()
        self._hash_cache[sql] = normalized
        return normalized

    def _get_ancestor_hashes(self, node: MCTSNode) -> set[str]:
        hashes: set[str] = set()
        current = node
        while current is not None:
            hashes.add(current.state_hash)
            current = current.parent
        return hashes

    def _get_transposition_priors(
        self,
        state_hash: str,
        available_rules: list[str],
    ) -> Optional[dict[str, float]]:
        stats_by_rule = self._transposition.get(state_hash)
        if not stats_by_rule:
            return None

        priors: dict[str, float] = {}
        for rule in available_rules:
            stats = stats_by_rule.get(rule)
            if stats is None:
                continue
            priors[rule] = max(stats.avg_reward, self.config.transposition_min_score)

        if not priors:
            return None

        total = sum(priors.values())
        if total <= 0:
            uniform = 1.0 / len(available_rules)
            return {rule: uniform for rule in available_rules}

        return {rule: score / total for rule, score in priors.items()}

    def close(self) -> None:
        if self.validator is not None:
            self.validator.close()

    def select(self) -> MCTSNode:
        """Select a node for expansion using PUCT."""
        node = self.root
        while node.expanded and node.children and node.depth < self.config.max_depth:
            node = self._select_best_child(node)
        return node

    def _select_best_child(self, node: MCTSNode) -> MCTSNode:
        best_child = None
        best_score = float("-inf")

        for child in node.children.values():
            score = child.puct_score(
                node.visit_count,
                self.config.c_puct,
                self.config.fpu,
            )
            if score > best_score:
                best_score = score
                best_child = child

        return best_child if best_child is not None else node

    def expand(self, node: MCTSNode) -> Optional[MCTSNode]:
        """Expand node by generating children for all valid transforms."""
        if node.expanded or node.depth >= self.config.max_depth:
            node.expanded = True
            return None

        valid_moves: dict[str, str] = {}
        seen_sql: set[str] = set()
        ancestor_hashes = self._get_ancestor_hashes(node)
        for transform_id in self.transform_ids:
            cache_key = (node.query_sql, transform_id)
            if cache_key in self._transform_cache:
                new_sql = self._transform_cache[cache_key]
            else:
                new_sql = apply_transform(node.query_sql, transform_id)
                self._transform_cache[cache_key] = new_sql

            if new_sql is not None:
                canonical_sql = new_sql.strip()
                if canonical_sql in seen_sql:
                    continue
                state_hash = self._get_state_hash(canonical_sql)
                if state_hash in ancestor_hashes:
                    continue
                seen_sql.add(canonical_sql)
                valid_moves[transform_id] = new_sql

        node.expanded = True

        if not valid_moves:
            return None

        available_rules = list(valid_moves.keys())
        priors = self._get_transposition_priors(node.state_hash, available_rules)
        if priors is None:
            priors = self.policy.get_priors(
                sql=node.query_sql,
                available_rules=available_rules,
            )

        for transform_id, new_sql in valid_moves.items():
            prior = priors.get(transform_id, 0.0)
            child_hash = self._get_state_hash(new_sql)
            node.add_child(
                transform=transform_id,
                sql=new_sql,
                prior=prior,
                state_hash=child_hash,
            )

        # Simulate the top-prior child first
        return max(node.children.values(), key=lambda c: c.prior, default=None)

    def simulate(self, node: MCTSNode) -> float:
        """Execute SQL and compute speedup reward."""
        if self.validator is not None:
            cached = self._validation_cache.get(node.query_sql)
            if cached is None:
                result = self.validator.validate(self.original_sql, node.query_sql)
                status = result.status
                valid = status in (ValidationStatus.PASS, ValidationStatus.WARN)
                self._validation_cache[node.query_sql] = valid
            else:
                valid = cached

            if not valid:
                return 0.0

        timeout_s = self.baseline_latency_s * 2.0
        result = self.benchmark.run_query_robust(node.query_sql, timeout_s=timeout_s)

        if result.timed_out or result.latency_s > timeout_s:
            return 0.4

        if result.latency_s <= 0:
            return 0.4

        return self.baseline_latency_s / result.latency_s

    def backpropagate(self, node: MCTSNode, reward: float) -> None:
        current = node
        while current is not None:
            current.visit_count += 1
            current.value_sum += reward
            if current.parent is not None and current.transform:
                state_hash = current.parent.state_hash
                by_rule = self._transposition.setdefault(state_hash, {})
                stats = by_rule.setdefault(current.transform, TranspositionStats())
                stats.visits += 1
                stats.value_sum += reward
            current = current.parent

    def iterate(self) -> tuple[Optional[MCTSNode], float]:
        self.total_iterations += 1

        selected = self.select()
        expanded = self.expand(selected)
        if expanded is None:
            self.backpropagate(selected, 0.0)
            return None, 0.0

        reward = self.simulate(expanded)
        self.backpropagate(expanded, reward)
        return expanded, reward

    def get_best_node(self) -> MCTSNode:
        """Return the node with highest average reward."""
        best = self.root
        best_reward = best.avg_reward

        def visit(node: MCTSNode) -> None:
            nonlocal best, best_reward
            if node.visit_count > 0 and node.avg_reward > best_reward:
                best = node
                best_reward = node.avg_reward
            for child in node.children.values():
                visit(child)

        visit(self.root)
        return best

    def get_tree_size(self) -> int:
        count = 0

        def visit(node: MCTSNode) -> None:
            nonlocal count
            count += 1
            for child in node.children.values():
                visit(child)

        visit(self.root)
        return count

    def get_stats(self) -> dict:
        return {
            "total_iterations": self.total_iterations,
            "tree_size": self.get_tree_size(),
            "baseline_latency_s": self.baseline_latency_s,
        }
