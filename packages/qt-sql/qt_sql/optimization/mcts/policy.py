"""Policy network for MCTS priors (non-LLM, deterministic)."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Optional


@dataclass
class PolicyConfig:
    """Configuration for MCTS priors."""

    min_score: float = 0.01


def _normalize_priors(priors: dict[str, float], candidates: list[str], min_score: float) -> dict[str, float]:
    if not candidates:
        return {}

    normalized: dict[str, float] = {}
    for rule in candidates:
        score = priors.get(rule, 0.0)
        normalized[rule] = max(float(score), min_score)

    total = sum(normalized.values())
    if total <= 0:
        uniform = 1.0 / len(candidates)
        return {rule: uniform for rule in candidates}

    return {rule: score / total for rule, score in normalized.items()}


def _uniform_priors(candidates: list[str]) -> dict[str, float]:
    if not candidates:
        return {}
    uniform = 1.0 / len(candidates)
    return {rule: uniform for rule in candidates}


class PolicyNetwork:
    """Deterministic policy network for ranking transforms."""

    def __init__(self, config: Optional[PolicyConfig] = None):
        self.config = config or PolicyConfig()
        self._cache: dict[tuple[tuple[str, ...], tuple[str, ...]], dict[str, float]] = {}

    def get_priors(self, *, sql: str, available_rules: list[str]) -> dict[str, float]:
        """Return normalized priors over available rules."""
        if not available_rules:
            return {}

        cache_key = (tuple(), tuple(sorted(available_rules)))
        cached = self._cache.get(cache_key)
        if cached is not None:
            return cached

        priors = _uniform_priors(available_rules)
        normalized = _normalize_priors(priors, available_rules, self.config.min_score)
        self._cache[cache_key] = normalized
        return normalized
