"""PUCT prior computation for MCTS transform selection.

This module provides prior probability computation for transforms using:
1. Knowledge base weights (baseline)
2. Opportunity detection boosts (context-aware)
3. LLM ranking (optional, batched)

The priors guide MCTS selection via PUCT formula:
    PUCT = Q(s,a) + c * P(s,a) * sqrt(N(s)) / (1 + N(s,a))

Usage:
    from qt_sql.optimization.mcts.priors import (
        PriorConfig,
        TransformPrior,
        compute_uniform_priors,
        compute_contextual_priors,
    )

    config = PriorConfig(use_opportunity_detection=True)
    priors = compute_contextual_priors(sql, candidates, applied, config)
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Optional


@dataclass
class TransformPrior:
    """Prior probability for a transform.

    Attributes:
        transform_id: Canonical transform ID (e.g., "push_pred").
        prior: Prior probability (0.0 to 1.0), normalized across candidates.
        source: How this prior was computed ("kb_weight", "contextual", "llm_rank", "uniform").
        boost_reason: Optional explanation if this prior was boosted.
    """

    transform_id: str
    prior: float
    source: str
    boost_reason: Optional[str] = None

    def __repr__(self) -> str:
        boost = f" ({self.boost_reason})" if self.boost_reason else ""
        return f"TransformPrior({self.transform_id}: {self.prior:.3f} via {self.source}{boost})"


@dataclass
class PriorConfig:
    """Configuration for prior computation.

    Attributes:
        use_puct: Enable PUCT selection. If False, uses original random selection.
        use_kb_weights: Use knowledge base weights as baseline priors.
        use_opportunity_detection: Boost priors for detected opportunities.
        use_llm_ranking: Use LLM to rank transforms (Phase 3).
        opportunity_boost: Multiplier for detected opportunities (1.5 = 50% boost).
        high_value_boost: Multiplier for high-value category transforms.
        diminishing_returns_penalty: Multiplier for already-applied transforms.
        llm_timeout_ms: Timeout for LLM ranking calls.
        c_puct: PUCT exploration constant.
        widening_factor: Progressive widening factor (k = factor * sqrt(visits)).
        min_widening: Minimum number of candidates to consider.
    """

    use_puct: bool = True  # Set to False for original random selection
    use_kb_weights: bool = True
    use_opportunity_detection: bool = True
    use_llm_ranking: bool = False
    opportunity_boost: float = 1.5
    high_value_boost: float = 1.2
    diminishing_returns_penalty: float = 0.5
    llm_timeout_ms: int = 5000
    c_puct: float = 2.0
    widening_factor: float = 1.5
    min_widening: int = 2


# Pre-defined configs for easy comparison
PUCT_CONFIG = PriorConfig(use_puct=True)  # Default PUCT with KB weights + opportunity detection
RANDOM_CONFIG = PriorConfig(use_puct=False)  # Original random selection
PUCT_LLM_CONFIG = PriorConfig(use_puct=True, use_llm_ranking=True)  # Full PUCT with LLM


def compute_uniform_priors(transform_ids: list[str]) -> dict[str, TransformPrior]:
    """Compute uniform priors based on knowledge base weights.

    Transforms with higher KB weights get proportionally higher priors.
    Priors are normalized to sum to 1.0.

    Args:
        transform_ids: List of transform IDs to compute priors for.

    Returns:
        Dict mapping transform_id to TransformPrior.
    """
    from ..knowledge_base import get_transform

    if not transform_ids:
        return {}

    # Get weights from knowledge base (default 5 if not found)
    weights: dict[str, float] = {}
    for tid in transform_ids:
        pattern = get_transform(tid)
        weights[tid] = float(pattern.weight) if pattern else 5.0

    # Normalize to sum to 1.0
    total = sum(weights.values())
    if total == 0:
        total = len(transform_ids)
        weights = {tid: 1.0 for tid in transform_ids}

    return {
        tid: TransformPrior(
            transform_id=tid,
            prior=w / total,
            source="kb_weight",
        )
        for tid, w in weights.items()
    }


def compute_contextual_priors(
    sql: str,
    transform_ids: list[str],
    applied_transforms: list[str],
    config: PriorConfig,
) -> dict[str, TransformPrior]:
    """Compute context-aware priors with opportunity detection and boosts.

    Enhances uniform priors with:
    1. Opportunity boost: transforms matched by detect_opportunities() get boosted
    2. High-value boost: high_value category transforms get boosted
    3. Diminishing returns: already-applied transforms get penalized

    Args:
        sql: Current SQL query to analyze.
        transform_ids: List of candidate transform IDs.
        applied_transforms: Transforms already applied in current path.
        config: Prior configuration.

    Returns:
        Dict mapping transform_id to TransformPrior with contextual adjustments.
    """
    from ..knowledge_base import detect_opportunities, get_transform

    if not transform_ids:
        return {}

    # Start with uniform KB-weighted priors
    if config.use_kb_weights:
        priors = compute_uniform_priors(transform_ids)
    else:
        # Pure uniform
        uniform_p = 1.0 / len(transform_ids)
        priors = {
            tid: TransformPrior(tid, uniform_p, "uniform")
            for tid in transform_ids
        }

    # Detect opportunities if enabled
    opportunity_ids: set[str] = set()
    if config.use_opportunity_detection:
        opportunities = detect_opportunities(sql)
        opportunity_ids = {opp.pattern.id.value for opp in opportunities}

    # Apply boosts and build adjusted weights
    adjusted_weights: dict[str, float] = {}
    boost_reasons: dict[str, str] = {}

    for tid in transform_ids:
        weight = priors[tid].prior
        reasons = []

        # Opportunity boost
        if tid in opportunity_ids:
            weight *= config.opportunity_boost
            reasons.append(f"opportunity:{tid}")

        # High-value category boost
        pattern = get_transform(tid)
        if pattern and pattern.category == "high_value":
            weight *= config.high_value_boost
            reasons.append("high_value")

        # Diminishing returns for applied transforms
        if tid in applied_transforms:
            weight *= config.diminishing_returns_penalty
            reasons.append("already_applied")

        adjusted_weights[tid] = weight
        if reasons:
            boost_reasons[tid] = ", ".join(reasons)

    # Re-normalize to sum to 1.0
    total = sum(adjusted_weights.values())
    if total == 0:
        total = 1.0

    return {
        tid: TransformPrior(
            transform_id=tid,
            prior=w / total,
            source="contextual",
            boost_reason=boost_reasons.get(tid),
        )
        for tid, w in adjusted_weights.items()
    }


def get_priors_for_node(
    sql: str,
    transform_ids: list[str],
    applied_transforms: list[str],
    config: PriorConfig,
    llm_client=None,
    query_context: Optional[dict] = None,
) -> dict[str, TransformPrior]:
    """Get priors for a node, using LLM ranking if enabled and triggered.

    This is the main entry point for prior computation. It:
    1. Computes contextual priors as baseline
    2. Optionally calls LLM ranking if enabled and conditions are met
    3. Falls back gracefully on LLM errors

    Args:
        sql: Current SQL query.
        transform_ids: Candidate transform IDs.
        applied_transforms: Already-applied transforms in path.
        config: Prior configuration.
        llm_client: Optional LLM client for ranking.
        query_context: Optional context dict with tables, has_ctes, etc.

    Returns:
        Dict mapping transform_id to TransformPrior.
    """
    # Always compute contextual priors as baseline/fallback
    contextual_priors = compute_contextual_priors(
        sql, transform_ids, applied_transforms, config
    )

    # If LLM ranking not enabled or no client, return contextual
    if not config.use_llm_ranking or llm_client is None:
        return contextual_priors

    # Try LLM ranking
    try:
        from .llm_ranker import rank_transforms_llm, ranking_to_priors

        ranking = rank_transforms_llm(
            candidates=transform_ids,
            sql=sql,
            applied_transforms=applied_transforms,
            llm_client=llm_client,
            timeout_ms=config.llm_timeout_ms,
            query_context=query_context,
        )

        if ranking:
            llm_priors = ranking_to_priors(ranking, transform_ids)
            return {
                tid: TransformPrior(
                    transform_id=tid,
                    prior=p,
                    source="llm_rank",
                )
                for tid, p in llm_priors.items()
            }

    except Exception:
        # Fallback to contextual on any error
        pass

    return contextual_priors
