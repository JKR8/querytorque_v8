"""PUCT prior computation for MCTS transform selection.

This module computes prior probabilities for transforms using:
1) Knowledge-base weights (baseline)
2) Opportunity detection (contextual boosts)
3) Optional LLM ranking (when enabled + triggered)

The priors guide MCTS selection via PUCT:
    PUCT = Q(s,a) + c * P(s,a) * sqrt(N(s)) / (1 + N(s,a))
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Optional

from ..knowledge_base import detect_opportunities, get_transform


# Knowledge-base weights (1-10). Higher = more promising transform.
# These are used as a baseline prior distribution.
KB_WEIGHTS: dict[str, int] = {
    "correlated_to_cte": 9,
    "push_pred": 8,
    "or_to_union": 8,
    "date_cte_isolate": 7,
    "consolidate_scans": 7,
    "multi_push_pred": 6,
    "materialize_cte": 5,
    "flatten_subq": 5,
    "reorder_join": 4,
    "inline_cte": 3,
    "remove_redundant": 2,
}


@dataclass
class TransformPrior:
    """Prior probability for a transform.

    Attributes:
        transform_id: Canonical transform ID (e.g., "push_pred").
        prior: Prior probability (0.0 to 1.0), normalized across candidates.
        source: How this prior was computed ("llm_rank" or "uniform").
        reason: Optional explanation for this prior value.
    """

    transform_id: str
    prior: float
    source: str
    reason: Optional[str] = None
    boost_reason: Optional[str] = None

    def __repr__(self) -> str:
        reason = f" ({self.reason})" if self.reason else ""
        return f"TransformPrior({self.transform_id}: {self.prior:.3f} via {self.source}{reason})"


@dataclass
class PriorConfig:
    """Configuration for prior computation.

    Attributes:
        use_puct: Enable PUCT selection. If False, uses original random selection.
        use_kb_weights: Use KB weights as baseline prior.
        use_opportunity_detection: Boost priors for detected opportunities.
        use_llm_ranking: Enable LLM-based ranking (gated by should_use_llm_ranking).
        opportunity_boost: Multiplier for detected opportunities.
        high_value_boost: Multiplier for high_value category transforms.
        diminishing_returns_penalty: Penalty for re-applying already-used transforms.
        llm_timeout_ms: Timeout for LLM ranking calls.
        c_puct: PUCT exploration constant.
        widening_factor: Progressive widening factor (k = factor * sqrt(visits)).
        min_widening: Minimum number of candidates to consider.
    """

    use_puct: bool = True
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


# Pre-defined configs for A/B comparison
RANDOM_CONFIG = PriorConfig(
    use_puct=False,
    use_kb_weights=False,
    use_opportunity_detection=False,
    use_llm_ranking=False,
)
PUCT_CONFIG = PriorConfig(
    use_puct=True,
    use_kb_weights=True,
    use_opportunity_detection=True,
    use_llm_ranking=False,
)
PUCT_LLM_CONFIG = PriorConfig(
    use_puct=True,
    use_kb_weights=True,
    use_opportunity_detection=True,
    use_llm_ranking=True,
)

# Legacy config name for backwards compatibility
PUCT_NO_LLM_CONFIG = PUCT_CONFIG


def compute_uniform_priors(transform_ids: list[str]) -> dict[str, TransformPrior]:
    """Compute priors from KB weights (baseline, no context).

    Args:
        transform_ids: List of transform IDs to compute priors for.

    Returns:
        Dict mapping transform_id to TransformPrior with KB-weighted priors.
    """
    if not transform_ids:
        return {}

    weights = [float(KB_WEIGHTS.get(tid, 3)) for tid in transform_ids]
    total = sum(weights)
    if total <= 0:
        total = float(len(transform_ids))
        weights = [1.0 for _ in transform_ids]

    return {
        tid: TransformPrior(
            transform_id=tid,
            prior=weights[i] / total,
            source="kb_weight",
            reason=f"kb_weight={KB_WEIGHTS.get(tid, 3)}",
        )
        for i, tid in enumerate(transform_ids)
    }


def compute_contextual_priors(
    sql: str,
    transform_ids: list[str],
    applied_transforms: list[str],
    config: PriorConfig,
) -> dict[str, TransformPrior]:
    """Compute contextual priors using KB weights + opportunity detection.

    Applies:
    - KB weights (optional)
    - Opportunity boosts (optional)
    - High-value boosts (optional)
    - Diminishing returns penalty for already-applied transforms
    """
    if not transform_ids:
        return {}

    applied_set = set(applied_transforms)
    opportunities = detect_opportunities(sql) if config.use_opportunity_detection else []
    opportunity_ids = {o.pattern.id.value for o in opportunities}
    opportunity_reasons = {o.pattern.id.value: o.trigger_match for o in opportunities}

    weights: dict[str, float] = {}
    reasons: dict[str, list[str]] = {}

    for tid in transform_ids:
        base = float(KB_WEIGHTS.get(tid, 3)) if config.use_kb_weights else 1.0
        reasons[tid] = []

        # High-value boost (from KB category)
        if config.high_value_boost and config.high_value_boost != 1.0:
            pattern = get_transform(tid)
            if pattern and pattern.category == "high_value":
                base *= config.high_value_boost
                reasons[tid].append("high_value")

        # Opportunity boost
        if tid in opportunity_ids:
            base *= config.opportunity_boost
            trigger = opportunity_reasons.get(tid, "opportunity detected")
            reasons[tid].append(f"opportunity: {trigger}")

        # Diminishing returns for repeated transforms
        if tid in applied_set:
            base *= config.diminishing_returns_penalty
            reasons[tid].append("diminishing_returns")

        weights[tid] = base

    total = sum(weights.values())
    if total <= 0:
        total = float(len(transform_ids))
        weights = {tid: 1.0 for tid in transform_ids}

    priors: dict[str, TransformPrior] = {}
    for tid in transform_ids:
        boost_reason = "; ".join(reasons[tid]) if reasons[tid] else None
        priors[tid] = TransformPrior(
            transform_id=tid,
            prior=weights[tid] / total,
            source="contextual" if (config.use_kb_weights or config.use_opportunity_detection) else "uniform",
            boost_reason=boost_reason,
        )

    return priors


def get_priors_for_node(
    sql: str,
    transform_ids: list[str],
    applied_transforms: list[str],
    config: PriorConfig,
    llm_client=None,
    query_context: Optional[dict] = None,
    attempt_summary: Optional[dict] = None,
) -> dict[str, TransformPrior]:
    """Get priors for a node using LLM ranking or contextual fallback.

    This is the main entry point for prior computation.

    When LLM ranking is enabled:
    - LLM analyzes the query structure and execution plan
    - Reviews previous attempt results (what worked, what failed)
    - Determines which transforms are applicable
    - Ranks by likelihood of performance improvement

    Fallback (LLM disabled or fails):
    - Pure uniform priors
    - MCTS exploration discovers what works

    Args:
        sql: Current SQL query.
        transform_ids: Candidate transform IDs.
        applied_transforms: Already-applied transforms in path.
        config: Prior configuration.
        llm_client: Optional LLM client for ranking.
        query_context: Optional context dict with plan, etc.
        attempt_summary: Optional dict of previous attempt results per transform.

    Returns:
        Dict mapping transform_id to TransformPrior.
    """
    if not transform_ids:
        return {}

    # If LLM ranking not enabled or no client, return contextual priors
    if not config.use_llm_ranking or llm_client is None:
        return compute_contextual_priors(sql, transform_ids, applied_transforms, config)

    # Try LLM ranking - the LLM analyzes the query and ranks transforms
    try:
        from .llm_ranker import rank_transforms_llm, ranking_to_priors

        # Extract execution plan from query_context if available
        execution_plan = query_context.get("plan") if query_context else None

        ranking = rank_transforms_llm(
            candidates=transform_ids,
            sql=sql,
            applied_transforms=applied_transforms,
            llm_client=llm_client,
            timeout_ms=config.llm_timeout_ms,
            query_context=query_context,
            execution_plan=execution_plan,
            attempt_summary=attempt_summary,
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
        # Fallback to uniform on any error
        pass

    # LLM failed - use contextual priors
    return compute_contextual_priors(sql, transform_ids, applied_transforms, config)
