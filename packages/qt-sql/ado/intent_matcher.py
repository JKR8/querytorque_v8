"""Intent-based principle matching for knowledge serving.

Matches a query's semantic intent against principles' `when` clauses
using keyword overlap scoring. Returns matched principles sorted by
relevance for inclusion in optimization prompts.
"""

from __future__ import annotations

import re
from typing import Any, Dict, List, Optional

from .schemas import GlobalKnowledge, KnowledgePrinciple


# Keywords that indicate specific optimization opportunities
INTENT_KEYWORDS = {
    "correlated": ["decorrelate", "correlated_subquery"],
    "subquery": ["decorrelate", "correlated_subquery", "intersect_to_exists"],
    "aggregate": ["single_pass_aggregation", "pushdown"],
    "date": ["date_cte_isolate", "multi_date_range_cte"],
    "date_dim": ["date_cte_isolate", "multi_date_range_cte"],
    "dimension": ["dimension_cte_isolate", "multi_dimension_prefetch"],
    "filter": ["early_filter", "pushdown"],
    "join": ["prefetch_fact_join", "decorrelate"],
    "or": ["or_to_union"],
    "union": ["union_cte_split"],
    "intersect": ["intersect_to_exists"],
    "cte": ["materialize_cte", "union_cte_split"],
    "repeated": ["single_pass_aggregation", "materialize_cte"],
    "scan": ["single_pass_aggregation", "early_filter"],
    "fact": ["prefetch_fact_join", "early_filter"],
    "partition": ["pushdown"],
    "window": ["pushdown"],
    "exists": ["intersect_to_exists"],
    "in_subquery": ["decorrelate", "intersect_to_exists"],
}


def match_by_intent(
    query_intent: str,
    knowledge: GlobalKnowledge,
    max_results: int = 3,
) -> List[KnowledgePrinciple]:
    """Match query intent against principles via keyword overlap.

    Args:
        query_intent: Semantic intent string for the query
        knowledge: Global knowledge containing principles
        max_results: Maximum principles to return

    Returns:
        List of matched KnowledgePrinciple sorted by relevance score.
    """
    if not query_intent or not knowledge.principles:
        return []

    # Tokenize intent
    intent_tokens = _tokenize(query_intent)

    scored: List[tuple[float, KnowledgePrinciple]] = []

    for principle in knowledge.principles:
        score = _score_principle(intent_tokens, principle)
        if score > 0:
            scored.append((score, principle))

    # Sort by score descending
    scored.sort(key=lambda x: -x[0])

    return [p for _, p in scored[:max_results]]


def match_by_transforms(
    transforms_in_query: List[str],
    knowledge: GlobalKnowledge,
) -> List[KnowledgePrinciple]:
    """Match principles by transform names found in query analysis.

    Direct lookup — if we know which transforms are relevant,
    find the corresponding principles.
    """
    if not transforms_in_query or not knowledge.principles:
        return []

    matched = []
    transform_set = set(t.lower() for t in transforms_in_query)

    for principle in knowledge.principles:
        principle_transforms = set(t.lower() for t in principle.transforms)
        if transform_set & principle_transforms:
            matched.append(principle)

    return matched


def match_anti_patterns(
    query_intent: str,
    knowledge: GlobalKnowledge,
    max_results: int = 2,
) -> List[Dict[str, Any]]:
    """Match anti-patterns relevant to this query.

    Returns anti-pattern dicts for inclusion in prompt warnings.
    """
    if not query_intent or not knowledge.anti_patterns:
        return []

    intent_tokens = _tokenize(query_intent)
    results = []

    for ap in knowledge.anti_patterns:
        # Check if anti-pattern's mechanism keywords overlap with intent
        mechanism_tokens = _tokenize(ap.mechanism + " " + ap.name)
        overlap = intent_tokens & mechanism_tokens
        if overlap:
            results.append(ap.to_dict())

    return results[:max_results]


def _score_principle(
    intent_tokens: set,
    principle: KnowledgePrinciple,
) -> float:
    """Score a principle against intent tokens.

    Scoring:
    - Direct transform name match: 3.0 per match
    - Keyword → transform mapping match: 2.0 per match
    - When clause keyword overlap: 1.0 per token
    - Bonus for higher avg_speedup: 0.1 * avg_speedup
    """
    score = 0.0

    # Direct transform match
    for token in intent_tokens:
        if token in (t.lower() for t in principle.transforms):
            score += 3.0

    # Keyword mapping match
    for token in intent_tokens:
        if token in INTENT_KEYWORDS:
            mapped_transforms = INTENT_KEYWORDS[token]
            for mt in mapped_transforms:
                if mt in (t.lower() for t in principle.transforms):
                    score += 2.0
                    break

    # When clause overlap
    if principle.when:
        when_tokens = _tokenize(principle.when)
        overlap = intent_tokens & when_tokens
        score += len(overlap) * 1.0

    # What/why description overlap
    desc_tokens = _tokenize(principle.what + " " + principle.why)
    overlap = intent_tokens & desc_tokens
    score += len(overlap) * 0.5

    # Speedup bonus
    if principle.avg_speedup > 1.0:
        score += 0.1 * principle.avg_speedup

    return score


def _tokenize(text: str) -> set:
    """Tokenize text into lowercase keyword set."""
    # Split on non-alphanumeric, lowercase
    tokens = set(re.findall(r'[a-z_]+', text.lower()))
    # Remove very common stop words
    stop_words = {
        "the", "a", "an", "and", "or", "is", "are", "in", "on", "for",
        "to", "of", "with", "by", "from", "as", "at", "it", "this",
        "that", "be", "not", "but", "will", "can", "has", "have",
        "been", "was", "were", "do", "does", "did", "would", "should",
        "could", "may", "might", "each", "all", "any", "both", "more",
        "most", "other", "some", "such", "than", "too", "very", "just",
    }
    return tokens - stop_words
