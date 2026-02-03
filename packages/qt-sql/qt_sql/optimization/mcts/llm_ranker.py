"""LLM-based transform ranking for MCTS PUCT priors.

This module provides batched LLM ranking of transforms to compute
prior probabilities for PUCT selection. It's designed for Phase 3
of the PUCT implementation.

Key features:
- Single batched LLM call to rank all candidates
- Timeout handling with graceful fallback
- JSON response parsing with error handling
- Conversion of rankings to prior probabilities

Usage:
    from qt_sql.optimization.mcts.llm_ranker import (
        rank_transforms_llm,
        ranking_to_priors,
    )

    ranking = rank_transforms_llm(candidates, sql, applied, llm_client)
    if ranking:
        priors = ranking_to_priors(ranking, candidates)
"""

from __future__ import annotations

import json
import logging
import re
from concurrent.futures import ThreadPoolExecutor, TimeoutError as FuturesTimeoutError
from typing import Optional

logger = logging.getLogger(__name__)


RANKING_PROMPT = """Rank these SQL transformations for the query below.
Return most-to-least likely to improve performance.

## Available Transforms
{transform_list}

## Query Context
- Tables: {tables}
- Has CTEs: {has_ctes}
- Has correlated subquery: {has_correlated}
- Has OR conditions: {has_or}
- Already applied: {applied}

## SQL (truncated to 2000 chars)
```sql
{sql}
```

## Instructions
Rank the transforms from MOST to LEAST likely to improve this query's performance.
Consider:
1. Which patterns are present in the query
2. What has already been applied (avoid redundant transforms)
3. Known high-value transforms for the detected patterns

Return ONLY valid JSON in this exact format:
{{"ranking": ["transform_id_1", "transform_id_2", ...]}}
"""


# Transform descriptions for the ranking prompt
TRANSFORM_DESCRIPTIONS = {
    "push_pred": "Push WHERE predicates closer to base tables",
    "reorder_join": "Reorder joins to put selective tables first",
    "materialize_cte": "Extract repeated subqueries to CTEs",
    "inline_cte": "Inline single-use CTEs back into query",
    "flatten_subq": "Convert correlated subqueries to JOINs",
    "remove_redundant": "Remove unnecessary DISTINCT, columns, ORDER BY",
    "multi_push_pred": "Push predicates through multiple CTE layers",
    "or_to_union": "Split OR conditions into UNION ALL branches (2.98x on Q15)",
    "correlated_to_cte": "Replace correlated subquery with pre-computed CTE (2.81x on Q1)",
    "date_cte_isolate": "Isolate date filter into small early CTE (2.67x on Q15)",
    "consolidate_scans": "Combine multiple scans into conditional aggregation (1.84x on Q90)",
}


def rank_transforms_llm(
    candidates: list[str],
    sql: str,
    applied_transforms: list[str],
    llm_client,
    timeout_ms: int = 5000,
    query_context: Optional[dict] = None,
) -> Optional[list[str]]:
    """Rank transforms using a single batched LLM call.

    Args:
        candidates: List of candidate transform IDs to rank.
        sql: Current SQL query (will be truncated for prompt).
        applied_transforms: Transforms already applied in current path.
        llm_client: LLM client with analyze() method.
        timeout_ms: Timeout in milliseconds for LLM call.
        query_context: Optional dict with tables, has_ctes, etc.

    Returns:
        Ordered list of transform IDs (best first), or None on failure.
    """
    if not candidates:
        return None

    # Build transform list for prompt
    transform_list = "\n".join(
        f"- `{tid}`: {TRANSFORM_DESCRIPTIONS.get(tid, tid)}"
        for tid in candidates
    )

    # Extract query context
    context = query_context or {}
    sql_lower = sql.lower()

    tables = context.get("tables", _extract_tables(sql))
    has_ctes = context.get("has_ctes", "with " in sql_lower)
    has_correlated = context.get(
        "has_correlated",
        bool(re.search(r"where.*[><]=?\s*\(\s*select", sql_lower, re.DOTALL))
    )
    has_or = context.get("has_or", " or " in sql_lower)

    # Truncate SQL for prompt
    sql_truncated = sql[:2000] + "..." if len(sql) > 2000 else sql

    # Build prompt
    prompt = RANKING_PROMPT.format(
        transform_list=transform_list,
        tables=", ".join(tables) if tables else "unknown",
        has_ctes=has_ctes,
        has_correlated=has_correlated,
        has_or=has_or,
        applied=", ".join(applied_transforms) if applied_transforms else "none",
        sql=sql_truncated,
    )

    # Call LLM with timeout
    try:
        with ThreadPoolExecutor(max_workers=1) as executor:
            future = executor.submit(llm_client.analyze, prompt)
            response = future.result(timeout=timeout_ms / 1000.0)

        # Parse response
        ranking = _parse_ranking_response(response, candidates)
        return ranking

    except FuturesTimeoutError:
        logger.debug(f"LLM ranking timed out after {timeout_ms}ms")
        return None
    except Exception as e:
        logger.debug(f"LLM ranking failed: {e}")
        return None


def _extract_tables(sql: str) -> list[str]:
    """Extract table names from SQL query (simple heuristic)."""
    sql_lower = sql.lower()
    tables = set()

    # Match FROM and JOIN clauses
    for pattern in [r'\bfrom\s+(\w+)', r'\bjoin\s+(\w+)']:
        matches = re.findall(pattern, sql_lower)
        tables.update(matches)

    # Remove common keywords that aren't tables
    keywords = {'select', 'where', 'group', 'order', 'having', 'union', 'with', 'as'}
    tables -= keywords

    return list(tables)


def _parse_ranking_response(response: str, candidates: list[str]) -> Optional[list[str]]:
    """Parse LLM response to extract ranking.

    Handles various response formats:
    - Clean JSON
    - JSON in markdown code blocks
    - JSON with surrounding text

    Args:
        response: Raw LLM response.
        candidates: Valid candidate IDs for validation.

    Returns:
        List of transform IDs in ranked order, or None on failure.
    """
    if not response:
        return None

    response = response.strip()

    # Try to extract JSON from code block
    json_pattern = r"```(?:json)?\s*\n?(.*?)\n?```"
    matches = re.findall(json_pattern, response, re.DOTALL | re.IGNORECASE)

    json_str = None
    if matches:
        json_str = matches[0].strip()
    else:
        # Try to find JSON object directly
        brace_start = response.find('{')
        brace_end = response.rfind('}')
        if brace_start != -1 and brace_end != -1:
            json_str = response[brace_start:brace_end + 1]

    if not json_str:
        return None

    try:
        data = json.loads(json_str)

        if isinstance(data, dict) and "ranking" in data:
            ranking = data["ranking"]
            if isinstance(ranking, list):
                # Validate: only include candidates that exist
                candidate_set = set(candidates)
                valid_ranking = [tid for tid in ranking if tid in candidate_set]

                if valid_ranking:
                    # Add any missing candidates at the end
                    missing = [tid for tid in candidates if tid not in valid_ranking]
                    return valid_ranking + missing

        return None

    except json.JSONDecodeError:
        return None


def ranking_to_priors(ranking: list[str], all_candidates: list[str]) -> dict[str, float]:
    """Convert ordered ranking to prior probabilities.

    Uses a decreasing distribution that heavily favors top-ranked transforms:
    - Rank 1: 0.30
    - Rank 2: 0.20
    - Rank 3: 0.15
    - Rank 4: 0.10
    - Rank 5: 0.08
    - Remaining: share 0.17 equally

    Args:
        ranking: Ordered list of transform IDs (best first).
        all_candidates: All candidate transform IDs (for completeness).

    Returns:
        Dict mapping transform_id to prior probability (sums to 1.0).
    """
    if not ranking:
        # Uniform fallback
        n = len(all_candidates) if all_candidates else 1
        return {tid: 1.0 / n for tid in all_candidates}

    # Base distribution for top ranks
    top_priors = [0.30, 0.20, 0.15, 0.10, 0.08]
    remaining_mass = 1.0 - sum(top_priors[:min(len(top_priors), len(ranking))])

    priors: dict[str, float] = {}

    for i, tid in enumerate(ranking):
        if i < len(top_priors):
            priors[tid] = top_priors[i]
        else:
            # Divide remaining mass among rest
            num_remaining = len(ranking) - len(top_priors)
            priors[tid] = remaining_mass / num_remaining if num_remaining > 0 else 0.01

    # Ensure all candidates have a prior
    candidate_set = set(all_candidates)
    for tid in candidate_set:
        if tid not in priors:
            priors[tid] = 0.01  # Small non-zero prior

    # Re-normalize to sum to 1.0
    total = sum(priors.values())
    if total > 0:
        priors = {tid: p / total for tid, p in priors.items()}

    return priors


def should_use_llm_ranking(
    node_visit_count: int,
    node_avg_reward: float,
    num_candidates: int,
    children_stats: Optional[dict] = None,
) -> bool:
    """Determine if LLM ranking should be triggered for this node.

    Triggers LLM ranking when:
    1. Many candidates (>4) make random selection inefficient
    2. Node is "stuck" (many visits, low reward, high failure rate)

    Args:
        node_visit_count: Number of visits to the node.
        node_avg_reward: Average reward of the node.
        num_candidates: Number of candidate transforms.
        children_stats: Optional dict with {transform_id: (visits, avg_reward)}.

    Returns:
        True if LLM ranking should be used.
    """
    # Trigger 1: Many candidates
    if num_candidates > 4:
        return True

    # Trigger 2: Node is stuck (many visits with poor results)
    if node_visit_count >= 5 and node_avg_reward < 0.3:
        if children_stats:
            # Check failure rate
            failed = sum(1 for _, (_, reward) in children_stats.items() if reward < 0.1)
            if len(children_stats) > 0 and failed / len(children_stats) > 0.5:
                return True

    return False
