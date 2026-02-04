"""LLM-based transform ranking for MCTS PUCT priors.

This module provides intelligent LLM ranking of transforms by analyzing:
1. The SQL query structure
2. The execution plan (EXPLAIN output)
3. Known patterns from the knowledge base (as reference, not priority)

The LLM determines which transforms are ACTUALLY APPLICABLE to the specific
query, not just which have high static weights.

Usage:
    from qt_sql.optimization.mcts.llm_ranker import (
        rank_transforms_llm,
        ranking_to_priors,
    )

    ranking = rank_transforms_llm(candidates, sql, applied, llm_client, plan=plan)
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


RANKING_PROMPT = """You are an expert SQL optimizer. Analyze this query and rank the transforms by likelihood of improving performance.

## CRITICAL: Learn from Previous Attempts
The MCTS tree has already tried some transforms. USE THIS DATA to make better decisions:
- If a transform achieved good speedup, similar transforms may help
- If a transform failed or regressed, avoid it or related approaches
- If a transform wasn't applicable (LLM couldn't apply it), deprioritize it

{attempt_history}

## Available Transforms (from Knowledge Base)
{kb_patterns}

## Execution Plan
```
{execution_plan}
```

## SQL Query
```sql
{sql}
```

## Already Applied in Current Path
{applied}

## Instructions
1. FIRST review the previous attempts - what worked, what failed, what regressed
2. Analyze the query structure and execution plan
3. Determine which transforms are ACTUALLY APPLICABLE (have matching patterns)
4. Rank transforms considering:
   - Previous attempt results (avoid what failed/regressed)
   - Query patterns present
   - Likelihood of improvement

Return ONLY valid JSON:
{{"ranking": ["transform_id_1", "transform_id_2", ...], "reasoning": "brief explanation of why top picks, considering previous attempts"}}
"""


def should_use_llm_ranking(
    *,
    node_visit_count: int,
    node_avg_reward: float,
    num_candidates: int,
    children_stats: Optional[dict[str, tuple[int, float]]] = None,
) -> bool:
    """Heuristic gate for when to invoke LLM ranking.

    Triggers when:
    - There are many candidates (>4), or
    - The node appears "stuck": many visits, low reward, and low-performing children
    """
    # Many candidates: LLM can triage
    if num_candidates > 4:
        return True

    # Too few candidates: no need for LLM
    if num_candidates <= 3 and node_avg_reward >= 0.2:
        return False

    # Stuck node: lots of visits, low reward, and children not improving
    if node_visit_count >= 5 and node_avg_reward < 0.2:
        if not children_stats:
            return True

        low_reward_children = [
            avg_reward for _, avg_reward in children_stats.values()
            if avg_reward < 0.2
        ]
        if len(low_reward_children) == len(children_stats):
            return True

    return False


def _get_kb_patterns_for_prompt() -> str:
    """Format KB patterns for the ranking prompt."""
    from ..knowledge_base import get_all_transforms

    lines = []
    for pattern in get_all_transforms():
        evidence = ""
        if pattern.benchmark_queries:
            evidence = f" (proven on {', '.join(pattern.benchmark_queries)})"
        lines.append(
            f"- `{pattern.id.value}`: {pattern.name}{evidence}\n"
            f"  Trigger: {pattern.trigger}\n"
            f"  Rewrite: {pattern.rewrite_hint}"
        )
    return "\n\n".join(lines)


def _format_attempt_history(attempt_summary: Optional[dict]) -> str:
    """Format attempt history for the LLM prompt."""
    if not attempt_summary:
        return "## Previous Attempts\nNo attempts yet - this is the first ranking call."

    lines = ["## Previous Attempts on This Query"]

    for transform_id, stats in attempt_summary.items():
        total = stats.get("total", 0)
        llm_success = stats.get("llm_success", 0)
        llm_failed = stats.get("llm_failed", 0)
        validation_pass = stats.get("validation_pass", 0)
        validation_fail = stats.get("validation_fail", 0)
        avg_speedup = stats.get("avg_speedup", 0)
        max_speedup = stats.get("max_speedup", 0)

        if llm_failed == total:
            lines.append(f"- `{transform_id}`: tried {total}x, LLM couldn't apply (not applicable?)")
        elif validation_fail > 0 and validation_pass == 0:
            lines.append(f"- `{transform_id}`: tried {total}x, all failed validation (breaks semantics)")
        elif avg_speedup < 1.0:
            lines.append(f"- `{transform_id}`: tried {total}x, REGRESSION avg {avg_speedup:.2f}x - AVOID")
        elif max_speedup > 1.1:
            lines.append(f"- `{transform_id}`: tried {total}x, best {max_speedup:.2f}x speedup - PROMISING")
        else:
            lines.append(f"- `{transform_id}`: tried {total}x, avg {avg_speedup:.2f}x (marginal)")

    if not lines[1:]:
        return "## Previous Attempts\nNo attempts yet - this is the first ranking call."

    return "\n".join(lines)


def rank_transforms_llm(
    candidates: list[str],
    sql: str,
    applied_transforms: list[str],
    llm_client,
    timeout_ms: int = 10000,
    query_context: Optional[dict] = None,
    execution_plan: Optional[str] = None,
    attempt_summary: Optional[dict] = None,
) -> Optional[list[str]]:
    """Rank transforms using LLM analysis of query, plan, and previous attempts.

    Args:
        candidates: List of candidate transform IDs to rank.
        sql: Current SQL query.
        applied_transforms: Transforms already applied in current path.
        llm_client: LLM client with analyze() method.
        timeout_ms: Timeout in milliseconds for LLM call.
        query_context: Optional dict with additional context.
        execution_plan: Optional EXPLAIN output for the query.
        attempt_summary: Optional dict of previous attempt results per transform.

    Returns:
        Ordered list of transform IDs (best first), or None on failure.
    """
    if not candidates:
        return None

    # Get KB patterns for prompt
    kb_patterns = _get_kb_patterns_for_prompt()

    # Format attempt history
    attempt_history = _format_attempt_history(attempt_summary)

    # Get execution plan if not provided
    plan_text = execution_plan or "Not available"
    if plan_text == "Not available" and query_context and "plan" in query_context:
        plan_text = query_context["plan"]

    # Truncate SQL if too long
    sql_truncated = sql[:3000] + "..." if len(sql) > 3000 else sql

    # Build prompt
    prompt = RANKING_PROMPT.format(
        kb_patterns=kb_patterns,
        attempt_history=attempt_history,
        execution_plan=plan_text[:2000] if plan_text else "Not available",
        sql=sql_truncated,
        applied=", ".join(applied_transforms) if applied_transforms else "none",
    )

    # Call LLM with timeout
    try:
        with ThreadPoolExecutor(max_workers=1) as executor:
            future = executor.submit(llm_client.analyze, prompt)
            response = future.result(timeout=timeout_ms / 1000.0)

        # Parse response
        ranking = _parse_ranking_response(response, candidates)

        if ranking:
            logger.debug(f"LLM ranking: {ranking}")

        return ranking

    except FuturesTimeoutError:
        logger.debug(f"LLM ranking timed out after {timeout_ms}ms")
        return None
    except Exception as e:
        logger.debug(f"LLM ranking failed: {e}")
        return None


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
    - Rank 1: 0.35
    - Rank 2: 0.25
    - Rank 3: 0.15
    - Rank 4: 0.10
    - Rank 5: 0.05
    - Remaining: share 0.10 equally

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

    # Base distribution for top ranks - more aggressive than before
    top_priors = [0.35, 0.25, 0.15, 0.10, 0.05]
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
