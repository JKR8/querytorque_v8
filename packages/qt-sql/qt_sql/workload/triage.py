"""Workload Triage — score and classify queries for optimization.

Scoring: pain × frequency × tractability (range 0-300).
Classification: SKIP / TIER_2 / TIER_3.
Quick-win fast path: top 3 queries >80% pain → direct to Tier 3.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)


class Tier(Enum):
    SKIP = "SKIP"
    TIER_1 = "TIER_1"     # Fleet-level only
    TIER_2 = "TIER_2"     # Light per-query optimization
    TIER_3 = "TIER_3"     # Deep per-query optimization (full pipeline)


@dataclass
class TriageResult:
    """Triage result for a single query."""
    query_id: str
    pain_score: int = 0
    frequency_score: int = 3    # Default: unknown
    tractability_score: int = 1
    priority: int = 0           # pain × frequency × tractability
    tier: Tier = Tier.SKIP
    quick_win: bool = False     # Part of quick-win fast path
    reason: str = ""


@dataclass
class WorkloadTriage:
    """Triage results for an entire workload."""
    results: List[TriageResult] = field(default_factory=list)
    quick_wins: List[str] = field(default_factory=list)  # query_ids
    tier_2_queries: List[str] = field(default_factory=list)
    tier_3_queries: List[str] = field(default_factory=list)
    skipped: List[str] = field(default_factory=list)
    total_pain: int = 0


def score_pain(
    query_duration_ms: Optional[float] = None,
    timed_out: bool = False,
    spill_detected: bool = False,
    exceeds_memory: bool = False,
    meets_sla: bool = False,
) -> int:
    """Score query pain on 0-10 scale.

    10: timeout or crash
    7:  spills to disk/remote
    5:  exceeds memory budget
    2:  slow but completes
    0:  already meets SLA
    """
    if timed_out or (query_duration_ms and query_duration_ms > 300_000):
        return 10
    if spill_detected:
        return 7
    if exceeds_memory:
        return 5
    if meets_sla:
        return 0
    if query_duration_ms and query_duration_ms > 10_000:
        return 2
    return 1


def score_frequency(executions_per_day: Optional[int] = None) -> int:
    """Score query frequency on 1-10 scale.

    10: 1000+ per day
    5:  100-999 per day
    2:  10-99 per day
    1:  under 10 per day
    3:  unknown (default)
    """
    if executions_per_day is None:
        return 3
    if executions_per_day >= 1000:
        return 10
    if executions_per_day >= 100:
        return 5
    if executions_per_day >= 10:
        return 2
    return 1


def score_tractability(
    has_gold_match: bool = False,
    has_known_blind_spot: bool = False,
    already_optimized: bool = False,
) -> int:
    """Score tractability on 0-3 scale.

    3: direct gold example match
    2: known optimizer blind spot
    1: complex/novel (default)
    0: already optimized
    """
    if already_optimized:
        return 0
    if has_gold_match:
        return 3
    if has_known_blind_spot:
        return 2
    return 1


def classify_tier(priority: int, pain: int, frequency: int) -> Tier:
    """Classify query into optimization tier.

    priority 0: SKIP
    priority 1-15: TIER_2 (light optimization)
    priority 16+: TIER_3 (deep optimization)

    Overrides:
    - Timeout + frequency > 100/day → TIER_3 regardless
    - Already meets SLA → SKIP
    """
    if priority == 0:
        return Tier.SKIP
    if pain >= 10 and frequency >= 5:
        return Tier.TIER_3
    if priority >= 16:
        return Tier.TIER_3
    if priority >= 1:
        return Tier.TIER_2
    return Tier.SKIP


def triage_workload(
    queries: List[Dict[str, Any]],
) -> WorkloadTriage:
    """Score and classify all queries in a workload.

    Args:
        queries: List of dicts with keys:
            - query_id: str
            - duration_ms: float (optional)
            - timed_out: bool (optional)
            - spill_detected: bool (optional, also accepts spills_remote)
            - exceeds_memory: bool (optional)
            - meets_sla: bool (optional)
            - frequency_per_day: int (optional)
            - has_gold_match: bool (optional)
            - has_known_blind_spot: bool (optional)
            - already_optimized: bool (optional)

    Returns:
        WorkloadTriage with scored and classified results.
    """
    results = []
    for q in queries:
        pain = score_pain(
            query_duration_ms=q.get("duration_ms"),
            timed_out=q.get("timed_out", False),
            spill_detected=q.get("spill_detected", False) or q.get("spills_remote", False),
            exceeds_memory=q.get("exceeds_memory", False),
            meets_sla=q.get("meets_sla", False),
        )
        freq = score_frequency(q.get("frequency_per_day"))
        tract = score_tractability(
            has_gold_match=q.get("has_gold_match", False),
            has_known_blind_spot=q.get("has_known_blind_spot", False),
            already_optimized=q.get("already_optimized", False),
        )
        priority = pain * freq * tract
        tier = classify_tier(priority, pain, freq)

        results.append(TriageResult(
            query_id=q["query_id"],
            pain_score=pain,
            frequency_score=freq,
            tractability_score=tract,
            priority=priority,
            tier=tier,
        ))

    # Sort by priority descending
    results.sort(key=lambda r: r.priority, reverse=True)

    # Quick-win fast path: top 3 queries with >80% of total pain
    triage = WorkloadTriage(results=results)
    triage.total_pain = sum(r.pain_score * r.frequency_score for r in results)

    if triage.total_pain > 0:
        cumulative = 0
        for r in results[:3]:
            cumulative += r.pain_score * r.frequency_score
            r.quick_win = True
            r.tier = Tier.TIER_3
            r.reason = "quick-win: top pain contributor"
            triage.quick_wins.append(r.query_id)
            if cumulative / triage.total_pain >= 0.8:
                break

    # Classify into lists
    for r in results:
        if r.tier == Tier.SKIP:
            triage.skipped.append(r.query_id)
        elif r.tier == Tier.TIER_2:
            triage.tier_2_queries.append(r.query_id)
        elif r.tier == Tier.TIER_3:
            if r.query_id not in triage.quick_wins:
                triage.tier_3_queries.append(r.query_id)

    logger.info(
        f"Triage: {len(triage.skipped)} skip, "
        f"{len(triage.tier_2_queries)} tier-2, "
        f"{len(triage.tier_3_queries)} tier-3, "
        f"{len(triage.quick_wins)} quick-win"
    )

    return triage
