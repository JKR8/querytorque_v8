"""Beam workload router — classify workload for BEAM execution only."""

from __future__ import annotations

import logging
from dataclasses import dataclass
from enum import Enum
from typing import Dict, List, Optional, Tuple

logger = logging.getLogger(__name__)


class BeamMode(str, Enum):
    BEAM = "beam"


@dataclass
class WorkloadAssignment:
    """Assignment for a single query."""
    query_id: str
    mode: BeamMode
    baseline_ms: float
    workload_pct: float  # this query's share of total workload


def assign_importance_stars(
    baselines: Dict[str, float],
    high_workload_pct: float = 80.0,
    medium_workload_pct: float = 10.0,
) -> Dict[str, int]:
    """Assign importance stars from workload distribution.

    Rules (descending by baseline runtime):
    - Top cumulative `high_workload_pct` of workload => 3 stars
    - Next cumulative `medium_workload_pct` => 2 stars
    - Remaining workload => 1 star
    """
    if not baselines:
        return {}

    total_ms = sum(max(0.0, float(ms or 0.0)) for ms in baselines.values())
    if total_ms <= 0:
        return {qid: 1 for qid in baselines}

    sorted_queries = sorted(
        baselines.items(), key=lambda x: float(x[1] or 0.0), reverse=True
    )

    stars: Dict[str, int] = {}
    cumulative_ms = 0.0
    medium_cutoff = high_workload_pct + medium_workload_pct

    for qid, ms in sorted_queries:
        prev_pct = (cumulative_ms / total_ms) * 100.0
        if prev_pct < high_workload_pct:
            stars[qid] = 3
        elif prev_pct < medium_cutoff:
            stars[qid] = 2
        else:
            stars[qid] = 1
        cumulative_ms += max(0.0, float(ms or 0.0))

    return stars


def classify_workload(
    baselines: Dict[str, float],
    mode: str = "beam",
    heavy_threshold_pct: float = 80.0,
    min_focused_ms: float = 500.0,
) -> Dict[str, WorkloadAssignment]:
    """Classify queries into BEAM mode based on workload.

    Args:
        baselines: {query_id: baseline_ms} for all queries in the batch.
        mode: Must be "beam" or "auto" (both produce BEAM assignments).
        heavy_threshold_pct: Unused in single-mode BEAM, kept for signature
            stability.
        min_focused_ms: Unused in single-mode BEAM, kept for signature
            stability.

    Returns:
        Dict mapping query_id to WorkloadAssignment.
    """
    if not baselines:
        return {}

    total_ms = sum(baselines.values())
    if total_ms <= 0:
        # All zeros — default to beam
        return {
            qid: WorkloadAssignment(
                query_id=qid,
                mode=BeamMode.BEAM,
                baseline_ms=ms,
                workload_pct=0.0,
            )
            for qid, ms in baselines.items()
        }

    if mode not in ("beam", "auto", ""):
        raise ValueError(
            f"Unsupported beam router mode '{mode}'. Only 'beam' is supported."
        )

    assignments = {}
    for qid, ms in baselines.items():
        pct = (ms / total_ms) * 100
        assignments[qid] = WorkloadAssignment(
            query_id=qid,
            mode=BeamMode.BEAM,
            baseline_ms=ms,
            workload_pct=pct,
        )

    light_ms = sum(a.baseline_ms for a in assignments.values())

    logger.info(
        f"Workload routing: {len(assignments)} BEAM "
        f"({light_ms:.0f}ms, {light_ms/total_ms*100:.0f}%)"
    )

    return assignments
