"""Beam workload router — classifies queries into wide or focused mode.

Routes based on baseline runtime:
- HEAVY queries (top 20% that account for ~80% of total runtime) → beam_focused
- LIGHT queries (remaining 80%) → beam_wide

Usage:
    from qt_sql.patches.beam_router import classify_workload, BeamMode

    assignments = classify_workload(baselines, mode="auto")
    for query_id, mode in assignments.items():
        if mode == BeamMode.WIDE:
            # fire 16 qwen probes
        else:
            # fire 4 R1 strikes
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from enum import Enum
from typing import Dict, List, Optional, Tuple

logger = logging.getLogger(__name__)


class BeamMode(str, Enum):
    WIDE = "wide"
    FOCUSED = "focused"


@dataclass
class WorkloadAssignment:
    """Assignment for a single query."""
    query_id: str
    mode: BeamMode
    baseline_ms: float
    workload_pct: float  # this query's share of total workload


def classify_workload(
    baselines: Dict[str, float],
    mode: str = "auto",
    heavy_threshold_pct: float = 80.0,
    min_focused_ms: float = 500.0,
) -> Dict[str, WorkloadAssignment]:
    """Classify queries into beam wide or focused based on workload.

    Args:
        baselines: {query_id: baseline_ms} for all queries in the batch.
        mode: "auto" (workload-based routing), "wide" (all wide),
              "focused" (all focused).
        heavy_threshold_pct: Percentage of total workload that defines
            the HEAVY partition (default 80%). Queries are sorted by
            baseline_ms descending and accumulated until this threshold.
        min_focused_ms: Minimum baseline ms to be eligible for focused
            mode (default 500ms). Queries below this always go wide
            even if they're in the heavy partition.

    Returns:
        Dict mapping query_id to WorkloadAssignment.
    """
    if not baselines:
        return {}

    total_ms = sum(baselines.values())
    if total_ms <= 0:
        # All zeros — default to wide
        return {
            qid: WorkloadAssignment(
                query_id=qid,
                mode=BeamMode.WIDE if mode != "focused" else BeamMode.FOCUSED,
                baseline_ms=ms,
                workload_pct=0.0,
            )
            for qid, ms in baselines.items()
        }

    # Force mode
    if mode == "wide":
        return {
            qid: WorkloadAssignment(
                query_id=qid,
                mode=BeamMode.WIDE,
                baseline_ms=ms,
                workload_pct=(ms / total_ms) * 100,
            )
            for qid, ms in baselines.items()
        }
    elif mode == "focused":
        return {
            qid: WorkloadAssignment(
                query_id=qid,
                mode=BeamMode.FOCUSED,
                baseline_ms=ms,
                workload_pct=(ms / total_ms) * 100,
            )
            for qid, ms in baselines.items()
        }

    # Auto mode: accumulate top queries until heavy_threshold_pct
    sorted_queries = sorted(
        baselines.items(), key=lambda x: x[1], reverse=True
    )

    accumulated = 0.0
    heavy_ids = set()

    for qid, ms in sorted_queries:
        if accumulated >= (heavy_threshold_pct / 100.0) * total_ms:
            break
        heavy_ids.add(qid)
        accumulated += ms

    assignments = {}
    n_heavy = 0
    n_light = 0

    for qid, ms in baselines.items():
        pct = (ms / total_ms) * 100

        if qid in heavy_ids and ms >= min_focused_ms:
            assignments[qid] = WorkloadAssignment(
                query_id=qid,
                mode=BeamMode.FOCUSED,
                baseline_ms=ms,
                workload_pct=pct,
            )
            n_heavy += 1
        else:
            assignments[qid] = WorkloadAssignment(
                query_id=qid,
                mode=BeamMode.WIDE,
                baseline_ms=ms,
                workload_pct=pct,
            )
            n_light += 1

    heavy_ms = sum(a.baseline_ms for a in assignments.values() if a.mode == BeamMode.FOCUSED)
    light_ms = sum(a.baseline_ms for a in assignments.values() if a.mode == BeamMode.WIDE)

    logger.info(
        f"Workload routing: {n_heavy} FOCUSED ({heavy_ms:.0f}ms, "
        f"{heavy_ms/total_ms*100:.0f}%) + {n_light} WIDE ({light_ms:.0f}ms, "
        f"{light_ms/total_ms*100:.0f}%)"
    )

    return assignments
