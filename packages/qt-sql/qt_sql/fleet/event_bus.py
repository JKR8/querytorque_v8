"""Thread-safe event bridge between orchestrator and WebSocket server."""

from __future__ import annotations

import json
import queue
import time
from dataclasses import asdict, dataclass, field
from enum import Enum
from typing import Any, Dict, List, Optional, TYPE_CHECKING

if TYPE_CHECKING:
    from ..dashboard.models import ForensicQuery
    from .orchestrator import TriageResult


class EventType(str, Enum):
    TRIAGE_READY = "triage_ready"
    QUERY_UPDATE = "query_update"
    QUERY_COMPLETE = "query_complete"
    WORKER_UPDATE = "worker_update"
    EVENT_LOG = "event_log"
    FLEET_DONE = "fleet_done"
    EDITOR_ITERATION = "editor_iteration"
    EDITOR_COMPLETE = "editor_complete"


@dataclass
class FleetEvent:
    type: EventType
    data: Dict[str, Any] = field(default_factory=dict)
    timestamp: float = field(default_factory=time.time)

    def to_json(self) -> str:
        return json.dumps({
            "type": self.type.value,
            "data": self.data,
            "timestamp": self.timestamp,
        }, default=str)


class EventBus:
    """Thread-safe event queue for orchestrator → WebSocket server communication."""

    def __init__(self, maxsize: int = 1000) -> None:
        self._queue: queue.Queue[FleetEvent] = queue.Queue(maxsize=maxsize)

    def emit(self, event_type: EventType, **data: Any) -> None:
        """Non-blocking put. Drop event if queue is full."""
        event = FleetEvent(type=event_type, data=data)
        try:
            self._queue.put_nowait(event)
        except queue.Full:
            pass  # drop oldest-unread event rather than block

    def get_event(self, timeout: float = 0.1) -> Optional[FleetEvent]:
        """Blocking get with timeout. Returns None if no event available."""
        try:
            return self._queue.get(timeout=timeout)
        except queue.Empty:
            return None

    def drain(self, max_events: int = 50) -> List[FleetEvent]:
        """Non-blocking batch get. Returns up to max_events."""
        events: List[FleetEvent] = []
        for _ in range(max_events):
            try:
                events.append(self._queue.get_nowait())
            except queue.Empty:
                break
        return events


def triage_to_fleet_c2(
    triaged: List["TriageResult"],
) -> List[Dict[str, Any]]:
    """Map TriageResult list → fleet_c2.html QUERIES JSON format.

    Lightweight alternative to forensic_to_fleet_c2 that uses survey data
    directly without requiring the heavy dashboard collector pipeline.
    """
    # Compute cost context (rank, pct_of_total, cumulative_pct)
    total_runtime = sum(
        t.survey.runtime_ms for t in triaged if t.survey.runtime_ms > 0
    )
    # Sort by runtime descending for ranking
    by_runtime = sorted(triaged, key=lambda t: -t.survey.runtime_ms)

    rank_map: Dict[str, int] = {}
    pct_map: Dict[str, float] = {}
    cum_map: Dict[str, float] = {}
    cumulative = 0.0
    for rank, t in enumerate(by_runtime, 1):
        rank_map[t.query_id] = rank
        pct = (t.survey.runtime_ms / total_runtime) if total_runtime > 0 else 0.0
        pct_map[t.query_id] = round(pct, 4)
        cumulative += pct
        cum_map[t.query_id] = round(cumulative, 4)

    result: List[Dict[str, Any]] = []
    for t in triaged:
        # Build transform matches from survey detection results
        transforms = []
        top_transform = ""
        top_overlap = 0.0
        for m in t.survey.matched_transforms:
            tid = getattr(m, "id", getattr(m, "transform_id", ""))
            overlap = getattr(m, "overlap_ratio", getattr(m, "overlap", 0.0))
            gap = getattr(m, "gap", "")
            family = getattr(m, "family", "")
            transforms.append({
                "id": tid,
                "overlap": round(overlap, 2),
                "gap": gap,
                "family": family,
            })
            if not top_transform:
                top_transform = tid
                top_overlap = round(overlap, 2)

        rt = t.survey.runtime_ms if t.survey.runtime_ms > 0 else 0
        est_annual = round(rt * 12) if rt > 0 else 0

        entry = {
            "id": t.query_id,
            "sql": t.sql,
            "runtime_ms": rt,
            "bucket": t.bucket,
            "iters": t.max_iterations,
            "transform": top_transform,
            "overlap": top_overlap,
            "est_annual": est_annual,
            "outcome": None,
            "speedup": None,
            "out_transform": "",
            "detail": {
                "cost_rank": rank_map.get(t.query_id, 0),
                "pct_of_total": pct_map.get(t.query_id, 0.0),
                "cumulative_pct": cum_map.get(t.query_id, 0.0),
                "structural_flags": [],
                "transforms": transforms,
                "qerror": None,
                "explain": getattr(t.survey, "explain_text", ""),
                "actual_rows": getattr(t.survey, "actual_rows", 0),
                "timing_source": getattr(t.survey, "timing_source", ""),
            },
        }
        result.append(entry)

    return result


def forensic_to_fleet_c2(
    forensic_queries: List["ForensicQuery"],
    triaged: List["TriageResult"],
) -> List[Dict[str, Any]]:
    """Map ForensicQuery + TriageResult → fleet_c2.html QUERIES JSON format.

    Produces the same shape as the mock QUERIES array in fleet_c2.html so
    the frontend can consume live data with zero changes.
    """
    # Build triage lookup: query_id → TriageResult
    triage_map = {t.query_id: t for t in triaged}

    result: List[Dict[str, Any]] = []
    for fq in forensic_queries:
        tr = triage_map.get(fq.query_id)
        iters = tr.max_iterations if tr else 0

        # Build transform matches for detail panel
        transforms = []
        for m in fq.matched_transforms:
            transforms.append({
                "id": m.id,
                "overlap": m.overlap,
                "gap": m.gap,
                "family": m.family,
            })

        # Q-error entry
        qerror = None
        if fq.qerror:
            qe = fq.qerror
            qerror = {
                "severity": qe.severity,
                "direction": qe.direction,
                "worst_node": qe.worst_node,
                "max_q_error": qe.max_q_error,
            }

        # Estimated annual cost (runtime_ms * 12 scaling factor, simplified)
        est_annual = round(fq.runtime_ms * 12) if fq.runtime_ms > 0 else 0

        entry = {
            "id": fq.query_id,
            "sql": tr.sql if tr else "",
            "runtime_ms": fq.runtime_ms,
            "bucket": fq.bucket,
            "iters": iters,
            "transform": fq.top_transform,
            "overlap": fq.top_overlap,
            "est_annual": est_annual,
            "outcome": None,
            "speedup": None,
            "out_transform": "",
            "detail": {
                "cost_rank": fq.cost_rank,
                "pct_of_total": fq.pct_of_total,
                "cumulative_pct": fq.cumulative_pct,
                "structural_flags": fq.structural_flags,
                "transforms": transforms,
                "qerror": qerror,
                "explain": fq.explain_text,
            },
        }
        result.append(entry)

    return result
