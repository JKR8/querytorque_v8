"""Tests for fleet event_bus: EventBus, FleetEvent, forensic_to_fleet_c2."""

import json
import threading
import time

import pytest

from qt_sql.fleet.event_bus import EventBus, EventType, FleetEvent, forensic_to_fleet_c2
from qt_sql.dashboard.models import ForensicQuery, ForensicTransformMatch, QErrorEntry
from qt_sql.fleet.orchestrator import TriageResult, SurveyResult


# ---------------------------------------------------------------------------
# FleetEvent
# ---------------------------------------------------------------------------

class TestFleetEvent:
    def test_to_json_basic(self):
        ev = FleetEvent(type=EventType.QUERY_UPDATE, data={"qid": "q1"}, timestamp=1000.0)
        parsed = json.loads(ev.to_json())
        assert parsed["type"] == "query_update"
        assert parsed["data"]["qid"] == "q1"
        assert parsed["timestamp"] == 1000.0

    def test_to_json_default_str(self):
        """Non-serializable values should be str()-ified via default=str."""
        from pathlib import Path
        ev = FleetEvent(type=EventType.FLEET_DONE, data={"path": Path("/tmp")})
        parsed = json.loads(ev.to_json())
        assert parsed["data"]["path"] == "/tmp"

    def test_default_timestamp(self):
        before = time.time()
        ev = FleetEvent(type=EventType.EVENT_LOG)
        after = time.time()
        assert before <= ev.timestamp <= after

    def test_default_data(self):
        ev = FleetEvent(type=EventType.TRIAGE_READY)
        assert ev.data == {}


# ---------------------------------------------------------------------------
# EventBus
# ---------------------------------------------------------------------------

class TestEventBus:
    def test_emit_and_get(self):
        bus = EventBus()
        bus.emit(EventType.QUERY_UPDATE, query_id="q5", status="RUNNING")
        ev = bus.get_event(timeout=1.0)
        assert ev is not None
        assert ev.type == EventType.QUERY_UPDATE
        assert ev.data["query_id"] == "q5"
        assert ev.data["status"] == "RUNNING"

    def test_get_returns_none_on_empty(self):
        bus = EventBus()
        ev = bus.get_event(timeout=0.01)
        assert ev is None

    def test_drain_returns_batch(self):
        bus = EventBus()
        for i in range(5):
            bus.emit(EventType.EVENT_LOG, idx=i)
        events = bus.drain(max_events=10)
        assert len(events) == 5
        assert [e.data["idx"] for e in events] == [0, 1, 2, 3, 4]

    def test_drain_respects_max(self):
        bus = EventBus()
        for i in range(10):
            bus.emit(EventType.EVENT_LOG, idx=i)
        events = bus.drain(max_events=3)
        assert len(events) == 3
        # Remaining 7 still in queue
        rest = bus.drain(max_events=50)
        assert len(rest) == 7

    def test_drain_empty(self):
        bus = EventBus()
        events = bus.drain()
        assert events == []

    def test_emit_drops_when_full(self):
        bus = EventBus(maxsize=2)
        bus.emit(EventType.EVENT_LOG, idx=0)
        bus.emit(EventType.EVENT_LOG, idx=1)
        # Queue is full — this should be silently dropped
        bus.emit(EventType.EVENT_LOG, idx=2)
        events = bus.drain()
        assert len(events) == 2
        assert events[0].data["idx"] == 0
        assert events[1].data["idx"] == 1

    def test_thread_safety(self):
        """Multiple threads emitting concurrently should not raise."""
        bus = EventBus(maxsize=500)

        def _emitter(thread_id):
            for i in range(50):
                bus.emit(EventType.EVENT_LOG, thread=thread_id, idx=i)

        threads = [threading.Thread(target=_emitter, args=(t,)) for t in range(5)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        events = bus.drain(max_events=500)
        assert len(events) == 250  # 5 threads * 50 events


# ---------------------------------------------------------------------------
# forensic_to_fleet_c2 mapper
# ---------------------------------------------------------------------------

def _make_forensic(qid, runtime_ms=5000, bucket="HIGH", top_transform="or_to_union",
                   top_overlap=0.85, transforms=None, qerror=None):
    """Helper to build a ForensicQuery with sensible defaults."""
    return ForensicQuery(
        query_id=qid,
        runtime_ms=runtime_ms,
        bucket=bucket,
        top_transform=top_transform,
        top_overlap=top_overlap,
        matched_transforms=transforms or [],
        cost_rank=1,
        pct_of_total=0.5,
        cumulative_pct=0.5,
        structural_flags=["OR_PREDICATE"],
        qerror=qerror,
        explain_text="Seq Scan on t1",
    )


def _make_triage(qid, max_iterations=3):
    survey = SurveyResult(query_id=qid, runtime_ms=5000)
    return TriageResult(
        query_id=qid, sql="SELECT 1", bucket="HIGH",
        priority_score=10.0, max_iterations=max_iterations,
        survey=survey,
    )


class TestForensicToFleetC2:
    def test_basic_mapping(self):
        fq = _make_forensic("q1")
        tr = _make_triage("q1", max_iterations=3)
        result = forensic_to_fleet_c2([fq], [tr])

        assert len(result) == 1
        entry = result[0]
        assert entry["id"] == "q1"
        assert entry["runtime_ms"] == 5000
        assert entry["bucket"] == "HIGH"
        assert entry["iters"] == 3
        assert entry["transform"] == "or_to_union"
        assert entry["overlap"] == 0.85
        assert entry["outcome"] is None
        assert entry["speedup"] is None
        assert entry["out_transform"] == ""

    def test_detail_section(self):
        fq = _make_forensic("q1")
        tr = _make_triage("q1")
        result = forensic_to_fleet_c2([fq], [tr])
        detail = result[0]["detail"]

        assert detail["cost_rank"] == 1
        assert detail["pct_of_total"] == 0.5
        assert detail["cumulative_pct"] == 0.5
        assert detail["structural_flags"] == ["OR_PREDICATE"]
        assert detail["explain"] == "Seq Scan on t1"

    def test_transform_matches(self):
        matches = [
            ForensicTransformMatch(id="or_to_union", overlap=0.85, gap="OR_DECOMP", family="D"),
            ForensicTransformMatch(id="decorrelate", overlap=0.6, gap="CORR_SUB", family="B"),
        ]
        fq = _make_forensic("q1", transforms=matches)
        tr = _make_triage("q1")
        result = forensic_to_fleet_c2([fq], [tr])

        transforms = result[0]["detail"]["transforms"]
        assert len(transforms) == 2
        assert transforms[0] == {"id": "or_to_union", "overlap": 0.85, "gap": "OR_DECOMP", "family": "D"}
        assert transforms[1] == {"id": "decorrelate", "overlap": 0.6, "gap": "CORR_SUB", "family": "B"}

    def test_qerror(self):
        qe = QErrorEntry(severity="S2", direction="UNDER_EST", worst_node="Hash Join", max_q_error=42.5)
        fq = _make_forensic("q1", qerror=qe)
        tr = _make_triage("q1")
        result = forensic_to_fleet_c2([fq], [tr])

        qerror = result[0]["detail"]["qerror"]
        assert qerror["severity"] == "S2"
        assert qerror["direction"] == "UNDER_EST"
        assert qerror["worst_node"] == "Hash Join"
        assert qerror["max_q_error"] == 42.5

    def test_no_qerror(self):
        fq = _make_forensic("q1", qerror=None)
        tr = _make_triage("q1")
        result = forensic_to_fleet_c2([fq], [tr])
        assert result[0]["detail"]["qerror"] is None

    def test_missing_triage(self):
        """Query with no matching triage entry should get iters=0."""
        fq = _make_forensic("q99")
        result = forensic_to_fleet_c2([fq], [])  # no triage entries
        assert result[0]["iters"] == 0

    def test_est_annual(self):
        fq = _make_forensic("q1", runtime_ms=1000)
        tr = _make_triage("q1")
        result = forensic_to_fleet_c2([fq], [tr])
        assert result[0]["est_annual"] == 12000  # 1000 * 12

    def test_est_annual_zero_runtime(self):
        fq = _make_forensic("q1", runtime_ms=0)
        tr = _make_triage("q1")
        result = forensic_to_fleet_c2([fq], [tr])
        assert result[0]["est_annual"] == 0

    def test_multiple_queries(self):
        fqs = [_make_forensic(f"q{i}", runtime_ms=i * 1000) for i in range(1, 4)]
        trs = [_make_triage(f"q{i}", max_iterations=i) for i in range(1, 4)]
        result = forensic_to_fleet_c2(fqs, trs)

        assert len(result) == 3
        assert [r["id"] for r in result] == ["q1", "q2", "q3"]
        assert [r["iters"] for r in result] == [1, 2, 3]

    def test_output_is_json_serializable(self):
        fq = _make_forensic("q1")
        tr = _make_triage("q1")
        result = forensic_to_fleet_c2([fq], [tr])
        # Should not raise
        json.dumps(result)
