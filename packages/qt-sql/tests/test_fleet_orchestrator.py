"""Tests for fleet orchestrator: _emit, wait_for_triage_approval, pause_event, triage logic."""

import json
import threading
import time

import pytest

from qt_sql.fleet.event_bus import EventBus, EventType
from qt_sql.fleet.orchestrator import (
    FleetOrchestrator,
    SurveyResult,
    TriageResult,
    RUNTIME_THRESHOLDS,
    RUNTIME_WEIGHTS,
    MAX_ITERS_BASE,
)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def bus():
    return EventBus()


@pytest.fixture
def orch(bus):
    """Orchestrator with mocked pipeline and event bus — no real LLM calls."""
    from unittest.mock import MagicMock
    from pathlib import Path

    pipeline = MagicMock()
    pipeline.config.engine = "duckdb"

    return FleetOrchestrator(
        pipeline=pipeline,
        benchmark_dir=Path("/tmp/fake_bench"),
        concurrency=2,
        event_bus=bus,
        triage_gate=threading.Event(),
        pause_event=threading.Event(),
    )


# ---------------------------------------------------------------------------
# _emit
# ---------------------------------------------------------------------------

class TestEmit:
    def test_emit_puts_event_on_bus(self, orch, bus):
        orch._emit(EventType.QUERY_UPDATE, query_id="q1", status="RUNNING")
        ev = bus.get_event(timeout=1.0)
        assert ev is not None
        assert ev.type == EventType.QUERY_UPDATE
        assert ev.data["query_id"] == "q1"

    def test_emit_noop_without_bus(self):
        from unittest.mock import MagicMock
        from pathlib import Path
        orch = FleetOrchestrator(
            pipeline=MagicMock(),
            benchmark_dir=Path("/tmp"),
            event_bus=None,
        )
        # Should not raise
        orch._emit(EventType.FLEET_DONE, msg="done")

    def test_emit_with_string_event_type(self, orch, bus):
        """_emit passes through to bus.emit — works with string too."""
        orch._emit("query_complete", query_id="q2", status="WIN")
        ev = bus.get_event(timeout=1.0)
        assert ev is not None
        assert ev.data["query_id"] == "q2"


class TestRuntimeConfig:
    def test_apply_runtime_config_overrides_db_and_policy(self, tmp_path):
        from types import SimpleNamespace

        cfg_path = tmp_path / ".fleet_runtime_config.json"
        cfg_path.write_text(
            json.dumps(
                {
                    "source_mode": "local",
                    "db_dsn": "postgresql://runtime-db",
                    "explain_policy": "explain",
                }
            )
        )

        bus = EventBus()
        pipeline = SimpleNamespace(
            config=SimpleNamespace(
                engine="duckdb",
                db_path_or_dsn=":memory:",
                benchmark_dsn=":memory:",
            )
        )
        orch = FleetOrchestrator(
            pipeline=pipeline,
            benchmark_dir=tmp_path,
            event_bus=bus,
        )

        orch._apply_runtime_config()

        assert pipeline.config.db_path_or_dsn == "postgresql://runtime-db"
        assert pipeline.config.benchmark_dsn == "postgresql://runtime-db"
        assert getattr(pipeline.config, "explain_policy") == "explain"
        ev = bus.get_event(timeout=1.0)
        assert ev is not None
        assert ev.type == EventType.EVENT_LOG


# ---------------------------------------------------------------------------
# wait_for_triage_approval
# ---------------------------------------------------------------------------

class TestWaitForTriageApproval:
    def test_returns_true_when_no_gate(self):
        from unittest.mock import MagicMock
        from pathlib import Path
        orch = FleetOrchestrator(
            pipeline=MagicMock(),
            benchmark_dir=Path("/tmp"),
            triage_gate=None,
        )
        assert orch.wait_for_triage_approval() is True

    def test_blocks_until_set(self, orch):
        gate = orch.triage_gate
        result = [None]

        def _waiter():
            result[0] = orch.wait_for_triage_approval(timeout=5.0)

        t = threading.Thread(target=_waiter)
        t.start()
        time.sleep(0.05)
        assert result[0] is None  # still blocked
        gate.set()
        t.join(timeout=2.0)
        assert result[0] is True

    def test_timeout_returns_false(self):
        from unittest.mock import MagicMock
        from pathlib import Path
        orch = FleetOrchestrator(
            pipeline=MagicMock(),
            benchmark_dir=Path("/tmp"),
            triage_gate=threading.Event(),
        )
        # Gate never set — should timeout
        result = orch.wait_for_triage_approval(timeout=0.05)
        assert result is False


# ---------------------------------------------------------------------------
# pause_event integration
# ---------------------------------------------------------------------------

class TestPauseEvent:
    def test_pause_event_stored(self, orch):
        assert orch.pause_event is not None

    def test_no_pause_event(self):
        from unittest.mock import MagicMock
        from pathlib import Path
        orch = FleetOrchestrator(
            pipeline=MagicMock(),
            benchmark_dir=Path("/tmp"),
            pause_event=None,
        )
        assert orch.pause_event is None


# ---------------------------------------------------------------------------
# Triage: _bucket_runtime
# ---------------------------------------------------------------------------

class TestBucketRuntime:
    @pytest.mark.parametrize("ms,expected", [
        (50, "SKIP"),       # < 100ms
        (99, "SKIP"),       # just under 100ms
        (100, "LOW"),       # == 100ms
        (500, "LOW"),       # < 1000ms
        (999, "LOW"),
        (1000, "MEDIUM"),   # == 1000ms
        (5000, "MEDIUM"),
        (9999, "MEDIUM"),
        (10000, "HIGH"),    # == 10000ms
        (50000, "HIGH"),
    ])
    def test_thresholds(self, ms, expected):
        assert FleetOrchestrator._bucket_runtime(ms) == expected

    def test_negative_runtime(self):
        """Unknown runtime (-1) defaults to MEDIUM."""
        assert FleetOrchestrator._bucket_runtime(-1) == "MEDIUM"


# ---------------------------------------------------------------------------
# Triage: _compute_priority
# ---------------------------------------------------------------------------

class TestComputePriority:
    def test_skip_is_zero(self):
        sv = SurveyResult(query_id="q1", runtime_ms=50, tractability=3, structural_bonus=0.9)
        # SKIP weight = 0, so priority = 0 regardless of tractability
        assert FleetOrchestrator._compute_priority(sv, "SKIP") == 0.0

    def test_high_priority(self):
        sv = SurveyResult(query_id="q1", runtime_ms=15000, tractability=2, structural_bonus=0.8)
        # HIGH weight = 5, priority = 5 * (1.0 + 2 + 0.8) = 19.0
        assert FleetOrchestrator._compute_priority(sv, "HIGH") == 19.0

    def test_medium_no_tractability(self):
        sv = SurveyResult(query_id="q1", runtime_ms=3000, tractability=0, structural_bonus=0.0)
        # MEDIUM weight = 3, priority = 3 * (1.0 + 0 + 0.0) = 3.0
        assert FleetOrchestrator._compute_priority(sv, "MEDIUM") == 3.0


# ---------------------------------------------------------------------------
# Triage: _compute_max_iterations
# ---------------------------------------------------------------------------

class TestComputeMaxIterations:
    @pytest.mark.parametrize("bucket,tract,expected", [
        ("SKIP", 0, 0),
        ("LOW", 0, 1),
        ("MEDIUM", 0, 2),
        ("MEDIUM", 1, 2),
        ("MEDIUM", 2, 3),    # tractability >= 2 → boost to 3
        ("HIGH", 0, 3),
        ("HIGH", 1, 3),
        ("HIGH", 2, 5),      # tractability >= 2 → boost to 5
        ("HIGH", 5, 5),
    ])
    def test_iterations(self, bucket, tract, expected):
        assert FleetOrchestrator._compute_max_iterations(bucket, tract) == expected


# ---------------------------------------------------------------------------
# Triage: full triage()
# ---------------------------------------------------------------------------

class TestTriage:
    def test_sorted_by_priority_descending(self, orch):
        surveys = {
            "q1": SurveyResult(query_id="q1", runtime_ms=50),    # SKIP → 0 priority
            "q2": SurveyResult(query_id="q2", runtime_ms=15000), # HIGH → 5
            "q3": SurveyResult(query_id="q3", runtime_ms=3000),  # MEDIUM → 3
        }
        queries = {"q1": "SELECT 1", "q2": "SELECT 2", "q3": "SELECT 3"}

        results = orch.triage(surveys, queries)
        assert [r.query_id for r in results] == ["q2", "q3", "q1"]
        assert results[0].bucket == "HIGH"
        assert results[1].bucket == "MEDIUM"
        assert results[2].bucket == "SKIP"

    def test_triage_preserves_sql(self, orch):
        surveys = {"q1": SurveyResult(query_id="q1", runtime_ms=5000)}
        queries = {"q1": "SELECT * FROM big_table"}
        results = orch.triage(surveys, queries)
        assert results[0].sql == "SELECT * FROM big_table"


class TestTriageHistory:
    def test_triage_uses_prior_history_for_priority_and_seed(self, tmp_path):
        from types import SimpleNamespace

        prior_sql = "SELECT * FROM optimized_history"
        session_dir = tmp_path / "beam_sessions" / "query_7_20260216_010203"
        session_dir.mkdir(parents=True)
        (session_dir / "iter0_result.txt").write_text(
            json.dumps({"best_speedup": 1.8, "best_sql": prior_sql})
        )

        orch = FleetOrchestrator(
            pipeline=SimpleNamespace(config=SimpleNamespace(engine="duckdb")),
            benchmark_dir=tmp_path,
        )
        surveys = {"q7": SurveyResult(query_id="q7", runtime_ms=15000)}
        queries = {"q7": "SELECT * FROM original_query"}

        results = orch.triage(surveys, queries)
        assert len(results) == 1
        tri = results[0]
        assert tri.seed_sql == prior_sql
        assert tri.prior_best_sql == prior_sql
        assert tri.prior_source == "beam_sessions"
        # Base HIGH priority is 5.0 (tractability=0/bonus=0), history 1.8x => 9.0.
        assert tri.priority_score == pytest.approx(9.0, rel=1e-6)

    def test_triage_disables_history_when_runtime_config_says_off(self, tmp_path):
        from types import SimpleNamespace

        session_dir = tmp_path / "beam_sessions" / "query_8_20260216_010203"
        session_dir.mkdir(parents=True)
        (session_dir / "iter0_result.txt").write_text(
            json.dumps({"best_speedup": 2.2, "best_sql": "SELECT * FROM prior"})
        )
        (tmp_path / ".fleet_runtime_config.json").write_text(
            json.dumps({"use_blackboard_history": False})
        )

        orch = FleetOrchestrator(
            pipeline=SimpleNamespace(config=SimpleNamespace(engine="duckdb")),
            benchmark_dir=tmp_path,
        )
        surveys = {"query_8": SurveyResult(query_id="query_8", runtime_ms=15000)}
        queries = {"query_8": "SELECT * FROM original_query"}

        results = orch.triage(surveys, queries)
        assert len(results) == 1
        tri = results[0]
        assert tri.seed_sql == "SELECT * FROM original_query"
        assert tri.prior_best_sql == ""
        assert tri.prior_best_speedup is None
        assert tri.priority_score == pytest.approx(5.0, rel=1e-6)


class TestExecuteHistory:
    def test_execute_uses_original_sql_when_history_disabled(self, tmp_path):
        from types import SimpleNamespace
        from unittest.mock import MagicMock

        pipeline = MagicMock()
        pipeline.config = SimpleNamespace(
            engine="duckdb",
            db_path_or_dsn=":memory:",
            benchmark_dsn=":memory:",
        )
        pipeline.run_optimization_session.return_value = SimpleNamespace(
            status="WIN",
            best_speedup=1.2,
        )
        (tmp_path / ".fleet_runtime_config.json").write_text(
            json.dumps({"use_blackboard_history": False})
        )

        orch = FleetOrchestrator(
            pipeline=pipeline,
            benchmark_dir=tmp_path,
            concurrency=1,
        )
        triaged = [
            TriageResult(
                query_id="query_9",
                sql="SELECT * FROM original_query",
                bucket="HIGH",
                priority_score=10.0,
                max_iterations=1,
                survey=SurveyResult(query_id="query_9", runtime_ms=11000),
                seed_sql="SELECT * FROM prior_query",
                prior_best_speedup=2.0,
                prior_best_sql="SELECT * FROM prior_query",
                prior_source="beam_sessions",
            )
        ]

        out = tmp_path / "out"
        out.mkdir()
        results = orch.execute(
            triaged=triaged,
            completed_ids=set(),
            out=out,
            checkpoint_path=tmp_path / "checkpoint.json",
        )

        assert results[0]["status"] == "WIN"
        assert pipeline.run_optimization_session.call_args.kwargs["sql"] == "SELECT * FROM original_query"

    def test_execute_seeds_from_history_artifact_when_enabled(self, tmp_path):
        from types import SimpleNamespace
        from unittest.mock import MagicMock

        prior_sql = "SELECT * FROM optimized_seed"
        session_dir = tmp_path / "beam_sessions" / "query_10_20260216_010203"
        session_dir.mkdir(parents=True)
        (session_dir / "iter0_result.txt").write_text(
            json.dumps({"best_speedup": 1.6, "best_sql": prior_sql})
        )
        (tmp_path / ".fleet_runtime_config.json").write_text(
            json.dumps({"use_blackboard_history": True})
        )

        pipeline = MagicMock()
        pipeline.config = SimpleNamespace(
            engine="duckdb",
            db_path_or_dsn=":memory:",
            benchmark_dsn=":memory:",
        )
        pipeline.run_optimization_session.return_value = SimpleNamespace(
            status="WIN",
            best_speedup=1.4,
        )

        orch = FleetOrchestrator(
            pipeline=pipeline,
            benchmark_dir=tmp_path,
            concurrency=1,
        )
        triaged = [
            TriageResult(
                query_id="q10",
                sql="SELECT * FROM original_query",
                bucket="HIGH",
                priority_score=10.0,
                max_iterations=1,
                survey=SurveyResult(query_id="q10", runtime_ms=11000),
            )
        ]

        out = tmp_path / "out"
        out.mkdir()
        results = orch.execute(
            triaged=triaged,
            completed_ids=set(),
            out=out,
            checkpoint_path=tmp_path / "checkpoint.json",
        )

        assert results[0]["status"] == "WIN"
        assert pipeline.run_optimization_session.call_args.kwargs["sql"] == prior_sql


# ---------------------------------------------------------------------------
# Compile
# ---------------------------------------------------------------------------

class TestCompile:
    def test_scorecard_contains_key_sections(self, orch):
        surveys = {
            "q1": SurveyResult(query_id="q1", runtime_ms=15000, tractability=2),
            "q2": SurveyResult(query_id="q2", runtime_ms=3000),
        }
        queries = {"q1": "SELECT 1", "q2": "SELECT 2"}
        triaged = orch.triage(surveys, queries)

        results = [
            {"query_id": "q1", "status": "WIN", "speedup": 2.5},
            {"query_id": "q2", "status": "NEUTRAL", "speedup": 1.01},
        ]
        scorecard = orch.compile(results, triaged)

        assert "# Fleet Scorecard" in scorecard
        assert "## Summary" in scorecard
        assert "WIN" in scorecard
        assert "NEUTRAL" in scorecard
        assert "## Top Winners" in scorecard
        assert "q1" in scorecard
        assert "2.50x" in scorecard
        assert "## Triage Distribution" in scorecard

    def test_pct_helper(self):
        assert FleetOrchestrator._pct(3, 10) == "30%"
        assert FleetOrchestrator._pct(0, 0) == "0%"
        assert FleetOrchestrator._pct(1, 3) == "33%"
