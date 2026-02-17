from __future__ import annotations

import json
from pathlib import Path
from types import SimpleNamespace

import pytest

from qt_sql.pipeline import Pipeline
from qt_sql.patches.beam_wide_prompts import build_beam_worker_retry_prompt
from qt_sql.schemas import OptimizationMode
from qt_sql.sessions.beam_session import AppliedPatch, BeamSession


def _make_session() -> BeamSession:
    config = SimpleNamespace(
        engine="duckdb",
        benchmark="tpcds",
        db_path_or_dsn=":memory:",
        benchmark_dsn=":memory:",
        scale_factor=1,
        semantic_validation_enabled=True,
        semantic_sample_pct=0.0,
        validation_method="3-run",
    )
    pipeline = SimpleNamespace(
        config=config,
        benchmark_dir=Path("/tmp"),
        provider=None,
        model=None,
        analyze_fn=None,
    )
    return BeamSession(
        pipeline=pipeline,
        query_id="query_guard",
        original_sql="SELECT 1",
    )


def test_equivalence_failure_blocks_benchmark_and_marks_patch_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import qt_sql.execution.factory as execution_factory
    import qt_sql.validation.mini_validator as mini_validator

    class DummyMiniValidator:
        def __init__(self, *args, **kwargs):
            pass

        def _tier1_structural(self, original_sql: str, optimized_sql: str) -> dict:
            return {"passed": True}

    def fail_executor(*args, **kwargs):
        raise RuntimeError("executor unavailable")

    session = _make_session()

    def should_not_benchmark(*args, **kwargs):
        raise AssertionError("benchmark should not run when equivalence is unavailable")

    monkeypatch.setattr(mini_validator, "MiniValidator", DummyMiniValidator)
    monkeypatch.setattr(execution_factory, "create_executor_from_dsn", fail_executor)
    monkeypatch.setattr(session, "_sequential_benchmark", should_not_benchmark)

    patch = AppliedPatch(
        patch_id="p01",
        family="A",
        transform="push_filter",
        relevance_score=0.9,
        output_sql="SELECT 1",
    )
    session._validate_and_benchmark_patches(
        patches=[patch],
        db_path=":memory:",
        session_dir=Path("/tmp"),
        shot=0,
    )

    assert patch.semantic_passed is False
    assert patch.status == "ERROR"
    assert patch.apply_error is not None
    assert "Equivalence check unavailable" in patch.apply_error


def test_synthetic_failure_blocks_db_equivalence_and_benchmark(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import qt_sql.execution.factory as execution_factory
    import qt_sql.validation.mini_validator as mini_validator
    import qt_sql.validation.synthetic_validator as synthetic_validator

    class DummyMiniValidator:
        def __init__(self, *args, **kwargs):
            pass

        def _tier1_structural(self, original_sql: str, optimized_sql: str) -> dict:
            return {"passed": True}

    class DummySyntheticValidator:
        def __init__(self, *args, **kwargs):
            pass

        def validate_sql_pair(self, *args, **kwargs) -> dict:
            return {"match": False, "reason": "forced mismatch"}

    def should_not_run_equivalence(*args, **kwargs):
        raise AssertionError(
            "full-dataset equivalence should not run when synthetic gate fails"
        )

    session = _make_session()

    def should_not_benchmark(*args, **kwargs):
        raise AssertionError("benchmark should not run when synthetic gate fails")

    monkeypatch.setattr(mini_validator, "MiniValidator", DummyMiniValidator)
    monkeypatch.setattr(
        synthetic_validator,
        "SyntheticValidator",
        DummySyntheticValidator,
    )
    monkeypatch.setattr(
        execution_factory,
        "create_executor_from_dsn",
        should_not_run_equivalence,
    )
    monkeypatch.setattr(session, "_sequential_benchmark", should_not_benchmark)

    patch = AppliedPatch(
        patch_id="p01",
        family="A",
        transform="push_filter",
        relevance_score=0.9,
        output_sql="SELECT 1",
    )
    session._validate_and_benchmark_patches(
        patches=[patch],
        db_path=":memory:",
        session_dir=Path("/tmp"),
        shot=0,
    )

    assert patch.semantic_passed is False
    assert patch.status == "FAIL"
    assert patch.apply_error is not None
    assert "Synthetic semantic mismatch" in patch.apply_error


def _make_pipeline(tmp_path: Path) -> Pipeline:
    bench_dir = tmp_path / "bench"
    bench_dir.mkdir(parents=True, exist_ok=True)
    (bench_dir / "config.json").write_text(
        json.dumps(
            {
                "engine": "duckdb",
                "benchmark": "tpcds",
                "db_path": ":memory:",
            }
        )
    )
    return Pipeline(bench_dir)


def test_run_optimization_session_preserves_configured_benchmark_dsn(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    class StubBeamSession:
        def __init__(self, pipeline, **kwargs):
            self.pipeline = pipeline

        def run(self):
            return self.pipeline.config.benchmark_dsn

    monkeypatch.setattr("qt_sql.sessions.beam_session.BeamSession", StubBeamSession)
    pipeline = _make_pipeline(tmp_path)
    pipeline.config.db_path_or_dsn = "db-primary"
    pipeline.config.benchmark_dsn = "db-benchmark"

    result = pipeline.run_optimization_session("q1", "SELECT 1")

    assert result == "db-benchmark"


def test_run_optimization_session_falls_back_to_db_path_when_benchmark_dsn_missing(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    class StubBeamSession:
        def __init__(self, pipeline, **kwargs):
            self.pipeline = pipeline

        def run(self):
            return self.pipeline.config.benchmark_dsn

    monkeypatch.setattr("qt_sql.sessions.beam_session.BeamSession", StubBeamSession)
    pipeline = _make_pipeline(tmp_path)
    pipeline.config.db_path_or_dsn = "db-primary"
    pipeline.config.benchmark_dsn = ""

    result = pipeline.run_optimization_session("q1", "SELECT 1")

    assert result == "db-primary"


def test_worker_retry_revalidates_tier1_failure(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    session = _make_session()

    patch = AppliedPatch(
        patch_id="p01",
        family="B",
        transform="decorrelate",
        relevance_score=0.8,
        output_sql="SELECT * FROM t",
        apply_error="Tier-1: missing alias x in FROM",
        worker_prompt="BASE WORKER PROMPT",
        worker_response='{"steps":[]}',
        status="FAIL",
    )

    def fake_apply(_response, _script_ir, _dialect_enum):
        return "SELECT 2"

    def fake_validate(patches, db_path, session_dir, shot):
        for p in patches:
            if p.output_sql == "SELECT 2":
                p.semantic_passed = True
                p.status = "WIN"
                p.speedup = 1.2

    monkeypatch.setattr(session, "_apply_wide_worker_response", fake_apply)
    monkeypatch.setattr(session, "_validate_and_benchmark_patches", fake_validate)
    monkeypatch.setattr(session, "_save_to_disk", lambda *args, **kwargs: None)

    retry_calls = session._retry_tier1_worker_failures(
        patches=[patch],
        worker_call_fn=lambda prompt: '{"plan_id":"p01","steps":[]}',
        build_retry_prompt_fn=build_beam_worker_retry_prompt,
        script_ir=None,
        dialect_enum=None,
        db_path=":memory:",
        session_dir=tmp_path,
        shot=0,
    )

    assert retry_calls == 1
    assert patch.output_sql == "SELECT 2"
    assert patch.semantic_passed is True
    assert patch.status == "WIN"


def test_run_forces_beam_when_reasoning_not_enabled(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    session = _make_session()
    session.pipeline.config.beam_mode = "reasoning"
    session.pipeline.config.enable_reasoning_mode = False

    monkeypatch.setattr(session, "_run_reasoning", lambda: "reasoning")
    monkeypatch.setattr(session, "_run_beam", lambda: "beam")

    assert session.run() == "beam"


def test_run_optimization_session_reasoning_argument_falls_back_to_beam_when_disabled(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    class StubBeamSession:
        def __init__(self, pipeline, **kwargs):
            self.pipeline = pipeline

        def run(self):
            return self.pipeline.config.beam_mode

    monkeypatch.setattr("qt_sql.sessions.beam_session.BeamSession", StubBeamSession)
    pipeline = _make_pipeline(tmp_path)

    result = pipeline.run_optimization_session(
        "q1",
        "SELECT 1",
        mode=OptimizationMode.REASONING,
    )

    assert result == "beam"


def test_run_routes_to_reasoning_mode_when_explicitly_enabled(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    session = _make_session()
    session.pipeline.config.beam_mode = "reasoning"
    session.pipeline.config.enable_reasoning_mode = True

    monkeypatch.setattr(session, "_run_reasoning", lambda: "reasoning")
    monkeypatch.setattr(session, "_run_beam", lambda: "beam")

    assert session.run() == "reasoning"


def test_editor_strike_uses_single_worker_call(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    import qt_sql.execution.database_utils as database_utils

    session = _make_session()
    calls = {"llm": 0, "prompt": ""}

    monkeypatch.setattr(session, "_create_session_dir", lambda: tmp_path)
    monkeypatch.setattr(session, "_save_to_disk", lambda *args, **kwargs: None)
    monkeypatch.setattr(session, "_build_schema_context", lambda _db: "")
    monkeypatch.setattr(session, "_lookup_transform_family", lambda _tid: "B")
    monkeypatch.setattr(
        database_utils,
        "run_explain_analyze",
        lambda _db, _sql: {
            "plan_text": "SEQ_SCAN t [100ms]",
            "execution_time_ms": 100.0,
        },
    )

    def fake_make_llm_call_fn(_model_spec=None):
        def _call(prompt: str) -> str:
            calls["llm"] += 1
            calls["prompt"] = prompt
            return '{"plan_id":"strike_plan_01","dialect":"duckdb","steps":[]}'

        return _call

    monkeypatch.setattr(session, "_make_llm_call_fn", fake_make_llm_call_fn)
    monkeypatch.setattr(
        session,
        "_apply_wide_worker_response",
        lambda *_args, **_kwargs: "SELECT 1",
    )

    def fake_validate(patches, db_path, session_dir, shot):
        patch = patches[0]
        patch.semantic_passed = True
        patch.speedup = 1.23
        patch.status = "WIN"

    monkeypatch.setattr(session, "_validate_and_benchmark_patches", fake_validate)

    result = session.run_editor_strike(transform_id="decorrelate")

    assert calls["llm"] == 1
    assert "transform_id: decorrelate" in calls["prompt"]
    assert result.mode == "strike"
    assert result.n_api_calls == 1
    assert result.best_speedup == 1.23
    assert result.best_transforms == ["decorrelate"]


def test_sniper_retry_error_patch_detection() -> None:
    session = _make_session()

    tier1_fail = AppliedPatch(
        patch_id="p1",
        family="A",
        transform="x",
        relevance_score=0.5,
        status="FAIL",
        apply_error="Tier-1: syntax error near FROM",
    )
    parse_fail = AppliedPatch(
        patch_id="p2",
        family="A",
        transform="x",
        relevance_score=0.5,
        status="FAIL",
        apply_error="Failed to parse/apply PatchPlan",
        output_sql=None,
    )
    hard_error = AppliedPatch(
        patch_id="p3",
        family="A",
        transform="x",
        relevance_score=0.5,
        status="ERROR",
        apply_error="Execution timeout",
    )
    slow_but_valid = AppliedPatch(
        patch_id="p4",
        family="A",
        transform="x",
        relevance_score=0.5,
        status="NEUTRAL",
        speedup=0.98,
        output_sql="SELECT 1",
        apply_error=None,
    )

    assert session._is_sniper_retry_error_patch(tier1_fail) is True
    assert session._is_sniper_retry_error_patch(parse_fail) is True
    assert session._is_sniper_retry_error_patch(hard_error) is True
    assert session._is_sniper_retry_error_patch(slow_but_valid) is False


def test_make_llm_call_fn_requires_global_model() -> None:
    session = _make_session()
    session.pipeline.provider = "openrouter"
    session.pipeline.model = ""

    with pytest.raises(RuntimeError, match="QT_LLM_MODEL"):
        session._make_llm_call_fn()


def test_pipeline_require_llm_config_requires_provider_and_model(tmp_path: Path) -> None:
    pipeline = _make_pipeline(tmp_path)
    pipeline.provider = ""
    pipeline.model = ""

    with pytest.raises(RuntimeError, match="QT_LLM_PROVIDER"):
        pipeline._require_llm_config(context="test")
