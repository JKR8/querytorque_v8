from __future__ import annotations

import json
from pathlib import Path
from types import SimpleNamespace

import pytest

from qt_sql.pipeline import Pipeline
from qt_sql.patches.beam_prompts import build_beam_worker_retry_prompt
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


def test_worker_retry_gate_classifies_parse_and_tier1_failures() -> None:
    tier1 = AppliedPatch(
        patch_id="p01",
        family="B",
        transform="decorrelate",
        relevance_score=0.8,
        output_sql="SELECT * FROM t",
        apply_error="Tier-1: missing alias x in FROM",
        status="FAIL",
    )
    parse_apply = AppliedPatch(
        patch_id="p02",
        family="B",
        transform="decorrelate",
        relevance_score=0.8,
        apply_error="Failed to parse/apply tree plan: Missing `tree` object",
        status="FAIL",
    )
    non_retry = AppliedPatch(
        patch_id="p03",
        family="B",
        transform="decorrelate",
        relevance_score=0.8,
        output_sql="SELECT * FROM t",
        apply_error="Row count: orig=10, patch=12",
        status="FAIL",
    )
    execution_fail = AppliedPatch(
        patch_id="p04",
        family="B",
        transform="decorrelate",
        relevance_score=0.8,
        output_sql="SELECT * FROM t",
        apply_error="Execution: SQL compilation error: invalid identifier 'X'",
        status="ERROR",
    )

    assert BeamSession._worker_retry_gate_name(tier1) == "tier1_structural"
    assert BeamSession._worker_retry_gate_name(parse_apply) == "parse_apply_failure"
    assert BeamSession._worker_retry_gate_name(non_retry) == "semantic_failure"
    assert BeamSession._worker_retry_gate_name(execution_fail) == "execution_failure"


def test_compiler_retry_error_patch_detection() -> None:
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

    assert session._is_compiler_retry_error_patch(tier1_fail) is True
    assert session._is_compiler_retry_error_patch(parse_fail) is True
    assert session._is_compiler_retry_error_patch(hard_error) is True
    assert session._is_compiler_retry_error_patch(slow_but_valid) is False


def test_worker_lane_suffix_is_scout() -> None:
    scout_text = BeamSession._worker_lane_suffix()
    assert "Scout Lane" in scout_text
    assert "Stay within ONE family strategy" in scout_text
    assert "status to 'failed'" in scout_text


def test_make_llm_call_fn_requires_global_model() -> None:
    session = _make_session()
    session.pipeline.provider = "openrouter"
    session.pipeline.model = ""

    with pytest.raises(RuntimeError, match="QT_LLM_MODEL"):
        session._make_llm_call_fn()


def test_make_llm_call_fn_accepts_beam_override_model_and_provider(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import qt_sql.generate as generate_mod

    session = _make_session()
    session.pipeline.provider = "openrouter"
    session.pipeline.model = "deepseek/deepseek-v3.2"
    session.pipeline.config.beam_llm_provider = "openrouter"
    session.pipeline.config.beam_llm_model = "qwen/qwen3-coder"

    captured = {}

    class StubGenerator:
        def __init__(self, provider=None, model=None, analyze_fn=None, enable_reasoning=None):
            captured["provider"] = provider
            captured["model"] = model
            captured["analyze_fn"] = analyze_fn
            captured["enable_reasoning"] = enable_reasoning
            self._llm_client = SimpleNamespace(last_usage={})

    monkeypatch.setattr(generate_mod, "CandidateGenerator", StubGenerator)
    monkeypatch.setattr(
        session,
        "_call_llm_with_timeout",
        lambda _generator, _prompt: "{}",
    )

    provider_override, model_override = session._beam_llm_override()
    call_fn = session._make_llm_call_fn(
        provider_spec=provider_override,
        model_spec=model_override,
    )
    call_fn("SELECT 1")

    assert captured["provider"] == "openrouter"
    assert captured["model"] == "qwen/qwen3-coder"


def test_pipeline_require_llm_config_requires_provider_and_model(tmp_path: Path) -> None:
    pipeline = _make_pipeline(tmp_path)
    pipeline.provider = ""
    pipeline.model = ""

    with pytest.raises(RuntimeError, match="QT_LLM_PROVIDER"):
        pipeline._require_llm_config(context="test")


def test_apply_tree_worker_response_allows_multiple_changed_nodes() -> None:
    session = _make_session()
    base_tree = {
        "root_node_id": "final_select",
        "nodes": [
            {
                "node_id": "final_select",
                "parent_node_id": None,
                "sources": ["n1"],
                "outputs": ["a"],
                "sql": "SELECT a FROM n1",
            },
            {
                "node_id": "n1",
                "parent_node_id": "final_select",
                "sources": [],
                "outputs": ["a"],
                "sql": "SELECT 1 AS a",
            },
        ],
    }

    valid = json.dumps(
        {
            "probe_id": "p01",
            "tree": {
                "root_node_id": "final_select",
                "nodes": [
                    {
                        "node_id": "final_select",
                        "parent_node_id": None,
                        "sources": ["n1"],
                        "outputs": ["a"],
                        "changed": False,
                    },
                    {
                        "node_id": "n1",
                        "parent_node_id": "final_select",
                        "sources": [],
                        "outputs": ["a"],
                        "changed": True,
                        "sql": "SELECT 2 AS a",
                    },
                ],
            },
        }
    )
    multi_changed = json.dumps(
        {
            "probe_id": "p01",
            "tree": {
                "root_node_id": "final_select",
                "nodes": [
                    {
                        "node_id": "final_select",
                        "parent_node_id": None,
                        "sources": ["n1"],
                        "outputs": ["a"],
                        "changed": True,
                        "sql": "SELECT a FROM n1",
                    },
                    {
                        "node_id": "n1",
                        "parent_node_id": "final_select",
                        "sources": [],
                        "outputs": ["a"],
                        "changed": True,
                        "sql": "SELECT 2 AS a",
                    },
                ],
            },
        }
    )
    syntax_invalid = json.dumps(
        {
            "probe_id": "p01",
            "tree": {
                "root_node_id": "final_select",
                "nodes": [
                    {
                        "node_id": "final_select",
                        "parent_node_id": None,
                        "sources": ["n1"],
                        "outputs": ["a"],
                        "changed": True,
                        "sql": "SELECT ( FROM n1",
                    },
                    {
                        "node_id": "n1",
                        "parent_node_id": "final_select",
                        "sources": [],
                        "outputs": ["a"],
                        "changed": True,
                        "sql": "SELECT 2 AS a",
                    },
                ],
            },
        }
    )

    assert session._apply_tree_worker_response(valid, base_tree) is not None
    assert session._apply_tree_worker_response(multi_changed, base_tree) is not None
    assert session._apply_tree_worker_response(syntax_invalid, base_tree) is None


def test_compiler_tree_shape_accepts_single_object_or_array() -> None:
    session = _make_session()

    as_object = json.dumps(
        {
            "plan_id": "s1",
            "tree": {
                "root_node_id": "final_select",
                "nodes": [
                    {
                        "node_id": "final_select",
                        "parent_node_id": None,
                        "sources": [],
                        "outputs": ["x"],
                        "changed": True,
                        "sql": "SELECT 1 AS x",
                    }
                ],
            },
        }
    )
    as_array = json.dumps(
        [
            {
                "plan_id": "s1",
                "tree": {
                    "root_node_id": "final_select",
                    "nodes": [
                        {
                            "node_id": "final_select",
                            "parent_node_id": None,
                            "sources": [],
                            "outputs": ["x"],
                            "changed": True,
                            "sql": "SELECT 1 AS x",
                        }
                    ],
                },
            }
        ]
    )

    assert session._is_compiler_tier0_shape_failure(as_object, tree_mode=True) is False
    assert session._is_compiler_tier0_shape_failure(as_array, tree_mode=True) is False


def test_beam_edit_mode_defaults_to_tree_but_allows_patchplan_override() -> None:
    session = _make_session()
    assert session._beam_edit_mode() == "tree"

    session.pipeline.config.beam_edit_mode = "patchplan"
    assert session._beam_edit_mode() == "patchplan"


def test_apply_tree_compiler_response_accepts_single_plan_object() -> None:
    session = _make_session()
    base_tree = {
        "root_node_id": "final_select",
        "nodes": [
            {
                "node_id": "final_select",
                "parent_node_id": None,
                "sources": [],
                "outputs": ["x"],
                "sql": "SELECT 1 AS x",
            }
        ],
    }
    response = json.dumps(
        {
            "plan_id": "snipe_p1",
            "family": "B",
            "transform": "tree_rewrite",
            "tree": {
                "root_node_id": "final_select",
                "nodes": [
                    {
                        "node_id": "final_select",
                        "parent_node_id": None,
                        "sources": [],
                        "outputs": ["x"],
                        "changed": True,
                        "sql": "SELECT 3 AS x",
                    }
                ],
            },
        }
    )

    patches = session._apply_tree_compiler_response(response, base_tree, prefix="s1")
    assert len(patches) == 1
    assert patches[0].patch_id == "snipe_p1"
    assert patches[0].status == "applied"
    assert "SELECT 3 AS x" in (patches[0].output_sql or "")


def test_check_sqlglot_parse_marks_valid_sql() -> None:
    session = _make_session()
    patch = AppliedPatch(
        patch_id="p01",
        family="A",
        transform="push_filter",
        relevance_score=0.9,
        output_sql="SELECT 1",
    )
    session.check_sqlglot_parse([patch])
    assert patch.semantic_passed is True


def test_check_sqlglot_parse_marks_invalid_sql() -> None:
    session = _make_session()
    patch = AppliedPatch(
        patch_id="p01",
        family="A",
        transform="push_filter",
        relevance_score=0.9,
        output_sql="SELECT ( FROM",
    )
    session.check_sqlglot_parse([patch])
    assert patch.semantic_passed is False
    assert patch.status == "FAIL"
    assert "SQLGlot parse error" in patch.apply_error
