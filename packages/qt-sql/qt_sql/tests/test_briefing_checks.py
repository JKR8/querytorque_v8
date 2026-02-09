"""Tests for V2 analyst briefing checklists and semantic validation."""

from __future__ import annotations

from qt_sql.prompts.analyst_briefing import build_analyst_briefing_prompt
from qt_sql.prompts.briefing_checks import validate_parsed_briefing
from qt_sql.prompts.swarm_parsers import BriefingShared, BriefingWorker, ParsedBriefing
from qt_sql.prompts.worker import build_worker_prompt
from qt_sql.dag import DagNode, QueryDag


def _semantic_contract_90_tokens() -> str:
    return " ".join(["contract"] * 90)


def _shared_valid() -> BriefingShared:
    return BriefingShared(
        semantic_contract=_semantic_contract_90_tokens(),
        bottleneck_diagnosis=(
            "Dominant cost is join-bound due to a non-equi join expansion; "
            "cardinality falls late and optimizer already handles dimension lookup ordering well."
        ),
        active_constraints=(
            "- LITERAL_PRESERVATION: preserve all constants exactly.\n"
            "- SEMANTIC_EQUIVALENCE: preserve join and filter semantics.\n"
            "- COMPLETE_OUTPUT: preserve projected columns and order.\n"
            "- CTE_COLUMN_COMPLETENESS: keep downstream-required columns.\n"
            "- NON_EQUI_JOIN_INPUT_BLINDNESS: EXPLAIN shows nested-loop on large inputs."
        ),
        regression_warnings="None applicable.",
    )


def _valid_target_dag() -> str:
    return (
        "TARGET_DAG:\n"
        "  base_scan -> grouped\n\n"
        "NODE_CONTRACTS:\n"
        "  base_scan:\n"
        "    FROM: catalog_sales\n"
        "    WHERE: cs_wholesale_cost BETWEEN 35 AND 55\n"
        "    OUTPUT: cs_item_sk, cs_quantity\n"
        "    EXPECTED_ROWS: ~1000\n"
        "    CONSUMERS: grouped\n\n"
        "  grouped:\n"
        "    FROM: base_scan\n"
        "    GROUP BY: cs_item_sk\n"
        "    AGGREGATE: COUNT(*) AS total_cnt\n"
        "    OUTPUT: cs_item_sk, total_cnt\n"
        "    EXPECTED_ROWS: ~100\n"
        "    CONSUMERS: result"
    )


def _worker(worker_id: int, target_dag: str | None = None) -> BriefingWorker:
    return BriefingWorker(
        worker_id=worker_id,
        strategy=f"strategy_{worker_id}_custom",
        target_dag=target_dag or _valid_target_dag(),
        examples=[f"ex_{worker_id}"],
        example_reasoning="Pattern matches bottleneck and preserves semantics.",
        hazard_flags="- Preserve non-equi predicates and output schema.",
    )


def test_analyst_prompt_includes_section_validation_checklist() -> None:
    dag = QueryDag(
        nodes={
            "main_query": DagNode(
                node_id="main_query",
                node_type="main",
                sql="SELECT 1",
                tables=["t1"],
                refs=[],
                flags=[],
            )
        },
        edges=[],
        original_sql="SELECT 1",
    )

    prompt = build_analyst_briefing_prompt(
        query_id="q_test",
        sql="SELECT 1",
        explain_plan_text=None,
        dag=dag,
        costs={},
        semantic_intents=None,
        global_knowledge=None,
        matched_examples=[],
        all_available_examples=[],
        constraints=[
            {"id": "LITERAL_PRESERVATION", "severity": "CRITICAL", "prompt_instruction": "a"},
            {"id": "SEMANTIC_EQUIVALENCE", "severity": "CRITICAL", "prompt_instruction": "b"},
            {"id": "COMPLETE_OUTPUT", "severity": "CRITICAL", "prompt_instruction": "c"},
            {"id": "CTE_COLUMN_COMPLETENESS", "severity": "CRITICAL", "prompt_instruction": "d"},
        ],
        regression_warnings=None,
        dialect="postgresql",
        dialect_version="14.3",
        strategy_leaderboard=None,
        query_archetype=None,
        engine_profile=None,
        resource_envelope=None,
    )

    assert "## Section Validation Checklist (MUST pass before final output)" in prompt
    assert "`SEMANTIC_CONTRACT`: 80-150 tokens" in prompt
    assert "`NODE_CONTRACTS`: every DAG node has a contract" in prompt
    assert "WORKER 4 EXPLORATION FIELDS" in prompt


def test_worker_prompt_includes_rewrite_checklist() -> None:
    prompt = build_worker_prompt(
        worker_briefing=_worker(1),
        shared_briefing=_shared_valid(),
        examples=[],
        original_sql="SELECT 1",
        output_columns=["c1"],
        dialect="postgresql",
        engine_version="14.3",
        resource_envelope="## System Resource Envelope\n\nMemory budget: shared_buffers=4GB",
    )

    assert "## Rewrite Checklist (must pass before final SQL)" in prompt
    assert "Follow every node in `TARGET_DAG`" in prompt
    assert "Preserve all literals and the exact final output schema/order." in prompt


def test_validate_parsed_briefing_flags_incorrect_population() -> None:
    bad_worker_1 = _worker(
        1,
        target_dag=(
            "TARGET_DAG:\n"
            "  a -> b -> c\n\n"
            "NODE_CONTRACTS:\n"
            "  a:\n"
            "    FROM: t1\n"
            "    OUTPUT: all columns from t1\n"
            "    EXPECTED_ROWS: ~100\n"
            "    CONSUMERS: b\n\n"
            "  b:\n"
            "    FROM: a\n"
            "    OUTPUT: k1\n"
            "    EXPECTED_ROWS: ~10\n"
            "    CONSUMERS: result"
        ),
    )
    parsed = ParsedBriefing(
        shared=_shared_valid(),
        workers=[bad_worker_1, _worker(2), _worker(3), _worker(4)],
        raw="",
    )

    issues = validate_parsed_briefing(parsed)

    assert any("NODE_CONTRACTS missing node 'c'" in i for i in issues)
    assert any("OUTPUT uses placeholder text" in i for i in issues)


def test_validate_parsed_briefing_accepts_well_populated_briefing() -> None:
    parsed = ParsedBriefing(
        shared=_shared_valid(),
        workers=[_worker(1), _worker(2), _worker(3), _worker(4)],
        raw="",
    )

    issues = validate_parsed_briefing(parsed)

    assert issues == []
