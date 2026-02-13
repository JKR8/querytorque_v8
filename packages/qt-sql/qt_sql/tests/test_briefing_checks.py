"""Tests for V2 analyst briefing checklists and semantic validation."""

from __future__ import annotations

from qt_sql.prompts.analyst_briefing import build_analyst_briefing_prompt
from qt_sql.prompts.briefing_checks import validate_parsed_briefing
from qt_sql.prompts.swarm_parsers import (
    BriefingShared,
    BriefingWorker,
    ParsedBriefing,
    parse_briefing_response,
)
from qt_sql.prompts.worker import build_worker_prompt
from qt_sql.dag import LogicalTreeNode, QueryLogicalTree


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


def _valid_target_logical_tree() -> str:
    return (
        "TARGET_LOGICAL_TREE:\n"
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


def _worker(worker_id: int, target_logical_tree: str | None = None) -> BriefingWorker:
    return BriefingWorker(
        worker_id=worker_id,
        strategy=f"strategy_{worker_id}_custom",
        target_logical_tree=target_logical_tree or _valid_target_logical_tree(),
        examples=[f"ex_{worker_id}"],
        example_adaptation="Pattern matches bottleneck and preserves semantics.",
        hazard_flags="- Preserve non-equi predicates and output schema.",
    )


def test_analyst_prompt_includes_section_validation_checklist() -> None:
    dag = QueryLogicalTree(
        nodes={
            "main_query": LogicalTreeNode(
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
        constraints=[
            {"id": "LITERAL_PRESERVATION", "severity": "CRITICAL", "prompt_instruction": "a"},
            {"id": "SEMANTIC_EQUIVALENCE", "severity": "CRITICAL", "prompt_instruction": "b"},
            {"id": "COMPLETE_OUTPUT", "severity": "CRITICAL", "prompt_instruction": "c"},
            {"id": "CTE_COLUMN_COMPLETENESS", "severity": "CRITICAL", "prompt_instruction": "d"},
        ],
        dialect="postgresql",
        dialect_version="14.3",
        strategy_leaderboard=None,
        query_archetype=None,
        engine_profile=None,
        resource_envelope=None,
    )

    assert "## Section Validation Checklist (MUST pass before final output)" in prompt
    assert "`SEMANTIC_CONTRACT`: 30-250 tokens" in prompt
    assert "`NODE_CONTRACTS`: every logical tree node has a contract" in prompt
    assert "EXPLORATION FIELDS" in prompt


def test_analyst_prompt_exploit_algorithm_branch() -> None:
    """Exploit algorithm text replaces engine profile and uses correct framing."""
    dag = QueryLogicalTree(
        nodes={
            "main_query": LogicalTreeNode(
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

    fake_algo = "## Pathology P1: comma join\n\nSome exploit steps here."
    prompt = build_analyst_briefing_prompt(
        query_id="q_algo",
        sql="SELECT 1",
        explain_plan_text=None,
        dag=dag,
        costs={},
        semantic_intents=None,
        global_knowledge=None,
        constraints=[],
        dialect="postgresql",
        exploit_algorithm_text=fake_algo,
        engine_profile={"briefing_note": "should not appear"},
    )

    # Exploit algorithm section present with correct framing (§4 header)
    assert "## §4. Exploit Algorithm: Evidence-Based Gap Intelligence" in prompt
    assert fake_algo in prompt
    # Framing does NOT say "YAML"
    assert "YAML" not in prompt
    # Engine profile section NOT present (exploit algorithm replaces it)
    assert "## §4. Engine Profile: Field Intelligence Briefing" not in prompt
    assert "should not appear" not in prompt


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
    assert "Follow every node in `TARGET_LOGICAL_TREE`" in prompt
    assert "Preserve all literals and the exact final output schema/order." in prompt


def test_validate_parsed_briefing_flags_missing_fields() -> None:
    bad_worker_1 = BriefingWorker(
        worker_id=1,
        strategy="strategy_1_custom",
        target_logical_tree="",  # empty — should flag
        examples=["ex_1"],
        example_adaptation="Apply pattern.",
        hazard_flags="- Risk.",
    )
    parsed = ParsedBriefing(
        shared=_shared_valid(),
        workers=[bad_worker_1, _worker(2), _worker(3), _worker(4)],
        raw="",
    )

    issues = validate_parsed_briefing(parsed)

    assert any("TARGET_LOGICAL_TREE/NODE_CONTRACTS missing" in i for i in issues)


def test_validate_parsed_briefing_accepts_well_populated_briefing() -> None:
    parsed = ParsedBriefing(
        shared=_shared_valid(),
        workers=[_worker(1), _worker(2), _worker(3), _worker(4)],
        raw="",
    )

    issues = validate_parsed_briefing(parsed)

    assert issues == []


def test_validate_parsed_briefing_expert_mode_accepts_single_worker() -> None:
    parsed = ParsedBriefing(
        shared=_shared_valid(),
        workers=[
            _worker(1),
            BriefingWorker(worker_id=2),
            BriefingWorker(worker_id=3),
            BriefingWorker(worker_id=4),
        ],
        raw="",
    )

    issues = validate_parsed_briefing(parsed, expected_workers=1)

    assert issues == []


def test_validate_parsed_briefing_rejects_too_many_gap_ids() -> None:
    shared = _shared_valid()
    shared.active_constraints = (
        "- LITERAL_PRESERVATION: preserve all constants exactly.\n"
        "- SEMANTIC_EQUIVALENCE: preserve join and filter semantics.\n"
        "- COMPLETE_OUTPUT: preserve projected columns and order.\n"
        "- CTE_COLUMN_COMPLETENESS: keep downstream-required columns.\n"
        "- GAP_A: signal A.\n"
        "- GAP_B: signal B.\n"
        "- GAP_C: signal C.\n"
        "- GAP_D: signal D."
    )
    parsed = ParsedBriefing(
        shared=shared,
        workers=[_worker(1), _worker(2), _worker(3), _worker(4)],
        raw="",
    )

    issues = validate_parsed_briefing(parsed)

    assert any("too many gap/hypothesis IDs" in i for i in issues)


def test_parse_worker_hazard_flags_stop_before_exploration_fields() -> None:
    response = (
        "=== SHARED BRIEFING ===\n"
        "SEMANTIC_CONTRACT: " + "word " * 40 + "\n"
        "BOTTLENECK_DIAGNOSIS: join-bound with optimizer overlap\n"
        "ACTIVE_CONSTRAINTS:\n"
        "- LITERAL_PRESERVATION: a\n"
        "- SEMANTIC_EQUIVALENCE: b\n"
        "- COMPLETE_OUTPUT: c\n"
        "- CTE_COLUMN_COMPLETENESS: d\n"
        "REGRESSION_WARNINGS:\n"
        "None applicable.\n\n"
        "=== WORKER 1 BRIEFING ===\n"
        "STRATEGY: strategy_1_custom\n"
        "TARGET_LOGICAL_TREE:\n"
        "  a -> b\n"
        "NODE_CONTRACTS:\n"
        "  a:\n"
        "    FROM: t\n"
        "    OUTPUT: c\n"
        "    CONSUMERS: b\n"
        "EXAMPLES: ex_1\n"
        "EXAMPLE_ADAPTATION:\n"
        "  Apply core shape only.\n"
        "HAZARD_FLAGS:\n"
        "- Preserve output schema\n"
        "CONSTRAINT_OVERRIDE: None\n"
        "EXPLORATION_TYPE: novel_combination\n"
        "HYPOTHESIS_TAG: H1_TEST\n"
    )

    parsed = parse_briefing_response(response)

    assert parsed.workers[0].hazard_flags == "- Preserve output schema"
