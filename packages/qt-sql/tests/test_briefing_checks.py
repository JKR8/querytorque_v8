"""Tests for analyst briefing checklists and semantic validation."""

from __future__ import annotations

from qt_sql.prompts.analyst_briefing import build_analyst_briefing_prompt
from qt_sql.prompts.briefing_checks import validate_parsed_briefing
from qt_sql.prompts.parsers import (
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
        optimal_path=(
            "Dominant cost is join-bound due to a non-equi join expansion; "
            "cardinality falls late and optimizer already handles dimension lookup ordering well."
        ),
        current_plan_gap=(
            "- MINIMIZE ROWS TOUCHED: late cardinality reduction causes excessive scans."
        ),
        active_constraints=(
            "- LITERAL_PRESERVATION: preserve all constants exactly.\n"
            "- SEMANTIC_EQUIVALENCE: preserve join and filter semantics.\n"
            "- COMPLETE_OUTPUT: preserve projected columns and order.\n"
            "- CTE_COLUMN_COMPLETENESS: keep downstream-required columns.\n"
            "- NON_EQUI_JOIN_INPUT_BLINDNESS: EXPLAIN shows nested-loop on large inputs."
        ),
        regression_warnings="None applicable.",
        diversity_map=(
            "| Worker | Role | Primary Family | Secondary | Key Structural Idea |\n"
            "| W1 | proven_compound | A | C | Early filtering then aggregate |\n"
            "| W2 | structural_alt | B | D | Decorrelation path |\n"
            "| W3 | aggressive_compound | E | F | Materialize and reshape joins |\n"
            "| W4 | novel_orthogonal | D | A | Set-operation exploration |"
        ),
    )


def _valid_target_query_map() -> str:
    return "base_scan -> grouped"


def _valid_node_contracts() -> str:
    return (
        "base_scan:\n"
        "  FROM: catalog_sales\n"
        "  WHERE: cs_wholesale_cost BETWEEN 35 AND 55\n"
        "  OUTPUT: cs_item_sk, cs_quantity\n"
        "  EXPECTED_ROWS: ~1000\n"
        "  CONSUMERS: grouped\n\n"
        "grouped:\n"
        "  FROM: base_scan\n"
        "  GROUP BY: cs_item_sk\n"
        "  AGGREGATE: COUNT(*) AS total_cnt\n"
        "  OUTPUT: cs_item_sk, total_cnt\n"
        "  EXPECTED_ROWS: ~100\n"
        "  CONSUMERS: result"
    )


def _worker(
    worker_id: int,
    target_query_map: str | None = None,
    node_contracts: str | None = None,
) -> BriefingWorker:
    return BriefingWorker(
        worker_id=worker_id,
        strategy=f"strategy_{worker_id}_custom",
        role="proven_compound",
        primary_family="A",
        approach="Apply early filtering before aggregation to reduce row volume.",
        target_query_map=target_query_map or _valid_target_query_map(),
        node_contracts=node_contracts or _valid_node_contracts(),
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
        constraints=[
            {"id": "LITERAL_PRESERVATION", "severity": "CRITICAL", "prompt_instruction": "a"},
            {"id": "SEMANTIC_EQUIVALENCE", "severity": "CRITICAL", "prompt_instruction": "b"},
            {"id": "COMPLETE_OUTPUT", "severity": "CRITICAL", "prompt_instruction": "c"},
            {"id": "CTE_COLUMN_COMPLETENESS", "severity": "CRITICAL", "prompt_instruction": "d"},
        ],
        dialect="postgresql",
        dialect_version="14.3",
        engine_profile=None,
        resource_envelope=None,
    )

    assert "## §VI. OUTPUT FORMAT" in prompt
    assert "SEMANTIC_CONTRACT" in prompt
    assert "NODE_CONTRACTS" in prompt
    assert "EXPLORATION_TYPE" in prompt


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
        constraints=[],
        dialect="postgresql",
        exploit_algorithm_text=fake_algo,
        engine_profile={"briefing_note": "should not appear"},
    )

    # Exploit algorithm text should be injected as-is.
    assert fake_algo in prompt
    # Engine profile briefing note should not leak when exploit text is supplied.
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
    )

    assert "## Rewrite Checklist (must pass before final SQL)" in prompt
    assert "Follow every node in `TARGET_QUERY_MAP`" in prompt
    assert "Preserve all literals and the exact final output schema/order." in prompt


def test_validate_parsed_briefing_flags_missing_fields() -> None:
    bad_worker_1 = BriefingWorker(
        worker_id=1,
        strategy="strategy_1_custom",
        role="proven_compound",
        primary_family="A",
        approach="Apply early filtering.",
        target_query_map="",
        node_contracts="",
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

    assert any("TARGET_QUERY_MAP missing" in i for i in issues)
    assert any("NODE_CONTRACTS missing" in i for i in issues)


def test_validate_parsed_briefing_accepts_well_populated_briefing() -> None:
    parsed = ParsedBriefing(
        shared=_shared_valid(),
        workers=[_worker(1), _worker(2), _worker(3), _worker(4)],
        raw="",
    )

    issues = validate_parsed_briefing(parsed)

    assert issues == []


def test_validate_parsed_briefing_single_worker_accepts_expected_workers_1() -> None:
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

    assert any("too many gap IDs" in i for i in issues)


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
