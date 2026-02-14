"""Comprehensive tests for V2 analyst briefing prompt template.

Covers:
  - §I ROLE: senior investigator framing + 6 principles + self-sufficiency
  - §II THE CASE: SQL, query map, execution plan, estimation errors
  - §III THIS ENGINE: tabular strengths + blind spots
  - §IV CONSTRAINTS: 4 constraints + aggregation note
  - §V INVESTIGATE: 6-step reasoning (including Match Gold Examples) + worker diversity
  - §VI OUTPUT FORMAT: shared briefing + worker briefings
  - §VII REFERENCE APPENDIX: documented cases by blind spot, transform mapping, regression registry, structural matches, verification
  - V2 Parser: OPTIMAL_PATH, CURRENT_PLAN_GAP, APPROACH, TARGET_QUERY_MAP, NODE_CONTRACTS
  - V2 Validator: all new fields validated
  - V2 Worker: new fields wired into worker prompt
"""

from __future__ import annotations

import pytest

from qt_sql.dag import LogicalTreeNode, QueryLogicalTree
from qt_sql.prompts.v2_analyst_briefing import (
    build_v2_analyst_briefing_prompt,
    section_role,
    section_the_case,
    section_this_engine,
    section_constraints,
    section_investigate,
    section_output_format,
    section_reference_appendix,
    _detect_aggregate_functions,
    _detect_query_features,
    _format_blind_spot_id,
)
from qt_sql.prompts.v2_swarm_parsers import (
    V2BriefingShared,
    V2BriefingWorker,
    V2ParsedBriefing,
    parse_v2_briefing_response,
)
from qt_sql.prompts.v2_briefing_checks import (
    build_v2_analyst_checklist,
    build_v2_expert_checklist,
    build_v2_oneshot_checklist,
    build_v2_worker_rewrite_checklist,
    validate_v2_parsed_briefing,
    VALID_GOALS,
)
from qt_sql.prompts.v2_worker import build_v2_worker_prompt


# ═══════════════════════════════════════════════════════════════════════
# Fixtures
# ═══════════════════════════════════════════════════════════════════════

def _minimal_dag():
    return QueryLogicalTree(
        nodes={
            "main_query": LogicalTreeNode(
                node_id="main_query",
                node_type="main",
                sql="SELECT COUNT(*) FROM t1",
                tables=["t1"],
                refs=[],
                flags=[],
            )
        },
        edges=[],
        original_sql="SELECT COUNT(*) FROM t1",
    )


def _dag_with_aggs():
    return QueryLogicalTree(
        nodes={
            "main_query": LogicalTreeNode(
                node_id="main_query",
                node_type="main",
                sql="SELECT COUNT(*), SUM(x), MAX(y) FROM t1 GROUP BY z",
                tables=["t1"],
                refs=[],
                flags=[],
            )
        },
        edges=[],
        original_sql="SELECT COUNT(*), SUM(x), MAX(y) FROM t1 GROUP BY z",
    )


def _dag_with_unsafe_aggs():
    return QueryLogicalTree(
        nodes={
            "main_query": LogicalTreeNode(
                node_id="main_query",
                node_type="main",
                sql="SELECT STDDEV_SAMP(x), AVG(y) FROM t1 GROUP BY z",
                tables=["t1"],
                refs=[],
                flags=[],
            )
        },
        edges=[],
        original_sql="SELECT STDDEV_SAMP(x), AVG(y) FROM t1 GROUP BY z",
    )


def _dag_with_left_join():
    return QueryLogicalTree(
        nodes={
            "main_query": LogicalTreeNode(
                node_id="main_query",
                node_type="main",
                sql="SELECT * FROM t1 LEFT JOIN t2 ON t1.id = t2.id WHERE t2.col > 5",
                tables=["t1", "t2"],
                refs=[],
                flags=[],
            )
        },
        edges=[],
        original_sql="SELECT * FROM t1 LEFT JOIN t2 ON t1.id = t2.id WHERE t2.col > 5",
    )


def _dag_with_exists():
    return QueryLogicalTree(
        nodes={
            "main_query": LogicalTreeNode(
                node_id="main_query",
                node_type="main",
                sql="SELECT * FROM t1 WHERE EXISTS (SELECT 1 FROM t2 WHERE t2.id = t1.id)",
                tables=["t1", "t2"],
                refs=[],
                flags=[],
            )
        },
        edges=[],
        original_sql="SELECT * FROM t1 WHERE EXISTS (SELECT 1 FROM t2 WHERE t2.id = t1.id)",
    )


def _constraints():
    return [
        {"id": "LITERAL_PRESERVATION", "severity": "CRITICAL", "prompt_instruction": "Preserve all literals exactly."},
        {"id": "SEMANTIC_EQUIVALENCE", "severity": "CRITICAL", "prompt_instruction": "Same rows, columns, ordering."},
        {"id": "COMPLETE_OUTPUT", "severity": "CRITICAL", "prompt_instruction": "All original SELECT columns preserved."},
        {"id": "CTE_COLUMN_COMPLETENESS", "severity": "CRITICAL", "prompt_instruction": "Every CTE SELECTs all required columns."},
    ]


def _engine_profile():
    return {
        "briefing_note": "Field intelligence from 88 TPC-DS queries.",
        "strengths": [
            {"id": "PREDICATE_PUSHDOWN", "summary": "Pushes WHERE into scan", "implication": "Leave it alone"},
            {"id": "EXISTS_SEMI_JOIN", "summary": "EXISTS uses semi-join", "implication": "Never materialize EXISTS"},
        ],
        "gaps": [
            {
                "id": "CROSS_CTE_PREDICATE_BLINDNESS",
                "priority": "HIGH",
                "goal": "SMALLEST_SET_FIRST",
                "detect": "Row counts flat through CTE chain",
                "gates": "Filter ratio >5:1",
                "what": "Cannot push predicates backward into CTEs",
                "why": "CTEs planned independently",
                "opportunity": "Move filters INTO CTE definitions",
                "what_worked": ["4.76x — split CTE", "2.97x — pre-filter dim"],
                "what_didnt_work": ["0.0076x — cross-joined 3 dims"],
                "field_notes": ["~35% of wins", "Never cross-join 3+ dim CTEs"],
            },
        ],
    }


def _semantic_contract_text():
    return " ".join(["contract"] * 90)


def _shared_valid():
    return V2BriefingShared(
        semantic_contract=_semantic_contract_text(),
        optimal_path=(
            "date_dim(d_year=2001 AND d_qoy<4) -> ~274 rows -> "
            "store_sales(HASH JOIN on d_date_sk) -> ~200K rows -> "
            "GROUP BY customer_sk -> ~50K rows"
        ),
        current_plan_gap=(
            "- MINIMIZE ROWS TOUCHED: store_sales scanned 3.1M rows but only 200K survive "
            "the date join. Moving the date filter into a CTE and joining against its output "
            "reduces input by 93%."
        ),
        active_constraints=(
            "- LITERAL_PRESERVATION: preserve all constants.\n"
            "- SEMANTIC_EQUIVALENCE: preserve join and filter semantics.\n"
            "- COMPLETE_OUTPUT: preserve columns and order.\n"
            "- CTE_COLUMN_COMPLETENESS: keep downstream-required columns.\n"
            "- CROSS_CTE_PREDICATE_BLINDNESS: EXPLAIN shows flat row counts through CTE chain."
        ),
        regression_warnings="None applicable.",
        diversity_map=(
            "| Worker | Approach | Key Structural Idea |\n"
            "|--------|----------|---------------------|\n"
            "| 1 | minimal_restructuring | Explicit JOINs + date CTE |\n"
            "| 2 | keyset_strategy | Channel keysets + EXISTS probe |\n"
            "| 3 | early_reduction | Selective channel first, narrow set |\n"
            "| 4 | novel_compound | Unified channel scan |"
        ),
    )


def _worker_valid(worker_id: int = 1):
    # Map worker IDs to roles and families for diversity
    role_map = {
        1: "proven_compound",
        2: "structural_alt",
        3: "aggressive_compound",
        4: "novel_orthogonal",
    }
    family_map = {
        1: "A",  # Early filtering
        2: "B",  # Decorrelation
        3: "C",  # Aggregation
        4: "D",  # Set operations
    }

    return V2BriefingWorker(
        worker_id=worker_id,
        strategy=f"strategy_{worker_id}_custom",
        role=role_map.get(worker_id, "proven_compound"),
        primary_family=family_map.get(worker_id, "A"),
        approach=(
            f"Worker {worker_id} approach: restructure the date filtering "
            f"into an isolated CTE to close the CROSS_CTE_PREDICATE_BLINDNESS gap."
        ),
        target_query_map=(
            "filtered_dates -> fact_scan -> grouped"
        ),
        node_contracts=(
            "filtered_dates:\n"
            "  FROM: date_dim\n"
            "  WHERE: d_year = 2001 AND d_qoy < 4\n"
            "  OUTPUT: d_date_sk\n"
            "  EXPECTED_ROWS: ~274\n"
            "  CONSUMERS: fact_scan"
        ),
        examples=[f"ex_{worker_id}"],
        example_adaptation="Apply date isolation pattern. Ignore column names.",
        hazard_flags="- Preserve EXISTS semi-join semantics. Do not materialize.",
    )


# ═══════════════════════════════════════════════════════════════════════
# §I. ROLE Tests
# ═══════════════════════════════════════════════════════════════════════

class TestSectionRole:
    def test_swarm_mode(self):
        result = section_role("swarm")
        assert "§I. ROLE" in result
        assert "senior query optimization architect" in result
        assert "4 specialist workers" in result
        assert "ONLY what you provide" in result

    def test_expert_mode(self):
        result = section_role("expert")
        assert "§I. ROLE" in result
        assert "single specialist worker" in result

    def test_oneshot_mode(self):
        result = section_role("oneshot")
        assert "§I. ROLE" in result
        assert "produce the optimized SQL directly" in result

    def test_contains_six_principles(self):
        result = section_role("swarm")
        assert "MINIMIZE ROWS TOUCHED" in result
        assert "SMALLEST SET FIRST" in result
        assert "DON'T REPEAT WORK" in result
        assert "SETS OVER LOOPS" in result
        assert "ARM THE OPTIMIZER" in result
        assert "MINIMIZE DATA MOVEMENT" in result

    def test_principles_are_numbered(self):
        result = section_role("swarm")
        for i in range(1, 7):
            assert f"{i}." in result

    def test_no_doctor_metaphor(self):
        result = section_role("swarm")
        assert "doctor" not in result
        assert "patient" not in result
        assert "optimization architect" in result

    def test_diagnostic_lens_framing(self):
        result = section_role("swarm")
        assert "diagnostic lens is six principles" in result

    def test_gold_examples_paragraph(self):
        result = section_role("swarm")
        assert "gold examples" in result
        assert "primary asset" in result
        assert "highest-leverage" in result
        assert "examples are the edge" in result

    def test_swarm_mentions_query_map(self):
        result = section_role("swarm")
        assert "query map" in result

    def test_expert_mentions_query_map(self):
        result = section_role("expert")
        assert "query map" in result


# ═══════════════════════════════════════════════════════════════════════
# §II. THE CASE Tests
# ═══════════════════════════════════════════════════════════════════════

class TestSectionTheCase:
    def test_contains_original_sql(self):
        result = section_the_case(
            "query_35", "SELECT 1", None, _minimal_dag(), {},
            None, "duckdb", "1.1", None, None,
        )
        assert "§II. THE CASE" in result
        assert "### A. Original SQL: query_35 (duckdb v1.1)" in result
        assert "SELECT 1" in result

    def test_contains_query_map(self):
        result = section_the_case(
            "q1", "SELECT 1", None, _minimal_dag(), {},
            None, "duckdb", None, None, None,
        )
        assert "### C. Query Map" in result

    def test_contains_explain_plan(self):
        explain_text = "TOP_N [100 rows, 2.4ms]\n  HASH_GROUP_BY [58K rows]"
        result = section_the_case(
            "q1", "SELECT 1", explain_text, _minimal_dag(), {},
            None, "duckdb", None, None, None,
        )
        assert "### B. Current Execution Plan (EXPLAIN ANALYZE)" in result
        assert "TOP_N" in result
        assert "EXPLAIN is ground truth" in result

    def test_estimate_plan_label(self):
        explain_text = "est_rows=100 cost=500"
        result = section_the_case(
            "q1", "SELECT 1", explain_text, _minimal_dag(), {},
            None, "postgresql", None, None, None,
        )
        assert "planner estimates" in result

    def test_no_explain_plan(self):
        result = section_the_case(
            "q1", "SELECT 1", None, _minimal_dag(), {},
            None, "duckdb", None, None, None,
        )
        assert "not available" in result

    def test_semantic_intent(self):
        intents = {"query_intent": "Find customers who bought in-store and online."}
        result = section_the_case(
            "q1", "SELECT 1", None, _minimal_dag(), {},
            intents, "duckdb", None, None, None,
        )
        assert "Find customers who bought" in result

    def test_iteration_history(self):
        history = {
            "attempts": [
                {"status": "error", "speedup": 0, "transforms": ["decorrelate"], "error": "syntax error"},
                {"status": "REGRESSION", "speedup": 0.5, "transforms": ["pushdown"]},
                {"status": "WIN", "speedup": 2.0, "transforms": ["scan_consolidation"]},
            ]
        }
        result = section_the_case(
            "q1", "SELECT 1", None, _minimal_dag(), {},
            None, "duckdb", None, None, history,
        )
        assert "Previous Optimization Attempts" in result
        assert "ERROR" in result
        assert "REGRESSION" in result
        assert "WIN" in result


# ═══════════════════════════════════════════════════════════════════════
# §III. THIS ENGINE Tests
# ═══════════════════════════════════════════════════════════════════════

class TestSectionThisEngine:
    def test_exploit_algorithm_takes_precedence(self):
        result = section_this_engine(
            _engine_profile(),
            "## CUSTOM ALGORITHM\nSteps here.",
            "duckdb", None,
        )
        assert "CUSTOM ALGORITHM" in result
        # Engine profile tables should NOT appear
        assert "| Capability | Implication |" not in result

    def test_strengths_table(self):
        result = section_this_engine(_engine_profile(), None, "duckdb", None)
        assert "§III. THIS ENGINE" in result
        assert "Handles Well" in result
        assert "| Capability | Implication |" in result
        assert "Pushes WHERE into scan" in result
        assert "EXISTS uses semi-join" in result

    def test_blind_spots_table(self):
        result = section_this_engine(_engine_profile(), None, "duckdb", None)
        assert "Blind Spots" in result
        assert "| Blind Spot | Consequence |" in result
        assert "CROSS_CTE_PREDICATE_BLINDNESS" in result

    def test_no_profile(self):
        result = section_this_engine(None, None, "duckdb", None)
        assert "No engine profile available" in result

    def test_resource_envelope_pg(self):
        result = section_this_engine(
            _engine_profile(), None, "postgresql",
            "shared_buffers=4GB work_mem=256MB",
        )
        assert "System Resource Envelope" in result
        assert "shared_buffers=4GB" in result

    def test_resource_envelope_not_shown_for_duckdb(self):
        result = section_this_engine(
            _engine_profile(), None, "duckdb",
            "some_envelope",
        )
        assert "System Resource Envelope" not in result

    def test_exploit_algorithm_with_pg_scanner_and_envelope(self):
        """§III should render plan_scanner_text and resource_envelope even with exploit algorithm."""
        result = section_this_engine(
            _engine_profile(),
            "## CUSTOM ALGORITHM\nSteps here.",
            "postgresql",
            "shared_buffers=4GB work_mem=256MB",
            plan_scanner_text="Scanner found: HashJoin preferred on dim tables",
        )
        assert "CUSTOM ALGORITHM" in result
        assert "Plan-Space Scanner Intelligence" in result
        assert "HashJoin preferred" in result
        assert "System Resource Envelope" in result
        assert "shared_buffers=4GB" in result

    def test_no_detected_transforms_in_engine_section(self):
        """§III should NOT contain detected transforms (moved to §VII.D only)."""
        result = section_this_engine(_engine_profile(), None, "duckdb", None)
        assert "Detected Transforms" not in result


# ═══════════════════════════════════════════════════════════════════════
# §IV. CONSTRAINTS Tests
# ═══════════════════════════════════════════════════════════════════════

class TestSectionConstraints:
    def test_lists_four_constraints(self):
        result = section_constraints(_constraints(), _minimal_dag(), {})
        assert "§IV. CONSTRAINTS" in result
        assert "LITERAL_PRESERVATION" in result
        assert "SEMANTIC_EQUIVALENCE" in result
        assert "COMPLETE_OUTPUT" in result
        assert "CTE_COLUMN_COMPLETENESS" in result

    def test_fallback_when_no_constraints_provided(self):
        result = section_constraints(None, _minimal_dag(), {})
        assert "LITERAL_PRESERVATION" in result
        assert "SEMANTIC_EQUIVALENCE" in result

    def test_aggregation_note_safe(self):
        result = section_constraints(_constraints(), _dag_with_aggs(), {})
        assert "all safe" in result
        assert "COUNT, MAX, SUM" in result

    def test_aggregation_note_unsafe(self):
        result = section_constraints(_constraints(), _dag_with_unsafe_aggs(), {})
        assert "grouping-sensitive" in result
        assert "STDDEV_SAMP" in result

    def test_aggregation_note_no_aggs(self):
        dag = QueryLogicalTree(
            nodes={
                "main_query": LogicalTreeNode(
                    node_id="main_query", node_type="main",
                    sql="SELECT * FROM t1", tables=["t1"], refs=[], flags=[],
                )
            },
            edges=[], original_sql="SELECT * FROM t1",
        )
        result = section_constraints(_constraints(), dag, {})
        assert "No aggregate functions" in result


# ═══════════════════════════════════════════════════════════════════════
# §V. INVESTIGATE Tests
# ═══════════════════════════════════════════════════════════════════════

class TestSectionInvestigate:
    def test_seven_steps_in_swarm(self):
        result = section_investigate("swarm", _minimal_dag(), None, None)
        assert "§V. INVESTIGATE" in result
        assert "Step 1:" in result
        assert "Step 2:" in result
        assert "Step 3:" in result
        assert "Step 4:" in result
        assert "Step 5:" in result
        assert "Step 6:" in result
        assert "Step 7:" in result

    def test_step1_analyze_plan(self):
        result = section_investigate("swarm", _minimal_dag(), None, None)
        assert "Analyze the Current Plan" in result
        assert "cost spine" in result

    def test_step2_read_map(self):
        result = section_investigate("swarm", _minimal_dag(), None, None)
        assert "Read the Map" in result
        assert "query map" in result.lower()

    def test_step3_optimal_path(self):
        result = section_investigate("swarm", _minimal_dag(), None, None)
        assert "Optimal Path" in result
        assert "best entry point" in result
        assert "running rowcount" in result

    def test_step4_diagnose_gap(self):
        result = section_investigate("swarm", _minimal_dag(), None, None)
        assert "Diagnose the Gap" in result
        assert "violated goal" in result

    def test_step4_novel_blind_spot_detection(self):
        result = section_investigate("swarm", _minimal_dag(), None, None)
        assert "novel blind spot" in result
        assert "describe the mechanism" in result

    def test_step4_self_sufficiency(self):
        result = section_investigate("swarm", _minimal_dag(), None, None)
        assert "complete and actionable on its own" in result
        assert "even for problems you've never seen before" in result

    def test_step5_match_gold_examples(self):
        result = section_investigate("swarm", _minimal_dag(), None, None)
        assert "Match Gold Examples" in result
        assert "highest-leverage step" in result
        assert "Match found" in result
        assert "No match" in result

    def test_step5_references_example_catalog(self):
        result = section_investigate("swarm", _minimal_dag(), None, None)
        assert "Example Catalog" in result
        assert "§VII.B" in result

    def test_step6_select_examples_per_worker(self):
        result = section_investigate("swarm", _minimal_dag(), None, None)
        assert "Select Examples Per Worker" in result
        assert "Matching criteria" in result
        assert "Structural similarity" in result
        assert "Transform relevance" in result
        assert "Hazard coverage" in result

    def test_step6_adaptation_guidance(self):
        result = section_investigate("swarm", _minimal_dag(), None, None)
        assert "Adaptation guidance" in result
        assert "APPLY" in result
        assert "IGNORE" in result
        assert "ADAPT" in result

    def test_step6_anti_patterns(self):
        result = section_investigate("swarm", _minimal_dag(), None, None)
        assert "Anti-patterns" in result
        assert "dilute attention" in result

    def test_step7_four_strategies_swarm(self):
        result = section_investigate("swarm", _minimal_dag(), None, None)
        assert "Design Four Strategies" in result
        assert "NEW QUERY MAP" in result

    def test_step7_selection_rules(self):
        result = section_investigate("swarm", _minimal_dag(), None, None)
        assert "Selection rules:" in result
        assert "structural prerequisites" in result
        assert "compound strategies" in result

    def test_worker_diversity_in_swarm(self):
        result = section_investigate("swarm", _minimal_dag(), None, None)
        assert "Worker Diversity" in result
        assert "W1" in result
        assert "W2" in result
        assert "W3" in result
        assert "W4" in result

    def test_transform_families_in_swarm(self):
        result = section_investigate("swarm", _minimal_dag(), None, None)
        assert "Transform Families" in result
        assert "Family A" in result
        assert "Family B" in result
        assert "Family C" in result
        assert "Family D" in result
        assert "Family E" in result
        assert "Family F" in result

    def test_worker_roles_in_swarm(self):
        result = section_investigate("swarm", _minimal_dag(), None, None)
        assert "Worker Roles" in result
        assert "Proven compound" in result
        assert "Structural alternative" in result
        assert "Aggressive compound" in result
        assert "Novel / orthogonal" in result

    def test_family_coverage_rule_in_swarm(self):
        result = section_investigate("swarm", _minimal_dag(), None, None)
        assert "Family Coverage Rule" in result
        assert "at least 3 of the 6 transform families" in result

    def test_worker_diversity_generic_not_query_specific(self):
        """Worker diversity descriptions should be generic, not referencing specific schemas."""
        result = section_investigate("swarm", _minimal_dag(), None, None)
        diversity_start = result.index("Worker Diversity")
        diversity_section = result[diversity_start:]
        assert "SCAN REDUCTION" in diversity_section
        assert "JOIN RESTRUCTURING" in diversity_section
        assert "AGGREGATION REWRITE" in diversity_section
        assert "SCAN CONSOLIDATION" in diversity_section
        assert "SUBQUERY ELIMINATION" in diversity_section
        assert "PREDICATE RESTRUCTURE" in diversity_section
        # Worker diversity should NOT reference specific table/column names
        assert "web_sales" not in diversity_section
        assert "customer_sk" not in diversity_section

    def test_expert_mode_best_strategy(self):
        result = section_investigate("expert", _minimal_dag(), None, None)
        assert "Best Strategy" in result
        assert "Four Strategies" not in result
        assert "Worker Diversity" not in result

    def test_oneshot_mode_implement(self):
        result = section_investigate("oneshot", _minimal_dag(), None, None)
        assert "Implement" in result
        assert "Four Strategies" not in result

    def test_step5_appears_in_all_modes(self):
        """Step 5 (Match Gold Examples) appears in all modes."""
        for mode in ("swarm", "expert", "oneshot"):
            result = section_investigate(mode, _minimal_dag(), None, None)
            assert "Match Gold Examples" in result, f"Missing Step 5 in {mode} mode"

    def test_step6_appears_in_all_modes(self):
        """Step 6 (Select Examples) appears in all modes."""
        for mode in ("swarm", "expert", "oneshot"):
            result = section_investigate(mode, _minimal_dag(), None, None)
            assert "Select Examples" in result, f"Missing Step 6 in {mode} mode"

    def test_references_section_vii(self):
        result = section_investigate("swarm", _minimal_dag(), None, None)
        assert "§VII" in result


# ═══════════════════════════════════════════════════════════════════════
# §VI. OUTPUT FORMAT Tests
# ═══════════════════════════════════════════════════════════════════════

class TestSectionOutputFormat:
    def test_shared_briefing_format(self):
        result = section_output_format("swarm", False, "duckdb")
        assert "§VI. OUTPUT FORMAT" in result
        assert "=== SHARED BRIEFING ===" in result
        assert "SEMANTIC_CONTRACT:" in result
        assert "OPTIMAL_PATH:" in result
        assert "CURRENT_PLAN_GAP:" in result
        assert "ACTIVE_CONSTRAINTS:" in result
        assert "REGRESSION_WARNINGS:" in result

    def test_diversity_map_in_swarm(self):
        result = section_output_format("swarm", False, "duckdb")
        assert "DIVERSITY_MAP:" in result
        assert "| Worker | Role" in result
        assert "Primary Family" in result
        assert "Key Structural Idea |" in result

    def test_no_diversity_map_in_expert(self):
        result = section_output_format("expert", False, "duckdb")
        assert "DIVERSITY_MAP:" not in result

    def test_worker_briefing_new_fields(self):
        result = section_output_format("swarm", False, "duckdb")
        assert "STRATEGY:" in result
        assert "APPROACH:" in result
        assert "TARGET_QUERY_MAP:" in result
        assert "NODE_CONTRACTS:" in result

    def test_example_adaptation_structured_format(self):
        result = section_output_format("swarm", False, "duckdb")
        assert "EXAMPLE_ADAPTATION:" in result
        assert "APPLY:" in result
        assert "IGNORE:" in result
        assert "ADAPT:" in result

    def test_worker_4_exploration_fields(self):
        result = section_output_format("swarm", False, "duckdb")
        assert "EXPLORATION_TYPE:" in result
        assert "HYPOTHESIS_TAG:" in result
        assert "Worker 4 adds:" in result

    def test_oneshot_format(self):
        result = section_output_format("oneshot", False, "duckdb")
        assert "=== OPTIMIZED SQL ===" in result
        assert "STRATEGY:" in result
        assert "WORKER 1 BRIEFING" not in result

    def test_expert_single_worker(self):
        result = section_output_format("expert", False, "duckdb")
        assert "=== WORKER 1 BRIEFING ===" in result

    def test_swarm_worker_n_template(self):
        """Swarm generates explicit WORKER 1..4 briefing blocks (not generic N template)."""
        result = section_output_format("swarm", False, "duckdb")
        # Parser requires explicit numeric headers, not generic "N" placeholder
        assert "=== WORKER 1 BRIEFING ===" in result
        assert "=== WORKER 2 BRIEFING ===" in result
        assert "=== WORKER 3 BRIEFING ===" in result
        assert "=== WORKER 4 BRIEFING ===" in result

    def test_discovery_mode_note(self):
        result = section_output_format("swarm", True, "duckdb")
        assert "Discovery mode" in result or "discovery mode" in result


# ═══════════════════════════════════════════════════════════════════════
# §VII. REFERENCE APPENDIX Tests
# ═══════════════════════════════════════════════════════════════════════

class TestSectionReferenceAppendix:
    def test_documented_cases_by_blind_spot(self):
        result = section_reference_appendix(_engine_profile(), None, _minimal_dag())
        assert "§VII. REFERENCE APPENDIX" in result
        assert "Documented Cases by Blind Spot" in result

    def test_case_file_format(self):
        result = section_reference_appendix(_engine_profile(), None, _minimal_dag())
        assert "Blind spot:" in result
        assert "Detect:" in result
        assert "Treatments:" in result

    def test_gold_example_catalog_with_examples(self):
        examples = [
            {"id": "decorrelate", "description": "Convert correlated subquery to CTE",
             "verified_speedup": "2.92x", "principle": "Decorrelation pattern",
             "_match_score": 0.85,
             "example": {"key_insight": "Pre-compute with GROUP BY"}},
            {"id": "date_cte_isolate", "description": "Isolate date keys into CTE",
             "verified_speedup": "1.34x", "principle": "Date pre-filtering",
             "_match_score": 0.60,
             "example": {"key_insight": "Join to fact after filtering"}},
        ]
        result = section_reference_appendix(
            _engine_profile(), None, _minimal_dag(), matched_examples=examples,
        )
        assert "Gold Example Catalog" in result
        assert "| Match |" in result
        assert "decorrelate" in result
        assert "2.92x" in result
        assert "85%" in result
        assert "date_cte_isolate" in result
        assert "1.34x" in result
        assert "60%" in result

    def test_gold_example_catalog_empty(self):
        result = section_reference_appendix(
            _engine_profile(), None, _minimal_dag(), matched_examples=[],
        )
        assert "No gold examples available" in result

    def test_regression_registry(self):
        result = section_reference_appendix(None, None, _minimal_dag())
        assert "Regression Registry" in result
        assert "Materialized EXISTS" in result
        assert "0.14x" in result
        assert "3 dim CTE cross-join" in result
        assert "0.0076x" in result

    def test_regression_registry_pg(self):
        result = section_reference_appendix(None, None, _minimal_dag(), dialect="postgresql")
        assert "Regression Registry" in result
        assert "Multi-scan rewrite" in result
        assert "Double fact table scan" in result
        # Should NOT have DuckDB-specific entries
        assert "3 dim CTE cross-join" not in result

    def test_regression_registry_skipped_with_exploit_algorithm(self):
        result = section_reference_appendix(
            None, "## Algorithm text", _minimal_dag(),
        )
        assert "Regression Registry" not in result

    def test_verification_checklist_skipped_with_exploit_algorithm(self):
        result = section_reference_appendix(
            None, "## Algorithm text", _minimal_dag(),
        )
        assert "Verification Checklist" not in result

    def test_what_doesnt_apply_no_left_join(self):
        result = section_reference_appendix(None, None, _minimal_dag())
        assert "What Doesn't Apply" in result
        assert "No LEFT JOINs" in result

    def test_what_doesnt_apply_with_left_join(self):
        result = section_reference_appendix(None, None, _dag_with_left_join())
        # Should NOT exclude LEFT->INNER
        assert "No LEFT JOINs" not in result

    def test_verification_checklist(self):
        result = section_reference_appendix(None, None, _minimal_dag())
        assert "Verification Checklist" in result
        assert "every CTE has WHERE" in result
        assert "no orphaned CTEs" in result
        assert "EXISTS remains EXISTS" in result

    def test_case_files_intro_text(self):
        result = section_reference_appendix(None, None, _minimal_dag())
        assert "gold examples from past investigations" in result
        assert "Consult during Step 5" in result

    def test_structural_matches_in_appendix(self):
        """Detected transforms should appear in §VII.D."""
        class FakeMatch:
            id = "test_transform"
            overlap_ratio = 0.85
            matched_features = ["EXISTS", "OR_BRANCH"]
            missing_features = []
            contraindications = []
            gap = "TEST_GAP"
        result = section_reference_appendix(None, None, _minimal_dag(), [FakeMatch()])
        assert "Structural Matches" in result
        assert "test_transform" in result
        assert "85%" in result


# ═══════════════════════════════════════════════════════════════════════
# Full Prompt Build Tests
# ═══════════════════════════════════════════════════════════════════════

class TestFullPromptBuild:
    def test_swarm_has_all_seven_sections(self):
        prompt = build_v2_analyst_briefing_prompt(
            query_id="q1", sql="SELECT 1",
            explain_plan_text=None, dag=_minimal_dag(), costs={},
            semantic_intents=None, constraints=_constraints(),
            dialect="duckdb", mode="swarm",
            engine_profile=_engine_profile(),
        )
        assert "§I. ROLE" in prompt
        assert "§II. THE CASE" in prompt
        assert "§III. THIS ENGINE" in prompt
        assert "§IV. CONSTRAINTS" in prompt
        assert "§V. INVESTIGATE" in prompt
        assert "§VI. OUTPUT FORMAT" in prompt
        assert "§VII. REFERENCE APPENDIX" in prompt

    def test_expert_has_all_sections(self):
        prompt = build_v2_analyst_briefing_prompt(
            query_id="q1", sql="SELECT 1",
            explain_plan_text=None, dag=_minimal_dag(), costs={},
            semantic_intents=None, constraints=_constraints(),
            dialect="postgresql", mode="expert",
        )
        assert "§I. ROLE" in prompt
        assert "§VI. OUTPUT FORMAT" in prompt

    def test_oneshot_has_all_sections(self):
        prompt = build_v2_analyst_briefing_prompt(
            query_id="q1", sql="SELECT 1",
            explain_plan_text=None, dag=_minimal_dag(), costs={},
            semantic_intents=None, constraints=_constraints(),
            dialect="duckdb", mode="oneshot",
        )
        assert "§I. ROLE" in prompt
        assert "§VI. OUTPUT FORMAT" in prompt

    def test_invalid_mode_raises(self):
        with pytest.raises(ValueError, match="Invalid mode"):
            build_v2_analyst_briefing_prompt(
                query_id="q1", sql="SELECT 1",
                explain_plan_text=None, dag=_minimal_dag(), costs={},
                semantic_intents=None, mode="invalid",
            )

    def test_exploit_algorithm_replaces_engine_profile(self):
        prompt = build_v2_analyst_briefing_prompt(
            query_id="q1", sql="SELECT 1",
            explain_plan_text=None, dag=_minimal_dag(), costs={},
            semantic_intents=None, dialect="duckdb",
            exploit_algorithm_text="## Custom Algorithm",
            engine_profile=_engine_profile(),
        )
        assert "Custom Algorithm" in prompt
        assert "| Capability | Implication |" not in prompt

    def test_section_order_is_correct(self):
        prompt = build_v2_analyst_briefing_prompt(
            query_id="q1", sql="SELECT 1",
            explain_plan_text=None, dag=_minimal_dag(), costs={},
            semantic_intents=None, constraints=_constraints(),
            dialect="duckdb", mode="swarm",
            engine_profile=_engine_profile(),
        )
        # Verify sections appear in order §I < §II < §III < §IV < §V < §VI < §VII
        pos_1 = prompt.index("§I. ROLE")
        pos_2 = prompt.index("§II. THE CASE")
        pos_3 = prompt.index("§III. THIS ENGINE")
        pos_4 = prompt.index("§IV. CONSTRAINTS")
        pos_5 = prompt.index("§V. INVESTIGATE")
        pos_6 = prompt.index("§VI. OUTPUT FORMAT")
        pos_7 = prompt.index("§VII. REFERENCE APPENDIX")
        assert pos_1 < pos_2 < pos_3 < pos_4 < pos_5 < pos_6 < pos_7


# ═══════════════════════════════════════════════════════════════════════
# V2 Parser Tests
# ═══════════════════════════════════════════════════════════════════════

class TestV2Parser:
    def _full_response(self) -> str:
        """Response in the NEW §VI output format with OPTIMAL_PATH, CURRENT_PLAN_GAP, APPROACH, TARGET_QUERY_MAP."""
        return (
            "<reasoning>Some internal analysis here.</reasoning>\n\n"
            "=== SHARED BRIEFING ===\n\n"
            "SEMANTIC_CONTRACT: " + " ".join(["word"] * 50) + "\n\n"
            "OPTIMAL_PATH:\n"
            "date_dim(d_year=2001) -> ~274 rows -> store_sales(HASH JOIN) -> ~200K rows -> GROUP BY -> ~50K\n\n"
            "CURRENT_PLAN_GAP:\n"
            "- MINIMIZE ROWS TOUCHED: store_sales scanned 3.1M, only 200K needed.\n"
            "  Blind spot: CROSS_CTE_PREDICATE_BLINDNESS. Excess rows: ~2.9M.\n\n"
            "ACTIVE_CONSTRAINTS:\n"
            "- LITERAL_PRESERVATION: preserve all constants.\n"
            "- SEMANTIC_EQUIVALENCE: preserve semantics.\n"
            "- COMPLETE_OUTPUT: preserve columns.\n"
            "- CTE_COLUMN_COMPLETENESS: keep required columns.\n"
            "- CROSS_CTE_PREDICATE_BLINDNESS: flat row counts.\n\n"
            "REGRESSION_WARNINGS:\n"
            "None applicable.\n\n"
            "DIVERSITY_MAP:\n"
            "| Worker | Approach | Key Structural Idea |\n"
            "|--------|----------|---------------------|\n"
            "| 1 | minimal_restructuring | Explicit JOINs + date CTE |\n"
            "| 2 | keyset_strategy | Channel keysets + EXISTS probe |\n"
            "| 3 | early_reduction | Selective channel first |\n"
            "| 4 | novel_compound | Unified channel scan |\n\n"
            "=== WORKER 1 BRIEFING ===\n"
            "STRATEGY: explicit_joins\n"
            "APPROACH: Use explicit JOINs with a date CTE to close the predicate blindness gap. "
            "This targets MINIMIZE ROWS TOUCHED by pre-filtering dates.\n"
            "TARGET_QUERY_MAP:\n"
            "  filtered_dates -> fact_scan -> grouped\n"
            "NODE_CONTRACTS:\n"
            "  filtered_dates:\n"
            "    FROM: date_dim\n"
            "    OUTPUT: d_date_sk\n"
            "    CONSUMERS: fact_scan\n"
            "EXAMPLES: date_cte_isolate, prefetch_fact_join\n"
            "EXAMPLE_ADAPTATION:\n"
            "  Apply date isolation. Ignore column names.\n"
            "HAZARD_FLAGS:\n"
            "- Preserve EXISTS semi-join semantics.\n\n"
            "=== WORKER 2 BRIEFING ===\n"
            "STRATEGY: keyset_exists\n"
            "APPROACH: Build distinct customer_sk sets per channel, then EXISTS-probe against keysets. "
            "Targets SMALLEST SET FIRST.\n"
            "TARGET_QUERY_MAP:\n"
            "  store_keys -> web_keys -> catalog_keys -> main\n"
            "NODE_CONTRACTS:\n"
            "  store_keys:\n"
            "    FROM: store_sales\n"
            "    OUTPUT: ss_customer_sk\n"
            "    CONSUMERS: main\n"
            "EXAMPLES: date_cte_isolate\n"
            "EXAMPLE_ADAPTATION:\n"
            "  Apply core pattern.\n"
            "HAZARD_FLAGS:\n"
            "- Max 2 cascading CTE chains.\n\n"
            "=== WORKER 3 BRIEFING ===\n"
            "STRATEGY: early_reduction\n"
            "APPROACH: Build the most selective channel first, narrow the customer set, then probe "
            "remaining channels. Targets DON'T REPEAT WORK.\n"
            "TARGET_QUERY_MAP:\n"
            "  shared_dates -> channel_keys -> main\n"
            "NODE_CONTRACTS:\n"
            "  shared_dates:\n"
            "    FROM: date_dim\n"
            "    OUTPUT: d_date_sk\n"
            "    CONSUMERS: channel_keys\n"
            "EXAMPLES: composite_decorrelate_union\n"
            "EXAMPLE_ADAPTATION:\n"
            "  Full pattern. Keep EXISTS form.\n"
            "HAZARD_FLAGS:\n"
            "- Never materialize EXISTS.\n\n"
            "=== WORKER 4 BRIEFING === (EXPLORATION WORKER)\n"
            "STRATEGY: novel_scan_consolidation\n"
            "APPROACH: Unify all three channel scans into a single pass with conditional aggregation. "
            "Exploratory approach testing DON'T REPEAT WORK at scale.\n"
            "TARGET_QUERY_MAP:\n"
            "  combined_scan -> final\n"
            "NODE_CONTRACTS:\n"
            "  combined_scan:\n"
            "    FROM: store_sales, web_sales, catalog_sales\n"
            "    OUTPUT: customer_sk, channel\n"
            "    CONSUMERS: final\n"
            "EXAMPLES: single_pass_aggregation\n"
            "EXAMPLE_ADAPTATION:\n"
            "  Adapt multi-channel pattern.\n"
            "HAZARD_FLAGS:\n"
            "- High risk of cross-join.\n"
            "EXPLORATION_TYPE: novel_technique\n"
            "HYPOTHESIS_TAG: NOVEL_MULTI_CHANNEL_SCAN\n"
        )

    def test_parses_shared_semantic_contract(self):
        parsed = parse_v2_briefing_response(self._full_response())
        assert len(parsed.shared.semantic_contract.split()) >= 30

    def test_parses_optimal_path(self):
        parsed = parse_v2_briefing_response(self._full_response())
        assert "date_dim" in parsed.shared.optimal_path
        assert "274 rows" in parsed.shared.optimal_path

    def test_parses_current_plan_gap(self):
        parsed = parse_v2_briefing_response(self._full_response())
        assert "MINIMIZE ROWS TOUCHED" in parsed.shared.current_plan_gap
        assert "store_sales" in parsed.shared.current_plan_gap

    def test_backwards_compat_bottleneck_diagnosis(self):
        parsed = parse_v2_briefing_response(self._full_response())
        # bottleneck_diagnosis should be populated from optimal_path
        assert parsed.shared.bottleneck_diagnosis != ""

    def test_backwards_compat_goal_violations(self):
        parsed = parse_v2_briefing_response(self._full_response())
        # goal_violations should be populated from current_plan_gap
        assert parsed.shared.goal_violations != ""

    def test_parses_active_constraints(self):
        parsed = parse_v2_briefing_response(self._full_response())
        assert "LITERAL_PRESERVATION" in parsed.shared.active_constraints

    def test_parses_regression_warnings(self):
        parsed = parse_v2_briefing_response(self._full_response())
        assert "None applicable" in parsed.shared.regression_warnings

    def test_parses_diversity_map(self):
        parsed = parse_v2_briefing_response(self._full_response())
        assert "Worker" in parsed.shared.diversity_map

    def test_parses_four_workers(self):
        parsed = parse_v2_briefing_response(self._full_response())
        assert len(parsed.workers) == 4

    def test_worker_strategy(self):
        parsed = parse_v2_briefing_response(self._full_response())
        assert parsed.workers[0].strategy == "explicit_joins"

    def test_worker_approach(self):
        parsed = parse_v2_briefing_response(self._full_response())
        assert "explicit JOINs" in parsed.workers[0].approach
        assert "predicate blindness" in parsed.workers[0].approach

    def test_worker_target_query_map(self):
        parsed = parse_v2_briefing_response(self._full_response())
        assert "filtered_dates" in parsed.workers[0].target_query_map

    def test_worker_node_contracts(self):
        parsed = parse_v2_briefing_response(self._full_response())
        assert "d_date_sk" in parsed.workers[0].node_contracts

    def test_worker_backwards_compat_target_logical_tree(self):
        parsed = parse_v2_briefing_response(self._full_response())
        # target_logical_tree should be composed from target_query_map + node_contracts
        tlt = parsed.workers[0].target_logical_tree
        assert "TARGET_QUERY_MAP" in tlt
        assert "NODE_CONTRACTS" in tlt

    def test_worker_examples(self):
        parsed = parse_v2_briefing_response(self._full_response())
        assert "date_cte_isolate" in parsed.workers[0].examples
        assert "prefetch_fact_join" in parsed.workers[0].examples

    def test_worker_example_adaptation(self):
        parsed = parse_v2_briefing_response(self._full_response())
        assert "date isolation" in parsed.workers[0].example_adaptation

    def test_worker_hazard_flags(self):
        parsed = parse_v2_briefing_response(self._full_response())
        assert "semi-join" in parsed.workers[0].hazard_flags

    def test_worker4_exploration_fields(self):
        parsed = parse_v2_briefing_response(self._full_response())
        w4 = parsed.workers[3]
        assert w4.exploration_type == "novel_technique"
        assert w4.hypothesis_tag == "NOVEL_MULTI_CHANNEL_SCAN"

    def test_strips_reasoning_block(self):
        parsed = parse_v2_briefing_response(self._full_response())
        assert "internal analysis" not in parsed.shared.semantic_contract

    def test_fault_tolerant_empty_response(self):
        parsed = parse_v2_briefing_response("")
        assert parsed.shared.semantic_contract == ""
        assert len(parsed.workers) == 4

    def test_preserves_raw(self):
        resp = self._full_response()
        parsed = parse_v2_briefing_response(resp)
        assert parsed.raw == resp

    def test_backwards_compat_old_format_still_parses(self):
        """Test that old-format responses with BOTTLENECK_DIAGNOSIS/GOAL_VIOLATIONS still parse."""
        old_response = (
            "=== SHARED BRIEFING ===\n\n"
            "SEMANTIC_CONTRACT: " + " ".join(["word"] * 50) + "\n\n"
            "BOTTLENECK_DIAGNOSIS: scan-bound on store_sales.\n\n"
            "GOAL_VIOLATIONS:\n"
            "- MINIMIZE ROWS TOUCHED: too many rows.\n\n"
            "ACTIVE_CONSTRAINTS:\n"
            "- LITERAL_PRESERVATION: keep.\n\n"
            "REGRESSION_WARNINGS:\n"
            "None applicable.\n\n"
            "DIVERSITY_MAP:\n"
            "| Worker | Cost Region | Risk | Strategy Summary |\n"
            "| 1 | scan | Low | JOINs |\n\n"
            "=== WORKER 1 BRIEFING ===\n"
            "STRATEGY: test\n"
            "COST_REGION: scan\n"
            "RISK_LEVEL: Low\n"
            "GOAL_ADDRESSED: MINIMIZE ROWS TOUCHED\n"
            "TARGET_LOGICAL_TREE:\n"
            "  base -> grouped\n"
            "NODE_CONTRACTS:\n"
            "  base:\n"
            "    FROM: t1\n"
            "    OUTPUT: col1\n"
            "EXAMPLES: ex1\n"
            "EXAMPLE_ADAPTATION: adapt.\n"
            "HAZARD_FLAGS: risk.\n"
        )
        parsed = parse_v2_briefing_response(old_response)
        # Should populate via backwards-compat aliases
        assert parsed.shared.bottleneck_diagnosis != ""
        assert parsed.shared.goal_violations != ""
        assert parsed.workers[0].strategy == "test"

    def test_markdown_bold_without_colons(self):
        """LLMs often use **FIELD_NAME** without a colon — parser must tolerate this."""
        md_response = (
            "### SHARED BRIEFING\n\n"
            "**SEMANTIC_CONTRACT**\n" + " ".join(["word"] * 50) + "\n\n"
            "**OPTIMAL_PATH**\n"
            "date_dim(d_year=2001) -> 274 rows -> store_sales -> 200K rows\n\n"
            "**CURRENT_PLAN_GAP**\n"
            "- MINIMIZE ROWS TOUCHED: excess scans.\n"
            "  Blind spot: CROSS_CTE_PREDICATE_BLINDNESS.\n\n"
            "**ACTIVE_CONSTRAINTS**\n"
            "- LITERAL_PRESERVATION: keep constants.\n"
            "- SEMANTIC_EQUIVALENCE: preserve semantics.\n"
            "- COMPLETE_OUTPUT: preserve columns.\n"
            "- CTE_COLUMN_COMPLETENESS: keep cols.\n\n"
            "**REGRESSION_WARNINGS**\n"
            "None applicable.\n\n"
            "**DIVERSITY_MAP**\n"
            "| Worker | Approach | Key Structural Idea |\n"
            "| 1 | minimal | Explicit JOINs |\n"
            "| 2 | keyset | Channel keysets |\n"
            "| 3 | early | Selective first |\n"
            "| 4 | novel | Unified scan |\n\n"
            "### WORKER 1 BRIEFING\n"
            "**STRATEGY**: explicit_joins\n"
            "**APPROACH**: Use explicit JOINs with a date CTE. "
            "Targets MINIMIZE ROWS TOUCHED.\n"
            "**TARGET_QUERY_MAP**\n"
            "  filtered_dates -> fact_scan -> grouped\n"
            "**NODE_CONTRACTS**\n"
            "  filtered_dates:\n"
            "    FROM: date_dim\n"
            "    OUTPUT: d_date_sk\n"
            "**EXAMPLES**: date_cte_isolate\n"
            "**EXAMPLE_ADAPTATION**\n"
            "  Apply date isolation.\n"
            "**HAZARD_FLAGS**\n"
            "- Preserve semi-join.\n\n"
            "### WORKER 2 BRIEFING\n"
            "**STRATEGY**: keyset_exists\n"
            "**APPROACH**: Channel keysets. Targets SMALLEST SET FIRST.\n"
            "**TARGET_QUERY_MAP**\n"
            "  keys -> main\n"
            "**NODE_CONTRACTS**\n"
            "  keys: FROM: store_sales OUTPUT: sk\n"
            "**EXAMPLES**: prefetch_fact_join\n"
            "**EXAMPLE_ADAPTATION**\n  Apply core.\n"
            "**HAZARD_FLAGS**\n- Watch CTE chains.\n\n"
            "### WORKER 3 BRIEFING\n"
            "**STRATEGY**: early_reduction\n"
            "**APPROACH**: Selective channel first. DON'T REPEAT WORK.\n"
            "**TARGET_QUERY_MAP**\n"
            "  dates -> keys -> main\n"
            "**NODE_CONTRACTS**\n"
            "  dates: FROM: date_dim OUTPUT: d_date_sk\n"
            "**EXAMPLES**: composite_decorrelate_union\n"
            "**EXAMPLE_ADAPTATION**\n  Full pattern.\n"
            "**HAZARD_FLAGS**\n- Don't materialize EXISTS.\n\n"
            "### WORKER 4 BRIEFING\n"
            "**STRATEGY**: novel_compound\n"
            "**APPROACH**: Unified scan. DON'T REPEAT WORK.\n"
            "**TARGET_QUERY_MAP**\n"
            "  combined -> final\n"
            "**NODE_CONTRACTS**\n"
            "  combined: FROM: all sales OUTPUT: customer_sk\n"
            "**EXAMPLES**: single_pass_aggregation\n"
            "**EXAMPLE_ADAPTATION**\n  Multi-channel adapt.\n"
            "**HAZARD_FLAGS**\n- Cross-join risk.\n"
            "**EXPLORATION_TYPE**: novel_technique\n"
            "**HYPOTHESIS_TAG**: MULTI_CHANNEL_SCAN\n"
        )
        parsed = parse_v2_briefing_response(md_response)
        # Shared fields must parse even without colons
        assert len(parsed.shared.semantic_contract.split()) >= 30, (
            f"SEMANTIC_CONTRACT not parsed: '{parsed.shared.semantic_contract[:60]}'"
        )
        assert "274 rows" in parsed.shared.optimal_path
        assert "MINIMIZE ROWS TOUCHED" in parsed.shared.current_plan_gap
        assert "LITERAL_PRESERVATION" in parsed.shared.active_constraints
        assert "None applicable" in parsed.shared.regression_warnings
        assert "Worker" in parsed.shared.diversity_map
        # Workers must parse
        assert len(parsed.workers) == 4
        assert parsed.workers[0].strategy == "explicit_joins"
        assert "filtered_dates" in parsed.workers[0].target_query_map
        assert "d_date_sk" in parsed.workers[0].node_contracts
        assert "date_cte_isolate" in parsed.workers[0].examples


# ═══════════════════════════════════════════════════════════════════════
# V2 Validator Tests
# ═══════════════════════════════════════════════════════════════════════

class TestV2Validator:
    def test_valid_briefing_passes(self):
        parsed = V2ParsedBriefing(
            shared=_shared_valid(),
            workers=[_worker_valid(i) for i in range(1, 5)],
        )
        issues = validate_v2_parsed_briefing(parsed)
        assert issues == [], f"Unexpected issues: {issues}"

    def test_missing_semantic_contract(self):
        shared = _shared_valid()
        shared.semantic_contract = ""
        parsed = V2ParsedBriefing(
            shared=shared,
            workers=[_worker_valid(i) for i in range(1, 5)],
        )
        issues = validate_v2_parsed_briefing(parsed)
        assert any("SEMANTIC_CONTRACT missing" in i for i in issues)

    def test_semantic_contract_too_short(self):
        shared = _shared_valid()
        shared.semantic_contract = "too short"
        parsed = V2ParsedBriefing(
            shared=shared,
            workers=[_worker_valid(i) for i in range(1, 5)],
        )
        issues = validate_v2_parsed_briefing(parsed)
        assert any("token count" in i for i in issues)

    def test_missing_optimal_path(self):
        shared = _shared_valid()
        shared.optimal_path = ""
        shared.bottleneck_diagnosis = ""  # Also clear alias
        parsed = V2ParsedBriefing(
            shared=shared,
            workers=[_worker_valid(i) for i in range(1, 5)],
        )
        issues = validate_v2_parsed_briefing(parsed)
        assert any("OPTIMAL_PATH missing" in i for i in issues)

    def test_missing_current_plan_gap(self):
        shared = _shared_valid()
        shared.current_plan_gap = ""
        shared.goal_violations = ""  # Also clear alias
        parsed = V2ParsedBriefing(
            shared=shared,
            workers=[_worker_valid(i) for i in range(1, 5)],
        )
        issues = validate_v2_parsed_briefing(parsed)
        assert any("CURRENT_PLAN_GAP missing" in i for i in issues)

    def test_missing_diversity_map(self):
        shared = _shared_valid()
        shared.diversity_map = ""
        parsed = V2ParsedBriefing(
            shared=shared,
            workers=[_worker_valid(i) for i in range(1, 5)],
        )
        issues = validate_v2_parsed_briefing(parsed)
        assert any("DIVERSITY_MAP missing" in i for i in issues)

    def test_diversity_map_not_required_for_expert(self):
        shared = _shared_valid()
        shared.diversity_map = ""
        parsed = V2ParsedBriefing(
            shared=shared,
            workers=[_worker_valid(1)],
        )
        issues = validate_v2_parsed_briefing(parsed, expected_workers=1)
        assert not any("DIVERSITY_MAP" in i for i in issues)

    def test_missing_approach(self):
        w = _worker_valid(1)
        w.approach = ""
        parsed = V2ParsedBriefing(
            shared=_shared_valid(),
            workers=[w, _worker_valid(2), _worker_valid(3), _worker_valid(4)],
        )
        issues = validate_v2_parsed_briefing(parsed)
        assert any("APPROACH missing" in i for i in issues)

    def test_missing_target_query_map(self):
        w = _worker_valid(1)
        w.target_query_map = ""
        w.target_logical_tree = ""  # Also clear alias
        parsed = V2ParsedBriefing(
            shared=_shared_valid(),
            workers=[w, _worker_valid(2), _worker_valid(3), _worker_valid(4)],
        )
        issues = validate_v2_parsed_briefing(parsed)
        assert any("TARGET_QUERY_MAP missing" in i for i in issues)

    def test_missing_node_contracts(self):
        w = _worker_valid(1)
        w.node_contracts = ""
        w.target_logical_tree = ""  # Also clear alias
        parsed = V2ParsedBriefing(
            shared=_shared_valid(),
            workers=[w, _worker_valid(2), _worker_valid(3), _worker_valid(4)],
        )
        issues = validate_v2_parsed_briefing(parsed)
        assert any("NODE_CONTRACTS missing" in i for i in issues)

    def test_duplicate_strategy_flagged(self):
        w1 = _worker_valid(1)
        w2 = _worker_valid(2)
        w2.strategy = w1.strategy
        parsed = V2ParsedBriefing(
            shared=_shared_valid(),
            workers=[w1, w2, _worker_valid(3), _worker_valid(4)],
        )
        issues = validate_v2_parsed_briefing(parsed)
        assert any("duplicates" in i for i in issues)

    def test_missing_one_correctness_id_flagged(self):
        """Even 1 missing correctness ID should be flagged in strict mode."""
        shared = _shared_valid()
        shared.active_constraints = (
            "- LITERAL_PRESERVATION: a\n"
            "- SEMANTIC_EQUIVALENCE: b\n"
            "- COMPLETE_OUTPUT: c\n"
            # Missing CTE_COLUMN_COMPLETENESS
        )
        parsed = V2ParsedBriefing(
            shared=shared,
            workers=[_worker_valid(i) for i in range(1, 5)],
        )
        issues = validate_v2_parsed_briefing(parsed)
        assert any("CTE_COLUMN_COMPLETENESS" in i for i in issues)

    def test_missing_one_correctness_id_lenient_passes(self):
        """In lenient mode (post-retry), 1 missing correctness ID is tolerated."""
        shared = _shared_valid()
        shared.active_constraints = (
            "- LITERAL_PRESERVATION: a\n"
            "- COMPLETE_OUTPUT: c\n"
            "- CTE_COLUMN_COMPLETENESS: d\n"
            # Missing SEMANTIC_EQUIVALENCE — tolerated in lenient mode
        )
        parsed = V2ParsedBriefing(
            shared=shared,
            workers=[_worker_valid(i) for i in range(1, 5)],
        )
        issues = validate_v2_parsed_briefing(parsed, lenient=True)
        assert not any("SEMANTIC_EQUIVALENCE" in i for i in issues), (
            f"Lenient mode should tolerate 1 missing ID, got: {issues}"
        )

    def test_missing_two_correctness_ids_lenient_fails(self):
        """In lenient mode, 2+ missing correctness IDs still fail."""
        shared = _shared_valid()
        shared.active_constraints = (
            "- LITERAL_PRESERVATION: a\n"
            "- COMPLETE_OUTPUT: c\n"
            # Missing SEMANTIC_EQUIVALENCE and CTE_COLUMN_COMPLETENESS
        )
        parsed = V2ParsedBriefing(
            shared=shared,
            workers=[_worker_valid(i) for i in range(1, 5)],
        )
        issues = validate_v2_parsed_briefing(parsed, lenient=True)
        assert any("SEMANTIC_EQUIVALENCE" in i for i in issues)
        assert any("CTE_COLUMN_COMPLETENESS" in i for i in issues)

    def test_too_many_gap_ids(self):
        shared = _shared_valid()
        shared.active_constraints = (
            "- LITERAL_PRESERVATION: a\n"
            "- SEMANTIC_EQUIVALENCE: b\n"
            "- COMPLETE_OUTPUT: c\n"
            "- CTE_COLUMN_COMPLETENESS: d\n"
            "- GAP_A: e\n- GAP_B: f\n- GAP_C: g\n- GAP_D: h\n"
        )
        parsed = V2ParsedBriefing(
            shared=shared,
            workers=[_worker_valid(i) for i in range(1, 5)],
        )
        issues = validate_v2_parsed_briefing(parsed)
        assert any("too many gap IDs" in i for i in issues)

    def test_expert_mode_single_worker(self):
        parsed = V2ParsedBriefing(
            shared=_shared_valid(),
            workers=[_worker_valid(1)],
        )
        issues = validate_v2_parsed_briefing(parsed, expected_workers=1)
        assert issues == [], f"Unexpected issues: {issues}"

    def test_missing_worker_flagged(self):
        parsed = V2ParsedBriefing(
            shared=_shared_valid(),
            workers=[_worker_valid(1), _worker_valid(2)],  # missing 3, 4
        )
        issues = validate_v2_parsed_briefing(parsed)
        assert any("WORKER_3: missing" in i for i in issues)
        assert any("WORKER_4: missing" in i for i in issues)

    def test_backwards_compat_target_logical_tree_still_validates(self):
        """Worker with only target_logical_tree (no target_query_map) should still pass."""
        w = V2BriefingWorker(
            worker_id=1,
            strategy="old_style",
            approach="Some approach text.",
            target_logical_tree="TARGET_LOGICAL_TREE:\n  base -> grouped\n\nNODE_CONTRACTS:\n  base:\n    FROM: t1",
            examples=["ex1"],
            example_adaptation="Adapt.",
            hazard_flags="Risk.",
        )
        parsed = V2ParsedBriefing(
            shared=_shared_valid(),
            workers=[w, _worker_valid(2), _worker_valid(3), _worker_valid(4)],
        )
        issues = validate_v2_parsed_briefing(parsed)
        # Should NOT flag TARGET_QUERY_MAP or NODE_CONTRACTS missing when target_logical_tree is set
        assert not any("TARGET_QUERY_MAP missing" in i for i in issues)
        assert not any("NODE_CONTRACTS missing" in i for i in issues)


# ═══════════════════════════════════════════════════════════════════════
# V2 Checklist Tests
# ═══════════════════════════════════════════════════════════════════════

class TestV2Checklists:
    def test_analyst_checklist_has_optimal_path(self):
        result = build_v2_analyst_checklist()
        assert "OPTIMAL_PATH" in result

    def test_analyst_checklist_has_current_plan_gap(self):
        result = build_v2_analyst_checklist()
        assert "CURRENT_PLAN_GAP" in result

    def test_analyst_checklist_has_diversity_map(self):
        result = build_v2_analyst_checklist()
        assert "DIVERSITY_MAP" in result

    def test_analyst_checklist_has_approach(self):
        result = build_v2_analyst_checklist()
        assert "APPROACH" in result

    def test_analyst_checklist_has_target_query_map(self):
        result = build_v2_analyst_checklist()
        assert "TARGET_QUERY_MAP" in result

    def test_analyst_checklist_has_node_contracts(self):
        result = build_v2_analyst_checklist()
        assert "NODE_CONTRACTS" in result

    def test_expert_checklist(self):
        result = build_v2_expert_checklist()
        assert "OPTIMAL_PATH" in result
        assert "CURRENT_PLAN_GAP" in result
        assert "TARGET_QUERY_MAP" in result

    def test_oneshot_checklist(self):
        result = build_v2_oneshot_checklist()
        assert "OPTIMAL_PATH" in result
        assert "CURRENT_PLAN_GAP" in result

    def test_worker_rewrite_checklist(self):
        result = build_v2_worker_rewrite_checklist()
        assert "TARGET_QUERY_MAP" in result
        assert "CURRENT_PLAN_GAP" in result

    def test_discovery_mode_checklist(self):
        result = build_v2_analyst_checklist(is_discovery_mode=True)
        assert "all" in result.lower()


# ═══════════════════════════════════════════════════════════════════════
# V2 Worker Prompt Tests
# ═══════════════════════════════════════════════════════════════════════

class TestV2WorkerPrompt:
    def test_basic_worker_prompt(self):
        prompt = build_v2_worker_prompt(
            worker_briefing=_worker_valid(1),
            shared_briefing=_shared_valid(),
            examples=[],
            original_sql="SELECT 1",
            output_columns=["c1"],
            dialect="duckdb",
            engine_version="1.1",
        )
        assert "SQL rewrite engine for DuckDB" in prompt
        assert "Semantic Contract" in prompt
        assert "SELECT 1" in prompt

    def test_includes_current_plan_gap(self):
        prompt = build_v2_worker_prompt(
            worker_briefing=_worker_valid(1),
            shared_briefing=_shared_valid(),
            examples=[],
            original_sql="SELECT 1",
            output_columns=["c1"],
        )
        assert "Current Plan Gap" in prompt
        assert "MINIMIZE ROWS TOUCHED" in prompt

    def test_includes_approach(self):
        prompt = build_v2_worker_prompt(
            worker_briefing=_worker_valid(1),
            shared_briefing=_shared_valid(),
            examples=[],
            original_sql="SELECT 1",
            output_columns=["c1"],
        )
        assert "Approach" in prompt
        assert "restructure the date filtering" in prompt

    def test_includes_strategy_in_assignment(self):
        prompt = build_v2_worker_prompt(
            worker_briefing=_worker_valid(1),
            shared_briefing=_shared_valid(),
            examples=[],
            original_sql="SELECT 1",
            output_columns=["c1"],
        )
        assert "Strategy: strategy_1_custom" in prompt

    def test_includes_target_query_map(self):
        prompt = build_v2_worker_prompt(
            worker_briefing=_worker_valid(1),
            shared_briefing=_shared_valid(),
            examples=[],
            original_sql="SELECT 1",
            output_columns=["c1"],
        )
        assert "Target Query Map" in prompt
        assert "filtered_dates" in prompt

    def test_includes_node_contracts(self):
        prompt = build_v2_worker_prompt(
            worker_briefing=_worker_valid(1),
            shared_briefing=_shared_valid(),
            examples=[],
            original_sql="SELECT 1",
            output_columns=["c1"],
        )
        assert "NODE_CONTRACTS" in prompt
        assert "d_date_sk" in prompt

    def test_includes_hazard_flags(self):
        prompt = build_v2_worker_prompt(
            worker_briefing=_worker_valid(1),
            shared_briefing=_shared_valid(),
            examples=[],
            original_sql="SELECT 1",
            output_columns=["c1"],
        )
        assert "Hazard Flags" in prompt

    def test_includes_rewrite_checklist(self):
        prompt = build_v2_worker_prompt(
            worker_briefing=_worker_valid(1),
            shared_briefing=_shared_valid(),
            examples=[],
            original_sql="SELECT 1",
            output_columns=["c1"],
        )
        assert "Rewrite Checklist" in prompt

    def test_includes_column_completeness(self):
        prompt = build_v2_worker_prompt(
            worker_briefing=_worker_valid(1),
            shared_briefing=_shared_valid(),
            examples=[],
            original_sql="SELECT 1",
            output_columns=["col_a", "col_b"],
        )
        assert "`col_a`" in prompt
        assert "`col_b`" in prompt

    def test_includes_output_format(self):
        prompt = build_v2_worker_prompt(
            worker_briefing=_worker_valid(1),
            shared_briefing=_shared_valid(),
            examples=[],
            original_sql="SELECT 1",
            output_columns=["c1"],
        )
        assert "Component Payload JSON" in prompt

    def test_postgresql_dialect(self):
        prompt = build_v2_worker_prompt(
            worker_briefing=_worker_valid(1),
            shared_briefing=_shared_valid(),
            examples=[],
            original_sql="SELECT 1",
            output_columns=["c1"],
            dialect="postgresql",
            engine_version="14.3",
        )
        assert "PostgreSQL v14.3" in prompt

    def test_examples_rendered(self):
        examples = [{
            "id": "test_example",
            "verified_speedup": "2.5x",
            "principle": "Test principle",
            "example": {
                "before_sql": "SELECT * FROM t1",
                "output": {"sql": "SELECT * FROM t1 WHERE id > 0"},
            },
        }]
        prompt = build_v2_worker_prompt(
            worker_briefing=_worker_valid(1),
            shared_briefing=_shared_valid(),
            examples=examples,
            original_sql="SELECT 1",
            output_columns=["c1"],
        )
        assert "test_example" in prompt
        assert "Test principle" in prompt

    def test_backwards_compat_old_worker_briefing(self):
        """Worker prompt works with old-style briefing (target_logical_tree instead of target_query_map)."""
        old_worker = V2BriefingWorker(
            worker_id=1,
            strategy="old_strategy",
            target_logical_tree=(
                "TARGET_LOGICAL_TREE:\n"
                "  filtered_dates -> fact_scan\n\n"
                "NODE_CONTRACTS:\n"
                "  filtered_dates:\n"
                "    FROM: date_dim\n"
                "    OUTPUT: d_date_sk"
            ),
            examples=["ex1"],
            example_adaptation="Adapt.",
            hazard_flags="Risk.",
        )
        old_shared = V2BriefingShared(
            semantic_contract="Business intent.",
            goal_violations="- MINIMIZE ROWS TOUCHED: too many rows.",
            regression_warnings="None applicable.",
            active_constraints="- LITERAL_PRESERVATION: keep.",
        )
        prompt = build_v2_worker_prompt(
            worker_briefing=old_worker,
            shared_briefing=old_shared,
            examples=[],
            original_sql="SELECT 1",
            output_columns=["c1"],
        )
        assert "Target Query Map" in prompt
        assert "filtered_dates" in prompt
        # Should use goal_violations fallback for Current Plan Gap
        assert "Current Plan Gap" in prompt
        assert "MINIMIZE ROWS TOUCHED" in prompt


# ═══════════════════════════════════════════════════════════════════════
# Helper Function Tests
# ═══════════════════════════════════════════════════════════════════════

class TestHelpers:
    def test_detect_aggregate_functions_count_sum_max(self):
        aggs = _detect_aggregate_functions(_dag_with_aggs(), {})
        assert "COUNT" in aggs
        assert "SUM" in aggs
        assert "MAX" in aggs

    def test_detect_aggregate_functions_stddev(self):
        aggs = _detect_aggregate_functions(_dag_with_unsafe_aggs(), {})
        assert "STDDEV_SAMP" in aggs
        assert "AVG" in aggs

    def test_detect_aggregate_functions_none(self):
        dag = QueryLogicalTree(
            nodes={
                "main_query": LogicalTreeNode(
                    node_id="main_query", node_type="main",
                    sql="SELECT * FROM t1", tables=["t1"], refs=[], flags=[],
                )
            },
            edges=[], original_sql="SELECT * FROM t1",
        )
        aggs = _detect_aggregate_functions(dag, {})
        assert aggs == []

    def test_detect_query_features_left_join(self):
        features = _detect_query_features(_dag_with_left_join())
        assert features["has_left_join"] is True

    def test_detect_query_features_exists(self):
        features = _detect_query_features(_dag_with_exists())
        assert features["has_exists"] is True

    def test_detect_query_features_minimal(self):
        features = _detect_query_features(_minimal_dag())
        assert features["has_left_join"] is False
        assert features["has_exists"] is False
        assert features["has_window"] is False
        assert features["has_intersect"] is False

    def test_detect_query_features_group_by(self):
        features = _detect_query_features(_dag_with_aggs())
        assert features["has_group_by"] is True

    def test_format_blind_spot_id(self):
        assert _format_blind_spot_id("CROSS_CTE_PREDICATE_BLINDNESS") == "Cross CTE predicate blindness"
        assert _format_blind_spot_id("CORRELATED_SUBQUERY_PARALYSIS") == "Correlated subquery paralysis"

    def test_format_blind_spot_id_preserves_acronyms(self):
        assert "CTE" in _format_blind_spot_id("CROSS_CTE_PREDICATE_BLINDNESS")
        assert "CSE" in _format_blind_spot_id("SUBQUERY_CSE")
        assert "OR" in _format_blind_spot_id("CROSS_COLUMN_OR_DECOMPOSITION")
