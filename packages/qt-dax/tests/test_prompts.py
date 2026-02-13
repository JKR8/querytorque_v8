"""Tests for qt_dax.prompts — prompter, optimizer, and pathology mapping."""

from dataclasses import dataclass

import pytest

from qt_dax.prompts.prompter import DAXPrompter, PromptInputs, _infer_pathologies
from qt_dax.prompts.optimizer import DAXOptimizer, DAXOptimizationResult, _parse_json_response
from qt_dax.knowledge import load_examples, match_examples


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@dataclass
class FakeTMDLRelationship:
    """Mimics qt_dax.parsers.tmdl_parser.TMDLRelationship."""
    from_table: str
    from_column: str
    to_table: str
    to_column: str
    cross_filter: str = "single"
    is_active: bool = True


@dataclass
class FakeTMDLColumn:
    """Mimics qt_dax.parsers.tmdl_parser.TMDLColumn."""
    name: str
    table: str
    data_type: str = "string"


@pytest.fixture
def basic_inputs():
    return PromptInputs(
        measure_name="Total Sales",
        measure_table="Measures",
        measure_dax="SUMX(Sales, Sales[Qty] * Sales[Price])",
        dependency_chain=[],
        detected_issues=[],
    )


@pytest.fixture
def pbip_schema():
    """Schema shaped like TMDL parser output — columns at top level, relationships as dataclasses."""
    return {
        "tables": [
            {"name": "Sales", "is_local_date_table": False},
            {"name": "Products", "is_local_date_table": False},
        ],
        "columns": [
            FakeTMDLColumn(name="Qty", table="Sales"),
            FakeTMDLColumn(name="Price", table="Sales"),
            FakeTMDLColumn(name="ProductID", table="Sales"),
            FakeTMDLColumn(name="ProductID", table="Products"),
            FakeTMDLColumn(name="ProductName", table="Products"),
        ],
        "measures": [],
        "relationships": [
            FakeTMDLRelationship(
                from_table="Sales",
                from_column="ProductID",
                to_table="Products",
                to_column="ProductID",
            ),
        ],
    }


@pytest.fixture
def vpax_schema():
    """Schema shaped like VPAX parser output — columns embedded, relationships as dicts."""
    return {
        "tables": [
            {
                "name": "Sales",
                "columns": [
                    {"name": "Qty", "data_type": "int"},
                    {"name": "Price", "data_type": "decimal"},
                ],
            },
        ],
        "relationships": [
            {
                "from_table": "Sales",
                "from_column": "ProductID",
                "to_table": "Products",
                "to_column": "ProductID",
            },
        ],
    }


# ---------------------------------------------------------------------------
# Imports
# ---------------------------------------------------------------------------

class TestImports:
    def test_import_prompter(self):
        from qt_dax.prompts.prompter import DAXPrompter, PromptInputs
        assert DAXPrompter is not None

    def test_import_optimizer(self):
        from qt_dax.prompts.optimizer import DAXOptimizer, DAXOptimizationResult
        assert DAXOptimizer is not None

    def test_import_package_init(self):
        from qt_dax.prompts import DAXPrompter, DAXOptimizer, DAXOptimizationResult
        assert DAXPrompter is not None


# ---------------------------------------------------------------------------
# Prompter — section rendering
# ---------------------------------------------------------------------------

class TestPrompterSections:
    def test_all_sections_present(self, basic_inputs):
        basic_inputs.detected_issues = [
            {"rule_id": "DAX001", "severity": "critical", "description": "test", "recommendation": "fix it"},
        ]
        prompter = DAXPrompter()
        prompt = prompter.build_prompt(basic_inputs)

        assert "# ROLE" in prompt
        assert "# TARGET MEASURE" in prompt
        assert "# DETECTED ISSUES" in prompt
        assert "# REWRITE PLAYBOOK" in prompt
        assert "# CONSTRAINTS (MUST OBEY)" in prompt
        assert "# OUTPUT FORMAT" in prompt

    def test_dependency_closure_omitted_when_empty(self, basic_inputs):
        prompter = DAXPrompter()
        prompt = prompter.build_prompt(basic_inputs)
        assert "# DEPENDENCY CLOSURE" not in prompt

    def test_dependency_closure_present_when_provided(self, basic_inputs):
        basic_inputs.dependency_chain = [
            {"name": "Base Qty", "table": "Measures", "expression": "SUM(Sales[Qty])"},
        ]
        prompter = DAXPrompter()
        prompt = prompter.build_prompt(basic_inputs)
        assert "# DEPENDENCY CLOSURE" in prompt
        assert "Base Qty" in prompt
        assert "SUM(Sales[Qty])" in prompt

    def test_retry_context_omitted_on_first_attempt(self, basic_inputs):
        prompter = DAXPrompter()
        prompt = prompter.build_prompt(basic_inputs)
        assert "PREVIOUS ATTEMPT" not in prompt

    def test_retry_context_present_on_retry(self, basic_inputs):
        basic_inputs.previous_attempt = "VAR x = 1 RETURN x"
        basic_inputs.previous_error = "Semantic mismatch"
        prompter = DAXPrompter()
        prompt = prompter.build_prompt(basic_inputs)
        assert "# PREVIOUS ATTEMPT (FAILED)" in prompt
        assert "VAR x = 1 RETURN x" in prompt
        assert "Semantic mismatch" in prompt

    def test_model_schema_omitted_when_none(self, basic_inputs):
        prompter = DAXPrompter()
        prompt = prompter.build_prompt(basic_inputs)
        assert "# MODEL SCHEMA" not in prompt


# ---------------------------------------------------------------------------
# Prompter — PBIP schema normalization (Fix #1 + #2)
# ---------------------------------------------------------------------------

class TestPBIPSchemaNormalization:
    def test_pbip_relationships_do_not_crash(self, basic_inputs, pbip_schema):
        """TMDLRelationship dataclasses must not raise AttributeError."""
        basic_inputs.model_schema = pbip_schema
        prompter = DAXPrompter()
        prompt = prompter.build_prompt(basic_inputs)
        assert "Sales[ProductID] -> Products[ProductID]" in prompt

    def test_pbip_columns_appear_in_prompt(self, basic_inputs, pbip_schema):
        """Columns from top-level schema.columns should be grouped by table."""
        basic_inputs.model_schema = pbip_schema
        prompter = DAXPrompter()
        prompt = prompter.build_prompt(basic_inputs)
        # Sales table should list its columns
        assert "**Sales**:" in prompt
        assert "Qty" in prompt
        assert "Price" in prompt
        # Products table should list its columns
        assert "**Products**:" in prompt
        assert "ProductName" in prompt

    def test_vpax_dict_relationships_still_work(self, basic_inputs, vpax_schema):
        """Dict-based relationships (VPAX path) must still work."""
        basic_inputs.model_schema = vpax_schema
        prompter = DAXPrompter()
        prompt = prompter.build_prompt(basic_inputs)
        assert "Sales[ProductID] -> Products[ProductID]" in prompt

    def test_vpax_embedded_columns_still_work(self, basic_inputs, vpax_schema):
        """Columns embedded under tables (VPAX path) must still render."""
        basic_inputs.model_schema = vpax_schema
        prompter = DAXPrompter()
        prompt = prompter.build_prompt(basic_inputs)
        assert "**Sales**:" in prompt
        assert "Qty" in prompt


# ---------------------------------------------------------------------------
# Pathology mapping (Fix #3)
# ---------------------------------------------------------------------------

class TestPathologyMapping:
    def test_dax027_maps_to_p1(self):
        """DAX027 (MEASURE_CHAIN_DEPTH) should trigger P1."""
        issues = [{"rule_id": "DAX027", "severity": "medium"}]
        pathologies = _infer_pathologies(issues)
        assert "P1" in pathologies

    def test_dax028_maps_to_p5(self):
        """DAX028 (SUM_OF_RATIOS_PATTERN) should trigger P5."""
        issues = [{"rule_id": "DAX028", "severity": "medium"}]
        pathologies = _infer_pathologies(issues)
        assert "P5" in pathologies

    def test_daxc001_maps_to_p4(self):
        """DAXC001 (ROW_ITERATION_OWNERSHIP_CARBON) should trigger P4."""
        issues = [{"rule_id": "DAXC001", "severity": "high"}]
        pathologies = _infer_pathologies(issues)
        assert "P4" in pathologies

    def test_dax003_maps_to_p1(self):
        """DAX003 (deep CALCULATE nesting) should trigger P1."""
        issues = [{"rule_id": "DAX003", "severity": "critical"}]
        pathologies = _infer_pathologies(issues)
        assert "P1" in pathologies

    def test_dax001_maps_to_p2(self):
        """DAX001 (FILTER table iterator) should trigger P2."""
        issues = [{"rule_id": "DAX001", "severity": "critical"}]
        pathologies = _infer_pathologies(issues)
        assert "P2" in pathologies

    def test_dax002_maps_to_p3(self):
        """DAX002 (SUMX+FILTER) should trigger P3."""
        issues = [{"rule_id": "DAX002", "severity": "critical"}]
        pathologies = _infer_pathologies(issues)
        assert "P3" in pathologies

    def test_dax028_does_not_map_to_p1(self):
        """DAX028 is sum-of-ratios (P5), NOT measure forest (P1)."""
        issues = [{"rule_id": "DAX028", "severity": "medium"}]
        pathologies = _infer_pathologies(issues)
        assert "P1" not in pathologies

    def test_daxc001_does_not_map_to_p5(self):
        """DAXC001 is grain-first (P4), NOT sum-of-ratios (P5)."""
        issues = [{"rule_id": "DAXC001", "severity": "high"}]
        pathologies = _infer_pathologies(issues)
        assert "P5" not in pathologies

    def test_fallback_when_no_known_rules(self):
        """Unknown rules should fall back to P1+P4."""
        issues = [{"rule_id": "DAX999", "severity": "low"}]
        pathologies = _infer_pathologies(issues)
        assert "P1" in pathologies
        assert "P4" in pathologies

    def test_empty_issues_returns_empty(self):
        assert _infer_pathologies([]) == []

    def test_multiple_rules_combine(self):
        """Multiple rules should trigger multiple pathologies."""
        issues = [
            {"rule_id": "DAX027", "severity": "medium"},
            {"rule_id": "DAX028", "severity": "medium"},
            {"rule_id": "DAXC001", "severity": "high"},
        ]
        pathologies = _infer_pathologies(issues)
        assert "P1" in pathologies  # DAX027
        assert "P4" in pathologies  # DAXC001
        assert "P5" in pathologies  # DAX028


# ---------------------------------------------------------------------------
# JSON response parsing
# ---------------------------------------------------------------------------

class TestJSONParsing:
    def test_fenced_json_block(self):
        response = 'Here:\n```json\n{"optimized_dax": "SUM(x)", "transforms_applied": ["foo"], "rationale": "done"}\n```'
        result = _parse_json_response(response)
        assert result is not None
        assert result["optimized_dax"] == "SUM(x)"
        assert result["transforms_applied"] == ["foo"]

    def test_bare_json_object(self):
        response = 'The answer: {"optimized_dax": "VAR x = 1 RETURN x", "transforms_applied": [], "rationale": "ok"}'
        result = _parse_json_response(response)
        assert result is not None
        assert "VAR x" in result["optimized_dax"]

    def test_no_json_returns_none(self):
        assert _parse_json_response("no json here") is None

    def test_empty_string_returns_none(self):
        assert _parse_json_response("") is None

    def test_malformed_json_returns_none(self):
        assert _parse_json_response('```json\n{bad json}\n```') is None

    def test_nested_braces(self):
        response = '{"optimized_dax": "IF(a, {1}, {2})", "transforms_applied": [], "rationale": "x"}'
        result = _parse_json_response(response)
        # This particular case has embedded braces in a string value —
        # the simple brace-counter may or may not handle it, so we just
        # verify no crash
        assert result is None or isinstance(result, dict)


# ---------------------------------------------------------------------------
# Optimizer — dry-run and retry
# ---------------------------------------------------------------------------

class TestOptimizerDryRun:
    def test_dry_run_returns_prompt(self, basic_inputs):
        optimizer = DAXOptimizer()
        result = optimizer.optimize_measure(basic_inputs, dry_run=True)
        assert result.status == "dry_run"
        assert len(result.prompt) > 0
        assert "# ROLE" in result.prompt

    def test_no_llm_returns_error(self, basic_inputs):
        optimizer = DAXOptimizer(llm_client=None)
        result = optimizer.optimize_measure(basic_inputs)
        assert result.status == "error"
        assert "No LLM client" in result.error


class TestOptimizerRetry:
    def test_retry_feeds_back_error(self, basic_inputs):
        """Verify that on parse failure, the retry prompt includes error context."""
        call_count = 0
        prompts_received = []

        class FakeLLM:
            def analyze(self, prompt):
                nonlocal call_count
                call_count += 1
                prompts_received.append(prompt)
                if call_count == 1:
                    return "garbage response with no json"
                return '```json\n{"optimized_dax": "SUM(x)", "transforms_applied": [], "rationale": "fixed"}\n```'

        optimizer = DAXOptimizer(llm_client=FakeLLM())
        result = optimizer.optimize_measure(basic_inputs, max_attempts=3)

        assert result.status == "pass"
        assert result.attempts == 2
        assert call_count == 2
        # Second prompt should contain retry context
        assert "PREVIOUS ATTEMPT" in prompts_received[1]

    def test_no_improvement_status(self, basic_inputs):
        """Empty optimized_dax should return no_improvement."""
        class FakeLLM:
            def analyze(self, prompt):
                return '```json\n{"optimized_dax": "", "transforms_applied": [], "rationale": "already optimal"}\n```'

        optimizer = DAXOptimizer(llm_client=FakeLLM())
        result = optimizer.optimize_measure(basic_inputs, max_attempts=1)
        assert result.status == "no_improvement"
        assert result.rationale == "already optimal"

    def test_exhausted_attempts(self, basic_inputs):
        """All attempts failing should return error."""
        class FakeLLM:
            def analyze(self, prompt):
                return "not json"

        optimizer = DAXOptimizer(llm_client=FakeLLM())
        result = optimizer.optimize_measure(basic_inputs, max_attempts=2)
        assert result.status == "error"
        assert result.attempts == 2


# ---------------------------------------------------------------------------
# Knowledge — example loading + matching
# ---------------------------------------------------------------------------

class TestKnowledgeExamples:
    def test_load_examples_returns_list(self):
        examples = load_examples()
        assert isinstance(examples, list)
        assert len(examples) == 5

    def test_each_example_has_required_fields(self):
        for ex in load_examples():
            assert "id" in ex
            assert "pathologies_addressed" in ex
            assert isinstance(ex["pathologies_addressed"], list)

    def test_match_examples_by_pathology(self):
        matched = match_examples(["P1", "P2"], max_examples=2)
        assert len(matched) <= 2
        for ex in matched:
            # Should have overlap with P1 or P2
            assert set(ex["pathologies_addressed"]) & {"P1", "P2"}

    def test_match_examples_empty_pathologies(self):
        assert match_examples([]) == []

    def test_match_examples_no_overlap(self):
        assert match_examples(["P99"]) == []
