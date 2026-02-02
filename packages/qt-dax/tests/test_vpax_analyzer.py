"""Phase 3: DAX Analyzer Tests - VPAX Analyzer.

Tests for VPAX file parsing, analysis, and quality gate calculation.
"""

import pytest
import json
from pathlib import Path

from qt_dax.analyzers.vpax_analyzer import (
    VPAXParser,
    DAXAnalyzer,
    ModelAnalyzer,
    ReportGenerator,
    VPAXIssue,
    MeasureAnalysis,
    QualityGate,
    DAX_RULES,
    MODEL_RULES,
    Severity,
    Category,
)


class TestVPAXParser:
    """Tests for VPAX file parsing."""

    def test_parser_parses_vpax(self, sample_vpax_file):
        """Test that parser can parse a VPAX file."""
        parser = VPAXParser(str(sample_vpax_file))
        data = parser.parse()

        assert data is not None
        assert "vpa_view" in data
        assert data["vpa_view"] is not None

    def test_parser_extracts_tables(self, sample_vpax_file):
        """Test that parser extracts tables."""
        parser = VPAXParser(str(sample_vpax_file))
        data = parser.parse()

        tables = data["vpa_view"]["Tables"]
        assert len(tables) > 0

    def test_parser_extracts_measures(self, sample_vpax_file):
        """Test that parser extracts measures."""
        parser = VPAXParser(str(sample_vpax_file))
        data = parser.parse()

        measures = data["vpa_view"]["Measures"]
        assert len(measures) > 0

    def test_parser_extracts_columns(self, sample_vpax_file):
        """Test that parser extracts columns."""
        parser = VPAXParser(str(sample_vpax_file))
        data = parser.parse()

        columns = data["vpa_view"]["Columns"]
        assert len(columns) > 0


class TestDAXAnalyzer:
    """Tests for DAX measure analysis."""

    def test_analyzer_instantiation(self, dax_analyzer):
        """Test DAXAnalyzer can be instantiated."""
        assert dax_analyzer is not None

    def test_analyze_simple_measure(self, dax_analyzer):
        """Test analyzing a simple measure."""
        analysis = dax_analyzer.analyze_measure(
            name="Total Sales",
            table="Sales",
            expression="SUM('Sales'[Amount])"
        )

        assert isinstance(analysis, MeasureAnalysis)
        assert analysis.name == "Total Sales"
        assert analysis.length > 0

    def test_analyze_measure_with_division_issue(self, dax_analyzer):
        """Test detecting division without DIVIDE."""
        analysis = dax_analyzer.analyze_measure(
            name="Bad Ratio",
            table="Sales",
            expression="[Total Sales] / [Total Quantity]"
        )

        # Should detect DAX004 - DIVISION_WITHOUT_DIVIDE
        rule_ids = [i.rule_id for i in analysis.issues]
        assert "DAX004" in rule_ids

    def test_analyze_measure_with_divide_no_issue(self, dax_analyzer):
        """Test DIVIDE function doesn't trigger division rule."""
        analysis = dax_analyzer.analyze_measure(
            name="Good Ratio",
            table="Sales",
            expression="DIVIDE([Total Sales], [Total Quantity], 0)"
        )

        # Should NOT detect DAX004
        rule_ids = [i.rule_id for i in analysis.issues]
        assert "DAX004" not in rule_ids

    def test_analyze_nested_calculate(self, dax_analyzer):
        """Test detecting nested CALCULATE."""
        expression = """
        CALCULATE(
            CALCULATE(
                CALCULATE(
                    CALCULATE(
                        SUM('Sales'[Amount]),
                        'A'[X] = 1
                    ),
                    'B'[Y] = 2
                ),
                'C'[Z] = 3
            ),
            'D'[W] = 4
        )
        """
        analysis = dax_analyzer.analyze_measure(
            name="Deep Calculate",
            table="Sales",
            expression=expression
        )

        # Should detect DAX003 or DAX005 for CALCULATE nesting
        rule_ids = [i.rule_id for i in analysis.issues]
        assert "DAX003" in rule_ids or "DAX005" in rule_ids

    def test_analyze_sumx_filter_pattern(self, dax_analyzer, sample_sumx_filter_dax):
        """Test detecting SUMX + FILTER anti-pattern."""
        analysis = dax_analyzer.analyze_measure(
            name="Bad Iterator",
            table="Sales",
            expression=sample_sumx_filter_dax
        )

        # Should detect DAX002 - SUMX_FILTER_COMBO
        rule_ids = [i.rule_id for i in analysis.issues]
        assert "DAX002" in rule_ids

    def test_analyze_complex_measure_without_var(self, dax_analyzer):
        """Test detecting complex measure without VAR."""
        # Long expression without VAR
        expression = "SUM('Sales'[Amount]) + SUM('Sales'[Cost]) + SUM('Sales'[Discount]) + " * 20
        expression = expression.rstrip(" + ")  # Clean up

        analysis = dax_analyzer.analyze_measure(
            name="Long Measure",
            table="Sales",
            expression=expression
        )

        # May detect DAX006 if long enough
        # Just verify no crash
        assert isinstance(analysis, MeasureAnalysis)


class TestModelAnalyzer:
    """Tests for model structure analysis."""

    def test_analyze_tables(self, sample_vpax_data):
        """Test analyzing tables."""
        analyzer = ModelAnalyzer(sample_vpax_data)
        table_analyses = analyzer.analyze_tables()

        assert len(table_analyses) > 0

    def test_detect_local_date_table(self, sample_vpax_data):
        """Test detecting local date tables (auto date/time)."""
        analyzer = ModelAnalyzer(sample_vpax_data)
        table_analyses = analyzer.analyze_tables()

        # Should detect the LocalDateTable
        local_tables = [t for t in table_analyses if t.is_local_date_table]
        assert len(local_tables) > 0

        # Should have MDL001 issue
        for t in local_tables:
            rule_ids = [i.rule_id for i in t.issues]
            assert "MDL001" in rule_ids

    def test_analyze_columns(self, sample_vpax_data):
        """Test analyzing columns."""
        analyzer = ModelAnalyzer(sample_vpax_data)
        column_analyses = analyzer.analyze_columns()

        assert len(column_analyses) > 0

    def test_detect_high_cardinality(self, sample_vpax_data):
        """Test detecting high cardinality columns."""
        analyzer = ModelAnalyzer(sample_vpax_data)
        column_analyses = analyzer.analyze_columns()

        # Should detect the HighCardColumn
        high_card = [c for c in column_analyses if c.cardinality > 1000000]
        assert len(high_card) > 0

        # Should have MDL003 issue
        for c in high_card:
            rule_ids = [i.rule_id for i in c.issues]
            assert "MDL003" in rule_ids

    def test_analyze_relationships(self, sample_vpax_data):
        """Test analyzing relationships."""
        analyzer = ModelAnalyzer(sample_vpax_data)
        relationship_analyses = analyzer.analyze_relationships()

        assert len(relationship_analyses) > 0

    def test_detect_bidirectional_relationship(self, sample_vpax_data):
        """Test detecting bi-directional relationships."""
        analyzer = ModelAnalyzer(sample_vpax_data)
        relationship_analyses = analyzer.analyze_relationships()

        # Should detect bi-directional relationship
        bidir = [r for r in relationship_analyses if r.cross_filter == "Both"]
        assert len(bidir) > 0

        # Should have MDL006 issue
        for r in bidir:
            rule_ids = [i.rule_id for i in r.issues]
            assert "MDL006" in rule_ids

    def test_detect_referential_integrity_violation(self, sample_vpax_data):
        """Test detecting referential integrity violations."""
        analyzer = ModelAnalyzer(sample_vpax_data)
        relationship_analyses = analyzer.analyze_relationships()

        # Should detect RI violations
        ri_issues = [
            r for r in relationship_analyses
            if r.missing_keys > 0
        ]
        assert len(ri_issues) > 0


class TestQualityGate:
    """Tests for quality gate calculation."""

    def test_peak_torque_threshold(self):
        """Test Peak Torque gate (score >= 90)."""
        gate = QualityGate.from_score(95)
        assert gate.label == "Peak Torque"
        assert gate.status == "pass"

        gate = QualityGate.from_score(90)
        assert gate.label == "Peak Torque"

    def test_power_band_threshold(self):
        """Test Power Band gate (70 <= score < 90)."""
        gate = QualityGate.from_score(85)
        assert gate.label == "Power Band"
        assert gate.status == "warn"

        gate = QualityGate.from_score(70)
        assert gate.label == "Power Band"

    def test_stall_zone_threshold(self):
        """Test Stall Zone gate (50 <= score < 70)."""
        gate = QualityGate.from_score(65)
        assert gate.label == "Stall Zone"
        assert gate.status == "fail"

        gate = QualityGate.from_score(50)
        assert gate.label == "Stall Zone"

    def test_redline_threshold(self):
        """Test Redline gate (score < 50)."""
        gate = QualityGate.from_score(40)
        assert gate.label == "Redline"
        assert gate.status == "deny"

        gate = QualityGate.from_score(0)
        assert gate.label == "Redline"


class TestReportGenerator:
    """Tests for report generation."""

    def test_report_generator_creates_report(self, sample_vpax_file):
        """Test that report generator creates a report."""
        generator = ReportGenerator(str(sample_vpax_file))
        report = generator.generate()

        assert report is not None
        assert report.summary is not None
        assert isinstance(report.all_issues, list)

    def test_report_has_torque_score(self, sample_vpax_file):
        """Test that report includes Torque Score."""
        generator = ReportGenerator(str(sample_vpax_file))
        report = generator.generate()

        assert hasattr(report.summary, "torque_score")
        assert 0 <= report.summary.torque_score <= 100

    def test_report_has_quality_gate(self, sample_vpax_file):
        """Test that report includes quality gate."""
        generator = ReportGenerator(str(sample_vpax_file))
        report = generator.generate()

        assert report.summary.quality_gate is not None
        assert hasattr(report.summary.quality_gate, "label")

    def test_report_groups_issues_by_severity(self, sample_vpax_file):
        """Test that issues are grouped by severity."""
        generator = ReportGenerator(str(sample_vpax_file))
        report = generator.generate()

        assert isinstance(report.critical_issues, list)
        assert isinstance(report.high_issues, list)
        assert isinstance(report.medium_issues, list)
        assert isinstance(report.low_issues, list)

    def test_report_to_json(self, sample_vpax_file):
        """Test converting report to JSON."""
        generator = ReportGenerator(str(sample_vpax_file))
        report = generator.generate()
        json_output = generator.to_json(report)

        assert isinstance(json_output, str)
        parsed = json.loads(json_output)
        assert "summary" in parsed

    def test_report_to_markdown(self, sample_vpax_file):
        """Test converting report to Markdown."""
        generator = ReportGenerator(str(sample_vpax_file))
        report = generator.generate()
        md_output = generator.to_markdown(report)

        assert isinstance(md_output, str)
        assert "TORQUE SCORE" in md_output

    def test_clean_model_high_score(self, sample_clean_vpax_file):
        """Test that clean model gets high score."""
        generator = ReportGenerator(str(sample_clean_vpax_file))
        report = generator.generate()

        # Clean model should score well
        assert report.summary.torque_score >= 70


class TestDAXRulesDefinitions:
    """Tests for DAX rule definitions."""

    def test_dax_rules_have_required_fields(self):
        """Test that all DAX rules have required fields."""
        required_fields = ["name", "description", "severity", "category", "penalty", "recommendation"]

        for rule_id, rule in DAX_RULES.items():
            for field in required_fields:
                if field != "pattern":  # pattern can be None for count-based rules
                    assert field in rule, f"Rule {rule_id} missing {field}"

    def test_dax_rules_have_valid_severity(self):
        """Test that all rules have valid severity."""
        for rule_id, rule in DAX_RULES.items():
            assert rule["severity"] in Severity, f"Rule {rule_id} has invalid severity"

    def test_dax_rules_have_valid_category(self):
        """Test that all rules have valid category."""
        for rule_id, rule in DAX_RULES.items():
            assert rule["category"] in Category, f"Rule {rule_id} has invalid category"

    def test_model_rules_have_required_fields(self):
        """Test that all model rules have required fields."""
        required_fields = ["name", "description", "severity", "category", "penalty", "recommendation"]

        for rule_id, rule in MODEL_RULES.items():
            for field in required_fields:
                assert field in rule, f"Rule {rule_id} missing {field}"


class TestVPAXIssue:
    """Tests for VPAXIssue dataclass."""

    def test_vpax_issue_creation(self):
        """Test creating a VPAXIssue."""
        issue = VPAXIssue(
            rule_id="DAX001",
            rule_name="Test Rule",
            severity="high",
            category="dax_anti_pattern",
            description="Test description",
            recommendation="Test recommendation",
            object_type="measure",
            object_name="Test Measure",
        )

        assert issue.rule_id == "DAX001"
        assert issue.severity == "high"

    def test_vpax_issue_optional_fields(self):
        """Test VPAXIssue optional fields."""
        issue = VPAXIssue(
            rule_id="DAX001",
            rule_name="Test Rule",
            severity="high",
            category="dax_anti_pattern",
            description="Test description",
            recommendation="Test recommendation",
            object_type="measure",
            object_name="Test Measure",
        )

        assert issue.table_name is None
        assert issue.details is None
        assert issue.code_snippet is None
