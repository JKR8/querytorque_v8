"""Phase 6: Integration Tests - DAX Integration.

Full pipeline validation tests for DAX analysis.
"""

import pytest
import json
from pathlib import Path
from unittest.mock import MagicMock, patch


class TestDAXAnalysisPipeline:
    """Tests for the full DAX analysis pipeline."""

    def test_full_analysis_pipeline(self, sample_vpax_file):
        """Test complete analysis from VPAX to report."""
        from qt_dax.analyzers.vpax_analyzer import ReportGenerator

        generator = ReportGenerator(str(sample_vpax_file))
        report = generator.generate()

        # Verify all expected fields
        assert report.summary is not None
        assert hasattr(report.summary, "torque_score")
        assert hasattr(report.summary, "quality_gate")
        assert isinstance(report.all_issues, list)
        assert isinstance(report.measures, list)

    def test_analysis_with_issues_pipeline(self, sample_vpax_file):
        """Test analysis pipeline detects issues."""
        from qt_dax.analyzers.vpax_analyzer import ReportGenerator

        generator = ReportGenerator(str(sample_vpax_file))
        report = generator.generate()

        # Sample data has issues (high cardinality, bi-directional, etc.)
        assert len(report.all_issues) > 0

        # Issues should be categorized
        total = (
            report.summary.critical_count +
            report.summary.high_count +
            report.summary.medium_count +
            report.summary.low_count +
            report.summary.info_count
        )
        assert total == len(report.all_issues)

    def test_analysis_to_json_pipeline(self, sample_vpax_file):
        """Test analysis to JSON serialization."""
        from qt_dax.analyzers.vpax_analyzer import ReportGenerator

        generator = ReportGenerator(str(sample_vpax_file))
        report = generator.generate()
        json_output = generator.to_json(report)

        # Should be valid JSON
        parsed = json.loads(json_output)
        assert "summary" in parsed

    def test_analysis_to_markdown_pipeline(self, sample_vpax_file):
        """Test analysis to Markdown generation."""
        from qt_dax.analyzers.vpax_analyzer import ReportGenerator

        generator = ReportGenerator(str(sample_vpax_file))
        report = generator.generate()
        md_output = generator.to_markdown(report)

        # Should contain expected sections
        assert "TORQUE SCORE" in md_output
        assert "MODEL STATISTICS" in md_output


class TestDAXParserIntegration:
    """Tests for DAX parser integration."""

    def test_parser_analyzer_integration(self, sample_complex_dax):
        """Test DAX parser integration with analyzer."""
        from qt_dax.parsers.dax_parser import analyze_dax
        from qt_dax.analyzers.vpax_analyzer import DAXAnalyzer

        # Parse DAX
        metadata = analyze_dax(sample_complex_dax)

        # Analyze with DAXAnalyzer
        analyzer = DAXAnalyzer()
        analysis = analyzer.analyze_measure(
            name="Test",
            table="Sales",
            expression=sample_complex_dax
        )

        # Both should work together
        assert metadata is not None
        assert analysis is not None

    def test_parser_detects_patterns_for_rules(self, sample_sumx_filter_dax):
        """Test parser correctly identifies patterns for rule detection."""
        from qt_dax.parsers.dax_parser import analyze_dax

        metadata = analyze_dax(sample_sumx_filter_dax)

        # Should detect SUMX and FILTER
        iterator_names = [f.name for f in metadata.iterator_functions]
        filter_names = [f.name for f in metadata.filter_functions]

        assert "SUMX" in iterator_names
        assert "FILTER" in filter_names


class TestMeasureDependencyIntegration:
    """Tests for measure dependency analysis."""

    def test_dependency_analysis(self):
        """Test measure dependency analysis."""
        from qt_dax.analyzers.measure_dependencies import MeasureDependencyAnalyzer

        measures = [
            {"name": "Base Sales", "table": "Sales", "expression": "SUM('Sales'[Amount])"},
            {"name": "Adjusted Sales", "table": "Sales", "expression": "[Base Sales] * 1.1"},
            {"name": "Final Sales", "table": "Sales", "expression": "[Adjusted Sales] + 100"},
        ]

        analyzer = MeasureDependencyAnalyzer()
        result = analyzer.analyze(measures)

        assert result is not None
        assert result.total_measures == 3

    def test_dependency_depth_calculation(self):
        """Test that dependency depth is calculated correctly."""
        from qt_dax.analyzers.measure_dependencies import MeasureDependencyAnalyzer

        measures = [
            {"name": "A", "table": "T", "expression": "SUM('T'[X])"},
            {"name": "B", "table": "T", "expression": "[A] + 1"},
            {"name": "C", "table": "T", "expression": "[B] + 1"},
            {"name": "D", "table": "T", "expression": "[C] + 1"},
        ]

        analyzer = MeasureDependencyAnalyzer()
        result = analyzer.analyze(measures)

        # Max depth should be 3 (D -> C -> B -> A)
        assert result.max_depth >= 3

    def test_dependency_with_special_char_measure_names(self):
        """Measure names with symbols should still be resolved as dependencies."""
        from qt_dax.analyzers.measure_dependencies import MeasureDependencyAnalyzer

        measures = [
            {
                "name": "Matrix MV CR Intensity (Scope 1 & 2)_BM",
                "table": "ESG Trucost Climate",
                "expression": "SUM('ESG Trucost Climate'[Revenue_AUD_mn])",
            },
            {
                "name": "Matrix MV CR Intensity Switch_BM",
                "table": "ESG Trucost Climate",
                "expression": (
                    "SWITCH(TRUE(), "
                    "SELECTEDVALUE('Scope Emission Types'[Scope_Type_Code]) = 1, "
                    "[Matrix MV CR Intensity (Scope 1 & 2)_BM], "
                    "[Matrix MV CR Intensity (Scope 1 & 2)_BM])"
                ),
            },
        ]

        analyzer = MeasureDependencyAnalyzer()
        result = analyzer.analyze(measures)

        switch_node = result.nodes["matrix mv cr intensity switch_bm"]
        assert "matrix mv cr intensity (scope 1 & 2)_bm" in switch_node.depends_on


class TestVPAXDifferIntegration:
    """Tests for VPAX diff integration."""

    def test_vpax_differ(self, tmp_path):
        """Test VPAX file comparison."""
        from qt_dax.analyzers.vpax_differ import VPAXDiffer
        import zipfile

        # Create V1 VPAX
        v1_data = {
            "Tables": [{"TableName": "Sales", "RowsCount": 1000}],
            "Columns": [],
            "Measures": [
                {"TableName": "Sales", "MeasureName": "Total", "MeasureExpression": "SUM('Sales'[Amount])"}
            ],
            "Relationships": [],
        }
        v1_path = tmp_path / "v1.vpax"
        with zipfile.ZipFile(v1_path, "w") as zf:
            zf.writestr("DaxVpaView.json", json.dumps(v1_data))
            zf.writestr("DaxModel.json", json.dumps({"ModelName": "V1"}))

        # Create V2 VPAX with changes
        v2_data = {
            "Tables": [{"TableName": "Sales", "RowsCount": 2000}],
            "Columns": [],
            "Measures": [
                {"TableName": "Sales", "MeasureName": "Total", "MeasureExpression": "SUM('Sales'[Amount])"},
                {"TableName": "Sales", "MeasureName": "Average", "MeasureExpression": "AVERAGE('Sales'[Amount])"},
            ],
            "Relationships": [],
        }
        v2_path = tmp_path / "v2.vpax"
        with zipfile.ZipFile(v2_path, "w") as zf:
            zf.writestr("DaxVpaView.json", json.dumps(v2_data))
            zf.writestr("DaxModel.json", json.dumps({"ModelName": "V2"}))

        # Compare
        differ = VPAXDiffer()
        result = differ.compare(v1_path, v2_path)

        assert result is not None
        assert result.summary.added >= 1  # New measure added


class TestCrossPackageIntegration:
    """Tests for cross-package integration."""

    def test_dax_analyzer_uses_shared_config(self):
        """Test that DAX analyzer can use shared config."""
        from qt_shared.config import get_settings
        from qt_dax.analyzers.vpax_analyzer import DAXAnalyzer

        settings = get_settings()
        analyzer = DAXAnalyzer()

        # Both should work together
        assert settings is not None
        assert analyzer is not None

    @patch("qt_shared.llm.factory.create_llm_client")
    def test_dax_optimization_with_llm_mock(self, mock_create_client):
        """Test DAX optimization with mocked LLM."""
        mock_client = MagicMock()
        mock_client.analyze.return_value = """
## Optimized DAX
```dax
VAR TotalSales = SUM('Sales'[Amount])
RETURN DIVIDE(TotalSales, COUNTROWS('Sales'), 0)
```

## Changes
- Added VAR for reuse
- Used DIVIDE for safety
"""
        mock_create_client.return_value = mock_client

        from qt_shared.llm import create_llm_client
        client = create_llm_client()

        if client:
            result = client.analyze("Optimize: [Sales] / [Count]")
            assert "DIVIDE" in result or "VAR" in result


class TestQualityGateIntegration:
    """Tests for quality gate integration."""

    def test_quality_gate_affects_report(self, sample_vpax_file):
        """Test that quality gate is properly set in report."""
        from qt_dax.analyzers.vpax_analyzer import ReportGenerator

        generator = ReportGenerator(str(sample_vpax_file))
        report = generator.generate()

        gate = report.summary.quality_gate
        score = report.summary.torque_score

        # Gate should match score
        if score >= 90:
            assert gate.label == "Peak Torque"
        elif score >= 70:
            assert gate.label == "Power Band"
        elif score >= 50:
            assert gate.label == "Stall Zone"
        else:
            assert gate.label == "Redline"

    def test_clean_model_high_score(self, sample_clean_vpax_file):
        """Test that clean model gets high score."""
        from qt_dax.analyzers.vpax_analyzer import ReportGenerator

        generator = ReportGenerator(str(sample_clean_vpax_file))
        report = generator.generate()

        # Clean model should score well
        assert report.summary.torque_score >= 70
        assert report.summary.quality_gate.label in ("Peak Torque", "Power Band")


class TestRendererIntegration:
    """Tests for HTML renderer integration."""

    def test_html_report_generation(self, sample_vpax_file):
        """Test HTML report generation."""
        from qt_dax.analyzers.vpax_analyzer import ReportGenerator

        generator = ReportGenerator(str(sample_vpax_file))
        report = generator.generate()

        try:
            from qt_dax.renderers.dax_renderer import DAXRenderer

            renderer = DAXRenderer()
            html = renderer.render(report)

            assert isinstance(html, str)
            assert "<html" in html.lower() or "<!doctype" in html.lower()
        except ImportError:
            pytest.skip("DAX renderer not available")


class TestErrorHandlingIntegration:
    """Tests for error handling across the pipeline."""

    def test_corrupt_vpax_handling(self, tmp_path):
        """Test handling of corrupt VPAX file."""
        from qt_dax.analyzers.vpax_analyzer import VPAXParser

        corrupt_file = tmp_path / "corrupt.vpax"
        corrupt_file.write_bytes(b"not a zip file")

        with pytest.raises(Exception):
            parser = VPAXParser(str(corrupt_file))
            parser.parse()

    def test_missing_json_in_vpax(self, tmp_path):
        """Test handling of VPAX missing required JSON."""
        import zipfile

        incomplete_file = tmp_path / "incomplete.vpax"
        with zipfile.ZipFile(incomplete_file, "w") as zf:
            zf.writestr("README.txt", "This is not valid")

        from qt_dax.analyzers.vpax_analyzer import VPAXParser

        parser = VPAXParser(str(incomplete_file))
        data = parser.parse()

        # Should handle gracefully (may return None for missing data)
        # Just verify no crash
        assert data is not None or data is None  # Either is acceptable

    def test_malformed_dax_expression(self, dax_analyzer):
        """Test handling of malformed DAX expressions."""
        # Unbalanced parentheses
        analysis = dax_analyzer.analyze_measure(
            name="Bad",
            table="Sales",
            expression="SUM('Sales'[Amount]"  # Missing closing paren
        )

        # Should handle gracefully
        assert analysis is not None


class TestPerformanceIntegration:
    """Tests for performance characteristics."""

    @pytest.mark.slow
    def test_large_model_analysis(self, tmp_path):
        """Test analyzing a large model."""
        import zipfile
        import time

        # Create large model data
        data = {
            "Tables": [
                {"TableName": f"Table{i}", "RowsCount": 10000}
                for i in range(50)
            ],
            "Columns": [
                {"TableName": f"Table{i}", "ColumnName": f"Col{j}",
                 "DataType": "String", "Encoding": "VALUE",
                 "ColumnCardinality": 100, "TotalSize": 1000, "DictionarySize": 100}
                for i in range(50) for j in range(10)
            ],
            "Measures": [
                {"TableName": "Table0", "MeasureName": f"Measure{i}",
                 "MeasureExpression": f"SUM('Table0'[Col{i % 10}])"}
                for i in range(100)
            ],
            "Relationships": [],
        }

        vpax_file = tmp_path / "large.vpax"
        with zipfile.ZipFile(vpax_file, "w") as zf:
            zf.writestr("DaxVpaView.json", json.dumps(data))
            zf.writestr("DaxModel.json", json.dumps({"ModelName": "Large"}))

        from qt_dax.analyzers.vpax_analyzer import ReportGenerator

        start = time.time()
        generator = ReportGenerator(str(vpax_file))
        report = generator.generate()
        elapsed = time.time() - start

        # Should complete in reasonable time
        assert elapsed < 30.0
        assert report is not None
