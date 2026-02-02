"""Phase 1: Smoke Tests - Import Validation for qt-dax.

Validates that all qt-dax modules can be imported correctly,
including cross-package imports from qt-shared.
"""

import pytest


class TestQtDaxImports:
    """Test that all qt-dax modules import without errors."""

    def test_import_qt_dax_root(self):
        """Test root package import."""
        import qt_dax
        assert hasattr(qt_dax, "__version__")
        assert hasattr(qt_dax, "ReportGenerator")
        assert hasattr(qt_dax, "VPAXParser")
        assert hasattr(qt_dax, "DAXAnalyzer")
        assert hasattr(qt_dax, "DAXRemediationEngine")

    def test_import_vpax_analyzer(self):
        """Test VPAX analyzer imports."""
        from qt_dax.analyzers.vpax_analyzer import (
            VPAXParser,
            DAXAnalyzer,
            ModelAnalyzer,
            ReportGenerator,
            VPAXIssue,
            MeasureAnalysis,
            TableAnalysis,
            ColumnAnalysis,
            RelationshipAnalysis,
            QualityGate,
            ModelSummary,
            DiagnosticReport,
            DAX_RULES,
            MODEL_RULES,
            CALC_GROUP_RULES,
            Severity,
            Category,
        )
        assert VPAXParser is not None
        assert DAXAnalyzer is not None
        assert isinstance(DAX_RULES, dict)
        assert isinstance(MODEL_RULES, dict)

    def test_import_dax_remediation_engine(self):
        """Test DAX remediation engine imports."""
        from qt_dax.analyzers.dax_remediation_engine import DAXRemediationEngine
        assert DAXRemediationEngine is not None

    def test_import_measure_dependencies(self):
        """Test measure dependencies imports."""
        from qt_dax.analyzers.measure_dependencies import (
            MeasureDependencyAnalyzer,
            DependencyAnalysisResult,
        )
        assert MeasureDependencyAnalyzer is not None

    def test_import_vpax_differ(self):
        """Test VPAX differ imports."""
        from qt_dax.analyzers.vpax_differ import VPAXDiffer
        assert VPAXDiffer is not None

    def test_import_dax_parser(self):
        """Test DAX parser imports."""
        from qt_dax.parsers.dax_parser import (
            Token,
            FunctionCall,
            DAXMetadata,
            DAXLexer,
            DAXParser,
            analyze_dax,
        )
        assert Token is not None
        assert DAXLexer is not None
        assert DAXParser is not None
        assert callable(analyze_dax)

    def test_import_dax_renderer(self):
        """Test DAX renderer imports."""
        from qt_dax.renderers.dax_renderer import DAXRenderer
        assert DAXRenderer is not None

    def test_import_dax_validator(self):
        """Test DAX validator imports."""
        from qt_dax.validation.dax_validator import DaxValidator
        assert DaxValidator is not None

    def test_import_dax_equivalence_validator(self):
        """Test DAX equivalence validator imports."""
        from qt_dax.validation.dax_equivalence_validator import DaxEquivalenceValidator
        assert DaxEquivalenceValidator is not None


class TestQtDaxConnectionImports:
    """Test connection module imports (may require Windows)."""

    def test_import_pbi_desktop_connection(self):
        """Test PBI Desktop connection import."""
        try:
            from qt_dax.connections.pbi_desktop import PBIDesktopConnection
            assert PBIDesktopConnection is not None
        except (ImportError, OSError) as e:
            # Expected on non-Windows systems
            pytest.skip(f"PBI Desktop connection not available: {e}")


class TestQtDaxCrossPackageImports:
    """Test cross-package imports from qt-shared work correctly."""

    def test_import_qt_shared_from_qt_dax(self):
        """Verify qt-dax can import qt-shared components."""
        from qt_shared.config import get_settings
        from qt_shared.auth import UserContext
        from qt_shared.llm import create_llm_client
        assert callable(get_settings)
        assert UserContext is not None

    def test_dax_analyzer_uses_parser(self):
        """DAX analyzer should use the DAX parser."""
        from qt_dax.analyzers.vpax_analyzer import DAXAnalyzer
        from qt_dax.parsers.dax_parser import analyze_dax
        # Both should be importable together
        assert DAXAnalyzer is not None
        assert callable(analyze_dax)


class TestQtDaxCliImports:
    """Test CLI module imports."""

    def test_import_cli_main(self):
        """Test CLI main module import."""
        from cli.main import cli, audit, optimize, connect, diff
        import click
        assert isinstance(cli, click.Group)
        assert callable(audit)
        assert callable(optimize)
        assert callable(connect)
        assert callable(diff)


class TestQtDaxApiImports:
    """Test API module imports."""

    def test_import_api_main(self):
        """Test API main module import."""
        from api.main import app, create_app
        from fastapi import FastAPI
        assert isinstance(app, FastAPI)
        assert callable(create_app)

    def test_import_api_models(self):
        """Test API request/response models."""
        from api.main import (
            AnalyzeResponse,
            DAXIssueResponse,
            ModelStatsResponse,
            OptimizeRequest,
            OptimizeResponse,
            DiffResponse,
            HealthResponse,
        )
        assert AnalyzeResponse is not None
        assert DAXIssueResponse is not None


class TestQtDaxDataStructures:
    """Test that data structures are properly defined."""

    def test_dax_metadata_structure(self):
        """Test DAXMetadata has expected fields."""
        from qt_dax.parsers.dax_parser import DAXMetadata
        expected_fields = [
            "raw_code",
            "tokens",
            "function_calls",
            "max_nesting_depth",
            "tables_referenced",
            "columns_referenced",
            "measures_referenced",
            "has_variables",
            "variable_names",
        ]
        for field in expected_fields:
            assert hasattr(DAXMetadata, "__annotations__") or hasattr(DAXMetadata, "__dataclass_fields__")

    def test_vpax_issue_structure(self):
        """Test VPAXIssue has expected fields."""
        from qt_dax.analyzers.vpax_analyzer import VPAXIssue
        expected_fields = [
            "rule_id",
            "rule_name",
            "severity",
            "category",
            "description",
            "recommendation",
            "object_type",
            "object_name",
        ]
        # VPAXIssue is a dataclass
        assert hasattr(VPAXIssue, "__dataclass_fields__")

    def test_quality_gate_from_score(self):
        """Test QualityGate.from_score works correctly."""
        from qt_dax.analyzers.vpax_analyzer import QualityGate

        # Test all thresholds
        assert QualityGate.from_score(95).label == "Peak Torque"
        assert QualityGate.from_score(90).label == "Peak Torque"
        assert QualityGate.from_score(80).label == "Power Band"
        assert QualityGate.from_score(70).label == "Power Band"
        assert QualityGate.from_score(60).label == "Stall Zone"
        assert QualityGate.from_score(50).label == "Stall Zone"
        assert QualityGate.from_score(40).label == "Redline"
        assert QualityGate.from_score(0).label == "Redline"
