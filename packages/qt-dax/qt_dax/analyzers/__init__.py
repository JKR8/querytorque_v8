"""QueryTorque DAX Analyzers.

Provides VPAX model analysis, DAX anti-pattern detection,
and LLM-powered remediation.
"""

from .vpax_analyzer import (
    ReportGenerator,
    VPAXParser,
    DAXAnalyzer,
    ModelAnalyzer,
    DiagnosticReport,
    VPAXIssue,
    MeasureAnalysis,
    Severity,
    Category,
    DAX_RULES,
    MODEL_RULES,
    CALC_GROUP_RULES,
)
from .dax_remediation_engine import (
    DAXRemediationEngine,
    MeasureFix,
    BatchFixRequest,
    generate_fix_report,
    generate_tmdl_output,
)
from .measure_dependencies import (
    MeasureDependencyAnalyzer,
    DependencyAnalysisResult,
    MeasureNode,
    DependencyCycle,
)
from .vpax_differ import (
    VPAXDiffer,
    VPAXDiffResult,
    DiffReportGenerator,
)

__all__ = [
    # VPAX Analyzer
    "ReportGenerator",
    "VPAXParser",
    "DAXAnalyzer",
    "ModelAnalyzer",
    "DiagnosticReport",
    "VPAXIssue",
    "MeasureAnalysis",
    "Severity",
    "Category",
    "DAX_RULES",
    "MODEL_RULES",
    "CALC_GROUP_RULES",
    # Remediation Engine
    "DAXRemediationEngine",
    "MeasureFix",
    "BatchFixRequest",
    "generate_fix_report",
    "generate_tmdl_output",
    # Dependencies
    "MeasureDependencyAnalyzer",
    "DependencyAnalysisResult",
    "MeasureNode",
    "DependencyCycle",
    # Differ
    "VPAXDiffer",
    "VPAXDiffResult",
    "DiffReportGenerator",
]
