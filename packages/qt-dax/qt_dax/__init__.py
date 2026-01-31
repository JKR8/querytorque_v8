"""QueryTorque DAX - Power BI Model Analysis and DAX Optimization Product.

This package provides DAX query analysis and Power BI model optimization:
- VPAX model analysis (DAX, Model, Calculation Group rules)
- LLM-powered DAX remediation engine
- Measure dependency analysis
- Power BI Desktop connection (Windows only)
- HTML report generation
"""

__version__ = "0.1.0"

from .analyzers.vpax_analyzer import ReportGenerator, VPAXParser, DAXAnalyzer
from .analyzers.dax_remediation_engine import DAXRemediationEngine

__all__ = [
    "ReportGenerator",
    "VPAXParser",
    "DAXAnalyzer",
    "DAXRemediationEngine",
]
