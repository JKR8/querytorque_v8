"""QueryTorque SQL - SQL Optimization Product.

This package provides SQL query analysis and optimization:
- AST-based anti-pattern detection (119 rules)
- DuckDB and PostgreSQL execution engines
- Calcite integration for algebraic optimization
- LLM-powered suggestions
- CLI for command-line analysis
- FastAPI backend for web integration
"""

__version__ = "0.1.0"

from .analyzers.ast_detector import detect_antipatterns
from .calcite_client import CalciteClient, get_calcite_client

__all__ = [
    "detect_antipatterns",
    "CalciteClient",
    "get_calcite_client",
]
