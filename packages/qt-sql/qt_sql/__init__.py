"""QueryTorque SQL - SQL Optimization Product.

This package provides SQL query analysis and optimization:
- AST-based anti-pattern detection (119 rules)
- DuckDB and PostgreSQL execution engines
- LLM-powered suggestions
- CLI for command-line analysis
- FastAPI backend for web integration
"""

__version__ = "0.1.0"

from .analyzers.ast_detector import detect_antipatterns
__all__ = [
    "detect_antipatterns",
]
