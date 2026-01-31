"""SQL analyzers for QueryTorque SQL."""

from .ast_detector import detect_antipatterns

__all__ = ["detect_antipatterns"]
