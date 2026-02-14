"""Detectors — pattern detectors for labelling IR nodes."""
from .latest_date import detect_latest_date_filters
from .duplicate_expr import (
    detect_duplicate_expressions,
    detect_haversine_duplicates,
    detect_cross_join_on_true,
)

__all__ = [
    "detect_latest_date_filters",
    "detect_duplicate_expressions",
    "detect_haversine_duplicates",
    "detect_cross_join_on_true",
]
