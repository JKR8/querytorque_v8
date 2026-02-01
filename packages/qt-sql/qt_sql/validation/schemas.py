"""Data models for SQL validation."""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Optional


class ValidationMode(str, Enum):
    """Validation mode."""

    SAMPLE = "sample"  # 1% sample DB - gives signal
    FULL = "full"  # Full DB - gives confidence


class ValidationStatus(str, Enum):
    """Validation status."""

    PASS = "pass"
    FAIL = "fail"
    WARN = "warn"
    ERROR = "error"


class LimitStrategy(str, Enum):
    """Strategy for handling LIMIT without ORDER BY."""

    ADD_ORDER = "add_order"  # Inject ORDER BY 1, 2, 3... before LIMIT
    REMOVE_LIMIT = "remove_limit"  # Strip LIMIT clause entirely


@dataclass
class TimingResult:
    """Timing result from benchmarking."""

    warmup_time_ms: float
    measured_time_ms: float


@dataclass
class CostResult:
    """Cost result from EXPLAIN."""

    estimated_cost: float
    actual_rows: Optional[int] = None


@dataclass
class QueryExecutionResult:
    """Result from executing a single query."""

    timing: TimingResult
    cost: CostResult
    row_count: int
    checksum: Optional[str] = None
    rows: Optional[list[dict[str, Any]]] = None
    error: Optional[str] = None


@dataclass
class ValueDifference:
    """A single value difference between original and optimized results."""

    row_index: int
    column: str
    original_value: Any
    optimized_value: Any


@dataclass
class ValidationResult:
    """Complete validation result."""

    status: ValidationStatus
    mode: ValidationMode

    # Row counts
    original_row_count: int
    optimized_row_count: int
    row_counts_match: bool

    # Timing
    original_timing_ms: float
    optimized_timing_ms: float
    speedup: float  # original / optimized (>1 means improvement)

    # Cost
    original_cost: float
    optimized_cost: float
    cost_reduction_pct: float  # (original - optimized) / original * 100

    # Value comparison
    values_match: bool
    checksum_match: Optional[bool] = None
    value_differences: list[ValueDifference] = field(default_factory=list)

    # Normalization info
    limit_detected: bool = False
    limit_strategy_applied: Optional[LimitStrategy] = None
    normalized_original_sql: Optional[str] = None
    normalized_optimized_sql: Optional[str] = None

    # Errors and warnings
    errors: list[str] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)

    # Raw results for debugging
    original_result: Optional[QueryExecutionResult] = None
    optimized_result: Optional[QueryExecutionResult] = None

    def _safe_float(self, value: float) -> Any:
        """Convert float to JSON-safe value (handle inf/nan)."""
        import math
        if math.isinf(value):
            return None
        if math.isnan(value):
            return None
        return round(value, 2)

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary for JSON serialization."""
        return {
            "status": self.status.value,
            "mode": self.mode.value,
            "row_counts": {
                "original": self.original_row_count,
                "optimized": self.optimized_row_count,
                "match": self.row_counts_match,
            },
            "timing": {
                "original_ms": round(self.original_timing_ms, 2),
                "optimized_ms": round(self.optimized_timing_ms, 2),
                "speedup": round(self.speedup, 2),
            },
            "cost": {
                "original": self._safe_float(self.original_cost),
                "optimized": self._safe_float(self.optimized_cost),
                "reduction_pct": round(self.cost_reduction_pct, 2),
            },
            "values": {
                "match": self.values_match,
                "checksum_match": self.checksum_match,
                "differences_count": len(self.value_differences),
            },
            "normalization": {
                "limit_detected": self.limit_detected,
                "strategy_applied": self.limit_strategy_applied.value if self.limit_strategy_applied else None,
            },
            "errors": self.errors,
            "warnings": self.warnings,
        }
