"""Equivalence checker for comparing query results."""

from __future__ import annotations

import hashlib
import json
import math
from dataclasses import dataclass
from typing import Any, Optional

from .schemas import ValueDifference


@dataclass
class ChecksumResult:
    """Result of checksum comparison."""

    original_checksum: str
    optimized_checksum: str
    match: bool


@dataclass
class RowCountResult:
    """Result of row count comparison."""

    original_count: int
    optimized_count: int
    match: bool


@dataclass
class ValueComparisonResult:
    """Result of value-by-value comparison."""

    match: bool
    differences: list[ValueDifference]
    total_compared: int


class EquivalenceChecker:
    """Checks equivalence between query results.

    Supports:
    - Row count comparison
    - Checksum comparison (fast)
    - Value-by-value comparison with float tolerance
    """

    def __init__(self, float_tolerance: float = 1e-9):
        """Initialize checker.

        Args:
            float_tolerance: Tolerance for floating point comparison.
        """
        self.float_tolerance = float_tolerance

    def compare_row_counts(
        self, original_rows: list[dict], optimized_rows: list[dict]
    ) -> RowCountResult:
        """Compare row counts.

        Args:
            original_rows: Rows from original query.
            optimized_rows: Rows from optimized query.

        Returns:
            RowCountResult with counts and match status.
        """
        original_count = len(original_rows)
        optimized_count = len(optimized_rows)
        return RowCountResult(
            original_count=original_count,
            optimized_count=optimized_count,
            match=original_count == optimized_count,
        )

    def _normalize_value(self, value: Any) -> str:
        """Normalize a value for consistent comparison.

        Handles:
        - None/NULL values
        - NaN and Inf floats
        - String normalization
        - Numeric precision

        All values are converted to strings for consistent sorting.

        Args:
            value: Value to normalize.

        Returns:
            String representation suitable for comparison/hashing/sorting.
        """
        if value is None:
            return "__NULL__"

        if isinstance(value, float):
            if math.isnan(value):
                return "__NAN__"
            if math.isinf(value):
                return f"__INF_{'pos' if value > 0 else 'neg'}__"
            # Round to avoid floating point precision issues, format consistently
            return f"{round(value, 9):.9f}"

        if isinstance(value, str):
            # Normalize whitespace
            return value.strip()

        if isinstance(value, bool):
            return str(value)

        if isinstance(value, int):
            # Pad integers for proper string sorting
            return f"{value:020d}" if value >= 0 else f"-{abs(value):019d}"

        if isinstance(value, (list, tuple)):
            return "[" + ",".join(self._normalize_value(v) for v in value) + "]"

        if isinstance(value, dict):
            items = sorted((k, self._normalize_value(v)) for k, v in value.items())
            return "{" + ",".join(f"{k}:{v}" for k, v in items) + "}"

        # Fallback: convert to string
        return str(value)

    def _row_to_tuple(self, row: dict, columns: list[str]) -> tuple:
        """Convert a row dict to a normalized tuple.

        Args:
            row: Row as dictionary.
            columns: Column names in order.

        Returns:
            Tuple of normalized values.
        """
        return tuple(self._normalize_value(row.get(col)) for col in columns)

    def compute_checksum(self, rows: list[dict]) -> str:
        """Compute MD5 checksum of sorted, normalized rows.

        Args:
            rows: List of row dictionaries.

        Returns:
            MD5 hex digest of the normalized rows.
        """
        if not rows:
            return hashlib.md5(b"__EMPTY__").hexdigest()

        # Get consistent column order
        columns = sorted(rows[0].keys())

        # Normalize all rows
        normalized = []
        for row in rows:
            norm_row = self._row_to_tuple(row, columns)
            normalized.append(norm_row)

        # Sort for deterministic order
        normalized.sort()

        # Serialize to JSON for hashing
        serialized = json.dumps(normalized, sort_keys=True, default=str)
        return hashlib.md5(serialized.encode("utf-8")).hexdigest()

    def compare_checksums(
        self, original_rows: list[dict], optimized_rows: list[dict]
    ) -> ChecksumResult:
        """Compare checksums of two result sets.

        Args:
            original_rows: Rows from original query.
            optimized_rows: Rows from optimized query.

        Returns:
            ChecksumResult with checksums and match status.
        """
        original_checksum = self.compute_checksum(original_rows)
        optimized_checksum = self.compute_checksum(optimized_rows)
        return ChecksumResult(
            original_checksum=original_checksum,
            optimized_checksum=optimized_checksum,
            match=original_checksum == optimized_checksum,
        )

    def _values_equal(self, v1: Any, v2: Any) -> bool:
        """Check if two values are equal with tolerance for floats.

        Args:
            v1: First value.
            v2: Second value.

        Returns:
            True if values are considered equal.
        """
        # Handle floats with tolerance before normalization
        if isinstance(v1, float) and isinstance(v2, float):
            if math.isnan(v1) and math.isnan(v2):
                return True
            if math.isinf(v1) and math.isinf(v2):
                return (v1 > 0) == (v2 > 0)
            return abs(v1 - v2) <= self.float_tolerance

        # For non-floats, compare normalized string representations
        n1 = self._normalize_value(v1)
        n2 = self._normalize_value(v2)
        return n1 == n2

    def compare_values(
        self,
        original_rows: list[dict],
        optimized_rows: list[dict],
        max_differences: int = 10,
    ) -> ValueComparisonResult:
        """Compare values row-by-row with float tolerance.

        This is more expensive than checksum but provides detailed differences.
        Only call if checksum comparison fails and you need details.

        Args:
            original_rows: Rows from original query.
            optimized_rows: Rows from optimized query.
            max_differences: Maximum number of differences to record.

        Returns:
            ValueComparisonResult with match status and differences.
        """
        differences: list[ValueDifference] = []
        total_compared = 0

        # Must have same row count for value comparison
        if len(original_rows) != len(optimized_rows):
            return ValueComparisonResult(
                match=False,
                differences=[],
                total_compared=0,
            )

        if not original_rows:
            return ValueComparisonResult(
                match=True,
                differences=[],
                total_compared=0,
            )

        # Get consistent column order
        columns = sorted(original_rows[0].keys())

        # Sort both for comparison (order-independent)
        original_sorted = sorted(
            original_rows, key=lambda r: self._row_to_tuple(r, columns)
        )
        optimized_sorted = sorted(
            optimized_rows, key=lambda r: self._row_to_tuple(r, columns)
        )

        for i, (orig_row, opt_row) in enumerate(zip(original_sorted, optimized_sorted)):
            total_compared += len(columns)

            for col in columns:
                orig_val = orig_row.get(col)
                opt_val = opt_row.get(col)

                if not self._values_equal(orig_val, opt_val):
                    differences.append(
                        ValueDifference(
                            row_index=i,
                            column=col,
                            original_value=orig_val,
                            optimized_value=opt_val,
                        )
                    )

                    if len(differences) >= max_differences:
                        return ValueComparisonResult(
                            match=False,
                            differences=differences,
                            total_compared=total_compared,
                        )

        return ValueComparisonResult(
            match=len(differences) == 0,
            differences=differences,
            total_compared=total_compared,
        )

    def check_equivalence(
        self,
        original_rows: list[dict],
        optimized_rows: list[dict],
        detailed: bool = False,
    ) -> tuple[bool, Optional[ValueComparisonResult]]:
        """Check equivalence using checksum first, then values if needed.

        Args:
            original_rows: Rows from original query.
            optimized_rows: Rows from optimized query.
            detailed: If True, always do value comparison when checksum fails.

        Returns:
            Tuple of (is_equivalent, value_comparison_result or None).
        """
        # First check row counts
        row_count_result = self.compare_row_counts(original_rows, optimized_rows)
        if not row_count_result.match:
            return False, None

        # Then check checksums (fast)
        checksum_result = self.compare_checksums(original_rows, optimized_rows)
        if checksum_result.match:
            return True, None

        # Checksums differ - do detailed comparison if requested
        if detailed:
            value_result = self.compare_values(original_rows, optimized_rows)
            return value_result.match, value_result

        # Checksums differ but no detailed comparison requested
        return False, None
