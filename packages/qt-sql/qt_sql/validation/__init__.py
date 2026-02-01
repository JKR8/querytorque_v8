"""SQL Validation module for QueryTorque SQL.

This module provides tools for validating that optimized SQL queries
produce equivalent results to the original queries.

Key components:
- SQLValidator: Main orchestrator for validation pipeline
- ValidationResult: Complete validation output with all metrics
- ValidationMode: SAMPLE (signal) or FULL (confidence)
- EquivalenceChecker: Row count, checksum, and value comparison
- QueryBenchmarker: 1-1-2-2 benchmarking pattern
- QueryNormalizer: LIMIT/ORDER BY handling

Example usage:
    from qt_sql.validation import SQLValidator, ValidationMode

    with SQLValidator(
        database="tpcds_sf100.duckdb",
        mode=ValidationMode.FULL,
    ) as validator:
        result = validator.validate(original_sql, optimized_sql)

        if result.status == "pass":
            print(f"Validation passed! Speedup: {result.speedup:.2f}x")
        else:
            print(f"Validation failed: {result.errors}")
"""

from .schemas import (
    CostResult,
    LimitStrategy,
    QueryExecutionResult,
    TimingResult,
    ValidationMode,
    ValidationResult,
    ValidationStatus,
    ValueDifference,
)
from .equivalence_checker import (
    ChecksumResult,
    EquivalenceChecker,
    RowCountResult,
    ValueComparisonResult,
)
from .query_normalizer import (
    NormalizationResult,
    QueryNormalizer,
)
from .benchmarker import (
    BenchmarkResult,
    QueryBenchmarker,
)
from .sql_validator import (
    SQLValidator,
    validate_sql_files,
)

__all__ = [
    # Main validator
    "SQLValidator",
    "validate_sql_files",
    # Schemas
    "ValidationMode",
    "ValidationStatus",
    "ValidationResult",
    "LimitStrategy",
    "TimingResult",
    "CostResult",
    "QueryExecutionResult",
    "ValueDifference",
    # Equivalence checker
    "EquivalenceChecker",
    "ChecksumResult",
    "RowCountResult",
    "ValueComparisonResult",
    # Normalizer
    "QueryNormalizer",
    "NormalizationResult",
    # Benchmarker
    "QueryBenchmarker",
    "BenchmarkResult",
]
