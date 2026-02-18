"""SQL Validation module for QueryTorque SQL.

Kept components:
- EquivalenceChecker: Row count, checksum, and value comparison
- SQLDiffer: SQL diff utilities for retry prompt enrichment
- schemas: Validation result types
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
from .sql_differ import (
    SQLDiffer,
)

__all__ = [
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
    # SQL differ
    "SQLDiffer",
]
