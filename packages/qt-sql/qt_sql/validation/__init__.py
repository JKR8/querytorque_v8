"""SQL Validation module for QueryTorque SQL.

Components:
- benchmark: Unified benchmark (ONE connection per query, fail-fast)
- sample_checker: DuckDB TABLESAMPLE equivalence for timeout recovery
- cross_engine_checker: Cross-engine semantic validation (Gate 1.5)
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
from .benchmark import (
    BenchmarkSummary,
    CandidateResult,
    benchmark_query_patches,
    _timed_runs,
)
from .sample_checker import (
    SampleCheckResult,
    SampleChecker,
)
from .cross_engine_checker import (
    CrossCheckResult,
    CrossEngineChecker,
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
    # Benchmark
    "BenchmarkSummary",
    "CandidateResult",
    "benchmark_query_patches",
    "_timed_runs",
    # Sample checker
    "SampleCheckResult",
    "SampleChecker",
    # Cross-engine checker
    "CrossCheckResult",
    "CrossEngineChecker",
]
