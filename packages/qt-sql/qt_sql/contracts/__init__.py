"""Output Contract — structured optimization results.

Wraps SessionResult with diagnosis, expected impact, and validation summary.
"""

from .output_contract import QueryOutputContract, Diagnosis, ExpectedImpact, ValidationSummary

__all__ = [
    "QueryOutputContract",
    "Diagnosis",
    "ExpectedImpact",
    "ValidationSummary",
]
