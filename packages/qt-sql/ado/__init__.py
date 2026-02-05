"""ADO (Autonomous Data Optimization) package.

This package provides autonomous SQL optimization capabilities:
- Parallel candidate generation with LLM inference
- Validation against sample/full databases
- Automatic curation of validated wins into gold examples
- YAML summary generation

Usage:
    from ado.runner import ADORunner, ADOConfig

    config = ADOConfig(
        sample_db="postgres://...",
        candidates_per_round=10,
        provider="anthropic",
        model="claude-3-5-sonnet",
    )

    runner = ADORunner(config)
    result = runner.run_query("q1", sql)
"""

from .runner import ADORunner, ADOConfig, ADOResult
from .schemas import ValidationStatus, ValidationResult

__all__ = [
    "ADORunner",
    "ADOConfig",
    "ADOResult",
    "ValidationStatus",
    "ValidationResult",
]
