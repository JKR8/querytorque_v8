"""ADO schemas (standalone)."""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from typing import Optional


class ValidationStatus(str, Enum):
    PASS = "pass"
    FAIL = "fail"
    ERROR = "error"


@dataclass
class ValidationResult:
    worker_id: int
    status: ValidationStatus
    speedup: float
    error: Optional[str]
    optimized_sql: str
    errors: list[str] = None  # All errors for learning
    error_category: Optional[str] = None  # syntax | semantic | timeout | execution | unknown

    def __post_init__(self):
        if self.errors is None:
            self.errors = []
