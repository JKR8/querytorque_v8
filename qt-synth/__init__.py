"""Synthetic Data Validation Tool — CLI wrappers.

Canonical implementation lives in ``packages/qt-sql/qt_sql/validation/``.
This package re-exports classes for backward-compatible imports and provides
CLI entrypoints for DSB benchmark tooling.
"""

import sys
from pathlib import Path

# Ensure production packages are importable when running from qt-synth/
_PROJECT_ROOT = Path(__file__).resolve().parents[1]
for _p in (
    str(_PROJECT_ROOT / "packages" / "qt-shared"),
    str(_PROJECT_ROOT / "packages" / "qt-sql"),
):
    if _p not in sys.path:
        sys.path.insert(0, _p)

from qt_sql.validation.synthetic_validator import (  # noqa: E402, F401
    SchemaExtractor,
    SyntheticDataGenerator,
    SyntheticValidator,
)

__all__ = ["SyntheticValidator", "SchemaExtractor", "SyntheticDataGenerator"]
