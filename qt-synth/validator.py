"""Compatibility entrypoint for the consolidated synthetic validator.

Canonical implementation lives in ``packages/qt-sql/qt_sql/validation/synthetic_validator.py``.
"""

import sys
from pathlib import Path

_PROJECT_ROOT = Path(__file__).resolve().parents[1]
for _p in (
    str(_PROJECT_ROOT / "packages" / "qt-shared"),
    str(_PROJECT_ROOT / "packages" / "qt-sql"),
):
    if _p not in sys.path:
        sys.path.insert(0, _p)

from qt_sql.validation.synthetic_validator import *  # noqa: F401, F403, E402
from qt_sql.validation.synthetic_validator import (  # noqa: E402
    SchemaExtractor,
    SyntheticDataGenerator,
    SyntheticValidator,
    main,
)

__all__ = ["SchemaExtractor", "SyntheticDataGenerator", "SyntheticValidator"]


if __name__ == "__main__":
    raise SystemExit(main())
