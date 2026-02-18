"""Compatibility alias for the consolidated synthetic validator.

``validator_v2.py`` is retained so older scripts still import successfully.
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
from qt_sql.validation.synthetic_validator import main  # noqa: E402


if __name__ == "__main__":
    raise SystemExit(main())
