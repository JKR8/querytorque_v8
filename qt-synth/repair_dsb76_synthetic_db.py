"""Thin re-export — canonical implementation in qt_sql.validation.repair_dsb76_synthetic_db.

CLI entrypoint preserved for direct invocation:
    python3 qt-synth/repair_dsb76_synthetic_db.py --db <path>
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

from qt_sql.validation.repair_dsb76_synthetic_db import *  # noqa: F401, F403, E402
from qt_sql.validation.repair_dsb76_synthetic_db import main  # noqa: E402


if __name__ == "__main__":
    raise SystemExit(main())
