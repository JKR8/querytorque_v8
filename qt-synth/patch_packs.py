"""Thin re-export — canonical implementation in qt_sql.validation.patch_packs."""

import sys
from pathlib import Path

_PROJECT_ROOT = Path(__file__).resolve().parents[1]
for _p in (
    str(_PROJECT_ROOT / "packages" / "qt-shared"),
    str(_PROJECT_ROOT / "packages" / "qt-sql"),
):
    if _p not in sys.path:
        sys.path.insert(0, _p)

from qt_sql.validation.patch_packs import *  # noqa: F401, F403, E402
from qt_sql.validation.patch_packs import (  # noqa: E402
    WitnessPatchPack,
    available_patch_packs,
    load_witness_patch_pack,
)
