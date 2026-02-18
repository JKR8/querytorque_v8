"""DSB MVROWS witness recipe patch pack.

This module keeps benchmark-specific witness recipes behind an explicit
patch-pack boundary so core synthesis can remain AST-general.
"""

from __future__ import annotations

from typing import Any, Dict


DESCRIPTION = "DSB-76 MVROWS deterministic witness recipes"


def apply_recipe(
    conn: Any,
    qctx: Dict[str, Any],
    global_tables: Dict[str, Dict[str, Any]],
) -> bool:
    # Lazy import avoids import cycles at module import time.
    try:
        from .. import build_dsb76_synthetic_db as dsb_builder
    except ImportError:
        import build_dsb76_synthetic_db as dsb_builder

    return bool(dsb_builder._apply_mvrows_recipe(conn, qctx, global_tables))
