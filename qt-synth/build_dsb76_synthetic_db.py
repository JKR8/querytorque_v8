"""Thin re-export — canonical implementation in qt_sql.validation.build_dsb76_synthetic_db.

CLI entrypoint preserved for direct invocation:
    python3 qt-synth/build_dsb76_synthetic_db.py --out-db <path>
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

from qt_sql.validation.build_dsb76_synthetic_db import *  # noqa: F401, F403, E402
from qt_sql.validation.build_dsb76_synthetic_db import (  # noqa: E402
    main,
    # Private helpers used by sibling qt-synth scripts
    _build_query_context,
    _canonical_edge_type,
    _count_query_rows,
    _coerce_edge_value,
    _anchor_value_for_type,
    _detect_temporal_anchor,
    _fit_numeric_to_column,
    _force_seed_for_query,
    _from_filter_literal,
    _insert_rows,
    _is_key_like,
    _is_obviously_unsat,
    _merge_filters,
    _merge_fk,
    _merge_table_schemas,
    _tables_in_anti_patterns,
    _tables_in_not_exists,
    _to_duckdb_sql,
    _top_up_for_query,
)


if __name__ == "__main__":
    raise SystemExit(main())
