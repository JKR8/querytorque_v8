"""Build a persistent synthetic DuckDB database for postgres_dsb_76 queries.

Usage:
  PYTHONPATH=packages/qt-shared:packages/qt-sql \
  python3 -m qt_sql.validation.build_dsb76_synthetic_db \
    --out-db /mnt/d/qt_synth/postgres_dsb_76_synthetic.duckdb

Optional:
  --reference-db postgres://USER:PASS@127.0.0.1:5434/dsb_sf10
  (when omitted, schema extraction falls back to AST-only inference)
"""

from __future__ import annotations

import argparse
import json
import logging
import re
import threading
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Set, Tuple

import duckdb
import sqlglot

from .patch_packs import available_patch_packs, load_witness_patch_pack
from .synthetic_validator import (
    SchemaExtractor,
    SchemaFromDB,
    SyntheticDataGenerator,
    SyntheticValidator,
    find_primary_key_column,
)


logger = logging.getLogger(__name__)


QueryContext = Dict[str, Any]


def _read_first_statement(sql_path: Path) -> str:
    raw_sql = sql_path.read_text(encoding="utf-8")
    statements = [s.strip() for s in raw_sql.split(";") if s.strip()]
    statements = [
        s for s in statements
        if not all(line.strip().startswith("--") or not line.strip() for line in s.split("\n"))
    ]
    sql = statements[0] if statements else raw_sql
    return sql.strip().rstrip(";")


def _to_duckdb_sql(sql: str, source_dialect: str) -> str:
    if source_dialect.lower() == "duckdb":
        return SyntheticValidator._resolve_ambiguous_columns(sql)
    transpiled = sqlglot.transpile(sql, read=source_dialect, write="duckdb")
    return SyntheticValidator._resolve_ambiguous_columns("\n".join(transpiled))


def _merge_unique(dst: List[Any], values: Iterable[Any]) -> None:
    seen = set(dst)
    for value in values:
        if value not in seen:
            dst.append(value)
            seen.add(value)


def _merge_filters(
    dst: Dict[str, Dict[str, List[Any]]],
    src: Dict[str, Dict[str, List[Any]]],
) -> None:
    for table, cols in src.items():
        table_map = dst.setdefault(table, {})
        for col, values in cols.items():
            out = table_map.setdefault(col, [])
            _merge_unique(out, values)


def _merge_fk(
    dst: Dict[str, Dict[str, Tuple[str, str]]],
    src: Dict[str, Dict[str, Tuple[str, str]]],
) -> None:
    for table, fk_map in src.items():
        out = dst.setdefault(table, {})
        for col, target in fk_map.items():
            out.setdefault(col, target)


def _prefer_type(existing: str, new_value: str) -> str:
    ex = (existing or "").upper()
    nv = (new_value or "").upper()
    if ex == nv:
        return existing
    if ex.startswith("VARCHAR") and not nv.startswith("VARCHAR"):
        return new_value
    if ex in ("", "VARCHAR", "VARCHAR(50)") and nv:
        return new_value
    return existing


def _merge_table_schemas(
    dst: Dict[str, Dict[str, Any]],
    src: Dict[str, Dict[str, Any]],
) -> None:
    for table_name, table_info in src.items():
        table_entry = dst.setdefault(
            table_name,
            {"alias": None, "columns": {}, "key": table_info.get("key", f"{table_name}_sk")},
        )
        for col_name, col_info in table_info.get("columns", {}).items():
            if col_name not in table_entry["columns"]:
                table_entry["columns"][col_name] = dict(col_info)
            else:
                existing = table_entry["columns"][col_name]
                existing["type"] = _prefer_type(existing.get("type", ""), col_info.get("type", ""))
                existing["nullable"] = bool(existing.get("nullable", True)) and bool(
                    col_info.get("nullable", True)
                )


def _infer_type_from_col_name(col_name: str) -> str:
    col = (col_name or "").lower()
    if col in {"d_day_name"}:
        return "VARCHAR(100)"
    if any(
        k in col
        for k in (
            "name",
            "state",
            "city",
            "street",
            "zip",
            "status",
            "channel",
            "class",
            "gender",
            "country",
            "county",
        )
    ):
        return "VARCHAR(100)"
    if col.endswith("_sk") or col.endswith("_id") or col == "id":
        return "INTEGER"
    if col.endswith("_date") or col.endswith("_dt") or col in {"d_date"}:
        return "DATE"
    if "timestamp" in col or col.endswith("_ts"):
        return "TIMESTAMP"
    if any(k in col for k in ("qty", "quantity", "count", "year", "month", "week", "day", "seq", "number")):
        return "INTEGER"
    if any(
        k in col
        for k in (
            "price",
            "cost",
            "amount",
            "amt",
            "sales",
            "revenue",
            "profit",
            "loss",
            "cash",
            "credit",
            "charge",
            "discount",
            "tax",
            "fee",
            "total",
            "net",
            "gross",
            "wholesale",
            "list",
            "coupon",
            "return",
            "ratio",
            "rate",
            "margin",
        )
    ):
        return "DECIMAL(18,2)"
    return "VARCHAR(100)"


def _table_prefixes(table_name: str) -> Set[str]:
    tokens = [t for t in table_name.lower().split("_") if t]
    if not tokens:
        return set()
    first = tokens[0]
    initials = "".join(t[0] for t in tokens if t)
    out = {
        table_name.lower(),
        first,
        first[:1],
        first[:2],
        first[:3],
        initials,
    }
    return {p for p in out if p}


def _guess_table_for_unqualified_column(
    col_name: str,
    tables: Dict[str, Dict[str, Any]],
) -> Optional[str]:
    col = (col_name or "").lower()
    parts = [p for p in col.split("_") if p]
    if not parts:
        return None
    prefix = parts[0]

    # TPC-DS style hard hints (most precise).
    two_part = "_".join(parts[:2]) if len(parts) >= 2 else ""
    two_hint = {
        "web_site": "web_site",
        "catalog_page": "catalog_page",
        "call_center": "call_center",
        "customer_address": "customer_address",
        "customer_demographics": "customer_demographics",
        "household_demographics": "household_demographics",
        "income_band": "income_band",
        "date_dim": "date_dim",
    }
    hinted_two = two_hint.get(two_part)
    if hinted_two and hinted_two in tables:
        return hinted_two

    prefix_hint = {
        "ss": "store_sales",
        "sr": "store_returns",
        "cs": "catalog_sales",
        "cr": "catalog_returns",
        "ws": "web_sales",
        "wr": "web_returns",
        "inv": "inventory",
        "d": "date_dim",
        "i": "item",
        "c": "customer",
        "cd": "customer_demographics",
        "hd": "household_demographics",
        "ca": "customer_address",
        "ib": "income_band",
        "s": "store",
        "p": "promotion",
        "w": "warehouse",
        "cc": "call_center",
        "r": "reason",
        "wp": "web_page",
        "web": "web_site",
    }
    hinted = prefix_hint.get(prefix)
    if hinted and hinted in tables:
        return hinted

    # Prefer exact column ownership if unique.
    owners: List[str] = []
    for table_name, info in tables.items():
        for existing in info.get("columns", {}).keys():
            if existing.lower() == col:
                owners.append(table_name)
                break
    if len(owners) == 1:
        return owners[0]
    if len(owners) > 1:
        return None

    # Avoid over-aggressive guessing on long generic prefixes
    # (for example `item_sk` in subquery aliases should not be forced to `item`).
    if len(prefix) > 3:
        return None

    candidates = [
        table_name
        for table_name in tables.keys()
        if prefix in _table_prefixes(table_name)
        or any(tok.startswith(prefix) for tok in table_name.lower().split("_"))
    ]
    if len(candidates) == 1:
        return candidates[0]
    return None


def _rebuild_columns_from_ast(
    sql: str,
    tables: Dict[str, Dict[str, Any]],
) -> None:
    """Rebuild per-table columns from SQL AST to avoid heuristic drift."""
    try:
        ast = sqlglot.parse_one(sql, read="duckdb")
    except Exception:
        return

    alias_map: Dict[str, str] = {}
    cte_names: Set[str] = set()
    for cte in ast.find_all(sqlglot.exp.CTE):
        alias = (cte.alias or "").strip()
        if alias:
            cte_names.add(alias)
    for table_expr in ast.find_all(sqlglot.exp.Table):
        name = (table_expr.name or "").strip()
        alias = (table_expr.alias_or_name or "").strip()
        if name in tables:
            alias_map[name] = name
            if alias:
                alias_map[alias] = name

    # Start with empty columns and repopulate strictly from AST references.
    for table_name in tables.keys():
        tables[table_name]["columns"] = {}

    for col in ast.find_all(sqlglot.exp.Column):
        col_name = (col.name or "").strip()
        if not col_name:
            continue
        ref = (col.table or "").strip()
        if ref in cte_names:
            continue
        target, col_name = _normalize_ast_column_ref(col, alias_map, tables)
        if target not in tables:
            continue

        if col_name not in tables[target]["columns"]:
            tables[target]["columns"][col_name] = {
                "type": _infer_type_from_col_name(col_name),
                "nullable": True,
            }

    # Keep schema non-empty for each table.
    for table_name in tables.keys():
        if tables[table_name]["columns"]:
            continue
        pk = f"{table_name}_sk"
        tables[table_name]["columns"][pk] = {"type": "INTEGER", "nullable": False}


def _canonical_from_literal_token(token: str) -> str:
    txt = token.strip().strip("'\"")
    if re.match(r"^\d{4}-\d{2}-\d{2}\s+\d{2}:\d{2}:\d{2}$", txt):
        return "TIMESTAMP"
    if re.match(r"^\d{4}-\d{2}-\d{2}$", txt):
        return "DATE"
    if re.match(r"^-?\d+$", txt):
        return "INTEGER"
    if re.match(r"^-?\d+\.\d+$", txt):
        return "DECIMAL"
    return "VARCHAR"


def _infer_filter_canonical(values: List[Any]) -> str:
    inferred: List[str] = []
    for raw in values:
        if not isinstance(raw, str):
            inferred.append(_canonical_from_literal_token(str(raw)))
            continue
        txt = raw.strip()
        if txt.startswith("BETWEEN:"):
            parts = txt.split(":", 2)
            if len(parts) >= 2:
                inferred.append(_canonical_from_literal_token(parts[1]))
            if len(parts) >= 3:
                inferred.append(_canonical_from_literal_token(parts[2]))
            continue
        if txt.startswith("IN:"):
            body = txt.split(":", 1)[1]
            tokens = [p.strip() for p in body.split(",") if p.strip()]
            for tok in tokens[:4]:
                inferred.append(_canonical_from_literal_token(tok))
            continue
        if ":" in txt and txt[0] in "><":
            _, rhs = txt.split(":", 1)
            inferred.append(_canonical_from_literal_token(rhs))
            continue
        inferred.append(_canonical_from_literal_token(txt))

    if any(t in ("TIMESTAMP", "DATE") for t in inferred):
        return "TIMESTAMP" if "TIMESTAMP" in inferred else "DATE"
    if "DECIMAL" in inferred:
        return "DECIMAL"
    if "INTEGER" in inferred:
        return "INTEGER"
    return "VARCHAR"


def _promote_types_from_filters(
    tables: Dict[str, Dict[str, Any]],
    filter_values: Dict[str, Dict[str, List[Any]]],
) -> None:
    """Fix AST-only schema drift (e.g., numeric predicate columns inferred as VARCHAR)."""
    for table_name, cols in filter_values.items():
        schema = tables.get(table_name, {})
        table_cols = schema.get("columns", {})
        if not table_cols:
            continue
        for col_name, values in cols.items():
            if col_name not in table_cols:
                inferred = _infer_filter_canonical(values)
                if inferred == "INTEGER":
                    table_cols[col_name] = {"type": "INTEGER", "nullable": True}
                elif inferred == "DECIMAL":
                    table_cols[col_name] = {"type": "DECIMAL(18,2)", "nullable": True}
                elif inferred == "DATE":
                    table_cols[col_name] = {"type": "DATE", "nullable": True}
                elif inferred == "TIMESTAMP":
                    table_cols[col_name] = {"type": "TIMESTAMP", "nullable": True}
                else:
                    table_cols[col_name] = {"type": "VARCHAR(100)", "nullable": True}
                continue
            current = str(table_cols[col_name].get("type", "VARCHAR")).upper()
            if not current.startswith("VARCHAR"):
                continue
            inferred = _infer_filter_canonical(values)
            if inferred == "INTEGER":
                table_cols[col_name]["type"] = "INTEGER"
            elif inferred == "DECIMAL":
                table_cols[col_name]["type"] = "DECIMAL(18,2)"
            elif inferred == "DATE":
                table_cols[col_name]["type"] = "DATE"
            elif inferred == "TIMESTAMP":
                table_cols[col_name]["type"] = "TIMESTAMP"


def _promote_types_from_expressions(
    sql: str,
    tables: Dict[str, Dict[str, Any]],
) -> None:
    """Promote column types from arithmetic/aggregate AST usage."""
    try:
        ast = sqlglot.parse_one(sql, read="duckdb")
    except Exception:
        return

    alias_map: Dict[str, str] = {}
    for table_expr in ast.find_all(sqlglot.exp.Table):
        name = (table_expr.name or "").strip()
        alias = (table_expr.alias_or_name or "").strip()
        if name in tables:
            alias_map[name] = name
            if alias:
                alias_map[alias] = name

    numeric_cols: Set[Tuple[str, str]] = set()

    def _mark(col: sqlglot.exp.Column, *, aggregate_context: bool = False) -> None:
        table_name, raw_col = _normalize_ast_column_ref(col, alias_map, tables)
        if table_name not in tables:
            return
        resolved = _resolve_col_case(tables, table_name, raw_col)
        if not resolved:
            return
        if aggregate_context:
            guessed = _infer_type_from_col_name(resolved).upper()
            if guessed.startswith("VARCHAR"):
                return
        numeric_cols.add((table_name, resolved))

    for node in ast.find_all(sqlglot.exp.Add, sqlglot.exp.Sub, sqlglot.exp.Mul, sqlglot.exp.Div):
        for col in node.find_all(sqlglot.exp.Column):
            _mark(col, aggregate_context=False)

    for node in ast.find_all(sqlglot.exp.Avg, sqlglot.exp.Sum, sqlglot.exp.StddevSamp):
        for col in node.find_all(sqlglot.exp.Column):
            _mark(col, aggregate_context=True)

    for table_name, col_name in numeric_cols:
        cinfo = tables.get(table_name, {}).get("columns", {}).get(col_name)
        if not cinfo:
            continue
        ctype = str(cinfo.get("type", "VARCHAR")).upper()
        if ctype.startswith("DECIMAL") or ctype in ("INTEGER", "BIGINT"):
            continue
        cinfo["type"] = "DECIMAL(18,2)"


def _resolve_col_case(
    tables: Dict[str, Dict[str, Any]],
    table_name: str,
    col_name: str,
) -> Optional[str]:
    cols = tables.get(table_name, {}).get("columns", {})
    if col_name in cols:
        return col_name
    needle = col_name.lower()
    for existing in cols.keys():
        if existing.lower() == needle:
            return existing
    return None


def _resolve_unqualified_table(
    tables: Dict[str, Dict[str, Any]],
    col_name: str,
) -> Optional[str]:
    needle = col_name.lower()
    hits: List[str] = []
    for table_name, info in tables.items():
        for existing in info.get("columns", {}).keys():
            if existing.lower() == needle:
                hits.append(table_name)
                break
    if len(hits) == 1:
        return hits[0]
    return None


def _normalize_ast_column_ref(
    col: sqlglot.exp.Column,
    alias_map: Dict[str, str],
    tables: Dict[str, Dict[str, Any]],
) -> Tuple[Optional[str], str]:
    """Normalize SQLGlot column refs, including stitched prefix forms."""
    raw_name = (col.name or "").strip()
    raw_ref = (col.table or "").strip()
    if not raw_name:
        return None, ""

    if not raw_ref:
        return _guess_table_for_unqualified_column(raw_name, tables), raw_name

    mapped = alias_map.get(raw_ref, raw_ref)
    if mapped in tables:
        return mapped, raw_name

    # SQLGlot can split s_store_sk into table='s', name='store_sk'.
    stitched = f"{raw_ref}_{raw_name}"
    stitched_table = _guess_table_for_unqualified_column(stitched, tables)
    if stitched_table:
        return stitched_table, stitched

    return None, raw_name


def _extract_join_components(
    sql: str,
    tables: Dict[str, Dict[str, Any]],
) -> Tuple[Dict[Tuple[str, str], int], Dict[int, List[Tuple[str, str]]]]:
    """Extract column-equivalence components from equality predicates (AST join DAG edges)."""
    try:
        ast = sqlglot.parse_one(sql, read="duckdb")
    except Exception:
        return {}, {}

    alias_map: Dict[str, str] = {}
    for table_expr in ast.find_all(sqlglot.exp.Table):
        name = (table_expr.name or "").strip()
        alias = (table_expr.alias_or_name or "").strip()
        if name:
            alias_map[name] = name
        if alias and name:
            alias_map[alias] = name

    parent: Dict[Tuple[str, str], Tuple[str, str]] = {}

    def _find(x: Tuple[str, str]) -> Tuple[str, str]:
        parent.setdefault(x, x)
        if parent[x] != x:
            parent[x] = _find(parent[x])
        return parent[x]

    def _union(a: Tuple[str, str], b: Tuple[str, str]) -> None:
        ra = _find(a)
        rb = _find(b)
        if ra != rb:
            parent[rb] = ra

    for eq in ast.find_all(sqlglot.exp.EQ):
        left = eq.args.get("this")
        right = eq.args.get("expression")
        if not isinstance(left, sqlglot.exp.Column) or not isinstance(right, sqlglot.exp.Column):
            continue
        ltab, lname = _normalize_ast_column_ref(left, alias_map, tables)
        rtab, rname = _normalize_ast_column_ref(right, alias_map, tables)
        if ltab not in tables or rtab not in tables:
            continue
        lcol = _resolve_col_case(tables, ltab, lname or "")
        rcol = _resolve_col_case(tables, rtab, rname or "")
        if not lcol or not rcol:
            continue
        _union((ltab, lcol), (rtab, rcol))

    grouped: Dict[Tuple[str, str], List[Tuple[str, str]]] = {}
    for key in list(parent.keys()):
        root = _find(key)
        grouped.setdefault(root, []).append(key)

    comp_map: Dict[Tuple[str, str], int] = {}
    comp_members: Dict[int, List[Tuple[str, str]]] = {}
    for idx, members in enumerate(grouped.values(), start=1):
        if len(members) < 2:
            continue
        comp_members[idx] = members
        for key in members:
            comp_map[key] = idx

    return comp_map, comp_members


def _build_query_context(
    validator: SyntheticValidator,
    sql_file: Path,
    source_dialect: str,
) -> QueryContext:
    sql_source = _read_first_statement(sql_file)
    sql_duckdb = _to_duckdb_sql(sql_source, source_dialect)

    extractor = SchemaExtractor(sql_duckdb)
    tables = extractor.extract_tables()
    _rebuild_columns_from_ast(sql_duckdb, tables)

    if validator.schema_extractor:
        for table_name in list(tables.keys()):
            ref_schema = validator.schema_extractor.get_table_schema(table_name)
            if ref_schema:
                tables[table_name]["columns"] = dict(ref_schema)

    fk_exact = validator._detect_fk_from_joins(sql_duckdb, tables)
    fk_heur = validator._detect_foreign_keys(sql_duckdb, tables)
    _merge_fk(fk_exact, fk_heur)

    filter_values = validator._extract_filter_values(sql_duckdb, tables)
    join_graph = validator._build_join_column_graph(sql_duckdb, tables)
    filter_values = validator._propagate_filter_values_across_joins(
        filter_values, join_graph, tables
    )
    _promote_types_from_filters(tables, filter_values)
    _promote_types_from_expressions(sql_duckdb, tables)

    return {
        "name": sql_file.name,
        "sql_duckdb": sql_duckdb,
        "tables": tables,
        "fk_relationships": fk_exact,
        "filter_values": filter_values,
    }


def _canonical_edge_type(col_type: str) -> str:
    upper = (col_type or "").upper()
    if any(t in upper for t in ("BIGINT",)):
        return "BIGINT"
    if any(t in upper for t in ("INT", "SMALLINT")):
        return "INTEGER"
    if any(t in upper for t in ("DECIMAL", "NUMERIC", "DOUBLE", "FLOAT", "REAL")):
        return "DECIMAL"
    if "DATE" in upper and "TIMESTAMP" not in upper:
        return "DATE"
    if "TIMESTAMP" in upper:
        return "TIMESTAMP"
    return "VARCHAR"


def _coerce_edge_value(value: Any, canonical_type: str) -> Any:
    try:
        if canonical_type in ("INTEGER", "BIGINT"):
            iv = int(float(value))
            if canonical_type == "INTEGER":
                return max(-2147483648, min(2147483647, iv))
            return max(-9223372036854775808, min(9223372036854775807, iv))
        if canonical_type == "DECIMAL":
            return float(value)
        return value
    except Exception:
        return value


def _fit_numeric_to_column(value: Any, col_type: str, canonical_type: str) -> Any:
    """Clamp/round edge numeric values to target column precision."""
    if canonical_type != "DECIMAL":
        return value

    text = (col_type or "").upper()
    match = re.search(r"DECIMAL\s*\(\s*(\d+)\s*,\s*(\d+)\s*\)", text)
    if not match:
        return value

    precision = int(match.group(1))
    scale = int(match.group(2))
    max_abs = (10 ** (precision - scale)) - (10 ** (-scale))
    try:
        num = round(float(value), scale)
    except Exception:
        return value

    if num > max_abs:
        num = max_abs
    elif num < -max_abs:
        num = -max_abs
    return num


def _load_edge_template(path: Path) -> Dict[str, Any]:
    if not path.exists():
        raise FileNotFoundError(f"Edge template not found: {path}")
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError("Edge template must be a JSON object")
    payload.setdefault("type_edges", {})
    payload.setdefault("table_rows", {})
    payload.setdefault("column_edges", {})
    return payload


def _build_pk_counters(
    conn: duckdb.DuckDBPyConnection,
    tables: Dict[str, Dict[str, Any]],
) -> Dict[Tuple[str, str], int]:
    counters: Dict[Tuple[str, str], int] = {}
    for table_name, schema in tables.items():
        col_names = list(schema.get("columns", {}).keys())
        pk_col = find_primary_key_column(table_name, col_names)
        if not pk_col:
            continue
        try:
            row = conn.execute(f"SELECT COALESCE(MAX({pk_col}), 0) FROM {table_name}").fetchone()
            current = int(row[0]) if row and row[0] is not None else 0
            counters[(table_name, pk_col)] = current
        except Exception:
            counters[(table_name, pk_col)] = 0
    return counters


def _insert_rows(
    conn: duckdb.DuckDBPyConnection,
    table_name: str,
    col_names: List[str],
    rows: List[Tuple[Any, ...]],
) -> None:
    if not rows:
        return
    placeholders = ", ".join(["?"] * len(col_names))
    sql = f"INSERT INTO {table_name} ({', '.join(col_names)}) VALUES ({placeholders})"
    conn.executemany(sql, rows)


def _apply_edge_cases(
    conn: duckdb.DuckDBPyConnection,
    tables: Dict[str, Dict[str, Any]],
    fk_relationships: Dict[str, Dict[str, Tuple[str, str]]],
    template: Dict[str, Any],
    edge_rows_per_table: int,
) -> int:
    generator = SyntheticDataGenerator(conn, all_schemas=tables)
    pk_counters = _build_pk_counters(conn, tables)

    table_rows_tpl = template.get("table_rows", {})
    type_edges = template.get("type_edges", {})
    column_edges = template.get("column_edges", {})

    total_inserted = 0

    # Pre-load FK candidate values from existing table PKs.
    for table_name, schema in tables.items():
        col_names = list(schema.get("columns", {}).keys())
        pk_col = find_primary_key_column(table_name, col_names)
        if not pk_col:
            continue
        try:
            rows = conn.execute(f"SELECT {pk_col} FROM {table_name} LIMIT 50000").fetchall()
            values = [r[0] for r in rows if r and r[0] is not None]
            if values:
                generator.foreign_key_values[table_name] = values
        except Exception:
            continue

    for table_name, schema in tables.items():
        columns = schema.get("columns", {})
        col_names = list(columns.keys())
        if not col_names:
            continue

        pk_col = find_primary_key_column(table_name, col_names)
        fk_map = fk_relationships.get(table_name, {})
        rows_to_insert: List[Tuple[Any, ...]] = []

        # Explicit table rows from template.
        explicit_rows = table_rows_tpl.get(table_name, [])
        for row_index, explicit in enumerate(explicit_rows):
            values: List[Any] = []
            for col in col_names:
                col_info = columns[col]
                canonical = _canonical_edge_type(col_info.get("type", ""))
                if isinstance(explicit, dict) and col in explicit:
                    value = _coerce_edge_value(explicit[col], canonical)
                    value = _fit_numeric_to_column(value, col_info.get("type", ""), canonical)
                elif col.lower() in fk_map:
                    target_table, _ = fk_map[col.lower()]
                    candidates = generator.foreign_key_values.get(target_table, [])
                    if candidates:
                        value = candidates[row_index % len(candidates)]
                    else:
                        value = generator._generate_value(
                            col,
                            col_info.get("type", "VARCHAR"),
                            row_index,
                            max(10, edge_rows_per_table),
                            foreign_keys=fk_map,
                            table_name=table_name,
                            primary_key_col=pk_col,
                        )
                else:
                    value = generator._generate_value(
                        col,
                        col_info.get("type", "VARCHAR"),
                        row_index,
                        max(10, edge_rows_per_table),
                        foreign_keys=fk_map,
                        table_name=table_name,
                        primary_key_col=pk_col,
                    )
                if value is None and not bool(col_info.get("nullable", True)):
                    value = generator._generate_value(
                        col,
                        col_info.get("type", "VARCHAR"),
                        row_index + 1000,
                        max(10, edge_rows_per_table),
                        foreign_keys=fk_map,
                        table_name=table_name,
                        primary_key_col=pk_col,
                    )
                values.append(value)

            if pk_col and pk_col in col_names:
                pk_idx = col_names.index(pk_col)
                if values[pk_idx] is None and _canonical_edge_type(columns[pk_col]["type"]) in (
                    "INTEGER",
                    "BIGINT",
                ):
                    key = (table_name, pk_col)
                    pk_counters[key] = pk_counters.get(key, 0) + 1
                    values[pk_idx] = pk_counters[key]
            rows_to_insert.append(tuple(values))

        # Generic type-based edge rows.
        for edge_idx in range(edge_rows_per_table):
            values = []
            for col in col_names:
                col_info = columns[col]
                ctype = _canonical_edge_type(col_info.get("type", ""))

                explicit_col_edges = (
                    column_edges.get(table_name, {}).get(col, [])
                    if isinstance(column_edges.get(table_name, {}), dict)
                    else []
                )
                if explicit_col_edges:
                    edge_val = explicit_col_edges[edge_idx % len(explicit_col_edges)]
                    value = _coerce_edge_value(edge_val, ctype)
                    value = _fit_numeric_to_column(value, col_info.get("type", ""), ctype)
                elif col.lower() in fk_map:
                    target_table, _ = fk_map[col.lower()]
                    candidates = generator.foreign_key_values.get(target_table, [])
                    if candidates:
                        value = candidates[edge_idx % len(candidates)]
                    else:
                        value = generator._generate_value(
                            col,
                            col_info.get("type", "VARCHAR"),
                            edge_idx,
                            max(10, edge_rows_per_table),
                            foreign_keys=fk_map,
                            table_name=table_name,
                            primary_key_col=pk_col,
                        )
                else:
                    bucket = type_edges.get(ctype, [])
                    if bucket:
                        value = _coerce_edge_value(bucket[edge_idx % len(bucket)], ctype)
                        value = _fit_numeric_to_column(value, col_info.get("type", ""), ctype)
                    else:
                        value = generator._generate_value(
                            col,
                            col_info.get("type", "VARCHAR"),
                            edge_idx,
                            max(10, edge_rows_per_table),
                            foreign_keys=fk_map,
                            table_name=table_name,
                            primary_key_col=pk_col,
                        )
                if value is None and not bool(col_info.get("nullable", True)):
                    value = generator._generate_value(
                        col,
                        col_info.get("type", "VARCHAR"),
                        edge_idx + 1000,
                        max(10, edge_rows_per_table),
                        foreign_keys=fk_map,
                        table_name=table_name,
                        primary_key_col=pk_col,
                    )
                values.append(value)

            if pk_col and pk_col in col_names and _canonical_edge_type(columns[pk_col]["type"]) in (
                "INTEGER",
                "BIGINT",
            ):
                pk_idx = col_names.index(pk_col)
                key = (table_name, pk_col)
                pk_counters[key] = pk_counters.get(key, 0) + 1
                values[pk_idx] = pk_counters[key]

            rows_to_insert.append(tuple(values))

        _insert_rows(conn, table_name, col_names, rows_to_insert)
        total_inserted += len(rows_to_insert)

    return total_inserted


def _preload_pk_values(
    conn: duckdb.DuckDBPyConnection,
    tables: Dict[str, Dict[str, Any]],
) -> Dict[str, List[Any]]:
    cache: Dict[str, List[Any]] = {}
    for table_name, schema in tables.items():
        col_names = list(schema.get("columns", {}).keys())
        pk_col = find_primary_key_column(table_name, col_names)
        if not pk_col:
            continue
        try:
            rows = conn.execute(f"SELECT {pk_col} FROM {table_name} LIMIT 50000").fetchall()
            values = [r[0] for r in rows if r and r[0] is not None]
            if values:
                cache[table_name] = values
        except Exception:
            continue
    return cache


def _top_up_for_query(
    conn: duckdb.DuckDBPyConnection,
    validator: SyntheticValidator,
    qctx: QueryContext,
    global_tables: Dict[str, Dict[str, Any]],
    fact_rows: int,
    dim_rows: int,
) -> None:
    q_tables = qctx["tables"]
    q_fk = qctx["fk_relationships"]
    q_filters = qctx["filter_values"]
    order = validator._get_table_generation_order(q_tables, q_fk)

    generator = SyntheticDataGenerator(conn, all_schemas=global_tables)
    generator.filter_literal_values = q_filters
    generator.foreign_key_values = _preload_pk_values(conn, q_tables)

    for table_name in order:
        schema = global_tables[table_name]
        table_fk = q_fk.get(table_name, {})
        row_count = fact_rows if table_fk else dim_rows
        generator.generate_table_data(
            table_name=table_name,
            schema=schema,
            row_count=row_count,
            foreign_keys=table_fk,
        )
        validator._update_filter_matched_pks(generator, global_tables, [table_name], q_filters)
        validator._reverse_propagate_parent_key_matches(
            generator,
            table_name,
            global_tables,
            q_fk,
            q_filters,
        )


def _is_key_like(col_name: str) -> bool:
    lower = col_name.lower()
    return lower.endswith("_sk") or lower.endswith("_id") or lower == "id" or lower.endswith("_key")


def _infer_required_rows(sql: str, default_rows: int) -> int:
    """Minimum-viable row count from HAVING COUNT constraints only."""
    req = max(1, int(default_rows))
    try:
        ast = sqlglot.parse_one(sql, read="duckdb")
    except Exception:
        return req

    for cmp in ast.find_all(sqlglot.exp.GT, sqlglot.exp.GTE, sqlglot.exp.EQ):
        lhs = cmp.args.get("this")
        rhs = cmp.args.get("expression")
        if not isinstance(rhs, sqlglot.exp.Literal):
            continue
        if not isinstance(lhs, sqlglot.exp.Count):
            continue
        try:
            target = int(float(rhs.this))
        except Exception:
            continue
        if isinstance(cmp, sqlglot.exp.GT):
            req = max(req, target + 1)
        elif isinstance(cmp, sqlglot.exp.GTE):
            req = max(req, target)
        else:  # EQ
            req = max(req, target)

    return min(req, 1000)


def _literal_to_float(node: Any) -> Optional[float]:
    if isinstance(node, sqlglot.exp.Literal):
        try:
            return float(node.this)
        except Exception:
            return None
    return None


def _solve_aggregate_constraint(sql: str, col_name: str, canonical_type: str) -> Optional[Any]:
    """Try to satisfy SUM/AVG/MAX/MIN predicates with one super value."""
    try:
        ast = sqlglot.parse_one(sql, read="duckdb")
    except Exception:
        return None

    delta = 1.0 if canonical_type in ("INTEGER", "BIGINT") else 0.01
    best_value: Optional[float] = None

    for cmp in ast.find_all(
        sqlglot.exp.GT,
        sqlglot.exp.GTE,
        sqlglot.exp.LT,
        sqlglot.exp.LTE,
        sqlglot.exp.EQ,
    ):
        lhs = cmp.args.get("this")
        rhs = cmp.args.get("expression")
        if not isinstance(lhs, (sqlglot.exp.Sum, sqlglot.exp.Avg, sqlglot.exp.Max, sqlglot.exp.Min)):
            continue

        numeric_target = _literal_to_float(rhs)
        if numeric_target is None:
            continue

        # Aggregate expression must reference this column.
        agg_cols = [c.name.lower() for c in lhs.find_all(sqlglot.exp.Column)]
        if col_name.lower() not in agg_cols:
            continue

        candidate: Optional[float] = None
        if isinstance(cmp, sqlglot.exp.GT):
            candidate = numeric_target + delta
        elif isinstance(cmp, sqlglot.exp.GTE):
            candidate = numeric_target
        elif isinstance(cmp, sqlglot.exp.LT):
            candidate = numeric_target - delta
        elif isinstance(cmp, sqlglot.exp.LTE):
            candidate = numeric_target
        elif isinstance(cmp, sqlglot.exp.EQ):
            candidate = numeric_target

        if candidate is None:
            continue
        if best_value is None:
            best_value = candidate
        else:
            # Keep the more extreme candidate for strict satisfiability.
            if isinstance(cmp, (sqlglot.exp.GT, sqlglot.exp.GTE)):
                best_value = max(best_value, candidate)
            elif isinstance(cmp, (sqlglot.exp.LT, sqlglot.exp.LTE)):
                best_value = min(best_value, candidate)
            else:
                best_value = candidate

    if best_value is None:
        return None
    if canonical_type in ("INTEGER", "BIGINT"):
        return int(round(best_value))
    return float(best_value)


def _detect_temporal_anchor(sql: str) -> Optional[datetime]:
    """Detect a query-specific date anchor from literals (e.g., 1998, 2023-05-01)."""
    date_match = re.search(r"'((?:19|20)\d{2}-\d{2}-\d{2})'", sql)
    if date_match:
        try:
            return datetime.strptime(date_match.group(1), "%Y-%m-%d")
        except Exception:
            pass

    # Year filters can appear as string or numeric literals.
    years: List[int] = []
    for match in re.finditer(r"'((?:19|20)\d{2})'|(?<!\d)((?:19|20)\d{2})(?!\d)", sql):
        year_txt = match.group(1) or match.group(2)
        if not year_txt:
            continue
        try:
            year = int(year_txt)
        except Exception:
            continue
        if 1900 <= year <= 2100:
            years.append(year)
    if years:
        return datetime(years[0], 6, 15)
    return None


def _smart_string_from_literal(raw: str) -> str:
    """Handle LIKE/IN-style encoded literals more robustly."""
    txt = raw.strip()
    if txt.startswith("IN:"):
        body = txt.split(":", 1)[1]
        quoted = re.search(r"'([^']+)'", body)
        if quoted:
            return quoted.group(1)
        first = body.split(",", 1)[0].strip()
        return first.strip("'\"")

    if txt.startswith("LIKE:"):
        body = txt.split(":", 1)[1].strip().strip("'\"")
        # Materialize a concrete value that still satisfies the pattern.
        return body.replace("%", "").replace("_", "x")

    return txt.strip("'\"")


def _anchor_value_for_type(
    canonical_type: str,
    key_like: bool,
    variant: int = 0,
    time_center: Optional[datetime] = None,
) -> Any:
    variant = max(0, int(variant))
    if canonical_type in ("INTEGER", "BIGINT"):
        return (900001 + variant) if key_like else (42 + (variant % 97))
    if canonical_type == "DECIMAL":
        return float((900001 + variant) if key_like else (42.42 + (variant % 31) * 3.5))
    base_date = time_center if time_center else datetime(2000, 2, 29)
    if canonical_type == "DATE":
        # Spread values around the query's temporal anchor to satisfy date windows.
        offset_days = (variant % 730) - 365
        return (base_date + timedelta(days=offset_days)).strftime("%Y-%m-%d")
    if canonical_type == "TIMESTAMP":
        base_ts = datetime(base_date.year, base_date.month, base_date.day, 12, 0, 0)
        offset_min = (variant % 525600) - 262800
        return (base_ts + timedelta(minutes=offset_min)).strftime("%Y-%m-%d %H:%M:%S")
    return f"ANCHOR_KEY_{variant}" if key_like else f"ANCHOR_VAL_{variant}"


def _from_filter_literal(raw: Any, canonical_type: str) -> Any:
    if not isinstance(raw, str):
        return _coerce_edge_value(raw, canonical_type)

    txt = raw.strip()
    if canonical_type == "VARCHAR":
        return _smart_string_from_literal(txt)

    if txt.startswith("BETWEEN:"):
        _, low, _ = txt.split(":", 2)
        return _coerce_edge_value(low, canonical_type)

    if ":" in txt and txt[0] in "><":
        op, rhs = txt.split(":", 1)
        if canonical_type in ("INTEGER", "BIGINT"):
            base = int(float(rhs))
            if op in (">", ">="):
                return base + 1
            return max(0, base - 1)
        if canonical_type == "DECIMAL":
            base = float(rhs)
            if op in (">", ">="):
                return base + 0.01
            return base - 0.01
        if canonical_type in ("DATE", "TIMESTAMP"):
            try:
                if canonical_type == "TIMESTAMP":
                    dt = datetime.strptime(rhs, "%Y-%m-%d %H:%M:%S")
                    if op in (">", ">="):
                        return (dt + timedelta(seconds=1)).strftime("%Y-%m-%d %H:%M:%S")
                    return (dt - timedelta(seconds=1)).strftime("%Y-%m-%d %H:%M:%S")
                dt = datetime.strptime(rhs, "%Y-%m-%d")
                if op in (">", ">="):
                    return (dt + timedelta(days=1)).strftime("%Y-%m-%d")
                return (dt - timedelta(days=1)).strftime("%Y-%m-%d")
            except Exception:
                return rhs
        return rhs

    return _coerce_edge_value(txt, canonical_type)


def _tables_in_anti_patterns(sql: str) -> Set[str]:
    tables: Set[str] = set()
    try:
        ast = sqlglot.parse_one(sql, read="duckdb")
    except Exception:
        return tables

    for not_node in ast.find_all(sqlglot.exp.Not):
        exists_node = not_node.this
        if not isinstance(exists_node, sqlglot.exp.Exists):
            continue
        subquery = exists_node.this
        if subquery is None:
            continue
        for table_expr in subquery.find_all(sqlglot.exp.Table):
            table_name = (table_expr.name or "").strip().lower()
            if table_name:
                tables.add(table_name)

    # NOT IN (subquery) patterns.
    for not_node in ast.find_all(sqlglot.exp.Not):
        in_node = not_node.this
        if not isinstance(in_node, sqlglot.exp.In):
            continue
        for arg in in_node.args.values():
            if hasattr(arg, "find_all"):
                for table_expr in arg.find_all(sqlglot.exp.Table):
                    table_name = (table_expr.name or "").strip().lower()
                    if table_name:
                        tables.add(table_name)

    # EXCEPT / EXCEPT ALL patterns.
    for except_node in ast.find_all(sqlglot.exp.Except):
        rhs = except_node.expression
        if rhs is None:
            continue
        for table_expr in rhs.find_all(sqlglot.exp.Table):
            table_name = (table_expr.name or "").strip().lower()
            if table_name:
                tables.add(table_name)

    return {t for t in tables if t}


def _tables_in_not_exists(sql: str) -> Set[str]:
    """Backward-compatible alias for older call sites."""
    return _tables_in_anti_patterns(sql)


def _force_seed_for_query(
    conn: duckdb.DuckDBPyConnection,
    qctx: QueryContext,
    global_tables: Dict[str, Dict[str, Any]],
    fk_relationships: Dict[str, Dict[str, Tuple[str, str]]],
    *,
    seed_variant: int = 0,
    seed_rows: int = 1,
    skip_tables: Optional[Set[str]] = None,
) -> None:
    """Insert deterministic witness rows to satisfy AST predicates + join DAG."""
    q_tables = qctx["tables"]
    q_filters = qctx["filter_values"]
    q_fks = qctx["fk_relationships"]
    sql_source = qctx.get("sql_duckdb", "")
    skip_tables = {t.lower() for t in (skip_tables or set())}
    # Minimum-viable objective: produce one witness row unless COUNT requires more.
    inferred_rows = _infer_required_rows(sql_source, 1)
    n_rows = max(1, inferred_rows)
    time_center = _detect_temporal_anchor(sql_source)
    base_variant = max(0, int(seed_variant))
    join_comp_map, join_comp_members = _extract_join_components(sql_source, q_tables)

    # If one column in a join-component has a concrete filter literal, propagate that witness value
    # to every peer column in the same component to satisfy join DAG nodes deterministically.
    comp_fixed_values: Dict[int, Any] = {}
    for comp_id, members in join_comp_members.items():
        picked: Any = None
        for t_name, c_name in members:
            fvals = q_filters.get(t_name, {}).get(c_name, [])
            if not fvals:
                continue
            ctype = _canonical_edge_type(global_tables[t_name]["columns"][c_name].get("type", ""))
            picked = _from_filter_literal(fvals[0], ctype)
            if picked is not None:
                break
        if picked is not None:
            comp_fixed_values[comp_id] = picked

    # Stable anchors by table/row to keep FK joins deterministic while varying values.
    anchor_by_table: Dict[str, List[int]] = {}
    table_names = sorted(q_tables.keys())
    for table_idx, table_name in enumerate(table_names):
        offset = base_variant * 1000 + table_idx * 10000
        anchor_by_table[table_name] = [900001 + offset + i for i in range(n_rows)]
    helper = SyntheticValidator(reference_db=None, dialect="duckdb")
    order = helper._get_table_generation_order(q_tables, q_fks)

    for table_name in order:
        if table_name.lower() in skip_tables:
            continue
        schema = global_tables[table_name]
        cols = list(schema.get("columns", {}).keys())
        if not cols:
            continue
        super_values_by_col = {
            col: _solve_aggregate_constraint(
                sql_source,
                col,
                _canonical_edge_type(schema["columns"][col].get("type", "")),
            )
            for col in cols
        }
        pk_col = find_primary_key_column(table_name, cols)
        fk_map = fk_relationships.get(table_name, {})
        table_filters = q_filters.get(table_name, {})

        rows_to_insert: List[Tuple[Any, ...]] = []
        for row_idx in range(n_rows):
            row_values: List[Any] = []
            for col in cols:
                col_info = schema["columns"][col]
                canonical = _canonical_edge_type(col_info.get("type", ""))
                key_like = _is_key_like(col)
                variant = base_variant + row_idx

                super_val = super_values_by_col.get(col)
                filter_candidates = table_filters.get(col, [])
                comp_id = join_comp_map.get((table_name, col))
                if super_val is not None and row_idx == 0:
                    value = _fit_numeric_to_column(super_val, col_info.get("type", ""), canonical)
                elif filter_candidates:
                    cand = filter_candidates[row_idx % len(filter_candidates)]
                    value = _from_filter_literal(cand, canonical)
                    value = _fit_numeric_to_column(value, col_info.get("type", ""), canonical)
                elif comp_id is not None:
                    if comp_id in comp_fixed_values:
                        raw = comp_fixed_values[comp_id]
                        if canonical == "VARCHAR":
                            value = str(raw)
                        else:
                            value = _coerce_edge_value(raw, canonical)
                            value = _fit_numeric_to_column(
                                value, col_info.get("type", ""), canonical
                            )
                    else:
                        # Shared per-component witness variant guarantees equal-value joins.
                        shared_variant = base_variant + (comp_id * 1000) + row_idx
                        value = _anchor_value_for_type(
                            canonical, True, variant=shared_variant, time_center=time_center
                        )
                elif col.lower() in fk_map:
                    parent_table, _ = fk_map[col.lower()]
                    parent_rows = anchor_by_table.get(parent_table, [])
                    if parent_rows:
                        value = parent_rows[row_idx % len(parent_rows)]
                    else:
                        value = _anchor_value_for_type(
                            canonical, True, variant=variant, time_center=time_center
                        )
                elif pk_col and col == pk_col:
                    table_rows = anchor_by_table.get(table_name, [])
                    if table_rows:
                        value = table_rows[row_idx % len(table_rows)]
                    else:
                        value = _anchor_value_for_type(
                            canonical, True, variant=variant, time_center=time_center
                        )
                elif key_like:
                    value = _anchor_value_for_type(
                        canonical, True, variant=variant, time_center=time_center
                    )
                else:
                    value = _anchor_value_for_type(
                        canonical, False, variant=variant, time_center=time_center
                    )

                if value is None and not bool(col_info.get("nullable", True)):
                    value = _anchor_value_for_type(
                        canonical, key_like, variant=variant, time_center=time_center
                    )
                row_values.append(value)
            rows_to_insert.append(tuple(row_values))

        _insert_rows(conn, table_name, cols, rows_to_insert)


def _mv_insert_row(
    conn: duckdb.DuckDBPyConnection,
    global_tables: Dict[str, Dict[str, Any]],
    table: str,
    values: Dict[str, Any],
) -> bool:
    schema = (global_tables.get(table, {}).get("columns") or {})
    schema_cols = set(schema.keys())
    actual_by_lower = {c.lower(): c for c in schema_cols}
    remapped: Dict[str, Any] = {}
    for in_col, in_val in values.items():
        actual = actual_by_lower.get(str(in_col).lower())
        if actual:
            remapped[actual] = in_val
    cols = [c for c in remapped.keys() if c in schema_cols]
    if not cols:
        return False
    coerced: List[Any] = []
    for c in cols:
        raw = remapped[c]
        ctype = _canonical_edge_type(schema.get(c, {}).get("type", ""))
        try:
            val = _coerce_edge_value(raw, ctype)
        except Exception:
            if ctype in ("INTEGER", "BIGINT"):
                val = 1
            elif ctype == "DECIMAL":
                val = 1.0
            elif ctype in ("DATE", "TIMESTAMP"):
                val = "2000-01-01" if ctype == "DATE" else "2000-01-01 00:00:00"
            else:
                val = str(raw)
        if ctype in ("INTEGER", "BIGINT"):
            if isinstance(val, bool):
                val = int(val)
            elif not isinstance(val, int):
                try:
                    val = int(float(val))
                except Exception:
                    val = 1
        elif ctype == "DECIMAL":
            if not isinstance(val, (int, float)):
                try:
                    val = float(val)
                except Exception:
                    val = 1.0
        coerced.append(val)
    quoted_cols = ", ".join(f'"{c}"' for c in cols)
    placeholders = ", ".join("?" for _ in cols)
    sql = f'INSERT INTO "{table}" ({quoted_cols}) VALUES ({placeholders})'
    conn.execute(sql, tuple(coerced))
    return True


def _mv_insert_rows(
    conn: duckdb.DuckDBPyConnection,
    global_tables: Dict[str, Dict[str, Any]],
    table: str,
    rows: List[Dict[str, Any]],
) -> int:
    inserted = 0
    for row in rows:
        if _mv_insert_row(conn, global_tables, table, row):
            inserted += 1
    return inserted


def _mv_first_filter_value(
    qctx: QueryContext,
    global_tables: Dict[str, Dict[str, Any]],
    table: str,
    col: str,
    default: Any,
) -> Any:
    vals = (qctx.get("filter_values", {}).get(table, {}) or {}).get(col, [])
    if not vals:
        return default
    col_type = (
        global_tables.get(table, {})
        .get("columns", {})
        .get(col, {})
        .get("type", "")
    )
    canonical = _canonical_edge_type(col_type)
    try:
        value = _from_filter_literal(vals[0], canonical)
    except Exception:
        value = vals[0]
    if value is None:
        return default
    return value


def _mv_filter_bounds(
    qctx: QueryContext,
    table: str,
    col: str,
    default_low: float,
    default_high: float,
) -> Tuple[float, float]:
    vals = (qctx.get("filter_values", {}).get(table, {}) or {}).get(col, [])
    if not vals:
        return default_low, default_high
    raw = vals[0]
    if isinstance(raw, str) and raw.startswith("BETWEEN:"):
        try:
            _, lo, hi = raw.split(":", 2)
            return float(lo), float(hi)
        except Exception:
            return default_low, default_high
    try:
        v = float(raw)
        return v, v
    except Exception:
        return default_low, default_high


def _mv_re_first_int(sql: str, pattern: str, default: int) -> int:
    m = re.search(pattern, sql, flags=re.IGNORECASE)
    if not m:
        return default
    try:
        return int(m.group(1))
    except Exception:
        return default


def _mv_re_first_ratio_percent(sql: str, default_low: float, default_high: float) -> Tuple[float, float]:
    m = re.search(
        r"between\s+([0-9]+(?:\.[0-9]+)?)\s*\*\s*0\.01\s+and\s+([0-9]+(?:\.[0-9]+)?)\s*\*\s*0\.01",
        sql,
        flags=re.IGNORECASE,
    )
    if not m:
        return default_low, default_high
    try:
        return float(m.group(1)) / 100.0, float(m.group(2)) / 100.0
    except Exception:
        return default_low, default_high


def _mv_re_first_numeric_range(
    sql: str,
    left_col: str,
    right_col: str,
    default_low: float,
    default_high: float,
) -> Tuple[float, float]:
    pattern = (
        rf"{re.escape(left_col)}\s*/\s*{re.escape(right_col)}\s+between\s+"
        r"([0-9]+(?:\.[0-9]+)?)\s+and\s+([0-9]+(?:\.[0-9]+)?)"
    )
    m = re.search(pattern, sql, flags=re.IGNORECASE)
    if not m:
        return default_low, default_high
    try:
        return float(m.group(1)), float(m.group(2))
    except Exception:
        return default_low, default_high


def _mv_recipe_query001_or_030(
    conn: duckdb.DuckDBPyConnection,
    qctx: QueryContext,
    global_tables: Dict[str, Dict[str, Any]],
) -> bool:
    qname = str(qctx.get("name", "")).lower()
    sql = qctx.get("sql_duckdb", "")
    inserted = 0

    if qname.startswith("query001_"):
        year = int(_mv_first_filter_value(qctx, global_tables, "date_dim", "d_year", 2002))
        state = str(_mv_first_filter_value(qctx, global_tables, "store", "s_state", "TX"))
        reason_lo, _ = _mv_filter_bounds(qctx, "store_returns", "sr_reason_sk", 1, 1)
        reason = int(reason_lo)
        birth_month = int(_mv_first_filter_value(qctx, global_tables, "customer", "c_birth_month", 1))
        birth_lo, _ = _mv_filter_bounds(qctx, "customer", "c_birth_year", 1970, 1970)
        birth_year = int(birth_lo)
        gender = str(_mv_first_filter_value(qctx, global_tables, "customer_demographics", "cd_gender", "F"))
        education = str(_mv_first_filter_value(qctx, global_tables, "customer_demographics", "cd_education_status", "College"))
        marital = str(_mv_first_filter_value(qctx, global_tables, "customer_demographics", "cd_marital_status", "M"))
        ratio_lo, ratio_hi = _mv_re_first_numeric_range(
            sql, "sr_return_amt", "sr_return_quantity", 108.0, 167.0
        )
        ratio = max(1, int((ratio_lo + ratio_hi) / 2.0))

        inserted += _mv_insert_rows(
            conn,
            global_tables,
            "date_dim",
            [{"d_date_sk": 91001, "d_year": year}],
        )
        inserted += _mv_insert_rows(
            conn,
            global_tables,
            "store",
            [{"s_store_sk": 91010, "s_state": state, "s_store_id": "MV001"}],
        )
        inserted += _mv_insert_rows(
            conn,
            global_tables,
            "customer_demographics",
            [{"cd_demo_sk": 91020, "cd_gender": gender, "cd_marital_status": marital, "cd_education_status": education}],
        )
        inserted += _mv_insert_rows(
            conn,
            global_tables,
            "customer",
            [
                {
                    "c_customer_sk": 91030,
                    "c_customer_id": "MV001A",
                    "c_current_cdemo_sk": 91020,
                    "c_birth_month": birth_month,
                    "c_birth_year": birth_year,
                },
                {
                    "c_customer_sk": 91031,
                    "c_customer_id": "MV001B",
                    "c_current_cdemo_sk": 91020,
                    "c_birth_month": birth_month,
                    "c_birth_year": birth_year,
                },
            ],
        )
        inserted += _mv_insert_rows(
            conn,
            global_tables,
            "store_returns",
            [
                {
                    "sr_customer_sk": 91030,
                    "sr_store_sk": 91010,
                    "sr_reason_sk": reason,
                    "sr_returned_date_sk": 91001,
                    "sr_return_quantity": 1,
                    "sr_return_amt": ratio,
                    "sr_return_amt_inc_tax": 1000.0,
                },
                {
                    "sr_customer_sk": 91031,
                    "sr_store_sk": 91010,
                    "sr_reason_sk": reason,
                    "sr_returned_date_sk": 91001,
                    "sr_return_quantity": 1,
                    "sr_return_amt": ratio,
                    "sr_return_amt_inc_tax": 100.0,
                },
            ],
        )
        return inserted > 0

    if qname.startswith("query030_"):
        year = int(_mv_first_filter_value(qctx, global_tables, "date_dim", "d_year", 2000))
        state = str(_mv_first_filter_value(qctx, global_tables, "customer_address", "ca_state", "TX"))
        reason = int(_mv_first_filter_value(qctx, global_tables, "web_returns", "wr_reason_sk", 1))
        manager_lo, _ = _mv_filter_bounds(qctx, "item", "i_manager_id", 1, 1)
        manager_id = int(manager_lo)
        birth_lo, _ = _mv_filter_bounds(qctx, "customer", "c_birth_year", 1970, 1970)
        birth_year = int(birth_lo)
        ratio_lo, ratio_hi = _mv_re_first_numeric_range(
            sql, "wr_return_amt", "wr_return_quantity", 120.0, 150.0
        )
        low_amt = max(1, int(ratio_lo + 1))
        high_amt = max(low_amt, int(ratio_hi - 1))

        inserted += _mv_insert_rows(
            conn,
            global_tables,
            "date_dim",
            [{"d_date_sk": 92001, "d_year": year}],
        )
        inserted += _mv_insert_rows(
            conn,
            global_tables,
            "customer_address",
            [{"ca_address_sk": 92010, "ca_state": state}],
        )
        inserted += _mv_insert_rows(
            conn,
            global_tables,
            "item",
            [{"i_item_sk": 92020, "i_manager_id": manager_id}],
        )
        inserted += _mv_insert_rows(
            conn,
            global_tables,
            "customer",
            [
                {"c_customer_sk": 92030, "c_customer_id": "MV030A", "c_current_addr_sk": 92010, "c_birth_year": birth_year},
                {"c_customer_sk": 92031, "c_customer_id": "MV030B", "c_current_addr_sk": 92010, "c_birth_year": birth_year},
            ],
        )
        inserted += _mv_insert_rows(
            conn,
            global_tables,
            "web_returns",
            [
                {
                    "wr_returning_customer_sk": 92030,
                    "wr_returning_addr_sk": 92010,
                    "wr_item_sk": 92020,
                    "wr_returned_date_sk": 92001,
                    "wr_reason_sk": reason,
                    "wr_return_quantity": 1,
                    "wr_return_amt": float(high_amt),
                },
                {
                    "wr_returning_customer_sk": 92030,
                    "wr_returning_addr_sk": 92010,
                    "wr_item_sk": 92020,
                    "wr_returned_date_sk": 92001,
                    "wr_reason_sk": reason,
                    "wr_return_quantity": 1,
                    "wr_return_amt": float(high_amt),
                },
                {
                    "wr_returning_customer_sk": 92031,
                    "wr_returning_addr_sk": 92010,
                    "wr_item_sk": 92020,
                    "wr_returned_date_sk": 92001,
                    "wr_reason_sk": reason,
                    "wr_return_quantity": 1,
                    "wr_return_amt": float(low_amt),
                },
            ],
        )
        return inserted > 0

    return False


def _mv_recipe_query010(
    conn: duckdb.DuckDBPyConnection,
    qctx: QueryContext,
    global_tables: Dict[str, Dict[str, Any]],
) -> bool:
    sql = qctx.get("sql_duckdb", "")
    year = int(_mv_first_filter_value(qctx, global_tables, "date_dim", "d_year", 1999))
    moy = _mv_re_first_int(sql, r"d_moy\s+between\s+([0-9]+)\s+and", 1)
    county = str(_mv_first_filter_value(qctx, global_tables, "customer_address", "ca_county", "MV County"))
    birth_month = int(_mv_first_filter_value(qctx, global_tables, "customer", "c_birth_month", 1))
    marital = str(_mv_first_filter_value(qctx, global_tables, "customer_demographics", "cd_marital_status", "U"))
    education = str(_mv_first_filter_value(qctx, global_tables, "customer_demographics", "cd_education_status", "College"))
    gender = str(_mv_first_filter_value(qctx, global_tables, "customer_demographics", "cd_gender", "M"))
    category = str(_mv_first_filter_value(qctx, global_tables, "item", "i_category", "Electronics"))
    manager_lo, _ = _mv_filter_bounds(qctx, "item", "i_manager_id", 10, 20)
    manager = int(manager_lo)
    r_lo, r_hi = _mv_re_first_ratio_percent(sql, 0.2, 0.3)
    ratio = (r_lo + r_hi) / 2.0
    list_price = 100.0
    sales_price = list_price * ratio

    inserted = 0
    inserted += _mv_insert_rows(conn, global_tables, "date_dim", [{"d_date_sk": 93001, "d_year": year, "d_moy": moy}])
    inserted += _mv_insert_rows(conn, global_tables, "customer_address", [{"ca_address_sk": 93010, "ca_county": county}])
    inserted += _mv_insert_rows(
        conn,
        global_tables,
        "customer_demographics",
        [{
            "cd_demo_sk": 93020,
            "cd_marital_status": marital,
            "cd_education_status": education,
            "cd_gender": gender,
            "cd_purchase_estimate": 1,
            "cd_credit_rating": "A",
            "cd_dep_count": 1,
            "cd_dep_employed_count": 1,
            "cd_dep_college_count": 1,
        }],
    )
    inserted += _mv_insert_rows(
        conn,
        global_tables,
        "customer",
        [{"c_customer_sk": 93030, "c_current_addr_sk": 93010, "c_birth_month": birth_month, "c_current_cdemo_sk": 93020}],
    )
    inserted += _mv_insert_rows(conn, global_tables, "item", [{"i_item_sk": 93040, "i_category": category, "i_manager_id": manager}])
    inserted += _mv_insert_rows(
        conn,
        global_tables,
        "store_sales",
        [{
            "ss_customer_sk": 93030,
            "ss_sold_date_sk": 93001,
            "ss_item_sk": 93040,
            "ss_sales_price": sales_price,
            "ss_list_price": list_price,
        }],
    )
    inserted += _mv_insert_rows(
        conn,
        global_tables,
        "web_sales",
        [{
            "ws_bill_customer_sk": 93030,
            "ws_sold_date_sk": 93001,
            "ws_item_sk": 93040,
            "ws_sales_price": sales_price,
            "ws_list_price": list_price,
        }],
    )
    return inserted > 0


def _mv_recipe_query013(
    conn: duckdb.DuckDBPyConnection,
    qctx: QueryContext,
    global_tables: Dict[str, Dict[str, Any]],
) -> bool:
    year = int(_mv_first_filter_value(qctx, global_tables, "date_dim", "d_year", 2001))
    state = str(_mv_first_filter_value(qctx, global_tables, "customer_address", "ca_state", "TX"))
    inserted = 0
    inserted += _mv_insert_rows(conn, global_tables, "store", [{"s_store_sk": 94001}])
    inserted += _mv_insert_rows(conn, global_tables, "date_dim", [{"d_date_sk": 94002, "d_year": year}])
    inserted += _mv_insert_rows(conn, global_tables, "customer_demographics", [{"cd_demo_sk": 94003, "cd_marital_status": "M", "cd_education_status": "2 yr Degree"}])
    inserted += _mv_insert_rows(conn, global_tables, "household_demographics", [{"hd_demo_sk": 94004, "hd_dep_count": 3}])
    inserted += _mv_insert_rows(conn, global_tables, "customer_address", [{"ca_address_sk": 94005, "ca_country": "United States", "ca_state": state}])
    inserted += _mv_insert_rows(
        conn,
        global_tables,
        "store_sales",
        [{
            "ss_store_sk": 94001,
            "ss_sold_date_sk": 94002,
            "ss_cdemo_sk": 94003,
            "ss_hdemo_sk": 94004,
            "ss_addr_sk": 94005,
            "ss_sales_price": 120.0,
            "ss_net_profit": 150.0,
            "ss_quantity": 1,
            "ss_ext_sales_price": 120.0,
            "ss_ext_wholesale_cost": 90.0,
        }],
    )
    return inserted > 0


def _mv_recipe_query031(
    conn: duckdb.DuckDBPyConnection,
    qctx: QueryContext,
    global_tables: Dict[str, Dict[str, Any]],
) -> bool:
    state = str(_mv_first_filter_value(qctx, global_tables, "customer_address", "ca_state", "IA"))
    color = str(_mv_first_filter_value(qctx, global_tables, "item", "i_color", "orchid"))
    manager_lo, _ = _mv_filter_bounds(qctx, "item", "i_manager_id", 8, 27)
    list_lo, list_hi = _mv_filter_bounds(qctx, "store_sales", "ss_list_price", 80, 120)
    list_price = max(1.0, (list_lo + list_hi) / 2.0)
    inserted = 0
    inserted += _mv_insert_rows(conn, global_tables, "item", [{"i_item_sk": 95001, "i_color": color, "i_manager_id": int(manager_lo)}])
    inserted += _mv_insert_rows(conn, global_tables, "customer_address", [{"ca_address_sk": 95002, "ca_state": state, "ca_county": "MVCOUNTY"}])
    inserted += _mv_insert_rows(
        conn,
        global_tables,
        "date_dim",
        [
            {"d_date_sk": 95011, "d_year": 1998, "d_qoy": 1},
            {"d_date_sk": 95012, "d_year": 1998, "d_qoy": 2},
            {"d_date_sk": 95013, "d_year": 1998, "d_qoy": 3},
        ],
    )
    inserted += _mv_insert_rows(
        conn,
        global_tables,
        "store_sales",
        [
            {"ss_sold_date_sk": 95011, "ss_addr_sk": 95002, "ss_item_sk": 95001, "ss_list_price": list_price, "ss_ext_sales_price": 100.0},
            {"ss_sold_date_sk": 95012, "ss_addr_sk": 95002, "ss_item_sk": 95001, "ss_list_price": list_price, "ss_ext_sales_price": 110.0},
            {"ss_sold_date_sk": 95013, "ss_addr_sk": 95002, "ss_item_sk": 95001, "ss_list_price": list_price, "ss_ext_sales_price": 120.0},
        ],
    )
    inserted += _mv_insert_rows(
        conn,
        global_tables,
        "web_sales",
        [
            {"ws_sold_date_sk": 95011, "ws_bill_addr_sk": 95002, "ws_item_sk": 95001, "ws_list_price": list_price, "ws_ext_sales_price": 100.0},
            {"ws_sold_date_sk": 95012, "ws_bill_addr_sk": 95002, "ws_item_sk": 95001, "ws_list_price": list_price, "ws_ext_sales_price": 150.0},
            {"ws_sold_date_sk": 95013, "ws_bill_addr_sk": 95002, "ws_item_sk": 95001, "ws_list_price": list_price, "ws_ext_sales_price": 250.0},
        ],
    )
    return inserted > 0


def _mv_recipe_query039(
    conn: duckdb.DuckDBPyConnection,
    qctx: QueryContext,
    global_tables: Dict[str, Dict[str, Any]],
) -> bool:
    sql = str(qctx.get("sql_duckdb", ""))
    year = int(_mv_first_filter_value(qctx, global_tables, "date_dim", "d_year", 2002))
    base_moy = _mv_re_first_int(sql, r"inv1\.d_moy\s*=\s*([0-9]+)", 2)
    next_moy = base_moy + 1
    category = str(_mv_first_filter_value(qctx, global_tables, "item", "i_category", "Jewelry"))
    manager_lo, _ = _mv_filter_bounds(qctx, "item", "i_manager_id", 1, 100)
    q_low, q_high = _mv_filter_bounds(qctx, "inventory", "inv_quantity_on_hand", 0, 200)

    # Build extreme points inside bounds to maximize stddev/mean in each month-group.
    low_q = int(max(q_low, 1))
    high_q = int(max(low_q + 1, q_high))

    inserted = 0
    inserted += _mv_insert_rows(conn, global_tables, "warehouse", [{"w_warehouse_sk": 95901, "w_warehouse_name": "MV039"}])
    inserted += _mv_insert_rows(conn, global_tables, "item", [{"i_item_sk": 95902, "i_category": category, "i_manager_id": int(manager_lo)}])
    inserted += _mv_insert_rows(
        conn,
        global_tables,
        "date_dim",
        [
            {"d_date_sk": 95911, "d_year": year, "d_moy": base_moy},
            {"d_date_sk": 95912, "d_year": year, "d_moy": base_moy},
            {"d_date_sk": 95913, "d_year": year, "d_moy": next_moy},
            {"d_date_sk": 95914, "d_year": year, "d_moy": next_moy},
        ],
    )
    inserted += _mv_insert_rows(
        conn,
        global_tables,
        "inventory",
        [
            {"inv_warehouse_sk": 95901, "inv_item_sk": 95902, "inv_date_sk": 95911, "inv_quantity_on_hand": low_q},
            {"inv_warehouse_sk": 95901, "inv_item_sk": 95902, "inv_date_sk": 95912, "inv_quantity_on_hand": high_q},
            {"inv_warehouse_sk": 95901, "inv_item_sk": 95902, "inv_date_sk": 95913, "inv_quantity_on_hand": low_q},
            {"inv_warehouse_sk": 95901, "inv_item_sk": 95902, "inv_date_sk": 95914, "inv_quantity_on_hand": high_q},
        ],
    )
    return inserted > 0


def _mv_recipe_query054(
    conn: duckdb.DuckDBPyConnection,
    qctx: QueryContext,
    global_tables: Dict[str, Dict[str, Any]],
) -> bool:
    sql = qctx.get("sql_duckdb", "")
    base_year = _mv_re_first_int(sql, r"d_year\s*=\s*([0-9]{4})", 1998)
    base_moy = _mv_re_first_int(sql, r"d_moy\s*=\s*([0-9]+)", 1)
    category = str(_mv_first_filter_value(qctx, global_tables, "item", "i_category", "Electronics"))
    class_match = re.search(r"i_class\s*=\s*'([^']+)'", sql, flags=re.IGNORECASE)
    i_class = class_match.group(1) if class_match else "personal"
    state = str(_mv_first_filter_value(qctx, global_tables, "store", "s_state", "TX"))
    birth_lo, _ = _mv_filter_bounds(qctx, "customer", "c_birth_year", 1930, 1930)
    m_wh = re.search(
        r"wholesale_cost\s+between\s+([0-9]+(?:\.[0-9]+)?)\s+and\s+([0-9]+(?:\.[0-9]+)?)",
        sql,
        flags=re.IGNORECASE,
    )
    if m_wh:
        ws_lo = float(m_wh.group(1))
        ws_hi = float(m_wh.group(2))
    else:
        ws_lo, ws_hi = _mv_filter_bounds(qctx, "catalog_sales", "cs_wholesale_cost", 35.0, 65.0)
    ss_lo, ss_hi = _mv_filter_bounds(qctx, "store_sales", "ss_wholesale_cost", ws_lo, ws_hi)
    wholesale = (ws_lo + ws_hi) / 2.0
    wholesale_ss = (ss_lo + ss_hi) / 2.0
    inserted = 0
    # Stabilize scalar subqueries:
    #   (select distinct d_month_seq+1 from date_dim where d_year=? and d_moy=?)
    # Keep a single anchor row for (year, month) to avoid nondeterministic scalar choice.
    try:
        conn.execute(
            "DELETE FROM date_dim WHERE d_year = ? AND d_moy = ?",
            [int(base_year), int(base_moy)],
        )
    except Exception:
        pass
    inserted += _mv_insert_rows(
        conn,
        global_tables,
        "date_dim",
        [
            {"d_date_sk": 96001, "d_year": base_year, "d_moy": base_moy, "d_month_seq": 5000},
            {"d_date_sk": 96002, "d_year": base_year, "d_moy": base_moy + 1, "d_month_seq": 5001},
        ],
    )
    inserted += _mv_insert_rows(conn, global_tables, "item", [{"i_item_sk": 96010, "i_category": category, "i_class": i_class}])
    inserted += _mv_insert_rows(conn, global_tables, "customer_address", [{"ca_address_sk": 96020, "ca_state": state, "ca_county": "MVCOUNTY"}])
    inserted += _mv_insert_rows(conn, global_tables, "customer", [{"c_customer_sk": 96030, "c_current_addr_sk": 96020, "c_birth_year": int(birth_lo)}])
    inserted += _mv_insert_rows(conn, global_tables, "store", [{"s_store_sk": 96040, "s_state": state, "s_county": "MVCOUNTY"}])
    inserted += _mv_insert_rows(
        conn,
        global_tables,
        "catalog_sales",
        [{"cs_sold_date_sk": 96001, "cs_bill_customer_sk": 96030, "cs_item_sk": 96010, "cs_wholesale_cost": wholesale}],
    )
    inserted += _mv_insert_rows(
        conn,
        global_tables,
        "store_sales",
        [{"ss_sold_date_sk": 96002, "ss_customer_sk": 96030, "ss_wholesale_cost": wholesale_ss, "ss_ext_sales_price": 200.0}],
    )
    return inserted > 0


def _mv_recipe_query059(
    conn: duckdb.DuckDBPyConnection,
    qctx: QueryContext,
    global_tables: Dict[str, Dict[str, Any]],
) -> bool:
    sql = qctx.get("sql_duckdb", "")
    start_seq = _mv_re_first_int(sql, r"d_month_seq\s+between\s+([0-9]+)\s+and", 1187)
    state = str(_mv_first_filter_value(qctx, global_tables, "store", "s_state", "TX"))
    r_lo, r_hi = _mv_re_first_ratio_percent(sql, 0.6, 0.7)
    ratio = (r_lo + r_hi) / 2.0
    list_price = 100.0
    sales_price = list_price * ratio
    inserted = 0
    inserted += _mv_insert_rows(conn, global_tables, "store", [{"s_store_sk": 97001, "s_store_id": "MV059", "s_store_name": "MV059", "s_state": state}])
    inserted += _mv_insert_rows(
        conn,
        global_tables,
        "date_dim",
        [
            {"d_date_sk": 97010, "d_month_seq": start_seq, "d_week_seq": 100, "d_day_name": "Monday"},
            {"d_date_sk": 97011, "d_month_seq": start_seq + 12, "d_week_seq": 152, "d_day_name": "Monday"},
        ],
    )
    inserted += _mv_insert_rows(
        conn,
        global_tables,
        "store_sales",
        [
            {"ss_store_sk": 97001, "ss_sold_date_sk": 97010, "ss_sales_price": sales_price, "ss_list_price": list_price},
            {"ss_store_sk": 97001, "ss_sold_date_sk": 97011, "ss_sales_price": sales_price, "ss_list_price": list_price},
        ],
    )
    return inserted > 0


def _mv_recipe_query064(
    conn: duckdb.DuckDBPyConnection,
    qctx: QueryContext,
    global_tables: Dict[str, Dict[str, Any]],
) -> bool:
    year_values = (qctx.get("filter_values", {}).get("date_dim", {}) or {}).get("d_year", [])
    years: List[int] = []
    for y in year_values:
        try:
            years.append(int(str(y)))
        except Exception:
            continue
    base_year = min(years) if years else 1998
    state = str(_mv_first_filter_value(qctx, global_tables, "customer_address", "ca_state", "TX"))
    p_email = str(_mv_first_filter_value(qctx, global_tables, "promotion", "p_channel_email", "N"))
    p_tv = str(_mv_first_filter_value(qctx, global_tables, "promotion", "p_channel_tv", "Y"))
    p_radio = str(_mv_first_filter_value(qctx, global_tables, "promotion", "p_channel_radio", "N"))
    i_lo, i_hi = _mv_filter_bounds(qctx, "item", "i_current_price", 26.0, 36.0)
    ss_lo, ss_hi = _mv_filter_bounds(qctx, "store_sales", "ss_wholesale_cost", 35.0, 55.0)
    cs_lo, cs_hi = _mv_filter_bounds(qctx, "catalog_sales", "cs_wholesale_cost", ss_lo, ss_hi)
    cd_vals = (qctx.get("filter_values", {}).get("customer_demographics", {}) or {}).get("cd_marital_status", [])
    uniq_status = []
    for v in cd_vals:
        s = str(v)
        if s not in uniq_status:
            uniq_status.append(s)
    if len(uniq_status) < 2:
        return False
    cd1_status, cd2_status = uniq_status[0], uniq_status[1]
    edu = str(_mv_first_filter_value(qctx, global_tables, "customer_demographics", "cd_education_status", "Unknown"))

    inserted = 0
    inserted += _mv_insert_rows(
        conn,
        global_tables,
        "date_dim",
        [
            {"d_date_sk": 98001, "d_year": base_year},
            {"d_date_sk": 98002, "d_year": base_year + 1},
            {"d_date_sk": 98003, "d_year": base_year},
            {"d_date_sk": 98004, "d_year": base_year},
        ],
    )
    inserted += _mv_insert_rows(conn, global_tables, "income_band", [{"ib_income_band_sk": 98010}, {"ib_income_band_sk": 98011}])
    inserted += _mv_insert_rows(
        conn,
        global_tables,
        "household_demographics",
        [{"hd_demo_sk": 98012, "hd_income_band_sk": 98010}, {"hd_demo_sk": 98013, "hd_income_band_sk": 98011}],
    )
    inserted += _mv_insert_rows(
        conn,
        global_tables,
        "customer_demographics",
        [
            {"cd_demo_sk": 98014, "cd_marital_status": cd1_status, "cd_education_status": edu},
            {"cd_demo_sk": 98015, "cd_marital_status": cd2_status, "cd_education_status": edu},
        ],
    )
    inserted += _mv_insert_rows(
        conn,
        global_tables,
        "customer_address",
        [
            {"ca_address_sk": 98016, "ca_state": state, "ca_zip": "11111", "ca_city": "A", "ca_street_name": "A", "ca_street_number": "1"},
            {"ca_address_sk": 98017, "ca_state": state, "ca_zip": "11111", "ca_city": "B", "ca_street_name": "B", "ca_street_number": "2"},
        ],
    )
    inserted += _mv_insert_rows(
        conn,
        global_tables,
        "customer",
        [{
            "c_customer_sk": 98018,
            "c_current_cdemo_sk": 98015,
            "c_current_hdemo_sk": 98013,
            "c_current_addr_sk": 98017,
            "c_first_sales_date_sk": 98003,
            "c_first_shipto_date_sk": 98004,
        }],
    )
    inserted += _mv_insert_rows(conn, global_tables, "store", [{"s_store_sk": 98019, "s_store_name": "MV064", "s_zip": "11111"}])
    inserted += _mv_insert_rows(conn, global_tables, "item", [{"i_item_sk": 98020, "i_product_name": "MV064", "i_current_price": (i_lo + i_hi) / 2.0}])
    inserted += _mv_insert_rows(
        conn,
        global_tables,
        "promotion",
        [{
            "p_promo_sk": 98021,
            "p_channel_email": p_email,
            "p_channel_tv": p_tv,
            "p_channel_radio": p_radio,
        }],
    )
    inserted += _mv_insert_rows(
        conn,
        global_tables,
        "catalog_sales",
        [{"cs_item_sk": 98020, "cs_order_number": 98030, "cs_wholesale_cost": (cs_lo + cs_hi) / 2.0, "cs_ext_list_price": 1000.0}],
    )
    inserted += _mv_insert_rows(
        conn,
        global_tables,
        "catalog_returns",
        [{"cr_item_sk": 98020, "cr_order_number": 98030, "cr_refunded_cash": 10.0, "cr_reversed_charge": 0.0, "cr_store_credit": 0.0}],
    )
    inserted += _mv_insert_rows(
        conn,
        global_tables,
        "store_sales",
        [
            {
                "ss_item_sk": 98020,
                "ss_ticket_number": 98040,
                "ss_store_sk": 98019,
                "ss_sold_date_sk": 98001,
                "ss_customer_sk": 98018,
                "ss_cdemo_sk": 98014,
                "ss_hdemo_sk": 98012,
                "ss_addr_sk": 98016,
                "ss_promo_sk": 98021,
                "ss_wholesale_cost": (ss_lo + ss_hi) / 2.0,
                "ss_list_price": 100.0,
                "ss_coupon_amt": 1.0,
            },
            {
                "ss_item_sk": 98020,
                "ss_ticket_number": 98041,
                "ss_store_sk": 98019,
                "ss_sold_date_sk": 98002,
                "ss_customer_sk": 98018,
                "ss_cdemo_sk": 98014,
                "ss_hdemo_sk": 98012,
                "ss_addr_sk": 98016,
                "ss_promo_sk": 98021,
                "ss_wholesale_cost": (ss_lo + ss_hi) / 2.0,
                "ss_list_price": 100.0,
                "ss_coupon_amt": 1.0,
            },
        ],
    )
    inserted += _mv_insert_rows(
        conn,
        global_tables,
        "store_returns",
        [
            {"sr_item_sk": 98020, "sr_ticket_number": 98040},
            {"sr_item_sk": 98020, "sr_ticket_number": 98041},
        ],
    )
    return inserted > 0


def _mv_recipe_query065(
    conn: duckdb.DuckDBPyConnection,
    qctx: QueryContext,
    global_tables: Dict[str, Dict[str, Any]],
) -> bool:
    sql = qctx.get("sql_duckdb", "")
    month_lo, _ = _mv_filter_bounds(qctx, "date_dim", "d_month_seq", 1195, 1206)
    state = str(_mv_first_filter_value(qctx, global_tables, "store", "s_state", "IA"))
    manager_lo, _ = _mv_filter_bounds(qctx, "item", "i_manager_id", 80, 84)
    r_lo, r_hi = _mv_re_first_ratio_percent(sql, 0.38, 0.48)
    ratio = (r_lo + r_hi) / 2.0
    inserted = 0
    inserted += _mv_insert_rows(conn, global_tables, "date_dim", [{"d_date_sk": 99001, "d_month_seq": int(month_lo)}])
    inserted += _mv_insert_rows(conn, global_tables, "store", [{"s_store_sk": 99002, "s_store_name": "MV065", "s_state": state}])
    inserted += _mv_insert_rows(
        conn,
        global_tables,
        "item",
        [{
            "i_item_sk": 99003,
            "i_item_desc": "MV065 target",
            "i_manager_id": int(manager_lo),
            "i_current_price": 30.0,
            "i_wholesale_cost": 10.0,
            "i_brand": "MV",
        }],
    )
    inserted += _mv_insert_rows(
        conn,
        global_tables,
        "store_sales",
        [
            {
                "ss_store_sk": 99002,
                "ss_item_sk": 99003,
                "ss_sold_date_sk": 99001,
                "ss_sales_price": 40.0,
                "ss_list_price": 40.0 / max(0.01, ratio),
            },
            {
                "ss_store_sk": 99002,
                "ss_item_sk": 99004,
                "ss_sold_date_sk": 99001,
                "ss_sales_price": 800.0,
                "ss_list_price": 800.0 / max(0.01, ratio),
            },
        ],
    )
    return inserted > 0


def _mv_recipe_query075(
    conn: duckdb.DuckDBPyConnection,
    qctx: QueryContext,
    global_tables: Dict[str, Dict[str, Any]],
) -> bool:
    sql = qctx.get("sql_duckdb", "")
    years = (qctx.get("filter_values", {}).get("date_dim", {}) or {}).get("d_year", [])
    year_vals = sorted({int(str(y)) for y in years if str(y).isdigit()})
    prev_year = year_vals[0] if year_vals else 1998
    curr_year = prev_year + 1
    category = str(_mv_first_filter_value(qctx, global_tables, "item", "i_category", "Home"))
    reason = int(_mv_first_filter_value(qctx, global_tables, "store_returns", "sr_reason_sk", 7))
    r_lo, r_hi = _mv_re_first_ratio_percent(sql, 0.34, 0.54)
    ratio = (r_lo + r_hi) / 2.0
    list_price = 100.0
    sales_price = list_price * ratio
    inserted = 0
    inserted += _mv_insert_rows(
        conn,
        global_tables,
        "item",
        [{"i_item_sk": 99101, "i_category": category, "i_brand_id": 1, "i_class_id": 1, "i_category_id": 1, "i_manufact_id": 1}],
    )
    inserted += _mv_insert_rows(
        conn,
        global_tables,
        "date_dim",
        [{"d_date_sk": 99110, "d_year": prev_year}, {"d_date_sk": 99111, "d_year": curr_year}],
    )
    inserted += _mv_insert_rows(
        conn,
        global_tables,
        "store_sales",
        [
            {
                "ss_ticket_number": 99120,
                "ss_item_sk": 99101,
                "ss_sold_date_sk": 99110,
                "ss_quantity": 10,
                "ss_sales_price": sales_price,
                "ss_list_price": list_price,
                "ss_ext_sales_price": 1000.0,
            },
            {
                "ss_ticket_number": 99121,
                "ss_item_sk": 99101,
                "ss_sold_date_sk": 99111,
                "ss_quantity": 5,
                "ss_sales_price": sales_price,
                "ss_list_price": list_price,
                "ss_ext_sales_price": 400.0,
            },
        ],
    )
    inserted += _mv_insert_rows(
        conn,
        global_tables,
        "store_returns",
        [
            {"sr_ticket_number": 99120, "sr_item_sk": 99101, "sr_reason_sk": reason, "sr_return_quantity": 0, "sr_return_amt": 0.0},
            {"sr_ticket_number": 99121, "sr_item_sk": 99101, "sr_reason_sk": reason, "sr_return_quantity": 0, "sr_return_amt": 0.0},
        ],
    )
    return inserted > 0


def _mv_recipe_query080(
    conn: duckdb.DuckDBPyConnection,
    qctx: QueryContext,
    global_tables: Dict[str, Dict[str, Any]],
) -> bool:
    sql = qctx.get("sql_duckdb", "")
    m = re.search(r"cast\('([^']+)'\s+as\s+date\)", sql, flags=re.IGNORECASE)
    base_date = m.group(1) if m else "1998-08-29"
    state = str(_mv_first_filter_value(qctx, global_tables, "item", "i_category", "Children"))
    i_price = float(_mv_first_filter_value(qctx, global_tables, "item", "i_current_price", 60.0))
    p_email = str(_mv_first_filter_value(qctx, global_tables, "promotion", "p_channel_email", "N"))
    p_tv = str(_mv_first_filter_value(qctx, global_tables, "promotion", "p_channel_tv", "N"))
    p_radio = str(_mv_first_filter_value(qctx, global_tables, "promotion", "p_channel_radio", "N"))
    p_press = str(_mv_first_filter_value(qctx, global_tables, "promotion", "p_channel_press", "N"))
    p_event = str(_mv_first_filter_value(qctx, global_tables, "promotion", "p_channel_event", "N"))
    ss_lo, ss_hi = _mv_filter_bounds(qctx, "store_sales", "ss_wholesale_cost", 23, 38)
    inserted = 0
    inserted += _mv_insert_rows(conn, global_tables, "date_dim", [{"d_date_sk": 99201, "d_date": base_date}])
    inserted += _mv_insert_rows(conn, global_tables, "item", [{"i_item_sk": 99202, "i_current_price": max(60.0, i_price), "i_category": state}])
    inserted += _mv_insert_rows(conn, global_tables, "store", [{"s_store_sk": 99203, "s_store_id": "MV080"}])
    inserted += _mv_insert_rows(
        conn,
        global_tables,
        "promotion",
        [{
            "p_promo_sk": 99204,
            "p_channel_email": p_email,
            "p_channel_tv": p_tv,
            "p_channel_radio": p_radio,
            "p_channel_press": p_press,
            "p_channel_event": p_event,
        }],
    )
    inserted += _mv_insert_rows(
        conn,
        global_tables,
        "store_sales",
        [{
            "ss_sold_date_sk": 99201,
            "ss_store_sk": 99203,
            "ss_item_sk": 99202,
            "ss_promo_sk": 99204,
            "ss_wholesale_cost": (ss_lo + ss_hi) / 2.0,
            "ss_ext_sales_price": 200.0,
            "ss_net_profit": 50.0,
            "ss_ticket_number": 99205,
        }],
    )
    return inserted > 0


def _mv_recipe_query083(
    conn: duckdb.DuckDBPyConnection,
    qctx: QueryContext,
    global_tables: Dict[str, Dict[str, Any]],
) -> bool:
    sql = qctx.get("sql_duckdb", "")
    dates = re.findall(r"'((?:19|20)\d{2}-\d{2}-\d{2})'", sql)
    dtxt = dates[0] if dates else "2002-02-26"
    category = str(_mv_first_filter_value(qctx, global_tables, "item", "i_category", "Home"))
    manager_lo, _ = _mv_filter_bounds(qctx, "item", "i_manager_id", 8, 17)
    sr_reason = int(_mv_first_filter_value(qctx, global_tables, "store_returns", "sr_reason_sk", 6))
    cr_reason = int(_mv_first_filter_value(qctx, global_tables, "catalog_returns", "cr_reason_sk", sr_reason))
    wr_reason = int(_mv_first_filter_value(qctx, global_tables, "web_returns", "wr_reason_sk", sr_reason))
    sr_lo, sr_hi = _mv_re_first_numeric_range(sql, "sr_return_amt", "sr_return_quantity", 230, 270)
    ratio = (sr_lo + sr_hi) / 2.0
    inserted = 0
    inserted += _mv_insert_rows(conn, global_tables, "date_dim", [{"d_date_sk": 99301, "d_date": dtxt, "d_month_seq": 7000}])
    inserted += _mv_insert_rows(conn, global_tables, "item", [{"i_item_sk": 99302, "i_item_id": "MV083", "i_category": category, "i_manager_id": int(manager_lo)}])
    inserted += _mv_insert_rows(
        conn,
        global_tables,
        "store_returns",
        [{"sr_item_sk": 99302, "sr_returned_date_sk": 99301, "sr_reason_sk": sr_reason, "sr_return_quantity": 1, "sr_return_amt": ratio}],
    )
    inserted += _mv_insert_rows(
        conn,
        global_tables,
        "catalog_returns",
        [{"cr_item_sk": 99302, "cr_returned_date_sk": 99301, "cr_reason_sk": cr_reason, "cr_return_quantity": 1, "cr_return_amount": ratio}],
    )
    inserted += _mv_insert_rows(
        conn,
        global_tables,
        "web_returns",
        [{"wr_item_sk": 99302, "wr_returned_date_sk": 99301, "wr_reason_sk": wr_reason, "wr_return_quantity": 1, "wr_return_amt": ratio}],
    )
    return inserted > 0


def _mv_recipe_query085(
    conn: duckdb.DuckDBPyConnection,
    qctx: QueryContext,
    global_tables: Dict[str, Dict[str, Any]],
) -> bool:
    sql = str(qctx.get("sql_duckdb", ""))
    year = int(_mv_first_filter_value(qctx, global_tables, "date_dim", "d_year", 2000))

    demo_match = re.search(
        r"cd1\.cd_marital_status\s*=\s*'([^']+)'.*?"
        r"cd1\.cd_education_status\s*=\s*'([^']+)'.*?"
        r"ws_sales_price\s+between\s+([0-9.]+)\s+and\s+([0-9.]+)",
        sql,
        flags=re.IGNORECASE | re.DOTALL,
    )
    if demo_match:
        marital = demo_match.group(1)
        education = demo_match.group(2)
        ws_low = float(demo_match.group(3))
        ws_high = float(demo_match.group(4))
    else:
        marital = str(_mv_first_filter_value(qctx, global_tables, "customer_demographics", "cd_marital_status", "S"))
        education = str(_mv_first_filter_value(qctx, global_tables, "customer_demographics", "cd_education_status", "College"))
        ws_low, ws_high = 100.0, 150.0

    addr_match = re.search(
        r"ca_state\s+in\s*\(([^)]+)\)\s*and\s*ws_net_profit\s+between\s+([0-9.]+)\s+and\s+([0-9.]+)",
        sql,
        flags=re.IGNORECASE | re.DOTALL,
    )
    if addr_match:
        states_blob = addr_match.group(1)
        raw_states = re.findall(r"'([^']+)'", states_blob)
        state = raw_states[0] if raw_states else "TX"
        np_low = float(addr_match.group(2))
        np_high = float(addr_match.group(3))
    else:
        state = str(_mv_first_filter_value(qctx, global_tables, "customer_address", "ca_state", "TX"))
        np_low, np_high = 100.0, 200.0

    ws_price = (ws_low + ws_high) / 2.0
    ws_profit = (np_low + np_high) / 2.0
    inserted = 0
    inserted += _mv_insert_rows(conn, global_tables, "date_dim", [{"d_date_sk": 99401, "d_year": year}])
    inserted += _mv_insert_rows(conn, global_tables, "web_page", [{"wp_web_page_sk": 99402}])
    inserted += _mv_insert_rows(conn, global_tables, "reason", [{"r_reason_sk": 99403, "r_reason_desc": "MV085"}])
    inserted += _mv_insert_rows(
        conn,
        global_tables,
        "customer_demographics",
        [
            {"cd_demo_sk": 99404, "cd_marital_status": marital, "cd_education_status": education},
            {"cd_demo_sk": 99405, "cd_marital_status": marital, "cd_education_status": education},
        ],
    )
    inserted += _mv_insert_rows(conn, global_tables, "customer_address", [{"ca_address_sk": 99406, "ca_country": "United States", "ca_state": state}])
    inserted += _mv_insert_rows(
        conn,
        global_tables,
        "web_sales",
        [{"ws_web_page_sk": 99402, "ws_item_sk": 99407, "ws_order_number": 99408, "ws_sold_date_sk": 99401, "ws_sales_price": ws_price, "ws_net_profit": ws_profit, "ws_quantity": 1}],
    )
    inserted += _mv_insert_rows(
        conn,
        global_tables,
        "web_returns",
        [{
            "wr_item_sk": 99407,
            "wr_order_number": 99408,
            "wr_refunded_cdemo_sk": 99404,
            "wr_returning_cdemo_sk": 99405,
            "wr_refunded_addr_sk": 99406,
            "wr_reason_sk": 99403,
            "wr_refunded_cash": 10.0,
            "wr_fee": 1.0,
        }],
    )
    return inserted > 0


def _mv_recipe_query091(
    conn: duckdb.DuckDBPyConnection,
    qctx: QueryContext,
    global_tables: Dict[str, Dict[str, Any]],
) -> bool:
    inserted = 0
    inserted += _mv_insert_rows(conn, global_tables, "call_center", [{"cc_call_center_sk": 99501, "cc_call_center_id": "MV091", "cc_name": "MV091", "cc_manager": "MV"}])
    inserted += _mv_insert_rows(conn, global_tables, "date_dim", [{"d_date_sk": 99502, "d_year": 1998, "d_moy": 2}])
    inserted += _mv_insert_rows(conn, global_tables, "customer_demographics", [{"cd_demo_sk": 99503, "cd_marital_status": "M", "cd_education_status": "Unknown"}])
    inserted += _mv_insert_rows(conn, global_tables, "household_demographics", [{"hd_demo_sk": 99504, "hd_buy_potential": "1001-5000+"}])
    inserted += _mv_insert_rows(conn, global_tables, "customer_address", [{"ca_address_sk": 99505, "ca_gmt_offset": -7}])
    inserted += _mv_insert_rows(
        conn,
        global_tables,
        "customer",
        [{"c_customer_sk": 99506, "c_current_cdemo_sk": 99503, "c_current_hdemo_sk": 99504, "c_current_addr_sk": 99505}],
    )
    inserted += _mv_insert_rows(
        conn,
        global_tables,
        "catalog_returns",
        [{"cr_call_center_sk": 99501, "cr_returned_date_sk": 99502, "cr_returning_customer_sk": 99506, "cr_net_loss": 100.0}],
    )
    return inserted > 0


def _mv_recipe_query094(
    conn: duckdb.DuckDBPyConnection,
    qctx: QueryContext,
    global_tables: Dict[str, Dict[str, Any]],
) -> bool:
    sql = qctx.get("sql_duckdb", "")
    m = re.search(r"d_date\s+between\s+'([^']+)'", sql, flags=re.IGNORECASE)
    dtxt = m.group(1) if m else "2002-9-01"
    state = str(_mv_first_filter_value(qctx, global_tables, "customer_address", "ca_state", "TX"))
    lp_lo, lp_hi = _mv_filter_bounds(qctx, "web_sales", "ws_list_price", 200, 260)
    list_price = (lp_lo + lp_hi) / 2.0
    inserted = 0
    inserted += _mv_insert_rows(conn, global_tables, "date_dim", [{"d_date_sk": 99601, "d_date": dtxt}])
    inserted += _mv_insert_rows(conn, global_tables, "customer_address", [{"ca_address_sk": 99602, "ca_state": state}])
    inserted += _mv_insert_rows(conn, global_tables, "web_site", [{"web_site_sk": 99603, "web_gmt_offset": -5}])
    inserted += _mv_insert_rows(
        conn,
        global_tables,
        "web_sales",
        [
            {
                "ws_order_number": 99604,
                "ws_ship_date_sk": 99601,
                "ws_ship_addr_sk": 99602,
                "ws_web_site_sk": 99603,
                "ws_warehouse_sk": 1,
                "ws_list_price": list_price,
                "ws_ext_ship_cost": 10.0,
                "ws_net_profit": 5.0,
            },
            {
                "ws_order_number": 99604,
                "ws_ship_date_sk": 99601,
                "ws_ship_addr_sk": 99602,
                "ws_web_site_sk": 99603,
                "ws_warehouse_sk": 2,
                "ws_list_price": list_price,
                "ws_ext_ship_cost": 10.0,
                "ws_net_profit": 5.0,
            },
        ],
    )
    return inserted > 0


def _mv_recipe_query102(
    conn: duckdb.DuckDBPyConnection,
    qctx: QueryContext,
    global_tables: Dict[str, Dict[str, Any]],
) -> bool:
    sql = str(qctx.get("sql_duckdb", ""))
    year = int(_mv_first_filter_value(qctx, global_tables, "date_dim", "d_year", 2001))
    category = str(_mv_first_filter_value(qctx, global_tables, "item", "i_category", "Children"))
    state = str(_mv_first_filter_value(qctx, global_tables, "customer_address", "ca_state", "TX"))

    manager_ids = [int(x) for x in re.findall(r"i_manager_id\s+in\s*\(([^)]+)\)", sql, flags=re.IGNORECASE)[:1] for x in re.findall(r"\d+", x)]
    manager_id = manager_ids[0] if manager_ids else 21

    m_ws = re.search(
        r"ws_wholesale_cost\s+between\s+([0-9]+(?:\.[0-9]+)?)\s+and\s+([0-9]+(?:\.[0-9]+)?)",
        sql,
        flags=re.IGNORECASE,
    )
    if m_ws:
        ws_low = float(m_ws.group(1))
        ws_high = float(m_ws.group(2))
    else:
        ws_low, ws_high = 35.0, 55.0
    ws_wholesale = (ws_low + ws_high) / 2.0

    base_date = f"{year}-01-01"
    within_30d = f"{year}-01-15"

    inserted = 0
    inserted += _mv_insert_rows(
        conn,
        global_tables,
        "date_dim",
        [
            {"d_date_sk": 99701, "d_year": year, "d_date": base_date},
            {"d_date_sk": 99702, "d_year": year, "d_date": within_30d},
        ],
    )
    inserted += _mv_insert_rows(conn, global_tables, "store", [{"s_store_sk": 99702, "s_state": state}])
    inserted += _mv_insert_rows(conn, global_tables, "warehouse", [{"w_warehouse_sk": 99703, "w_state": state}])
    inserted += _mv_insert_rows(conn, global_tables, "item", [{"i_item_sk": 99704, "i_category": category, "i_manager_id": manager_id}])
    inserted += _mv_insert_rows(conn, global_tables, "customer_demographics", [{"cd_demo_sk": 99705, "cd_gender": "M", "cd_marital_status": "S", "cd_education_status": "College"}])
    inserted += _mv_insert_rows(conn, global_tables, "household_demographics", [{"hd_demo_sk": 99706, "hd_vehicle_count": 1}])
    inserted += _mv_insert_rows(conn, global_tables, "customer_address", [{"ca_address_sk": 99707, "ca_state": state}])
    inserted += _mv_insert_rows(
        conn,
        global_tables,
        "customer",
        [{
            "c_customer_sk": 99708,
            "c_current_cdemo_sk": 99705,
            "c_current_hdemo_sk": 99706,
            "c_current_addr_sk": 99707,
        }],
    )
    inserted += _mv_insert_rows(
        conn,
        global_tables,
        "store_sales",
        [{
            "ss_item_sk": 99704,
            "ss_sold_date_sk": 99701,
            "ss_customer_sk": 99708,
            "ss_quantity": 1,
        }],
    )
    inserted += _mv_insert_rows(
        conn,
        global_tables,
        "web_sales",
        [{
            "ws_item_sk": 99704,
            "ws_sold_date_sk": 99702,
            "ws_bill_customer_sk": 99708,
            "ws_warehouse_sk": 99703,
            "ws_wholesale_cost": ws_wholesale,
        }],
    )
    inserted += _mv_insert_rows(
        conn,
        global_tables,
        "inventory",
        [{"inv_warehouse_sk": 99703, "inv_item_sk": 99704, "inv_date_sk": 99701, "inv_quantity_on_hand": 2}],
    )
    return inserted > 0


def _apply_mvrows_recipe(
    conn: duckdb.DuckDBPyConnection,
    qctx: QueryContext,
    global_tables: Dict[str, Dict[str, Any]],
) -> bool:
    qname = str(qctx.get("name", "")).lower().replace(".sql", "")
    if qname.startswith("query001_") or qname.startswith("query030_"):
        return _mv_recipe_query001_or_030(conn, qctx, global_tables)
    if qname.startswith("query010_"):
        return _mv_recipe_query010(conn, qctx, global_tables)
    if qname.startswith("query013_"):
        return _mv_recipe_query013(conn, qctx, global_tables)
    if qname.startswith("query031_"):
        return _mv_recipe_query031(conn, qctx, global_tables)
    if qname.startswith("query039_"):
        return _mv_recipe_query039(conn, qctx, global_tables)
    if qname.startswith("query054_"):
        return _mv_recipe_query054(conn, qctx, global_tables)
    if qname.startswith("query059_"):
        return _mv_recipe_query059(conn, qctx, global_tables)
    if qname.startswith("query064_"):
        return _mv_recipe_query064(conn, qctx, global_tables)
    if qname.startswith("query065_"):
        return _mv_recipe_query065(conn, qctx, global_tables)
    if qname.startswith("query075_"):
        return _mv_recipe_query075(conn, qctx, global_tables)
    if qname.startswith("query080_"):
        return _mv_recipe_query080(conn, qctx, global_tables)
    if qname.startswith("query083_"):
        return _mv_recipe_query083(conn, qctx, global_tables)
    if qname.startswith("query085_"):
        return _mv_recipe_query085(conn, qctx, global_tables)
    if qname.startswith("query091_"):
        return _mv_recipe_query091(conn, qctx, global_tables)
    if qname.startswith("query094_"):
        return _mv_recipe_query094(conn, qctx, global_tables)
    if qname.startswith("query102_"):
        return _mv_recipe_query102(conn, qctx, global_tables)
    return False


def _is_obviously_unsat(
    qctx: QueryContext,
    global_tables: Dict[str, Dict[str, Any]],
) -> bool:
    """Detect simple contradictory predicates (deterministic UNSAT)."""
    sql = str(qctx.get("sql_duckdb", "")).strip()
    if not sql:
        return False
    try:
        ast = sqlglot.parse_one(sql, read="duckdb")
    except Exception:
        return False

    tables = qctx.get("tables") or global_tables
    alias_map: Dict[str, str] = {}
    for table_expr in ast.find_all(sqlglot.exp.Table):
        name = (table_expr.name or "").strip()
        alias = (table_expr.alias_or_name or "").strip()
        if name:
            alias_map[name] = name
        if alias and name:
            alias_map[alias] = name

    filter_values: Dict[str, Dict[str, List[Any]]] = qctx.get("filter_values", {}) or {}

    for neq in ast.find_all(sqlglot.exp.NEQ):
        left = neq.args.get("this")
        right = neq.args.get("expression")
        if not isinstance(left, sqlglot.exp.Column) or not isinstance(right, sqlglot.exp.Column):
            continue
        ltab, lname = _normalize_ast_column_ref(left, alias_map, tables)
        rtab, rname = _normalize_ast_column_ref(right, alias_map, tables)
        if ltab not in tables or rtab not in tables:
            continue
        lcol = _resolve_col_case(tables, ltab, lname or "")
        rcol = _resolve_col_case(tables, rtab, rname or "")
        if not lcol or not rcol:
            continue
        if ltab != rtab or lcol != rcol:
            continue

        vals = (filter_values.get(ltab, {}) or {}).get(lcol, [])
        concrete: Set[str] = set()
        for raw in vals:
            if isinstance(raw, str) and raw.startswith("BETWEEN:"):
                continue
            concrete.add(str(raw).strip())
        if len(concrete) == 1:
            return True

    # Variance-to-mean constraints can be impossible under tight positive bounds.
    # Pattern: stddev_samp(inv_quantity_on_hand)/avg(inv_quantity_on_hand) > 1
    # with inv_quantity_on_hand BETWEEN low AND high and low > 0, high-low <= low.
    lower_sql = sql.lower()
    cov_pred = re.search(r"stdev\s*/\s*mean\s*end\s*>\s*1", lower_sql) is not None
    if "stddev_samp(inv_quantity_on_hand)" in lower_sql and cov_pred:
        m = re.search(
            r"inv_quantity_on_hand\s+between\s+([0-9]+(?:\.[0-9]+)?)\s+and\s+([0-9]+(?:\.[0-9]+)?)",
            sql,
            flags=re.IGNORECASE,
        )
        if m:
            low = float(m.group(1))
            high = float(m.group(2))
            if low > 0 and (high - low) <= low:
                return True

    return False


def _count_query_rows(
    conn: duckdb.DuckDBPyConnection,
    sql: str,
    timeout_s: int,
    probe_limit: int = 11,
) -> int:
    probe_limit = max(0, int(probe_limit))

    def _count_sql() -> int:
        # Avoid scalar-subquery strict failures during synthetic readiness probing.
        conn.execute("SET scalar_subquery_error_on_multiple_rows=false")
        if probe_limit > 0:
            row = conn.execute(
                f"SELECT COUNT(*) AS n FROM (SELECT 1 FROM ({sql}) AS _qt_q LIMIT {probe_limit}) AS _qt_probe"
            ).fetchone()
        else:
            row = conn.execute(f"SELECT COUNT(*) AS n FROM ({sql}) AS _qt_q").fetchone()
        return int(row[0]) if row else 0

    if timeout_s <= 0:
        return _count_sql()

    outcome: Dict[str, Any] = {}

    def _runner() -> None:
        try:
            outcome["rows"] = _count_sql()
        except Exception as exc:  # pragma: no cover - exercised in runtime
            outcome["error"] = exc

    thread = threading.Thread(target=_runner, daemon=True)
    thread.start()
    thread.join(timeout_s)
    if thread.is_alive():
        try:
            conn.interrupt()
        except Exception:
            pass
        thread.join(5)
        raise TimeoutError(f"query count timed out after {timeout_s}s")

    if "error" in outcome:
        raise outcome["error"]
    return int(outcome.get("rows", 0))


def main() -> int:
    patch_pack_choices = ["none", *sorted(available_patch_packs().keys())]
    parser = argparse.ArgumentParser(
        description="Build persistent synthetic DuckDB DB for postgres_dsb_76 and verify 76 queries."
    )
    parser.add_argument(
        "--queries-dir",
        default="packages/qt-sql/qt_sql/benchmarks/postgres_dsb_76/queries",
        help="Directory containing 76 query SQL files.",
    )
    parser.add_argument(
        "--reference-db",
        default="",
        help=(
            "Optional reference DB DSN for authoritative schema extraction "
            "(typically PostgreSQL). If omitted, runs without external DB dependency."
        ),
    )
    parser.add_argument(
        "--out-db",
        required=True,
        help="Output DuckDB file path (recommended on /mnt/d for persistence).",
    )
    parser.add_argument(
        "--report",
        default="",
        help="Optional JSON report path. Defaults to <out-db>.report.json",
    )
    parser.add_argument(
        "--dialect",
        default="postgres",
        help="Source query dialect (default: postgres).",
    )
    parser.add_argument(
        "--dim-rows",
        type=int,
        default=6000,
        help="Base synthetic rows per dimension table.",
    )
    parser.add_argument(
        "--fact-rows",
        type=int,
        default=24000,
        help="Base synthetic rows per fact table.",
    )
    parser.add_argument(
        "--topup-dim-rows",
        type=int,
        default=1800,
        help="Top-up rows per dimension table for zero-row queries.",
    )
    parser.add_argument(
        "--topup-fact-rows",
        type=int,
        default=7200,
        help="Top-up rows per fact table for zero-row queries.",
    )
    parser.add_argument(
        "--topup-retries",
        type=int,
        default=2,
        help="Retries for zero-row queries after targeted top-up.",
    )
    parser.add_argument(
        "--query-timeout-s",
        type=int,
        default=45,
        help="Timeout for each verification count query (seconds).",
    )
    parser.add_argument(
        "--force-seed-on-zero",
        action="store_true",
        default=True,
        help="If still zero after retries, insert one deterministic query-specific seed row-pack.",
    )
    parser.add_argument(
        "--force-seed-attempts",
        type=int,
        default=8,
        help="Number of deterministic seed attempts for unresolved queries.",
    )
    parser.add_argument(
        "--force-seed-rows",
        type=int,
        default=1,
        help="Rows inserted per force-seed attempt per table.",
    )
    parser.add_argument(
        "--min-query-rows",
        type=int,
        default=1,
        help="Hard minimum rows required for success.",
    )
    parser.add_argument(
        "--preferred-query-rows",
        type=int,
        default=1,
        help="Preferred minimum rows for validator signal.",
    )
    parser.add_argument(
        "--edge-template",
        default="packages/qt-sql/qt_sql/validation/templates/dsb_edge_cases.json",
        help="JSON template with edge-case rows/values.",
    )
    parser.add_argument(
        "--edge-rows-per-table",
        type=int,
        default=4,
        help="Generic edge rows inserted per table in addition to explicit template rows.",
    )
    parser.add_argument(
        "--patch-pack",
        choices=patch_pack_choices,
        default="none",
        help="Optional benchmark patch pack for witness fallback (default: none).",
    )
    parser.add_argument(
        "--random-base",
        action="store_true",
        default=False,
        help="Populate random base data for all tables before per-query seeding (default: off).",
    )
    parser.add_argument(
        "--random-fallback",
        action="store_true",
        default=False,
        help="Enable random top-up as last resort after force-seed fails (default: off).",
    )
    parser.add_argument(
        "--verbose",
        action="store_true",
        help="Enable debug logs.",
    )

    args = parser.parse_args()

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
    )

    queries_dir = Path(args.queries_dir)
    out_db = Path(args.out_db)
    report_path = Path(args.report) if args.report else out_db.with_suffix(out_db.suffix + ".report.json")
    edge_template_path = Path(args.edge_template)

    sql_files = sorted(queries_dir.glob("*.sql"))
    if not sql_files:
        raise FileNotFoundError(f"No SQL files found in {queries_dir}")

    logger.info("Discovered %d SQL files in %s", len(sql_files), queries_dir)

    reference_db = (args.reference_db or "").strip()
    validator = SyntheticValidator(reference_db=None, dialect=args.dialect)
    validator.reference_db = reference_db
    if reference_db and SchemaFromDB.supports_dsn(reference_db):
        validator.schema_extractor = SchemaFromDB(reference_db)
        logger.info("Using reference DB schema extraction: %s", reference_db)
    elif reference_db:
        logger.warning(
            "Reference DB DSN is unsupported for schema extraction, falling back to AST-only mode: %s",
            reference_db,
        )
    else:
        logger.info("No reference DB provided; using AST-only synthetic schema inference.")

    patch_pack = load_witness_patch_pack(args.patch_pack)
    if patch_pack is None:
        logger.info("Patch pack: none (core AST/general flow only)")
    else:
        logger.info("Patch pack: %s (%s)", patch_pack.name, patch_pack.description)

    # 1) Build per-query contexts and merge global schema/constraints.
    query_contexts: List[QueryContext] = []
    global_tables: Dict[str, Dict[str, Any]] = {}
    global_fk: Dict[str, Dict[str, Tuple[str, str]]] = {}
    global_filters: Dict[str, Dict[str, List[Any]]] = {}

    for sql_file in sql_files:
        qctx = _build_query_context(validator, sql_file, args.dialect)
        query_contexts.append(qctx)
        _merge_table_schemas(global_tables, qctx["tables"])
        _merge_fk(global_fk, qctx["fk_relationships"])
        _merge_filters(global_filters, qctx["filter_values"])

    logger.info(
        "Merged global synthetic model: %d tables, %d FK-owner tables",
        len(global_tables),
        len(global_fk),
    )

    # 2) Create persistent DB and populate base synthetic data.
    out_db.parent.mkdir(parents=True, exist_ok=True)
    if out_db.exists():
        out_db.unlink()
    conn = duckdb.connect(str(out_db))
    validator.conn = conn

    validator._create_schema(global_tables)
    for qctx in query_contexts:
        validator._create_indexes(global_tables, qctx["sql_duckdb"])

    generation_order = validator._get_table_generation_order(global_tables, global_fk)

    # 2) Optional random base data (only with --random-base).
    if args.random_base:
        generator = SyntheticDataGenerator(conn, all_schemas=global_tables)
        generator.filter_literal_values = global_filters

        for table_name in generation_order:
            table_fk = global_fk.get(table_name, {})
            row_count = args.fact_rows if table_fk else args.dim_rows
            generator.generate_table_data(
                table_name=table_name,
                schema=global_tables[table_name],
                row_count=row_count,
                foreign_keys=table_fk,
            )
            validator._update_filter_matched_pks(generator, global_tables, [table_name], global_filters)
            validator._reverse_propagate_parent_key_matches(
                generator,
                table_name,
                global_tables,
                global_fk,
                global_filters,
            )
        logger.info("Random base data populated for %d tables", len(generation_order))
    else:
        logger.info("Skipping random base data (use --random-base to enable)")

    # 3) Edge-case insertion from template.
    edge_template = _load_edge_template(edge_template_path)
    inserted_edge_rows = _apply_edge_cases(
        conn=conn,
        tables=global_tables,
        fk_relationships=global_fk,
        template=edge_template,
        edge_rows_per_table=args.edge_rows_per_table,
    )
    logger.info("Inserted %d template edge rows", inserted_edge_rows)

    # 4) Per-query witness seeding: force-seed first, random as optional fallback.
    results: List[Dict[str, Any]] = []
    min_rows_required = max(1, int(args.min_query_rows))
    preferred_rows = max(min_rows_required, int(args.preferred_query_rows))
    probe_limit = preferred_rows if preferred_rows > 0 else 11

    for qctx in query_contexts:
        name = qctx["name"]
        sql_duckdb = qctx["sql_duckdb"]

        query_result: Dict[str, Any] = {
            "query": name,
            "success": False,
            "rows": 0,
            "min_rows_required": min_rows_required,
            "preferred_rows": preferred_rows,
            "preferred_rows_met": False,
            "unsat_expected": False,
            "topup_attempts": 0,
            "forced_seed": False,
            "forced_seed_attempts": 0,
            "error": None,
        }

        # Step A: Force-seed (primary — deterministic AST-driven witness insertion).
        if qctx.get("tables"):
            anti_tables = _tables_in_anti_patterns(sql_duckdb)
            max_seed_attempts = max(1, int(args.force_seed_attempts))
            for seed_attempt in range(1, max_seed_attempts + 1):
                try:
                    _force_seed_for_query(
                        conn=conn,
                        qctx=qctx,
                        global_tables=global_tables,
                        fk_relationships=global_fk,
                        seed_variant=seed_attempt * max(1, int(args.force_seed_rows)),
                        seed_rows=max(1, int(args.force_seed_rows)),
                        skip_tables=anti_tables,
                    )
                    rows = _count_query_rows(conn, sql_duckdb, args.query_timeout_s, probe_limit=probe_limit)
                    query_result["rows"] = rows
                    query_result["success"] = rows >= min_rows_required
                    query_result["forced_seed"] = True
                    query_result["forced_seed_attempts"] = seed_attempt
                    if query_result["success"]:
                        query_result["error"] = None
                        if rows >= preferred_rows:
                            break
                    else:
                        query_result["error"] = None
                except Exception as exc:
                    query_result["forced_seed_attempts"] = seed_attempt
                    query_result["error"] = str(exc)

        # Step B: Patch-pack recipe.
        if not query_result["success"] and patch_pack is not None:
            try:
                recipe_applied = bool(patch_pack.apply_recipe(conn, qctx, global_tables))
                if recipe_applied:
                    rows = _count_query_rows(conn, sql_duckdb, args.query_timeout_s, probe_limit=probe_limit)
                    query_result["rows"] = rows
                    query_result["success"] = rows >= min_rows_required
                    if query_result["success"]:
                        query_result["error"] = None
            except Exception as exc:
                query_result["error"] = str(exc)

        # Step C: Optional random top-up (only with --random-fallback).
        if not query_result["success"] and args.random_fallback:
            for attempt in range(1, args.topup_retries + 1):
                try:
                    _top_up_for_query(
                        conn=conn,
                        validator=validator,
                        qctx=qctx,
                        global_tables=global_tables,
                        fact_rows=args.topup_fact_rows,
                        dim_rows=args.topup_dim_rows,
                    )
                    rows = _count_query_rows(conn, sql_duckdb, args.query_timeout_s, probe_limit=probe_limit)
                    query_result["topup_attempts"] = attempt
                    query_result["rows"] = rows
                    if rows >= min_rows_required:
                        query_result["success"] = True
                        query_result["error"] = None
                        break
                except Exception as exc:
                    query_result["topup_attempts"] = attempt
                    query_result["error"] = str(exc)

        # Step D: Unsat declaration.
        if not query_result["success"] and _is_obviously_unsat(qctx, global_tables):
            query_result["unsat_expected"] = True
            query_result["success"] = True
            query_result["error"] = None

        query_result["preferred_rows_met"] = query_result["rows"] >= preferred_rows

        results.append(query_result)
        logger.info(
            "[%s] rows=%s success=%s forced_seed=%s topup=%d",
            name,
            query_result["rows"],
            query_result["success"],
            query_result["forced_seed"],
            query_result["topup_attempts"],
        )

    total = len(results)
    ok = sum(1 for r in results if r["success"])
    zero_or_fail = total - ok
    preferred_ok = sum(1 for r in results if r.get("preferred_rows_met"))
    unsat_expected = sum(1 for r in results if r.get("unsat_expected"))

    summary = {
        "total_queries": total,
        "queries_with_rows": ok,
        "queries_without_rows_or_failed": zero_or_fail,
        "queries_with_preferred_rows": preferred_ok,
        "queries_unsat_expected": unsat_expected,
        "preferred_rows_threshold": preferred_rows,
        "synthetic_tables": len(global_tables),
        "edge_rows_inserted": inserted_edge_rows,
        "out_db": str(out_db),
        "reference_db": reference_db,
        "edge_template": str(edge_template_path),
        "patch_pack": patch_pack.name if patch_pack else "none",
        "random_base": args.random_base,
        "random_fallback": args.random_fallback,
    }

    payload = {
        "summary": summary,
        "results": results,
    }
    report_path.parent.mkdir(parents=True, exist_ok=True)
    report_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")

    conn.close()

    logger.info(
        "Done. queries_with_rows=%d/%d, report=%s, db=%s",
        ok,
        total,
        report_path,
        out_db,
    )

    return 0 if ok == total else 2


if __name__ == "__main__":
    raise SystemExit(main())
