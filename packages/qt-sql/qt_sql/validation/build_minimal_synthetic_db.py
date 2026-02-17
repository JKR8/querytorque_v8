"""Build a minimum synthetic DuckDB database for a given SQL query.

The goal is to produce a "Minimum Viable Manifest" of rows that satisfy
all predicates and join conditions, including complex self-joins and
aggregate constraints.
"""

from __future__ import annotations

import argparse
import json
import logging
import re
import hashlib
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple, Union

import duckdb
import sqlglot
from sqlglot import exp
from sqlglot.optimizer import optimize
from sqlglot.optimizer.qualify_columns import qualify_columns

from .synthetic_validator import (
    SchemaExtractor,
    SyntheticValidator,
    find_primary_key_column,
)
from .build_dsb76_synthetic_db import (
    _to_duckdb_sql,
    _canonical_edge_type,
    _from_filter_literal,
    _anchor_value_for_type,
    _coerce_edge_value,
    _fit_numeric_to_column,
    _is_key_like,
    _detect_temporal_anchor,
    _insert_rows,
)

logger = logging.getLogger(__name__)


class AliasInstance:
    """Represents a specific alias reference in the query (base table or subquery)."""
    def __init__(self, alias: str, table_name: str):
        self.alias = alias
        self.table_name = table_name
        self.constraints: Dict[str, List[Any]] = {} 

    def __repr__(self):
        return f"AliasInstance({self.alias} -> {self.table_name})"


def _get_alias_map(ast: exp.Expression) -> Dict[str, str]:
    """Map all aliases to their underlying table or source type."""
    alias_map = {}
    for table in ast.find_all(exp.Table):
        name = table.name
        alias = table.alias or name
        alias_map[alias] = name
    for subquery in ast.find_all(exp.Subquery):
        if subquery.alias:
            alias_map[subquery.alias] = "SUBQUERY"
    return alias_map


def _evaluate_constant(node: exp.Expression) -> Any:
    """Evaluate simple constant expressions in the AST."""
    if isinstance(node, exp.Literal):
        if node.is_number:
            return float(node.this) if "." in node.this else int(node.this)
        return node.this
    if isinstance(node, exp.Binary):
        l = _evaluate_constant(node.left)
        r = _evaluate_constant(node.right)
        if l is not None and r is not None:
            try:
                if isinstance(node, exp.Add): return l + r
                if isinstance(node, exp.Sub): return l - r
                if isinstance(node, exp.Mul): return l * r
                if isinstance(node, exp.Div): return l / r
            except Exception:
                return None
    return None


def _inline_ctes(ast: exp.Expression, schema: Dict[str, Any]) -> exp.Expression:
    """Recursively inline CTEs and make aliases unique to allow constraint propagation."""
    new_ast = ast.copy()
    counter = [0]
    
    while True:
        with_ = new_ast.args.get("with")
        if not with_:
            break

        ctes = {cte.alias: cte.this for cte in with_.expressions}
        referenced_ctes = [t.name for t in new_ast.find_all(exp.Table) if t.name in ctes]
        if not referenced_ctes:
            break

        def _expand(node):
            if isinstance(node, exp.Table) and node.name in ctes:
                counter[0] += 1
                scope_id = counter[0]
                alias = node.alias or node.name
                subquery = ctes[node.name].copy()
                
                try:
                    subquery = qualify_columns(subquery, schema=schema)
                except Exception:
                    pass
                
                def _rename(n):
                    if isinstance(n, exp.Table):
                        old_alias = n.alias or n.name
                        new_alias = f"{old_alias}_{alias}_{scope_id}"
                        return n.as_(new_alias)
                    if isinstance(n, exp.Column) and n.table:
                        n.set("table", exp.Identifier(this=f"{n.table}_{alias}_{scope_id}", quoted=n.args["table"].quoted))
                    return n
                
                subquery = subquery.transform(_rename)
                return subquery.as_(alias)
            return node

        new_ast = new_ast.transform(_expand)
        
        new_with = [cte for cte in with_.expressions if cte.alias in [t.name for t in new_ast.find_all(exp.Table) if t.name in ctes]]
        if not new_with:
            new_ast.set("with", None)
            break
        else:
            with_.set("expressions", new_with)

    return new_ast


def _extract_alias_joins(ast: exp.Expression, alias_instances: Dict[str, AliasInstance]) -> List[Tuple[Tuple[str, str], Tuple[str, str]]]:
    """Extract equality joins between specific aliases."""
    joins = []
    
    for eq in ast.find_all(exp.EQ):
        left = eq.left
        right = eq.right
        if isinstance(left, exp.Column) and isinstance(right, exp.Column):
            l_alias = left.table
            r_alias = right.table
            if l_alias and r_alias and l_alias in alias_instances and r_alias in alias_instances:
                if l_alias != r_alias:
                    joins.append(((l_alias, left.name), (r_alias, right.name)))
                
    return joins


def _extract_literals(ast: exp.Expression, alias_instances: Dict[str, AliasInstance], tables: Dict[str, Any]) -> Tuple[Dict[Tuple[str, str], Any], Dict[Tuple[str, str], List[Any]]]:
    """Extract literal filters and allowed value lists for specific alias columns."""
    literals = {}
    allowed_values = {}
    
    def _resolve_alias(col_name):
        candidates = []
        for alias, inst in alias_instances.items():
            if inst.table_name in tables and col_name in tables[inst.table_name]['columns']:
                candidates.append(alias)
        return candidates[0] if len(candidates) == 1 else None

    for cmp in ast.find_all(exp.EQ, exp.Between, exp.In, exp.NEQ):
        col = cmp.find(exp.Column)
        if not col: continue
        
        alias = col.table or _resolve_alias(col.name)
        if not alias: continue
        
        col_key = (alias, col.name.lower())
        
        if isinstance(cmp, (exp.EQ, exp.NEQ)):
            other = cmp.right if col == cmp.left else cmp.left
            val = _evaluate_constant(other)
            if val is not None:
                if not isinstance(cmp, exp.NEQ):
                    literals[col_key] = val
        elif isinstance(cmp, exp.Between):
            val = _evaluate_constant(cmp.args.get("low"))
            if val is not None:
                literals[col_key] = val
        elif isinstance(cmp, exp.In):
            lits = cmp.args.get("expressions")
            if lits:
                vals = [v for v in [_evaluate_constant(l) for l in lits] if v is not None]
                if vals:
                    allowed_values[col_key] = vals
                    literals[col_key] = vals[0]
    return literals, allowed_values


def _extract_alias_inequalities(ast: exp.Expression, alias_instances: Dict[str, AliasInstance]) -> List[Tuple[Tuple[str, str], Tuple[str, str]]]:
    """Extract inequality constraints between alias columns."""
    neqs = []
    for binary in ast.find_all(exp.NEQ):
        left = binary.left
        right = binary.right
        if isinstance(left, exp.Column) and isinstance(right, exp.Column):
            l_alias = left.table
            r_alias = right.table
            if l_alias and r_alias and l_alias in alias_instances and r_alias in alias_instances:
                if l_alias != r_alias:
                    neqs.append(((l_alias, left.name), (r_alias, right.name)))
    return neqs


def build_minimal_db(sql: str, out_db_path: str, dialect: str = "postgres") -> Dict[str, Any]:
    """Build a minimal synthetic database for the given SQL using Manifest logic."""
    sql_duckdb = _to_duckdb_sql(sql, dialect)
    extractor = SchemaExtractor(sql_duckdb)
    tables = extractor.extract_tables()
    
    try:
        ast = sqlglot.parse_one(sql_duckdb, read="duckdb")
    except Exception as e:
        return {"success": False, "error": f"Parse error: {e}"}

    if Path(out_db_path).exists():
        Path(out_db_path).unlink()
    conn = duckdb.connect(out_db_path)
    
    validator = SyntheticValidator(reference_db=None, dialect="duckdb")
    validator.conn = conn
    validator._create_schema(tables)
    validator._create_indexes(tables, sql_duckdb)

    sg_schema = {tname: {cname: cinfo['type'] for cname, cinfo in tinfo['columns'].items()} for tname, tinfo in tables.items()}

    expanded_ast = _inline_ctes(ast, sg_schema)
    logger.debug(f"Expanded AST: {expanded_ast.sql()}")

    try:
        optimized_ast = optimize(expanded_ast, schema=sg_schema)
        logger.debug(f"Optimized AST: {optimized_ast.sql()}")
    except Exception as e:
        logger.debug(f"Optimization failed: {e}. Using expanded AST.")
        optimized_ast = expanded_ast

    alias_to_table = _get_alias_map(optimized_ast)
    alias_instances: Dict[str, AliasInstance] = {}
    for alias, table in alias_to_table.items():
        base_table = table
        best_match = ""
        for tname in tables:
            if table.startswith(tname) and len(tname) > len(best_match):
                if len(table) == len(tname) or table[len(tname)] == '_':
                    best_match = tname
        if best_match:
            base_table = best_match
        alias_instances[alias] = AliasInstance(alias, base_table)
    logger.debug(f"Alias instances: {alias_instances}")

    parent = {}
    def find(i):
        i = (i[0], i[1].lower())
        if parent.get(i) == i or i not in parent:
            parent[i] = i
            return i
        parent[i] = find(parent[i])
        return parent[i]

    def union(i, j):
        root_i = find(i)
        root_j = find(j)
        if root_i != root_j:
            parent[root_i] = root_j

    # Propagate through projections
    for subquery in optimized_ast.find_all(exp.Select):
        alias = ""
        if isinstance(subquery.parent, exp.Alias):
            alias = subquery.parent.alias
        elif isinstance(subquery.parent, (exp.Subquery, exp.Table)):
            alias = subquery.parent.alias
        if not alias: continue
        
        for projection in subquery.expressions:
            if isinstance(projection, exp.Alias) and isinstance(projection.this, exp.Column):
                union((alias, projection.alias), (projection.this.table, projection.this.name))
            elif isinstance(projection, exp.Column):
                union((alias, projection.name), (projection.table, projection.name))

    # Joins
    alias_joins = _extract_alias_joins(optimized_ast, alias_instances)
    for (l_alias, l_col), (r_alias, r_col) in alias_joins:
        union((l_alias, l_col), (r_alias, r_col))

    # Literals
    group_values = {}
    group_allowed = {}
    literals, allowed = _extract_literals(optimized_ast, alias_instances, tables)
    for (alias, col_name), val in literals.items():
        root = find((alias, col_name))
        group_values[root] = val
        if (alias, col_name) in allowed:
            group_allowed.setdefault(root, []).extend(allowed[(alias, col_name)])
        logger.debug(f"Literal filter: {alias}.{col_name} = {val} (group {root})")

    groups = {}
    for alias_col in list(parent.keys()):
        root = find(alias_col)
        groups.setdefault(root, set()).add(alias_col)

    solved_group_values = {}
    for root, members in groups.items():
        val = group_values.get(root)
        if val is None:
            alias, col = root
            if alias in alias_instances:
                table_name = alias_instances[alias].table_name
                if table_name in tables:
                    col_type = tables[table_name]['columns'].get(col, {}).get('type', 'VARCHAR')
                    key_like = _is_key_like(col)
                    group_id = int(hashlib.md5(f"{root}".encode()).hexdigest()[:8], 16) % 10000
                    val = _anchor_value_for_type(_canonical_edge_type(col_type), key_like, variant=group_id)
        solved_group_values[root] = val

    # Solve Inequalities
    neqs = _extract_alias_inequalities(optimized_ast, alias_instances)
    for (l_alias, l_col), (r_alias, r_col) in neqs:
        l_root = find((l_alias, l_col))
        r_root = find((r_alias, r_col))
        if l_root in solved_group_values and r_root in solved_group_values:
            if solved_group_values[l_root] == solved_group_values[r_root]:
                allowed_r = group_allowed.get(r_root, [])
                found_diff = False
                for alt in allowed_r:
                    if alt != solved_group_values[l_root]:
                        solved_group_values[r_root] = alt
                        found_diff = True
                        break
                if not found_diff:
                    val = solved_group_values[r_root]
                    if isinstance(val, (int, float)): solved_group_values[r_root] = val + 1
                    else: solved_group_values[r_root] = f"DIFF_{val}"
                logger.debug(f"Resolved NEQ: {l_alias}.{l_col} != {r_alias}.{r_col} -> {solved_group_values[l_root]} != {solved_group_values[r_root]}")

    # 6. Final Manifest
    table_rows: Dict[str, List[Dict[str, Any]]] = {}
    instance_rows: Dict[str, Dict[str, Any]] = {}
    for alias, instance in alias_instances.items():
        if instance.table_name in tables:
            instance_rows[alias] = {c: None for c in tables[instance.table_name]['columns']}

    for root, val in solved_group_values.items():
        if val is not None:
            for m_alias, m_col in groups.get(root, set()):
                if m_alias in instance_rows and m_col in instance_rows[m_alias]:
                    instance_rows[m_alias][m_col] = val

    for alias, row_data in instance_rows.items():
        table_name = alias_instances[alias].table_name
        for col, val in row_data.items():
            if val is None:
                col_type = tables[table_name]['columns'][col]['type']
                ctype = _canonical_edge_type(col_type)
                if any(k in col.lower() for k in ['price', 'total', 'sales', 'amt', 'list']):
                    row_data[col] = 1000.0 if ctype == 'DECIMAL' else 1000
                elif any(k in col.lower() for k in ['refund', 'discount', 'tax']):
                    row_data[col] = 1.0 if ctype == 'DECIMAL' else 1
                else:
                    row_data[col] = _anchor_value_for_type(ctype, _is_key_like(col), variant=0)

    for alias, row_data in instance_rows.items():
        table_name = alias_instances[alias].table_name
        logger.debug(f"Row for alias {alias} ({table_name}): {row_data}")
        if table_name in tables:
            table_rows.setdefault(table_name, []).append(row_data)

    for table_name, rows in table_rows.items():
        col_names = list(tables[table_name]['columns'].keys())
        rows_to_insert = [tuple(r[c] for c in col_names) for r in rows]
        _insert_rows(conn, table_name, col_names, rows_to_insert)

    try:
        row = conn.execute(f"SELECT COUNT(*) FROM ({sql_duckdb}) AS _qt_q").fetchone()
        actual_rows = int(row[0]) if row else 0
    except Exception as e:
        logger.error(f"Error verifying query: {e}")
        actual_rows = -1
        
    conn.close()
    return {"success": actual_rows > 0, "rows": actual_rows, "tables": list(tables.keys()), "db_path": out_db_path}


def main() -> int:
    parser = argparse.ArgumentParser(description="Build a minimal synthetic DuckDB for a query.")
    parser.add_argument("sql_file", help="Path to the SQL file.")
    parser.add_argument("--out-db", default="minimal_synth.duckdb", help="Output DuckDB path.")
    parser.add_argument("--dialect", default="postgres", help="Source SQL dialect.")
    parser.add_argument("--verbose", action="store_true", help="Enable verbose logging.")
    args = parser.parse_args()
    logging.basicConfig(level=logging.DEBUG if args.verbose else logging.INFO)
    sql = Path(args.sql_file).read_text()
    result = build_minimal_db(sql, args.out_db, args.dialect)
    print(json.dumps(result, indent=2))
    return 0 if result["success"] else 1


if __name__ == "__main__":
    exit(main())
