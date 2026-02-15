#!/usr/bin/env python3
"""Generate patch_plan fields for all gold examples.

For each gold example JSON that doesn't already have a patch_plan:
1. Parse original_sql + optimized_sql with sqlglot
2. Diff CTE lists, FROM, WHERE to determine patch operations
3. Write patch_plan back to the JSON file

Usage:
    PYTHONPATH=packages/qt-shared:packages/qt-sql:. python3 scripts/generate_gold_patch_plans.py
"""
from __future__ import annotations

import hashlib
import json
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import sqlglot
from sqlglot import exp


# ── canonical_hash (same as qt_sql.ir.schema.canonical_hash) ────────

def canonical_hash(sql: str) -> str:
    """Canonical hash of SQL text for anchor matching."""
    normalized = " ".join(sql.lower().split())
    return hashlib.sha256(normalized.encode()).hexdigest()[:16]


# ── CTE extraction ──────────────────────────────────────────────────

def extract_ctes(ast: Any, dialect: str) -> List[Tuple[str, str]]:
    """Extract CTEs as ordered list of (name, body_sql) tuples.

    Preserves ordering (important for insert_cte step generation).
    """
    result: List[Tuple[str, str]] = []

    # Find the WITH clause — could be on the Select node directly
    with_clause = ast.args.get("with") if isinstance(ast, exp.Select) else None
    if with_clause is None:
        with_clause = ast.find(exp.With)
    if with_clause is None:
        return result

    for cte_node in with_clause.expressions:
        if not isinstance(cte_node, exp.CTE):
            continue
        name = cte_node.alias
        body = cte_node.this
        try:
            body_sql = body.sql(dialect=dialect)
        except Exception:
            body_sql = str(body)
        result.append((name, body_sql))

    return result


def get_main_select(ast: Any) -> Optional[exp.Select]:
    """Get the main (outermost) SELECT node, skipping CTE bodies."""
    if isinstance(ast, exp.Select):
        return ast
    # For WITH nodes, the main query is .this
    w = ast.find(exp.With)
    if w and isinstance(w.this, exp.Select):
        return w.this
    return None


def extract_from_sql(ast: Any, dialect: str) -> Optional[str]:
    """Extract the main query's FROM clause as SQL text (without 'FROM' keyword)."""
    sel = get_main_select(ast)
    if sel is None:
        return None

    from_clause = sel.args.get("from")
    joins = sel.args.get("joins")

    if from_clause is None:
        return None

    try:
        from_text = from_clause.sql(dialect=dialect)
        # Remove "FROM " prefix
        if from_text.upper().startswith("FROM "):
            from_text = from_text[5:]

        # Append JOINs
        if joins:
            for j in joins:
                join_text = j.sql(dialect=dialect)
                from_text += " " + join_text

        return from_text
    except Exception:
        return None


def extract_where_sql(ast: Any, dialect: str) -> Optional[str]:
    """Extract the main query's WHERE predicate as SQL text (without 'WHERE' keyword)."""
    sel = get_main_select(ast)
    if sel is None:
        return None

    where_clause = sel.args.get("where")
    if where_clause is None:
        return None

    try:
        # where_clause.this is the predicate expression
        return where_clause.this.sql(dialect=dialect)
    except Exception:
        return None


# ── Patch plan generation ────────────────────────────────────────────

def generate_patch_plan(
    example_data: dict, dialect_str: str
) -> Optional[dict]:
    """Generate a patch_plan dict for a gold example."""
    original_sql = example_data.get("original_sql", "")
    optimized_sql = example_data.get("optimized_sql", "")
    example_id = example_data.get("id", "unknown")
    description = (
        example_data.get("description", "")
        or example_data.get("principle", "")
        or example_data.get("name", "")
    )

    if not original_sql or not optimized_sql:
        print(f"  SKIP {example_id}: missing original_sql or optimized_sql")
        return None

    # Determine sqlglot dialect
    sg_dialect = dialect_str

    # Parse both SQLs
    try:
        orig_ast = sqlglot.parse_one(original_sql, dialect=sg_dialect)
    except Exception as e:
        print(f"  ERROR {example_id}: cannot parse original_sql: {e}")
        return None

    try:
        opt_ast = sqlglot.parse_one(optimized_sql, dialect=sg_dialect)
    except Exception as e:
        print(f"  ERROR {example_id}: cannot parse optimized_sql: {e}")
        return None

    # Extract structures
    orig_ctes = extract_ctes(orig_ast, sg_dialect)
    opt_ctes = extract_ctes(opt_ast, sg_dialect)

    orig_cte_names = {name for name, _ in orig_ctes}
    opt_cte_dict = {name: body for name, body in opt_ctes}
    orig_cte_dict = {name: body for name, body in orig_ctes}

    orig_from = extract_from_sql(orig_ast, sg_dialect)
    opt_from = extract_from_sql(opt_ast, sg_dialect)

    orig_where = extract_where_sql(orig_ast, sg_dialect)
    opt_where = extract_where_sql(opt_ast, sg_dialect)

    # Compute WHERE anchor hash from original
    where_hash = None
    if orig_where:
        where_hash = canonical_hash(orig_where)

    # Generate steps
    steps: List[dict] = []
    step_num = 1

    # 1. New CTEs (in order they appear in optimized)
    for name, body_sql in opt_ctes:
        if name not in orig_cte_names:
            steps.append({
                "step_id": f"s{step_num}",
                "op": "insert_cte",
                "target": {"by_node_id": "S0"},
                "payload": {
                    "cte_name": name,
                    "cte_query_sql": body_sql,
                },
                "description": _cte_description(name, body_sql),
            })
            step_num += 1

    # 2. Modified CTEs (exist in both but body changed)
    for name, orig_body in orig_ctes:
        if name in opt_cte_dict:
            opt_body = opt_cte_dict[name]
            # Normalize for comparison
            orig_norm = " ".join(orig_body.lower().split())
            opt_norm = " ".join(opt_body.lower().split())
            if orig_norm != opt_norm:
                steps.append({
                    "step_id": f"s{step_num}",
                    "op": "replace_block_with_cte_pair",
                    "target": {"by_node_id": "S0", "by_label": name},
                    "payload": {
                        "sql_fragment": f"{name} AS ({opt_body})",
                    },
                    "description": f"Replace CTE '{name}' body with optimized version",
                })
                step_num += 1

    # 3. FROM clause changes
    if orig_from and opt_from:
        orig_from_norm = " ".join(orig_from.lower().split())
        opt_from_norm = " ".join(opt_from.lower().split())
        if orig_from_norm != opt_from_norm:
            steps.append({
                "step_id": f"s{step_num}",
                "op": "replace_from",
                "target": {"by_node_id": "S0"},
                "payload": {
                    "from_sql": opt_from,
                },
                "description": _from_description(orig_from, opt_from),
            })
            step_num += 1

    # 4. WHERE clause changes
    if orig_where and opt_where:
        orig_where_norm = " ".join(orig_where.lower().split())
        opt_where_norm = " ".join(opt_where.lower().split())
        if orig_where_norm != opt_where_norm:
            step: dict = {
                "step_id": f"s{step_num}",
                "op": "replace_where_predicate",
                "target": {"by_node_id": "S0"},
                "payload": {
                    "expr_sql": opt_where,
                },
                "description": "Replace WHERE predicate with optimized version",
            }
            if where_hash:
                step["target"]["by_anchor_hash"] = where_hash
            steps.append(step)
            step_num += 1
    elif orig_where and not opt_where:
        step = {
            "step_id": f"s{step_num}",
            "op": "delete_expr_subtree",
            "target": {"by_node_id": "S0"},
            "description": "Remove WHERE clause (conditions moved to CTEs)",
        }
        if where_hash:
            step["target"]["by_anchor_hash"] = where_hash
        steps.append(step)
        step_num += 1
    elif not orig_where and opt_where:
        # Original had no WHERE, optimized adds one (e.g., EXISTS semi-joins)
        steps.append({
            "step_id": f"s{step_num}",
            "op": "replace_where_predicate",
            "target": {"by_node_id": "S0"},
            "payload": {
                "expr_sql": opt_where,
            },
            "description": "Add WHERE clause with optimized predicates",
        })
        step_num += 1

    if not steps:
        print(f"  WARN {example_id}: no patch steps generated (no structural diff detected)")
        return None

    return {
        "plan_id": f"gold_{dialect_str}_{example_id}",
        "dialect": dialect_str,
        "description": description,
        "preconditions": [{"kind": "parse_ok"}],
        "postconditions": [{"kind": "parse_ok"}],
        "steps": steps,
    }


def _cte_description(name: str, body_sql: str) -> str:
    """Generate a human-readable description for an insert_cte step."""
    lower = body_sql.lower()

    if "avg(" in lower or "sum(" in lower:
        if "group by" in lower:
            return f"Insert CTE '{name}' for pre-aggregated computation"
        return f"Insert CTE '{name}' for aggregate computation"
    if "exists" in lower:
        return f"Insert CTE '{name}' for EXISTS-based semi-join"
    if "union" in lower:
        return f"Insert CTE '{name}' for UNION-based decomposition"
    if "date" in lower or "d_year" in lower or "d_date" in lower:
        return f"Insert CTE '{name}' for date dimension filtering"
    if "join" in lower:
        return f"Insert CTE '{name}' for pre-filtered join"
    return f"Insert CTE '{name}'"


def _from_description(orig_from: str, opt_from: str) -> str:
    """Generate a description for a replace_from step."""
    orig_lower = orig_from.lower()
    opt_lower = opt_from.lower()

    if "join" not in orig_lower and "join" in opt_lower:
        return "Replace comma-join FROM with explicit JOINs"
    if "join" in orig_lower and "join" in opt_lower:
        return "Replace FROM clause with CTE-based JOINs"
    return "Replace FROM clause with optimized version"


# ── Main ─────────────────────────────────────────────────────────────

EXAMPLE_DIRS = {
    "duckdb": Path("packages/qt-sql/qt_sql/examples/duckdb"),
    "postgres": Path("packages/qt-sql/qt_sql/examples/postgres"),
}

SKIP_DIRS = {"snowflake"}  # Already have patch plans


def main():
    import argparse
    parser = argparse.ArgumentParser(description="Generate patch plans for gold examples")
    parser.add_argument("--force", action="store_true", help="Regenerate even if patch_plan exists")
    args = parser.parse_args()

    total = 0
    generated = 0
    skipped = 0
    errors = 0

    for dialect, dir_path in EXAMPLE_DIRS.items():
        if not dir_path.exists():
            print(f"WARNING: {dir_path} does not exist, skipping")
            continue

        json_files = sorted(dir_path.glob("*.json"))
        print(f"\n{'='*60}")
        print(f"Processing {dialect} examples ({len(json_files)} files)")
        print(f"{'='*60}")

        for json_file in json_files:
            total += 1
            print(f"\n  [{total}] {json_file.name}")

            # Read example
            try:
                data = json.loads(json_file.read_text(encoding="utf-8"))
            except Exception as e:
                print(f"    ERROR reading: {e}")
                errors += 1
                continue

            # Skip if already has patch_plan (unless --force)
            if "patch_plan" in data and not args.force:
                print(f"    SKIP: already has patch_plan")
                skipped += 1
                continue

            # Skip regressions
            if data.get("type") == "regression":
                print(f"    SKIP: regression example")
                skipped += 1
                continue

            # Generate patch plan
            patch_plan = generate_patch_plan(data, dialect)
            if patch_plan is None:
                errors += 1
                continue

            # Write back
            data["patch_plan"] = patch_plan
            json_file.write_text(
                json.dumps(data, indent=2, ensure_ascii=False) + "\n",
                encoding="utf-8",
            )
            print(f"    OK: {len(patch_plan['steps'])} steps")
            generated += 1

    # Summary
    print(f"\n{'='*60}")
    print(f"Summary: {generated} generated, {skipped} skipped, {errors} errors out of {total} total")
    print(f"{'='*60}")

    return 0 if errors == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
