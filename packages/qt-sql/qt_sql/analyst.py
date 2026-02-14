"""LLM-guided query analysis — deep structural/performance reasoning.

This module implements the iterative analysis methodology:
1. Structural breakdown: decompose query into logical blocks, explain what each does
2. Profile analysis: map costs to blocks, identify dominant cost center
3. Root cause: explain the MECHANISM — not just "it's slow" but WHY (sorting, scanning, etc.)
4. Propose specific structural changes with reasoning about correctness risks
5. Incorporate failure history: each failed attempt teaches something that constrains the next

This analysis is generated BEFORE the rewrite prompt, so the rewrite LLM
has specific, concrete guidance instead of just pattern names.
"""

from __future__ import annotations

import logging
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)



def _append_dag_analysis(
    lines: List[str],
    dag: Any,
    costs: Dict[str, Any],
    dialect: str = "duckdb",
    node_intents: Optional[Dict[str, str]] = None,
    output_columns: Optional[List[str]] = None,
) -> None:
    """Append structured logical-tree analysis — one card per node.

    Gold standard format per node:
      Role, Stats, Flags, Outputs, Dependencies, Joins, Filters,
      Operators, Key Logic (SQL)

    Args:
        node_intents: Optional {node_id: intent_string} from semantic_intents.json.
                      When present, each node card includes an Intent line.
        output_columns: Known output columns for the root node (fallback when
                        sqlglot contract extraction fails on complex queries).
    """
    if node_intents is None:
        node_intents = {}
    from .prompter import compute_depths
    depths = compute_depths(dag)

    max_depth = max(depths.values()) if depths else 0
    node_num = 0
    for depth in range(max_depth + 1):
        nodes_at_depth = [nid for nid, d in depths.items() if d == depth]
        if not nodes_at_depth:
            continue

        for nid in nodes_at_depth:
            node_num += 1
            node = dag.nodes[nid]
            cost = costs.get(nid)
            cost_pct = cost.cost_pct if cost and hasattr(cost, "cost_pct") else 0
            row_est = cost.row_estimate if cost and hasattr(cost, "row_estimate") else 0
            base_flags = node.flags if hasattr(node, "flags") and node.flags else []

            # Extract structured metadata from SQL
            meta = _extract_node_metadata(
                node.sql if hasattr(node, "sql") else "", dialect
            )

            # Role label
            if node.node_type == "main":
                role = "Root / Output"
            elif node.node_type == "cte":
                role = "CTE"
            elif node.node_type == "subquery":
                role = "Subquery"
            else:
                role = node.node_type.title()

            # --- Header ---
            lines.append(f"### {node_num}. {nid}")
            lines.append(f"**Role**: {role} (Definition Order: {depth})")

            # Semantic intent (LLM-generated, from semantic_intents.json)
            node_intent = node_intents.get(nid, "")
            if node_intent:
                lines.append(f"**Intent**: {node_intent}")

            # Stats — distinguish processing rows from output rows
            output_rows = meta.get("limit")
            has_subqueries = bool(meta.get("subqueries"))
            if output_rows:
                lines.append(
                    f"**Stats**: {cost_pct:.0f}% Cost | "
                    f"~{_fmt_rows(row_est)} rows processed → "
                    f"{output_rows} rows output"
                )
            elif has_subqueries and node.node_type == "main":
                # Main query with subqueries: row_est reflects subquery scans,
                # not output. Output depends on FROM table + filters.
                from_tables = meta.get("dependencies", [])
                filters = meta.get("filters", [])
                filter_note = f" (after {len(filters)} filter{'s' if len(filters) != 1 else ''})" if filters else ""
                lines.append(
                    f"**Stats**: {cost_pct:.0f}% Cost | "
                    f"~{_fmt_rows(row_est)} rows processed across subqueries | "
                    f"Output: from {', '.join(from_tables) if from_tables else 'base table'}{filter_note}"
                )
            else:
                lines.append(
                    f"**Stats**: {cost_pct:.0f}% Cost | "
                    f"~{_fmt_rows(row_est)} rows"
                )

            # Flags
            rich_flags = _build_rich_flags(base_flags, meta)
            if rich_flags:
                lines.append(f"**Flags**: {', '.join(rich_flags)}")

            # Outputs
            out_cols = []
            if hasattr(node, "contract") and node.contract and node.contract.output_columns:
                out_cols = node.contract.output_columns[:10]
            elif node.node_type == "main" and output_columns:
                out_cols = output_columns[:10]
            out_str = ", ".join(out_cols) if out_cols else "?"
            if hasattr(node, "contract") and node.contract and node.contract.output_columns:
                if len(node.contract.output_columns) > 10:
                    out_str += ", ..."
            out_suffix = ""
            if meta.get("order_by"):
                out_suffix = f" — ordered by {meta['order_by']}"
            lines.append(f"**Outputs**: [{out_str}]{out_suffix}")

            # Dependencies (direct tables only — subqueries shown separately)
            deps = meta.get("dependencies", [])
            if deps:
                lines.append(f"**Dependencies**: {', '.join(deps)}")
            else:
                tables = list(node.tables) if hasattr(node, "tables") else []
                if tables:
                    lines.append(f"**Dependencies**: {', '.join(tables)}")

            # Subqueries — structured detail
            subqueries = meta.get("subqueries", [])
            if subqueries:
                # Deduplicate by signature
                seen_sigs: dict = {}
                for sq in subqueries:
                    sig = f"{','.join(sq['tables'])}|{','.join(sq['filters'][:2])}"
                    seen_sigs[sig] = seen_sigs.get(sig, 0) + 1

                emitted: dict = {}
                sq_lines = []
                for sq in subqueries:
                    sig = f"{','.join(sq['tables'])}|{','.join(sq['filters'][:2])}"
                    if sig in emitted:
                        continue
                    emitted[sig] = True
                    count = seen_sigs[sig]
                    sq_type = sq["type"]
                    if sq["correlated"]:
                        sq_type = f"correlated {sq_type}"
                    label = f"({sq_type})"
                    if count > 1:
                        label += f" x{count}"
                    inner_parts = []
                    for tbl in sq["tables"]:
                        inner_parts.append(tbl)
                    for filt in sq["filters"][:2]:
                        inner_parts.append(f"FILTER: {filt}")
                    if sq["corr_pred"]:
                        inner_parts.append(f"CORR-PRED: {sq['corr_pred']}")
                    sq_lines.append(f"{label}: {' | '.join(inner_parts)}")
                lines.append(f"**Subqueries**: {'; '.join(sq_lines)}")

            # Joins
            joins = meta.get("joins", [])
            if joins:
                lines.append(f"**Joins**: {' | '.join(joins)}")

            # Filters
            filters = meta.get("filters", [])
            if filters:
                lines.append(f"**Filters**: {' | '.join(filters)}")

            # Operators
            if cost and hasattr(cost, "operators") and cost.operators:
                ops = [_clean_operator(op) for op in cost.operators[:6]]
                lines.append(f"**Operators**: {', '.join(ops)}")

            # Key Logic (SQL) — blank line before code block
            if hasattr(node, "sql") and node.sql:
                lines.append("**Key Logic (SQL)**:")
                lines.append("```sql")
                lines.append(_format_key_logic(node.sql, dialect))
                lines.append("```")

            # Blank line between nodes
            lines.append("")

    # Edges
    if dag.edges:
        lines.append("### Edges")
        for src, dst in dag.edges:
            lines.append(f"- {src} → {dst}")
        lines.append("")


# ------------------------------------------------------------------
# Helpers
# ------------------------------------------------------------------

def _fmt_rows(n: int) -> str:
    """Format row count: 557705 → '557k', 28000000 → '28M'."""
    if n >= 1_000_000:
        val = n / 1_000_000
        return f"{val:.1f}M" if val != int(val) else f"{int(val)}M"
    if n >= 1_000:
        return f"{n // 1_000}k"
    return str(n)


def _clean_operator(op: str) -> str:
    """Clean operator names: SEQ_SCAN[CTE_SCAN] → CTE_SCAN, keep SEQ_SCAN[store]."""
    import re
    m = re.match(r"SEQ_SCAN\[(.+)\]", op)
    if m:
        inner = m.group(1)
        # If inner is itself an operator type, unwrap
        if inner in ("CTE_SCAN", "COLUMN_DATA_SCAN", "TEMP_SCAN"):
            return inner
        return op
    return op


def _build_rich_flags(
    base_flags: List[str], meta: Dict[str, Any]
) -> List[str]:
    """Enrich structural flags with sqlglot-extracted details."""
    flags = []
    for f in base_flags:
        if f == "CORRELATED" and meta.get("correlated_detail"):
            flags.append(f"CORRELATED_SUBQUERY({meta['correlated_detail']})")
        else:
            flags.append(f)

    # Add ORDER_BY and LIMIT if detected
    if meta.get("order_by") and "ORDER_BY" not in flags:
        flags.append("ORDER_BY")
    if meta.get("limit") and "LIMIT" not in flags:
        flags.append(f"LIMIT({meta['limit']})")

    return flags


def _extract_node_metadata(sql: str, dialect: str = "duckdb") -> Dict[str, Any]:
    """Extract structured metadata from a node's SQL using sqlglot.

    Returns dict with: joins, filters, dependencies, subqueries, order_by,
    limit, correlated_detail, table_aliases.
    """
    result: Dict[str, Any] = {
        "joins": [],
        "filters": [],
        "dependencies": [],
        "subqueries": [],
        "order_by": None,
        "limit": None,
        "correlated_detail": None,
        "table_aliases": {},
    }

    if not sql or not sql.strip():
        return result

    try:
        import sqlglot
        from sqlglot import exp
        parsed = sqlglot.parse_one(sql, read=dialect)
    except Exception:
        return result

    # --- Table aliases (outer scope only) ---
    aliases: Dict[str, str] = {}  # alias → table_name
    # Only collect top-level table aliases (not those inside subqueries)
    for table in parsed.find_all(exp.Table):
        if table.find_ancestor(exp.Subquery):
            continue
        name = table.name
        alias = table.alias or name
        aliases[alias] = name

    result["table_aliases"] = aliases

    # --- ORDER BY ---
    order = parsed.find(exp.Order)
    if order:
        parts = []
        for ordered in order.find_all(exp.Ordered):
            col = ordered.this.sql(dialect=dialect)
            desc = " DESC" if ordered.args.get("desc") else " ASC"
            parts.append(f"{col}{desc}")
        if parts:
            result["order_by"] = ", ".join(parts)

    # --- LIMIT ---
    limit = parsed.find(exp.Limit)
    if limit:
        limit_expr = limit.args.get("expression") or limit.this
        if limit_expr:
            try:
                result["limit"] = int(limit_expr.sql(dialect=dialect))
            except (ValueError, TypeError):
                pass

    # --- WHERE conditions → joins vs filters (outer-level only) ---
    where = parsed.find(exp.Where)
    if where and not where.find_ancestor(exp.Subquery):
        conditions = _split_conditions(where.this)
        for cond in conditions:
            classified = _classify_condition(cond, aliases, dialect)
            if classified:
                cat, text = classified
                if cat == "join":
                    result["joins"].append(text)
                elif cat == "filter":
                    result["filters"].append(text)
                elif cat == "correlated":
                    result["joins"].append(text + " (correlated)")
                    result["correlated_detail"] = text

    # --- Correlated subquery detection ---
    for subq in parsed.find_all(exp.Subquery):
        _detect_correlated(subq, parsed, result, aliases, dialect)

    # --- Structured subquery info ---
    subq_infos = _extract_subquery_info(parsed, aliases, dialect)
    result["subqueries"] = subq_infos

    # --- Dependencies with roles (excludes subquery-internal tables) ---
    deps = _build_dependency_list(parsed, aliases, result, dialect)
    if deps:
        result["dependencies"] = deps

    return result


def _split_conditions(node) -> list:
    """Split an AND-chain into individual conditions."""
    from sqlglot import exp
    if isinstance(node, exp.And):
        return _split_conditions(node.left) + _split_conditions(node.right)
    return [node]


def _classify_condition(
    cond, aliases: Dict[str, str], dialect: str
) -> Optional[tuple]:
    """Classify a WHERE condition as join, filter, or correlated.

    Returns (category, text) or None if unclassifiable.
    """
    from sqlglot import exp

    # Subquery comparisons — abbreviate intelligently
    if cond.find(exp.Subquery):
        text = cond.sql(dialect=dialect)
        if len(text) > 80:
            # Try to produce a meaningful abbreviation
            left = cond.left.sql(dialect=dialect) if hasattr(cond, "left") else "?"
            subq = cond.find(exp.Subquery)
            abbrev = _abbreviate_subquery_filter(subq, dialect)
            op = ">" if isinstance(cond, exp.GT) else ">=" if isinstance(cond, exp.GTE) else \
                 "<" if isinstance(cond, exp.LT) else "<=" if isinstance(cond, exp.LTE) else "op"
            return ("filter", f"{left} {op} {abbrev}")
        return ("filter", text)

    cond_sql = cond.sql(dialect=dialect)

    # Equality: join or filter?
    if isinstance(cond, (exp.EQ, exp.Is)):
        left_cols = list(cond.left.find_all(exp.Column))
        right_cols = list(cond.right.find_all(exp.Column))

        if left_cols and right_cols:
            # Two column refs → join condition
            left_tables = {c.table for c in left_cols if c.table}
            right_tables = {c.table for c in right_cols if c.table}
            if left_tables != right_tables:
                return ("join", cond_sql)
            # Same table on both sides → still a join-like condition
            return ("join", cond_sql)
        elif left_cols and not right_cols:
            return ("filter", cond_sql)
        elif right_cols and not left_cols:
            return ("filter", cond_sql)
    elif isinstance(cond, (exp.GT, exp.GTE, exp.LT, exp.LTE, exp.NEQ)):
        if cond.find(exp.Subquery):
            return None
        return ("filter", cond_sql)
    elif isinstance(cond, exp.In):
        return ("filter", cond_sql)

    # Default: treat as filter
    if cond_sql and len(cond_sql) < 200:
        return ("filter", cond_sql)
    return None


def _abbreviate_subquery_filter(subq, dialect: str) -> str:
    """Produce a short description of a subquery used in a filter.

    e.g. SELECT AVG(x) * 1.2 FROM t WHERE corr → 'AVG(ctr_total_return) * 1.2 (per store)'
    """
    from sqlglot import exp

    try:
        # Find the SELECT expression
        select = subq.find(exp.Select)
        if not select:
            return "(subquery)"

        # Get the projected expression
        exprs = select.expressions
        if exprs:
            proj = exprs[0].sql(dialect=dialect)
            # Limit length
            if len(proj) > 60:
                # Try to find aggregate function name
                agg = select.find(exp.Avg) or select.find(exp.Sum) or \
                      select.find(exp.Count) or select.find(exp.Max) or \
                      select.find(exp.Min)
                if agg:
                    proj = f"{type(agg).__name__.upper()}(...)"
                else:
                    proj = proj[:40] + "..."

            # Check for correlation — describe what the grouping is
            where = subq.find(exp.Where)
            if where:
                for eq in where.find_all(exp.EQ):
                    cols = list(eq.find_all(exp.Column))
                    if len(cols) >= 2:
                        # Use the column name to describe the grouping
                        col_name = cols[0].name
                        # Strip common prefixes like ctr_
                        semantic = col_name
                        for prefix in ("ctr_", "sr_", "ss_", "cs_", "ws_"):
                            if semantic.startswith(prefix):
                                semantic = semantic[len(prefix):]
                                break
                        return f"{proj} (per {semantic})"
            return proj
    except Exception:
        pass
    return "(subquery)"


def _detect_correlated(subq, outer, result, aliases, dialect):
    """Detect correlated subquery and extract correlation columns."""
    from sqlglot import exp

    subq_where = subq.find(exp.Where)
    if not subq_where:
        return

    # Check for references to outer aliases inside the subquery WHERE
    for col in subq_where.find_all(exp.Column):
        table = col.table
        if table and table in aliases and table not in _get_subq_aliases(subq):
            # This column references an outer table → correlated
            inner_cond = subq_where.this
            for cond in _split_conditions(inner_cond):
                cond_sql = cond.sql(dialect=dialect)
                if table in cond_sql:
                    result["correlated_detail"] = cond_sql
                    return


def _get_subq_aliases(subq) -> set:
    """Get all table aliases defined inside a subquery."""
    from sqlglot import exp
    aliases = set()
    for table in subq.find_all(exp.Table):
        aliases.add(table.alias or table.name)
    return aliases


def _is_subquery_correlated(subq, outer_aliases: set, dialect: str) -> Optional[str]:
    """Check if a subquery references outer aliases. Return correlation predicate or None."""
    from sqlglot import exp

    subq_aliases = _get_subq_aliases(subq)
    for col in subq.find_all(exp.Column):
        table = col.table
        if table and table in outer_aliases and table not in subq_aliases:
            # Found outer reference — extract the correlation predicate
            where = subq.find(exp.Where)
            if where:
                for cond in _split_conditions(where.this):
                    cond_sql = cond.sql(dialect=dialect)
                    if table in cond_sql:
                        return cond_sql
            return f"{col.sql(dialect=dialect)} (outer ref)"
    return None


def _classify_subquery_type(subq, dialect: str) -> str:
    """Classify subquery as scalar, exists, or in-list."""
    from sqlglot import exp

    parent = subq.parent
    if isinstance(parent, exp.Exists):
        return "correlated exists"
    if isinstance(parent, exp.In):
        return "in-list"
    # Scalar subquery — check if it has aggregate (typical for scalar)
    select = subq.find(exp.Select)
    if select:
        for agg_type in (exp.Count, exp.Sum, exp.Avg, exp.Min, exp.Max):
            if select.find(agg_type):
                return "scalar aggregate"
    return "scalar"


def _extract_subquery_info(
    parsed, outer_aliases: Dict[str, str], dialect: str
) -> List[Dict[str, Any]]:
    """Extract structured info for each subquery in the statement.

    Returns list of dicts with: type, correlated, corr_pred, tables, filters.
    """
    from sqlglot import exp

    # Only look at top-level subqueries (not nested inside other subqueries)
    subqueries = []
    outer_alias_set = set(outer_aliases.keys())

    for subq in parsed.find_all(exp.Subquery):
        # Skip subqueries nested inside other subqueries (they belong to the inner scope)
        parent_subq = subq.find_ancestor(exp.Subquery)
        if parent_subq is not None:
            continue

        info: Dict[str, Any] = {
            "type": _classify_subquery_type(subq, dialect),
            "correlated": False,
            "corr_pred": None,
            "tables": [],
            "filters": [],
        }

        # Correlation check
        corr = _is_subquery_correlated(subq, outer_alias_set, dialect)
        if corr:
            info["correlated"] = True
            info["corr_pred"] = corr

        # Tables inside subquery
        for table in subq.find_all(exp.Table):
            name = table.name
            alias = table.alias or name
            label = f"{name} {alias}" if alias != name else name
            if label not in info["tables"]:
                info["tables"].append(label)

        # Filters inside subquery
        where = subq.find(exp.Where)
        if where:
            for cond in _split_conditions(where.this):
                cond_sql = cond.sql(dialect=dialect)
                if len(cond_sql) < 80:
                    info["filters"].append(cond_sql)

        subqueries.append(info)

    return subqueries


def _build_dependency_list(
    parsed, aliases: Dict[str, str], result: Dict[str, Any], dialect: str
) -> List[str]:
    """Build dependency list with alias roles — properly classifies subqueries."""
    from sqlglot import exp

    deps = []
    seen = set()

    # Collect tables that are inside subqueries (to exclude from main SCAN list)
    subq_table_keys = set()
    for subq in parsed.find_all(exp.Subquery):
        for table in subq.find_all(exp.Table):
            name = table.name
            alias = table.alias or name
            subq_table_keys.add(f"{name}_{alias}")

    # Main FROM tables (exclude subquery-internal tables)
    from_clause = parsed.find(exp.From)
    if from_clause:
        # Only get direct children tables, not those inside subqueries
        for table in from_clause.find_all(exp.Table):
            name = table.name
            alias = table.alias or name
            key = f"{name}_{alias}"
            if key in subq_table_keys:
                continue
            if key not in seen:
                seen.add(key)
                dep = f"{name} {alias}" if alias != name else name
                deps.append(dep)

    # JOIN tables
    for join in parsed.find_all(exp.Join):
        # Skip joins inside subqueries
        if join.find_ancestor(exp.Subquery):
            continue
        for table in join.find_all(exp.Table):
            name = table.name
            alias = table.alias or name
            key = f"{name}_{alias}"
            if key not in seen:
                seen.add(key)
                dep = f"{name} {alias}" if alias != name else name
                dep += " (join)"
                deps.append(dep)

    return deps


def _format_key_logic(sql: str, dialect: str = "duckdb") -> str:
    """Format node SQL as clean, readable Key Logic block.

    Uses sqlglot pretty-print. For short SQL (≤15 lines), shows full.
    For longer SQL, shows full but limits to 20 lines.
    """
    import re
    # Strip block and line comments before formatting
    sql = re.sub(r'/\*.*?\*/', '', sql, flags=re.DOTALL)
    sql = re.sub(r'--[^\n]*', '', sql)
    compact = " ".join(sql.split())
    try:
        import sqlglot
        pretty = sqlglot.transpile(compact, read=dialect, write=dialect, pretty=True)[0]
        pretty_lines = pretty.split("\n")
        if len(pretty_lines) <= 20:
            return pretty
        return "\n".join(pretty_lines[:20]) + "\n..."
    except Exception:
        if len(compact) > 500:
            return compact[:500] + " ..."
        return compact


def _append_history_analysis(
    lines: List[str],
    history: Dict[str, Any],
) -> None:
    """Append previous attempt history for failure analysis."""
    # Promotion context
    promotion = history.get("promotion")
    if promotion:
        lines.append(f"**Best previous result: {promotion.speedup:.2f}x** "
                      f"(transforms: {', '.join(promotion.transforms)})")
        lines.append("")
        if promotion.analysis:
            lines.append(f"Previous analysis: {promotion.analysis}")
            lines.append("")
        if promotion.suggestions:
            lines.append(f"Previous suggestions: {promotion.suggestions}")
            lines.append("")

    # All attempts
    attempts = history.get("attempts", [])
    if attempts:
        for i, attempt in enumerate(attempts):
            status = attempt.get("status", "unknown")
            speedup = attempt.get("speedup", 0)
            transforms = attempt.get("transforms", [])
            error = attempt.get("error", "")
            t_str = ", ".join(transforms) if transforms else "unknown"

            if status in ("error", "ERROR"):
                lines.append(f"- Attempt {i+1}: **{t_str}** → ERROR: {error}")
            elif speedup < 0.95:
                lines.append(
                    f"- Attempt {i+1}: **{t_str}** → REGRESSION ({speedup:.2f}x)"
                )
            elif speedup >= 1.10:
                lines.append(
                    f"- Attempt {i+1}: **{t_str}** → WIN ({speedup:.2f}x)"
                )
            else:
                lines.append(
                    f"- Attempt {i+1}: **{t_str}** → NEUTRAL ({speedup:.2f}x)"
                )

            # Include attempted SQL if available (for failure analysis)
            opt_sql = attempt.get("optimized_sql", "")
            if opt_sql and status not in ("WIN", "IMPROVED") and len(opt_sql) < 2000:
                lines.append(f"  Attempted SQL:")
                lines.append(f"  ```sql\n  {opt_sql}\n  ```")


def parse_analysis_response(response: str) -> Dict[str, str]:
    """Parse the LLM analysis response into structured sections.

    Returns dict with keys:
    - structural_breakdown
    - bottleneck
    - proposed_changes
    - failure_analysis (if present)
    - recommended_strategy
    - raw (full response)
    """
    import re

    result = {"raw": response}

    patterns = {
        "structural_breakdown": r"###?\s*1\.?\s*STRUCTURAL\s+BREAKDOWN\s*\n(.*?)(?=###?\s*2\.|$)",
        "bottleneck": r"###?\s*2\.?\s*BOTTLENECK\s+IDENTIFICATION\s*\n(.*?)(?=###?\s*3\.|$)",
        "proposed_changes": r"###?\s*3\.?\s*PROPOSED\s+OPTIMIZATION\s*\n(.*?)(?=###?\s*4\.|###?\s*5\.|$)",
        "failure_analysis": r"###?\s*4\.?\s*FAILURE\s+ANALYSIS\s*\n(.*?)(?=###?\s*5\.|$)",
        "recommended_strategy": r"###?\s*5\.?\s*RECOMMENDED\s+STRATEGY\s*\n(.*?)$",
    }

    for key, pattern in patterns.items():
        match = re.search(pattern, response, re.DOTALL | re.IGNORECASE)
        if match:
            result[key] = match.group(1).strip()

    return result


def parse_example_overrides(response: str) -> Optional[List[str]]:
    """Parse the analyst response for example override recommendations.

    Looks for a line like: EXAMPLES: decorrelate, early_filter, pushdown

    Returns:
        List of example IDs if the analyst recommended overrides, None otherwise.
    """
    import re

    match = re.search(r'EXAMPLES:\s*(.+)', response)
    if not match:
        return None

    raw = match.group(1).strip()
    # Split on comma, clean whitespace
    ids = [x.strip() for x in raw.split(",") if x.strip()]

    if not ids:
        return None

    return ids


def build_failure_analysis_prompt(
    query_id: str,
    original_sql: str,
    attempted_sql: str,
    target_speedup: float,
    actual_speedup: float,
    status: str,
    transforms: List[str],
    dag_original: Any,
    costs_original: Any,
    dag_attempted: Any,
    costs_attempted: Any,
    previous_attempts: List[Any],
    dialect: str = "duckdb",
) -> str:
    """Build prompt for LLM to analyze why optimization failed to reach target.

    Returns prompt asking 4 critical questions:
    1. What went wrong?
    2. Why was speedup insufficient?
    3. What should the NEXT attempt try?
    4. What constraints did we learn?
    """
    lines = []

    # Context
    lines.append(
        "You are a database performance specialist analyzing a failed optimization attempt."
    )
    lines.append(f"**Target**: {target_speedup:.1f}x speedup")
    lines.append(f"**Achieved**: {actual_speedup:.2f}x ({status})")
    lines.append("")

    if actual_speedup < 1.0:
        lines.append("WARNING: The optimization REGRESSED performance. This is critical.")
    elif actual_speedup < 1.1:
        lines.append("The optimization barely improved performance.")
    else:
        lines.append(
            f"The optimization improved performance but fell short of "
            f"the {target_speedup:.1f}x target."
        )
    lines.append("")

    # Pretty-print SQL helper
    def _pp(sql: str) -> str:
        try:
            import sqlglot
            return sqlglot.transpile(sql, read=dialect, write=dialect, pretty=True)[0]
        except Exception:
            return sql

    # Original query
    lines.append("## Original SQL")
    lines.append("```sql")
    lines.append(_pp(original_sql))
    lines.append("```")
    lines.append("")

    # Attempted optimization
    lines.append("## Attempted Optimization")
    lines.append(
        f"**Transforms applied**: "
        f"{', '.join(transforms) if transforms else 'unknown'}"
    )
    lines.append("```sql")
    lines.append(_pp(attempted_sql))
    lines.append("```")
    lines.append("")

    # Structural comparison
    lines.append("## Performance Analysis")
    lines.append("")
    lines.append("### Original Query Structure")
    _append_dag_summary(lines, dag_original, costs_original)
    lines.append("")
    lines.append("### Attempted Query Structure")
    _append_dag_summary(lines, dag_attempted, costs_attempted)
    lines.append("")

    # Previous attempts (if any)
    if previous_attempts:
        lines.append("## Previous Attempts")
        for i, att in enumerate(previous_attempts, 1):
            t_str = (
                ", ".join(att.transforms) if att.transforms else "unknown"
            )
            lines.append(
                f"**Attempt {i}**: {t_str} -> {att.status} ({att.speedup:.2f}x)"
            )
            if att.failure_analysis:
                preview = (
                    att.failure_analysis[:150] + "..."
                    if len(att.failure_analysis) > 150
                    else att.failure_analysis
                )
                lines.append(f"  Analysis: {preview}")
        lines.append("")

    # The 4 critical questions
    lines.append("## Your Task")
    lines.append("")
    lines.append("Analyze this failed attempt and answer these questions:")
    lines.append("")
    lines.append("### 1. What went wrong?")
    lines.append(
        "Explain the specific mechanism that prevented reaching the target speedup."
    )
    lines.append(
        "Don't just say 'the transform didn't work' - explain WHY "
        "at the query execution level."
    )
    lines.append("")
    lines.append("### 2. Why was the speedup insufficient?")
    lines.append(
        "Identify the bottleneck that is STILL present after this optimization."
    )
    lines.append(
        "Use the logical-tree cost analysis above to pinpoint the dominant cost center."
    )
    lines.append("")
    lines.append("### 3. What should the NEXT attempt try?")
    lines.append(
        f"Propose a DIFFERENT structural approach that could reach "
        f"{target_speedup:.1f}x."
    )
    lines.append(
        "Be specific: what transforms, what query restructuring, what mechanism."
    )
    lines.append(
        "If previous attempts tried similar approaches, suggest something NOVEL."
    )
    lines.append("")
    lines.append("### 4. What did this failure teach us?")
    lines.append(
        "What constraints or patterns should we AVOID in the next attempt?"
    )
    lines.append("")
    lines.append("Format your response with these exact section headers:")
    lines.append("```")
    lines.append("### What went wrong")
    lines.append("[Explain the mechanism]")
    lines.append("")
    lines.append("### Why speedup was insufficient")
    lines.append("[Bottleneck analysis]")
    lines.append("")
    lines.append("### Next approach")
    lines.append("[Specific recommendation]")
    lines.append("")
    lines.append("### Learned constraints")
    lines.append("[What to avoid]")
    lines.append("```")

    return "\n".join(lines)


def _append_dag_summary(lines: List[str], dag: Any, costs: Any) -> None:
    """Append logical-tree node summary with costs for failure analysis."""
    if not hasattr(dag, "nodes") or not dag.nodes:
        lines.append("(Logical tree unavailable)")
        return

    for nid in dag.nodes:
        node = dag.nodes[nid]
        cost = costs.get(nid) if isinstance(costs, dict) else None
        cost_pct = cost.cost_pct if cost and hasattr(cost, "cost_pct") else 0
        row_est = cost.row_estimate if cost and hasattr(cost, "row_estimate") else 0
        lines.append(
            f"- **{nid}**: ~{row_est} rows, {cost_pct:.0f}% cost"
        )


def format_analysis_for_prompt(analysis: Dict[str, str]) -> str:
    """Format the parsed analysis into a prompt section for the rewrite LLM.

    This goes into Section 5 (History/Context) of the rewrite prompt,
    giving the rewrite LLM concrete guidance.
    """
    lines = ["## Expert Analysis", ""]

    if analysis.get("structural_breakdown"):
        lines.append("### Query Structure")
        lines.append(analysis["structural_breakdown"])
        lines.append("")

    if analysis.get("bottleneck"):
        lines.append("### Performance Bottleneck")
        lines.append(analysis["bottleneck"])
        lines.append("")

    if analysis.get("proposed_changes"):
        lines.append("### Proposed Optimization Strategy")
        lines.append(analysis["proposed_changes"])
        lines.append("")

    if analysis.get("failure_analysis"):
        lines.append("### Lessons from Previous Failures")
        lines.append(analysis["failure_analysis"])
        lines.append("")

    if analysis.get("recommended_strategy"):
        lines.append("### Recommended Approach")
        lines.append(analysis["recommended_strategy"])
        lines.append("")

    lines.append(
        "Apply the recommended strategy above. The analysis has already "
        "identified the bottleneck and the specific structural change needed. "
        "Focus on implementing it correctly while preserving semantic equivalence."
    )

    return "\n".join(lines)
