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

import json
import logging
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)


def build_analysis_prompt(
    query_id: str,
    sql: str,
    dag: Any,
    costs: Dict[str, Any],
    history: Optional[Dict[str, Any]] = None,
    effective_patterns: Optional[Dict[str, Any]] = None,
    known_regressions: Optional[Dict[str, str]] = None,
    faiss_picks: Optional[List[str]] = None,
    available_examples: Optional[List[Dict[str, str]]] = None,
    dialect: str = "duckdb",
) -> str:
    """Build the LLM-guided analysis prompt.

    This prompt asks the LLM to do deep structural analysis of the query
    following the methodology from successful manual optimization sessions:

    1. STRUCTURAL BREAKDOWN — what does each CTE/subquery/block do in plain language
    2. PROFILE ANALYSIS — where is time being spent and why (map DAG costs to blocks)
    3. ROOT CAUSE — the mechanism: what operation is expensive and why
       (e.g., "3 separate sorts of 5.5M rows" not just "window functions are slow")
    4. PROPOSED CHANGES — specific structural changes with:
       - What to change (concrete: "move the date filter from X to Y")
       - Why it should be faster (mechanism: "reduces hash join probe from 73K to 365 rows")
       - Semantic risk (what could break: "NULL handling changes if...")
    5. FAILURE ANALYSIS — if previous attempts exist, explain why they failed
       and what constraint that teaches us

    Args:
        query_id: Query identifier
        sql: The SQL query to analyze
        dag: Parsed DAG from Phase 1
        costs: Per-node cost analysis
        history: Previous attempts and promotion context
        effective_patterns: Known effective patterns from history.json
        known_regressions: Known regression patterns to avoid
        dialect: SQL dialect

    Returns:
        Analysis prompt string
    """
    lines = []

    # Role
    lines.append(
        "You are an expert database performance analyst. Your job is to deeply "
        "analyze a slow SQL query, identify the root cause of its performance "
        "problems, and propose specific structural changes."
    )
    lines.append("")
    lines.append(
        "You follow a rigorous methodology: understand the structure, profile "
        "the costs, identify the mechanism (not just the symptom), propose "
        "changes with correctness reasoning, and learn from past failures."
    )
    lines.append("")

    # The query
    lines.append(f"## Query: {query_id}")
    lines.append(f"## Dialect: {dialect}")
    lines.append("")

    # Pretty-print SQL
    clean_sql = sql
    try:
        import sqlglot
        clean_sql = sqlglot.transpile(sql, read=dialect, write=dialect, pretty=True)[0]
    except Exception:
        pass
    lines.append("```sql")
    lines.append(clean_sql)
    lines.append("```")
    lines.append("")

    # DAG topology + costs
    lines.append("## Query Structure (DAG)")
    lines.append("")
    _append_dag_analysis(lines, dag, costs, dialect=dialect)
    lines.append("")

    # Previous attempts
    if history:
        lines.append("## Previous Optimization Attempts")
        lines.append("")
        _append_history_analysis(lines, history)
        lines.append("")

    # Known patterns
    if effective_patterns:
        lines.append("## Known Effective Patterns (from benchmark history)")
        lines.append("")
        for pat, info in effective_patterns.items():
            wins = info.get("wins", 0)
            avg = info.get("avg_speedup", 0)
            notes = info.get("notes", "")
            lines.append(f"- **{pat}**: {wins} wins, {avg:.2f}x avg. {notes}")
        lines.append("")

    # Known regressions
    if known_regressions:
        lines.append("## Known Regressions (DO NOT repeat these)")
        lines.append("")
        for name, desc in known_regressions.items():
            lines.append(f"- **{name}**: {desc}")
        lines.append("")

    # FAISS picks + available examples for override
    if faiss_picks or available_examples:
        lines.append("## Reference Examples")
        lines.append("")
        if faiss_picks:
            lines.append(f"**FAISS selected (by structural similarity):** {', '.join(faiss_picks)}")
            lines.append("")
        if available_examples:
            lines.append("**All available gold examples:**")
            lines.append("")
            for ex in available_examples:
                lines.append(
                    f"- **{ex['id']}** ({ex.get('speedup', '?')}x) — {ex.get('description', '')}"
                )
            lines.append("")

    # The task
    lines.append("## Your Task")
    lines.append("")
    lines.append("Analyze this query following these steps IN ORDER:")
    lines.append("")
    lines.append("### 1. STRUCTURAL BREAKDOWN")
    lines.append("For each CTE/subquery/block, explain in 1-2 sentences:")
    lines.append("- What it computes (in plain language)")
    lines.append("- What tables it reads and approximately how many rows")
    lines.append("- What it outputs (cardinality estimate)")
    lines.append("")
    lines.append("### 2. BOTTLENECK IDENTIFICATION")
    lines.append("Using the DAG costs above, identify the dominant cost center.")
    lines.append("Don't just name it — explain the MECHANISM:")
    lines.append("- Is it a full table scan that could be filtered?")
    lines.append("- Is it a sort for a window function that could be deferred?")
    lines.append("- Is it a hash join on a large build side that could be pre-filtered?")
    lines.append("- Is it scanning the same table multiple times when once would suffice?")
    lines.append("")
    lines.append("### 3. PROPOSED OPTIMIZATION")
    lines.append("Propose 1-3 specific structural changes. For EACH one:")
    lines.append("- **What**: Exactly what to change (e.g., 'merge CTEs X and Y into one scan')")
    lines.append("- **Why**: The performance mechanism (e.g., 'eliminates a 28M-row rescan of store_sales')")
    lines.append("- **Risk**: What semantic constraint could break (e.g., 'the HAVING filter must be preserved')")
    lines.append("- **Estimated impact**: minor / moderate / significant")
    lines.append("")

    if history and history.get("attempts"):
        lines.append("### 4. FAILURE ANALYSIS")
        lines.append("For each previous failed/regressed attempt, explain:")
        lines.append("- WHY it failed (the specific mechanism)")
        lines.append("- What constraint that teaches us for the next attempt")
        lines.append("")

    lines.append("### 5. RECOMMENDED STRATEGY")
    lines.append("Synthesize everything into a single recommended optimization approach.")
    lines.append("Be specific enough that another engineer could implement it from your description.")
    lines.append("")

    if available_examples:
        lines.append("### 6. EXAMPLE SELECTION")
        lines.append(f"FAISS selected these examples: {', '.join(faiss_picks or [])}")
        lines.append("Review the FAISS picks against the available examples above.")
        lines.append("If you think different examples would be more relevant for this query,")
        lines.append("list your preferred examples. Otherwise confirm the FAISS picks are good.")
        lines.append("")
        lines.append("```")
        lines.append("EXAMPLES: example_id_1, example_id_2, example_id_3")
        lines.append("```")
        lines.append("")
        lines.append("Use exact IDs from the available examples list above.")
        lines.append("")

    return "\n".join(lines)


def _append_dag_analysis(
    lines: List[str],
    dag: Any,
    costs: Dict[str, Any],
    dialect: str = "duckdb",
) -> None:
    """Append structured DAG analysis — one card per node.

    Gold standard format per node:
      Role, Stats, Flags, Outputs, Dependencies, Joins, Filters,
      Operators, Key Logic (SQL)
    """
    from .node_prompter import compute_depths
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

            # Stats
            output_rows = meta.get("limit")
            if output_rows:
                lines.append(
                    f"**Stats**: {cost_pct:.0f}% Cost | "
                    f"~{_fmt_rows(row_est)} rows processed → "
                    f"{output_rows} rows output"
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
            out_str = ", ".join(out_cols) if out_cols else "?"
            if hasattr(node, "contract") and node.contract and node.contract.output_columns:
                if len(node.contract.output_columns) > 10:
                    out_str += ", ..."
            out_suffix = ""
            if meta.get("order_by"):
                out_suffix = f" — ordered by {meta['order_by']}"
            lines.append(f"**Outputs**: [{out_str}]{out_suffix}")

            # Dependencies
            deps = meta.get("dependencies", [])
            if deps:
                lines.append(f"**Dependencies**: {', '.join(deps)}")
            else:
                tables = list(node.tables) if hasattr(node, "tables") else []
                if tables:
                    lines.append(f"**Dependencies**: {', '.join(tables)}")

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
    """Enrich DAG flags with sqlglot-extracted details."""
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

    Returns dict with: joins, filters, dependencies, order_by, limit,
    correlated_detail, table_aliases.
    """
    result: Dict[str, Any] = {
        "joins": [],
        "filters": [],
        "dependencies": [],
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

    # --- Table aliases ---
    aliases: Dict[str, str] = {}  # alias → table_name
    for table in parsed.find_all(exp.Table):
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

    # --- WHERE conditions → joins vs filters ---
    where = parsed.find(exp.Where)
    if where:
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

    # --- Dependencies with roles ---
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


def _build_dependency_list(
    parsed, aliases: Dict[str, str], result: Dict[str, Any], dialect: str
) -> List[str]:
    """Build dependency list with alias roles (join, correlated subquery, etc.)."""
    from sqlglot import exp

    deps = []
    seen = set()

    # Main FROM tables
    from_clause = parsed.find(exp.From)
    if from_clause:
        for table in from_clause.find_all(exp.Table):
            name = table.name
            alias = table.alias or name
            key = f"{name}_{alias}"
            if key not in seen:
                seen.add(key)
                role = "join" if alias != name else ""
                dep = f"{name} AS {alias}" if alias != name else name
                if role:
                    dep += f" ({role})"
                deps.append(dep)

    # JOIN tables
    for join in parsed.find_all(exp.Join):
        for table in join.find_all(exp.Table):
            name = table.name
            alias = table.alias or name
            key = f"{name}_{alias}"
            if key not in seen:
                seen.add(key)
                dep = f"{name} AS {alias}" if alias != name else name
                dep += " (join)"
                deps.append(dep)

    # Subquery tables (correlated)
    for subq in parsed.find_all(exp.Subquery):
        for table in subq.find_all(exp.Table):
            name = table.name
            alias = table.alias or name
            key = f"{name}_{alias}"
            if key not in seen:
                seen.add(key)
                dep = f"{name} AS {alias}" if alias != name else name
                dep += " (correlated subquery)"
                deps.append(dep)

    return deps


def _format_key_logic(sql: str, dialect: str = "duckdb") -> str:
    """Format node SQL as clean, readable Key Logic block.

    Uses sqlglot pretty-print. For short SQL (≤15 lines), shows full.
    For longer SQL, shows full but limits to 20 lines.
    """
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
        "You are a database performance expert analyzing a failed optimization attempt."
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

    # DAG comparison
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
        "Use the DAG cost analysis above to pinpoint the dominant cost center."
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
    """Append DAG node summary with costs for failure analysis."""
    if not hasattr(dag, "nodes") or not dag.nodes:
        lines.append("(DAG unavailable)")
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
