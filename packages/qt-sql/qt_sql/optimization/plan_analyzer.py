"""Lightweight plan analyzer for LLM optimization prompts.

Extracts the key signals from EXPLAIN plans that matter for optimization:
- Bottleneck operators (by cost/rows)
- Late-joined tables (could be pushed earlier)
- Filter selectivity (rows in vs rows out)
- Data flow between CTEs and main query

Generates a simple algorithm-based prompt that works better than
rule-based detection (see research/payload_comparison/).
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import Any, Optional

import sqlglot
from sqlglot import exp


@dataclass
class TableScan:
    """A table scan from the execution plan."""
    table: str
    rows_scanned: int
    rows_out: int
    cost_pct: float
    has_filter: bool
    filter_expr: Optional[str] = None

    @property
    def selectivity(self) -> float:
        """Ratio of rows out to rows scanned (lower = more selective)."""
        if self.rows_scanned == 0:
            return 1.0
        return self.rows_out / self.rows_scanned


@dataclass
class JoinInfo:
    """A join operation from the plan."""
    join_type: str  # HASH_JOIN, NESTED_LOOP, etc.
    left_table: str
    right_table: str
    left_rows: int
    right_rows: int
    output_rows: int
    cost_pct: float
    is_late: bool = False  # True if small table joined late


@dataclass
class CTEFlow:
    """Data flow for a CTE."""
    name: str
    input_tables: list[str]
    output_rows: int
    referenced_by: list[str]  # Other CTEs or main query
    has_aggregation: bool


@dataclass
class DataFlow:
    """Data flow representation for patch-based optimization.

    Shows how data moves through the query, making it easier for
    the LLM to identify where patches should be applied.
    """
    ctes: dict[str, CTEFlow] = field(default_factory=dict)
    main_query_tables: list[str] = field(default_factory=list)


@dataclass
class OptimizationContext:
    """All signals extracted for optimization.

    This is the structured output from plan analysis, containing
    everything the LLM needs to optimize the query.
    """
    # From EXPLAIN plan
    total_time_ms: float = 0.0
    bottleneck_operators: list[dict[str, Any]] = field(default_factory=list)
    table_scans: list[TableScan] = field(default_factory=list)
    joins: list[JoinInfo] = field(default_factory=list)
    cardinality_misestimates: list[dict[str, Any]] = field(default_factory=list)

    # From SQL parsing
    data_flow: DataFlow = field(default_factory=DataFlow)

    def get_top_operators(self, n: int = 5) -> list[dict]:
        """Get top N operators by cost."""
        sorted_ops = sorted(
            self.bottleneck_operators,
            key=lambda x: x.get("cost_pct", 0),
            reverse=True
        )
        return sorted_ops[:n]

    def get_large_scans(self, min_rows: int = 100_000) -> list[TableScan]:
        """Get table scans over a row threshold."""
        return [s for s in self.table_scans if s.rows_scanned >= min_rows]

    def get_late_joins(self) -> list[JoinInfo]:
        """Get joins where a small table is joined late."""
        return [j for j in self.joins if j.is_late]


def analyze_plan_for_optimization(
    plan_json: dict[str, Any],
    sql: str,
) -> OptimizationContext:
    """Extract optimization signals from EXPLAIN plan and SQL.

    Args:
        plan_json: DuckDB EXPLAIN (ANALYZE, FORMAT JSON) output
        sql: The original SQL query

    Returns:
        OptimizationContext with all extracted signals
    """
    ctx = OptimizationContext()

    # Extract from plan
    _extract_operators(plan_json, ctx)
    _extract_table_scans(plan_json, ctx)
    _extract_joins(plan_json, ctx)
    _extract_misestimates(plan_json, ctx)

    # Extract data flow from SQL
    _extract_data_flow(sql, ctx)

    return ctx


def _extract_operators(plan_json: dict[str, Any], ctx: OptimizationContext) -> None:
    """Extract operator timing and cost from plan."""
    operators = []
    total_time = 0.0

    def walk(node: dict[str, Any]) -> None:
        nonlocal total_time
        name = node.get("operator_name", node.get("name", "")).strip()
        timing = node.get("operator_timing", 0.0)
        rows = node.get("operator_cardinality", 0)

        if name and name != "EXPLAIN_ANALYZE":
            total_time += timing
            operators.append({
                "operator": name,
                "time_ms": round(timing * 1000, 1),
                "rows": rows,
            })

        for child in node.get("children", []):
            walk(child)

    for child in plan_json.get("children", []):
        walk(child)

    ctx.total_time_ms = round(total_time * 1000, 1)

    # Calculate cost percentages
    for op in operators:
        if total_time > 0:
            op["cost_pct"] = round(op["time_ms"] / (total_time * 1000) * 100, 1)
        else:
            op["cost_pct"] = 0

    # Sort by cost
    operators.sort(key=lambda x: x["cost_pct"], reverse=True)
    ctx.bottleneck_operators = operators


def _extract_table_scans(plan_json: dict[str, Any], ctx: OptimizationContext) -> None:
    """Extract table scan information."""

    def walk(node: dict[str, Any]) -> None:
        name = node.get("operator_name", node.get("name", "")).strip()

        if "SCAN" in name.upper():
            extra_info = node.get("extra_info", {})
            if isinstance(extra_info, str):
                extra_info = {}

            table = extra_info.get("Table", name.replace("SEQ_SCAN", "").strip())
            rows_scanned = node.get("operator_rows_scanned", 0)
            if rows_scanned == 0:
                rows_scanned = node.get("operator_cardinality", 0)
            rows_out = node.get("operator_cardinality", 0)
            timing = node.get("operator_timing", 0)

            # Check for filters
            has_filter = bool(extra_info.get("Filters"))
            filter_expr = extra_info.get("Filters")

            # Calculate cost percentage (will be updated later)
            cost_pct = 0.0
            if ctx.total_time_ms > 0:
                cost_pct = round(timing * 1000 / ctx.total_time_ms * 100, 1)

            ctx.table_scans.append(TableScan(
                table=table,
                rows_scanned=rows_scanned,
                rows_out=rows_out,
                cost_pct=cost_pct,
                has_filter=has_filter,
                filter_expr=filter_expr,
            ))

        for child in node.get("children", []):
            walk(child)

    for child in plan_json.get("children", []):
        walk(child)


def _extract_joins(plan_json: dict[str, Any], ctx: OptimizationContext) -> None:
    """Extract join information."""

    def walk(node: dict[str, Any], parent_rows: int = 0) -> int:
        name = node.get("operator_name", node.get("name", "")).strip()
        rows = node.get("operator_cardinality", 0)

        if "JOIN" in name.upper():
            children = node.get("children", [])
            left_rows = 0
            right_rows = 0
            left_table = "?"
            right_table = "?"

            if len(children) >= 2:
                left_rows = children[0].get("operator_cardinality", 0)
                right_rows = children[1].get("operator_cardinality", 0)

                # Try to get table names from children
                left_extra = children[0].get("extra_info", {})
                right_extra = children[1].get("extra_info", {})
                if isinstance(left_extra, dict):
                    left_table = left_extra.get("Table", children[0].get("name", "?"))
                if isinstance(right_extra, dict):
                    right_table = right_extra.get("Table", children[1].get("name", "?"))

            timing = node.get("operator_timing", 0)
            cost_pct = 0.0
            if ctx.total_time_ms > 0:
                cost_pct = round(timing * 1000 / ctx.total_time_ms * 100, 1)

            # Detect late joins: small table joined after large aggregation
            is_late = (
                right_rows < 1000 and left_rows > 100_000
            ) or (
                left_rows < 1000 and right_rows > 100_000
            )

            ctx.joins.append(JoinInfo(
                join_type=name,
                left_table=left_table,
                right_table=right_table,
                left_rows=left_rows,
                right_rows=right_rows,
                output_rows=rows,
                cost_pct=cost_pct,
                is_late=is_late,
            ))

        for child in node.get("children", []):
            walk(child, rows)

        return rows

    for child in plan_json.get("children", []):
        walk(child)


def _extract_misestimates(plan_json: dict[str, Any], ctx: OptimizationContext) -> None:
    """Extract cardinality misestimates."""

    def walk(node: dict[str, Any]) -> None:
        extra_info = node.get("extra_info", {})
        if not isinstance(extra_info, dict):
            return

        est_str = extra_info.get("Estimated Cardinality", "")
        if not est_str:
            for child in node.get("children", []):
                walk(child)
            return

        try:
            estimated = int(str(est_str).lstrip("~"))
            actual = node.get("operator_cardinality", 0)

            if max(estimated, actual) >= 1000:
                ratio = max(estimated, actual) / max(min(estimated, actual), 1)
                if ratio >= 5.0:
                    name = node.get("operator_name", node.get("name", "")).strip()
                    ctx.cardinality_misestimates.append({
                        "operator": name,
                        "estimated": estimated,
                        "actual": actual,
                        "ratio": round(ratio, 1),
                    })
        except (ValueError, TypeError):
            pass

        for child in node.get("children", []):
            walk(child)

    for child in plan_json.get("children", []):
        walk(child)


def _extract_data_flow(sql: str, ctx: OptimizationContext) -> None:
    """Extract data flow from SQL using sqlglot.

    Just the structure - CTEs and what tables they use.
    No prescriptive analysis.
    """
    try:
        parsed = sqlglot.parse_one(sql)
    except Exception:
        return

    data_flow = DataFlow()

    # Extract CTEs
    for cte in parsed.find_all(exp.CTE):
        cte_name = cte.alias
        if not cte_name:
            continue

        # Find tables used in CTE
        input_tables = []
        for table in cte.find_all(exp.Table):
            table_name = table.name
            if table_name:
                input_tables.append(table_name)

        # Check for aggregation
        has_agg = bool(cte.find(exp.Group))

        # Estimate output rows from plan if available
        output_rows = 0
        for scan in ctx.table_scans:
            if scan.table.lower() == cte_name.lower():
                output_rows = scan.rows_out
                break

        data_flow.ctes[cte_name] = CTEFlow(
            name=cte_name,
            input_tables=input_tables,
            output_rows=output_rows,
            referenced_by=[],
            has_aggregation=has_agg,
        )

    # Find tables in main query (after WITH)
    main_select = parsed.find(exp.Select)
    if main_select:
        for table in main_select.find_all(exp.Table):
            table_name = table.name
            if table_name and table_name not in data_flow.ctes:
                data_flow.main_query_tables.append(table_name)

    ctx.data_flow = data_flow




def build_optimization_prompt(
    sql: str,
    ctx: OptimizationContext,
    output_format: str = "patches",
) -> str:
    """Build the optimization prompt.

    Structure (all repeatable/automatable):
    1. Algorithm - generic 3-step process, works for any query
    2. Plan - parsed from EXPLAIN, automated Python process
    3. Data Flow - parsed from AST via sqlglot, automated
    4. SQL - the query
    5. Patch Spec - output format

    Args:
        sql: Original SQL query
        ctx: OptimizationContext from analyze_plan_for_optimization
        output_format: "patches" for JSON patches, "sql" for full rewrite

    Returns:
        Complete optimization prompt
    """
    lines = []

    # =========================================
    # 1. ALGORITHM (generic, works for any query)
    # =========================================
    lines.extend([
        "Optimize this SQL query.",
        "",
        "## Algorithm",
        "",
        "1. ANALYZE: Find where rows/cost are largest in the plan.",
        "2. OPTIMIZE: For each bottleneck, ask \"what could reduce it earlier?\"",
        "   - Can a filter be pushed inside a CTE instead of applied after?",
        "   - Can a small table join happen inside an aggregation to filter before GROUP BY?",
        "   - Is there a correlated subquery? Convert to CTE + JOIN.",
        "3. VERIFY: Result must be semantically equivalent.",
        "",
        "Principle: Reduce rows as early as possible.",
        "",
    ])

    # =========================================
    # 2. PLAN (from EXPLAIN - Python automated)
    # =========================================
    lines.append("## Plan")
    lines.append("")

    # Top operators by cost
    top_ops = ctx.get_top_operators(5)
    if top_ops:
        lines.append("Operators by cost:")
        for op in top_ops:
            lines.append(f"- {op['operator']}: {op['cost_pct']}% cost, {op['rows']:,} rows")
        lines.append("")

    # Table scans with filter status
    if ctx.table_scans:
        lines.append("Scans:")
        for scan in ctx.table_scans:
            if scan.has_filter:
                lines.append(f"- {scan.table}: {scan.rows_scanned:,} → {scan.rows_out:,} rows (filtered)")
            else:
                lines.append(f"- {scan.table}: {scan.rows_scanned:,} rows (no filter)")
        lines.append("")

    # Cardinality misestimates (if any significant ones)
    if ctx.cardinality_misestimates:
        lines.append("Misestimates:")
        for mis in ctx.cardinality_misestimates:
            lines.append(f"- {mis['operator']}: est {mis['estimated']:,} vs actual {mis['actual']:,} ({mis['ratio']}x)")
        lines.append("")

    # =========================================
    # 3. DATA FLOW (from AST - sqlglot automated)
    # =========================================
    if ctx.data_flow.ctes or ctx.data_flow.main_query_tables:
        lines.append("## Data Flow")
        lines.append("")

        if ctx.data_flow.ctes:
            for name, cte in ctx.data_flow.ctes.items():
                inputs = ", ".join(cte.input_tables) if cte.input_tables else "?"
                agg = " → GROUP BY" if cte.has_aggregation else ""
                rows = f" → {cte.output_rows:,} rows" if cte.output_rows else ""
                lines.append(f"- CTE {name}: [{inputs}]{agg}{rows}")

        if ctx.data_flow.main_query_tables:
            tables = ", ".join(ctx.data_flow.main_query_tables)
            lines.append(f"- Main query: [{tables}]")

        lines.append("")

    # =========================================
    # 4. SQL
    # =========================================
    lines.extend([
        "## SQL",
        "",
        "```sql",
        sql.strip(),
        "```",
        "",
    ])

    # =========================================
    # 5. PATCH SPEC (output format)
    # =========================================
    if output_format == "patches":
        lines.extend([
            "## Output",
            "",
            "Return JSON:",
            "```json",
            "{",
            '  "patches": [',
            '    {"search": "exact text from SQL", "replace": "new text", "description": "why"}',
            "  ],",
            '  "explanation": "summary"',
            "}",
            "```",
            "",
            "Rules: search must match EXACTLY (including whitespace), patches apply in order, valid JSON only.",
        ])
    else:
        lines.append("Return optimized SQL only.")

    return "\n".join(lines)


@dataclass
class SQLPatch:
    """A single patch to apply to SQL."""
    search: str
    replace: str
    description: str


@dataclass
class PatchResult:
    """Result of applying patches to SQL."""
    original_sql: str
    optimized_sql: str
    patches_applied: list[SQLPatch]
    patches_failed: list[tuple[SQLPatch, str]]  # (patch, error)
    explanation: str = ""


def _normalize_whitespace(s: str) -> str:
    """Normalize whitespace for fuzzy matching."""
    # Collapse all whitespace to single spaces
    return " ".join(s.split())


def _find_original_span(sql: str, normalized_search: str) -> tuple[int, int] | None:
    """Find the original span in sql that matches normalized_search.

    Returns (start, end) indices or None if not found.
    """
    # Try to find a substring of sql that normalizes to normalized_search
    # Use a sliding window approach
    sql_chars = list(sql)
    n = len(sql)

    # Find potential start points (non-whitespace that matches start of search)
    search_first_word = normalized_search.split()[0] if normalized_search.split() else ""
    if not search_first_word:
        return None

    i = 0
    while i < n:
        # Skip leading whitespace
        while i < n and sql[i].isspace():
            i += 1
        if i >= n:
            break

        # Check if this could be the start
        if sql[i:i+len(search_first_word)] == search_first_word:
            # Try to find the end
            for j in range(i + 1, n + 1):
                candidate = sql[i:j]
                if _normalize_whitespace(candidate) == normalized_search:
                    return (i, j)
                # If normalized is longer than search, stop
                if len(_normalize_whitespace(candidate)) > len(normalized_search):
                    break
        i += 1

    return None


def apply_patches(sql: str, patches_json: dict[str, Any]) -> PatchResult:
    """Apply patches from LLM response to SQL.

    Handles whitespace mismatches by normalizing and finding the
    original span in the SQL.

    Args:
        sql: Original SQL query
        patches_json: Parsed JSON response from LLM with patches

    Returns:
        PatchResult with optimized SQL and patch details
    """
    result = PatchResult(
        original_sql=sql,
        optimized_sql=sql,
        patches_applied=[],
        patches_failed=[],
        explanation=patches_json.get("explanation", ""),
    )

    patches = patches_json.get("patches", [])

    for p in patches:
        search = p.get("search", "")
        replace = p.get("replace", "")
        description = p.get("description", "")

        patch = SQLPatch(search=search, replace=replace, description=description)

        if not search:
            result.patches_failed.append((patch, "Empty search string"))
            continue

        # Try exact match first
        if search in result.optimized_sql:
            result.optimized_sql = result.optimized_sql.replace(search, replace, 1)
            result.patches_applied.append(patch)
            continue

        # Try whitespace-normalized match
        normalized_search = _normalize_whitespace(search)
        span = _find_original_span(result.optimized_sql, normalized_search)

        if span:
            start, end = span
            # Apply patch using original span
            result.optimized_sql = (
                result.optimized_sql[:start] +
                replace +
                result.optimized_sql[end:]
            )
            result.patches_applied.append(patch)
        else:
            result.patches_failed.append((patch, "Search text not found"))

    return result


def parse_llm_response(response: str) -> dict[str, Any]:
    """Parse LLM response to extract patches JSON.

    Handles responses that may include markdown code blocks.

    Args:
        response: Raw LLM response text

    Returns:
        Parsed patches dict
    """
    import json

    text = response.strip()

    # Remove markdown code blocks if present
    if text.startswith("```"):
        # Find the end of the opening fence
        first_newline = text.find("\n")
        if first_newline != -1:
            text = text[first_newline + 1:]
        # Remove closing fence
        if text.endswith("```"):
            text = text[:-3].strip()

    try:
        return json.loads(text)
    except json.JSONDecodeError as e:
        # Try to find JSON object in the response
        start = text.find("{")
        end = text.rfind("}") + 1
        if start != -1 and end > start:
            try:
                return json.loads(text[start:end])
            except json.JSONDecodeError:
                pass

        return {"error": str(e), "patches": []}


