"""Block Map generator for SQL optimization prompts.

Generates a clause-level view of query blocks (CTEs + main query).
Each block shows its clauses (.select, .from, .where, etc.) with:
- Content summary
- Tables scanned
- CTE references
- Filter status
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import Any, Optional

import sqlglot
from sqlglot import exp


@dataclass
class Clause:
    """A clause within a block."""
    name: str  # .select, .from, .where, .group_by, .having
    content: str  # Summarized content
    tables: list[str] = field(default_factory=list)
    refs: list[str] = field(default_factory=list)  # CTE references
    has_year_filter: bool = False


@dataclass
class Block:
    """A query block (CTE or UNION branch)."""
    name: str
    clauses: dict[str, Clause] = field(default_factory=dict)


@dataclass
class BlockMapResult:
    """Complete Block Map with analysis."""
    blocks: list[Block]
    refs: list[str]  # "block.clause → cte_name" format
    repeated_scans: dict[str, list[str]]  # table -> [block.clause paths]
    filter_gaps: list[str]


def generate_block_map(sql: str) -> BlockMapResult:
    """Generate Block Map from SQL query."""
    try:
        parsed = sqlglot.parse_one(sql)
    except Exception:
        return BlockMapResult([], [], {}, [])

    blocks = []
    cte_names = set()
    table_locations: dict[str, list[str]] = {}  # table -> [block.clause]
    refs_list = []

    # Extract CTEs
    for cte in parsed.find_all(exp.CTE):
        cte_name = cte.alias
        if not cte_name:
            continue
        cte_names.add(cte_name)

    # Process each CTE
    for cte in parsed.find_all(exp.CTE):
        cte_name = cte.alias
        if not cte_name:
            continue

        block = _extract_block_clauses(cte.this, cte_name, cte_names)
        blocks.append(block)

        # Track table locations and refs
        for clause_name, clause in block.clauses.items():
            path = f"{cte_name}{clause_name}"
            for table in clause.tables:
                table_locations.setdefault(table, []).append(path)
            for ref in clause.refs:
                refs_list.append(f"{path} → {ref}")

    # Extract main query
    main_query = _find_main_query(parsed, cte_names)
    if main_query:
        if isinstance(main_query, exp.Union):
            # Handle UNION - extract each branch
            branches = _extract_union_branches(main_query)
            for idx, branch in enumerate(branches):
                block = _extract_block_clauses(branch, f"main_query.union[{idx}]", cte_names)
                blocks.append(block)
                for clause_name, clause in block.clauses.items():
                    path = f"main_query.union[{idx}]{clause_name}"
                    for table in clause.tables:
                        table_locations.setdefault(table, []).append(path)
                    for ref in clause.refs:
                        refs_list.append(f"{path} → {ref}")
        else:
            block = _extract_block_clauses(main_query, "main_query", cte_names)
            blocks.append(block)
            for clause_name, clause in block.clauses.items():
                path = f"main_query{clause_name}"
                for table in clause.tables:
                    table_locations.setdefault(table, []).append(path)
                for ref in clause.refs:
                    refs_list.append(f"{path} → {ref}")

    # Find repeated scans
    repeated_scans = {t: locs for t, locs in table_locations.items() if len(locs) > 1}

    # Detect filter gaps
    filter_gaps = _detect_filter_gaps(blocks, cte_names, table_locations)

    return BlockMapResult(blocks, refs_list, repeated_scans, filter_gaps)


def _extract_block_clauses(node: exp.Expression, block_name: str,
                           cte_names: set[str]) -> Block:
    """Extract clauses from a query node."""
    block = Block(name=block_name)

    # Find the main SELECT (not in subqueries)
    select_node = None
    for sel in node.find_all(exp.Select):
        # Check it's not inside a subquery
        parent = sel.parent
        in_subq = False
        while parent and parent != node:
            if isinstance(parent, exp.Subquery):
                in_subq = True
                break
            parent = parent.parent
        if not in_subq:
            select_node = sel
            break

    if not select_node:
        return block

    # SELECT clause
    select_content = []
    for expr in select_node.expressions[:4]:
        if hasattr(expr, 'alias') and expr.alias:
            select_content.append(expr.alias)
        else:
            select_content.append(str(expr)[:20])
    block.clauses[".select"] = Clause(
        name=".select",
        content=", ".join(select_content)
    )

    # FROM clause
    from_clause = select_node.find(exp.From)
    if from_clause:
        tables = []
        refs = []
        has_subquery = False

        for table in from_clause.find_all(exp.Table):
            tname = table.name
            if tname:
                if tname in cte_names:
                    refs.append(tname)
                else:
                    tables.append(tname)

        # Check for subqueries in FROM
        for subq in from_clause.find_all(exp.Subquery):
            has_subquery = True
            for table in subq.find_all(exp.Table):
                tname = table.name
                if tname and tname not in cte_names and tname not in tables:
                    tables.append(tname)

        content = ", ".join(tables[:3])
        if has_subquery:
            content = f"(subquery: {content})"
        if len(tables) > 3:
            content += f" +{len(tables) - 3}"

        block.clauses[".from"] = Clause(
            name=".from",
            content=content,
            tables=tables,
            refs=refs
        )

    # JOIN clauses (add tables to FROM)
    for join in select_node.find_all(exp.Join):
        for table in join.find_all(exp.Table):
            tname = table.name
            if tname and tname not in cte_names:
                if ".from" in block.clauses:
                    if tname not in block.clauses[".from"].tables:
                        block.clauses[".from"].tables.append(tname)

    # WHERE clause
    where_node = select_node.find(exp.Where)
    if where_node:
        content = _summarize_expression(where_node.this, cte_names)
        refs = []
        has_year = _has_year_filter(where_node.this)

        # Find CTE refs in WHERE (IN subqueries)
        for subq in where_node.find_all(exp.Subquery):
            for table in subq.find_all(exp.Table):
                if table.name in cte_names:
                    refs.append(table.name)

        block.clauses[".where"] = Clause(
            name=".where",
            content=content,
            refs=refs,
            has_year_filter=has_year
        )

    # GROUP BY clause
    group_node = select_node.find(exp.Group)
    if group_node:
        group_cols = []
        for expr in group_node.expressions[:3]:
            if isinstance(expr, exp.Column):
                group_cols.append(expr.name)
            else:
                group_cols.append(str(expr)[:15])
        block.clauses[".group_by"] = Clause(
            name=".group_by",
            content=", ".join(group_cols)
        )

    # HAVING clause
    having_node = select_node.find(exp.Having)
    if having_node:
        content = _summarize_expression(having_node.this, cte_names)
        refs = []
        for subq in having_node.find_all(exp.Subquery):
            for table in subq.find_all(exp.Table):
                if table.name in cte_names:
                    refs.append(table.name)

        block.clauses[".having"] = Clause(
            name=".having",
            content=content,
            refs=refs
        )

    return block


def _find_main_query(parsed: exp.Expression,
                     cte_names: set[str]) -> Optional[exp.Expression]:
    """Find the main query (after WITH clause)."""
    # Look for SELECT or UNION not inside a CTE
    for node in parsed.walk():
        if isinstance(node, (exp.Select, exp.Union)):
            # Check it's not inside a CTE
            parent = node.parent
            in_cte = False
            while parent:
                if isinstance(parent, exp.CTE):
                    in_cte = True
                    break
                parent = parent.parent
            if not in_cte:
                return node
    return None


def _extract_union_branches(union: exp.Union) -> list[exp.Expression]:
    """Extract all branches from a UNION."""
    branches = []

    def collect(node):
        if isinstance(node, exp.Union):
            collect(node.left)
            collect(node.right)
        else:
            branches.append(node)

    collect(union)
    return branches


def _summarize_expression(expr: exp.Expression, cte_names: set[str]) -> str:
    """Summarize an expression for display."""
    text = str(expr)

    # Replace long IN subqueries with IN(cte_name)
    for cte in cte_names:
        pattern = rf"IN\s*\(\s*SELECT.*?FROM\s+{cte}\b.*?\)"
        text = re.sub(pattern, f"IN({cte})", text, flags=re.IGNORECASE | re.DOTALL)

    # Shorten
    if len(text) > 60:
        text = text[:57] + "..."

    return text


def _has_year_filter(expr: exp.Expression) -> bool:
    """Check if expression contains a year filter."""
    text = str(expr).lower()
    return 'd_year' in text or 'year' in text or any(
        str(y) in text for y in range(1998, 2010)
    )


def _detect_filter_gaps(blocks: list[Block], cte_names: set[str],
                        table_locations: dict[str, list[str]]) -> list[str]:
    """Detect filter gaps - tables scanned without year filter."""
    gaps = []

    # Find which blocks have year filters on which tables
    table_year_filter: dict[str, set[str]] = {}  # table -> blocks with year filter
    table_no_year_filter: dict[str, set[str]] = {}  # table -> blocks without

    for block in blocks:
        from_clause = block.clauses.get(".from")
        where_clause = block.clauses.get(".where")

        if not from_clause:
            continue

        has_year = where_clause.has_year_filter if where_clause else False

        for table in from_clause.tables:
            if has_year:
                table_year_filter.setdefault(table, set()).add(block.name)
            else:
                table_no_year_filter.setdefault(table, set()).add(block.name)

    # Find gaps
    for table, no_filter_blocks in table_no_year_filter.items():
        if table not in table_year_filter:
            continue

        filtered_blocks = table_year_filter[table]

        for block_name in no_filter_blocks:
            block = next((b for b in blocks if b.name == block_name), None)
            if not block:
                continue

            # Check if this block references a filtered block
            having = block.clauses.get(".having")
            where = block.clauses.get(".where")

            refs = []
            if having:
                refs.extend(having.refs)
            if where:
                refs.extend(where.refs)

            for ref in refs:
                if ref in filtered_blocks:
                    gaps.append(
                        f"⚠️ {block_name}.from: scans {table} WITHOUT year filter\n"
                        f"     but refs {ref} which HAS year filter"
                    )
                    break

    return gaps


def format_block_map(result: BlockMapResult) -> str:
    """Format Block Map as ASCII table."""
    lines = ["```"]

    # Header
    lines.append("┌" + "─" * 81 + "┐")
    lines.append("│ {:22s} │ {:8s} │ {:45s} │".format(
        "BLOCK", "CLAUSE", "CONTENT SUMMARY"))
    lines.append("├" + "─" * 81 + "┤")

    # Blocks
    for block in result.blocks:
        first_clause = True
        for clause_name, clause in block.clauses.items():
            block_col = block.name if first_clause else ""
            if len(block_col) > 22:
                block_col = block_col[:19] + "..."

            content = clause.content
            if len(content) > 45:
                content = content[:42] + "..."

            lines.append("│ {:22s} │ {:8s} │ {:45s} │".format(
                block_col, clause_name, content))
            first_clause = False

        lines.append("├" + "─" * 81 + "┤")

    # Remove last separator, add footer
    if lines[-1].startswith("├"):
        lines[-1] = "└" + "─" * 81 + "┘"

    lines.append("")

    # Refs
    if result.refs:
        lines.append("Refs:")
        for ref in result.refs:
            lines.append(f"  {ref}")
        lines.append("")

    # Repeated Scans
    if result.repeated_scans:
        lines.append("Repeated Scans:")
        for table, locs in sorted(result.repeated_scans.items(),
                                  key=lambda x: -len(x[1])):
            lines.append(f"  {table}: {len(locs)}× ({', '.join(locs[:3])})")
        lines.append("")

    # Filter Gaps
    if result.filter_gaps:
        lines.append("Filter Gaps:")
        for gap in result.filter_gaps:
            lines.append(f"  {gap}")

    lines.append("```")
    return "\n".join(lines)


def build_full_prompt(sql: str, plan_summary: Optional[dict] = None) -> str:
    """Build complete optimization prompt."""
    result = generate_block_map(sql)
    block_map_text = format_block_map(result)

    lines = [
        "Optimize this SQL query.",
        "",
    ]

    # Plan summary FIRST - this is the most important signal
    if plan_summary:
        lines.extend([
            "## Execution Plan",
            "",
        ])
        if "top_operators" in plan_summary:
            lines.append("**Operators by cost:**")
            for op in plan_summary["top_operators"][:5]:
                table_info = f" ({op['table']})" if op.get('table') and op['table'] != '-' else ""
                lines.append(f"- {op['op']}{table_info}: {op['cost_pct']}% cost, "
                           f"{op.get('rows_scanned', 0) or op.get('rows_out', 0):,} rows")
            lines.append("")

        if "scans" in plan_summary:
            lines.append("**Table scans:**")
            for scan in plan_summary["scans"]:
                table = scan.get("table", "")
                if table.startswith("CTE") or table.startswith("COLUMN") or not table:
                    continue  # Skip internal scans
                rows = scan.get("rows", 0)
                if scan.get("has_filter"):
                    filter_expr = scan.get("filter_expr", "filtered")
                    lines.append(f"- {table}: {rows:,} rows ← FILTERED by {filter_expr}")
                else:
                    lines.append(f"- {table}: {rows:,} rows (NO FILTER)")
            lines.append("")

        if "misestimates" in plan_summary and plan_summary["misestimates"]:
            # Only show top 3 misestimates
            top_misest = sorted(plan_summary["misestimates"],
                              key=lambda x: x.get("ratio", 0), reverse=True)[:3]
            lines.append("**Cardinality misestimates:**")
            for m in top_misest:
                lines.append(f"- {m['op']}: est {m['estimated']:,} vs actual {m['actual']:,} "
                           f"({m['ratio']:.0f}x)")
            lines.append("")

        lines.extend(["---", ""])

    lines.extend([
        "## Block Map",
        block_map_text,
        "",
        "---",
        "",
        "## Optimization Patterns",
        "",
        "These patterns have produced >2x speedups:",
        "",
        "1. **Dimension filter hoisting**: If a filtered dimension is in main_query "
        "but the CTE aggregates fact data that COULD be filtered by it (via FK), "
        "move the dimension join+filter INTO the CTE to filter early.",
        "",
        "2. **Correlated subquery to window function**: A correlated subquery computes "
        "an aggregate per group. Fix: Replace with a window function in the CTE "
        "(e.g., `AVG(...) OVER (PARTITION BY group_col)`).",
        "",
        "3. **Join elimination**: A table is joined only to validate a foreign key "
        "exists, but no columns from it are used. Fix: Remove the join, add "
        "`WHERE fk_column IS NOT NULL`.",
        "",
        "4. **UNION ALL decomposition**: Complex OR conditions cause full scans. "
        "Fix: Split into separate queries with simple filters, UNION ALL results.",
        "",
        "5. **Scan consolidation**: Same table scanned multiple times with different "
        "filters. Fix: Single scan with CASE WHEN expressions to compute multiple "
        "aggregates conditionally.",
        "",
        "**Verify**: Optimized query must return identical results.",
        "",
        "---",
        "",
    ])

    # SQL
    lines.extend([
        "## SQL",
        "```sql",
        sql.strip(),
        "```",
        "",
        "---",
        "",
    ])

    # Output format
    lines.extend([
        "## Output",
        "",
        "Return JSON:",
        "```json",
        "{",
        '  "operations": [...],',
        '  "semantic_warnings": [],',
        '  "explanation": "..."',
        "}",
        "```",
        "",
        "### Operations",
        "",
        "| Op | Fields | Description |",
        "|----|--------|-------------|",
        "| `add_cte` | `after`, `name`, `sql` | Insert new CTE |",
        "| `delete_cte` | `name` | Remove CTE |",
        "| `replace_cte` | `name`, `sql` | Replace entire CTE body |",
        "| `replace_clause` | `target`, `sql` | Replace clause (`\"\"` to remove) |",
        "| `patch` | `target`, `patches[]` | Snippet search/replace |",
        "",
        "### Example",
        "```json",
        "{",
        '  "operations": [',
        '    {"op": "replace_cte", "name": "my_cte", "sql": "SELECT sk, SUM(val) FROM t WHERE sk IS NOT NULL GROUP BY sk"}',
        "  ],",
        '  "semantic_warnings": ["Removed join - added IS NOT NULL to preserve filtering"],',
        '  "explanation": "Removed unnecessary dimension join, using FK directly"',
        "}",
        "```",
        "",
        "### Block ID Syntax",
        "```",
        "{cte}.select    {cte}.from    {cte}.where    {cte}.group_by    {cte}.having",
        "main_query.union[N].select    main_query.union[N].from    ...",
        "```",
        "",
        "### Rules",
        "1. **Return 1-5 operations maximum** - focus on highest-impact changes first",
        "2. Operations apply sequentially",
        "3. `patch.search` must be unique within target clause",
        "4. `add_cte.sql` = query body only (no CTE name)",
        "5. All CTE refs must resolve after ops",
        "6. When removing a join, update column references (e.g., `c_customer_sk` → `ss_customer_sk AS c_customer_sk`)",
        "",
        "The system will iterate if more optimization is possible. You don't need to fix everything at once.",
    ])

    return "\n".join(lines)
