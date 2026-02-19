"""Pre-process SQL queries into structured analysis for witness row generation.

Extracts joins, filters, aggregates, and column lineage using sqlglot AST.
Outputs a compact text representation that replaces the full 24-table schema dump.
"""

import re
from collections import defaultdict

import sqlglot
from sqlglot import exp


# ---------------------------------------------------------------------------
# Schema: column → type mapping (built from DDL)
# ---------------------------------------------------------------------------

_SCHEMA_TYPES: dict[str, dict[str, str]] = {}


def _build_schema_types(schema_ddl: str) -> dict[str, dict[str, str]]:
    """Parse DDL to build {table: {column: type}} mapping."""
    global _SCHEMA_TYPES
    if _SCHEMA_TYPES:
        return _SCHEMA_TYPES

    for stmt in schema_ddl.strip().split(';'):
        stmt = stmt.strip()
        if not stmt.upper().startswith('CREATE TABLE'):
            continue
        m = re.match(r'CREATE TABLE (\w+)\s*\((.+)\)', stmt, re.DOTALL | re.IGNORECASE)
        if not m:
            continue
        table = m.group(1).lower()
        cols_str = m.group(2)
        cols = {}
        for col_def in cols_str.split(','):
            col_def = col_def.strip()
            parts = col_def.split(None, 2)
            if len(parts) >= 2:
                cols[parts[0].lower()] = parts[1].upper()
        _SCHEMA_TYPES[table] = cols

    return _SCHEMA_TYPES


def _resolve_column_table(col_name: str, tables: set[str],
                          schema: dict[str, dict[str, str]]) -> str | None:
    """Resolve an unqualified column to its table using schema lookup."""
    col_lower = col_name.lower()
    matches = []
    for t in tables:
        if t in schema and col_lower in schema[t]:
            matches.append(t)
    if len(matches) == 1:
        return matches[0]
    # Ambiguous or not found — try TPC-DS prefix convention
    prefix_map = {
        'ss': 'store_sales', 'sr': 'store_returns', 'cs': 'catalog_sales',
        'cr': 'catalog_returns', 'ws': 'web_sales', 'wr': 'web_returns',
        'i': 'item', 'd': 'date_dim', 's': 'store', 'c': 'customer',
        'ca': 'customer_address', 'cd': 'customer_demographics',
        'hd': 'household_demographics', 'ib': 'income_band',
        'cc': 'call_center', 'cp': 'catalog_page', 'p': 'promotion',
        'w': 'warehouse', 'wp': 'web_page', 'sm': 'ship_mode',
        'r': 'reason', 't': 'time_dim', 'inv': 'inventory',
    }
    parts = col_lower.split('_')
    if parts[0] in prefix_map:
        candidate = prefix_map[parts[0]]
        if candidate in tables:
            return candidate
    return matches[0] if matches else None


# ---------------------------------------------------------------------------
# Main analyzer
# ---------------------------------------------------------------------------

class QueryAnalysis:
    """Structured analysis of a SQL query for witness generation."""

    def __init__(self):
        self.tables: dict[str, str] = {}  # alias → real_table_name
        self.joins: list[str] = []  # "table1.col1 → table2.col2"
        self.filters: list[str] = []  # "table.col = 'value'" or "table.col BETWEEN x AND y"
        self.columns_needed: dict[str, dict[str, str]] = defaultdict(dict)  # table → {col: type}
        self.aggregates: list[str] = []  # descriptions of aggregate logic
        self.ctes: list[str] = []  # CTE descriptions
        self.notes: list[str] = []  # special notes (division, correlated subquery, etc.)

    def format(self) -> str:
        """Format as compact text for LLM prompt."""
        lines = []

        lines.append("TABLES:")
        for alias, real in sorted(self.tables.items()):
            if alias == real:
                lines.append(f"  {real}")
            else:
                lines.append(f"  {real} (alias: {alias})")

        if self.joins:
            lines.append("\nJOINS:")
            for j in self.joins:
                lines.append(f"  {j}")

        if self.filters:
            lines.append("\nFILTERS:")
            for f in self.filters:
                lines.append(f"  {f}")

        if self.aggregates:
            lines.append("\nAGGREGATES:")
            for a in self.aggregates:
                lines.append(f"  {a}")

        if self.ctes:
            lines.append("\nCTEs:")
            for c in self.ctes:
                lines.append(f"  {c}")

        if self.notes:
            lines.append("\nNOTES:")
            for n in self.notes:
                lines.append(f"  {n}")

        lines.append("\nCOLUMNS NEEDED (table.column : type):")
        for table in sorted(self.columns_needed):
            cols = self.columns_needed[table]
            col_strs = [f"{c} ({t})" for c, t in sorted(cols.items())]
            lines.append(f"  {table}: {', '.join(col_strs)}")

        return "\n".join(lines)


def analyze_query(sql: str, schema_ddl: str) -> QueryAnalysis:
    """Analyze a SQL query and return structured analysis."""
    schema = _build_schema_types(schema_ddl)
    analysis = QueryAnalysis()

    # Clean SQL
    clean_sql = sql.strip()
    # Remove trailing comments
    clean_lines = [l for l in clean_sql.split('\n') if not l.strip().startswith('--')]
    clean_sql = '\n'.join(clean_lines).strip()
    if clean_sql.endswith(';'):
        clean_sql = clean_sql[:-1]

    try:
        parsed = sqlglot.parse_one(clean_sql, read='duckdb')
    except Exception:
        try:
            parsed = sqlglot.parse_one(clean_sql, read='postgres')
        except Exception as e:
            analysis.notes.append(f"Parse error: {e}")
            return analysis

    # --- Extract tables and aliases ---
    all_tables = set()
    alias_map = {}  # alias → real_name

    for table_node in parsed.find_all(exp.Table):
        real_name = table_node.name.lower()
        alias_node = table_node.args.get("alias")
        alias = alias_node.name.lower() if alias_node else real_name

        # Skip CTE references that aren't real tables
        if real_name in schema:
            all_tables.add(real_name)
            alias_map[alias] = real_name
            analysis.tables[alias] = real_name

    # Also collect CTE names for reference resolution
    cte_names = set()
    for cte in parsed.find_all(exp.CTE):
        cte_alias = cte.args.get("alias")
        if cte_alias:
            cte_names.add(cte_alias.name.lower())

    # Add CTE references to tables dict (for display)
    for table_node in parsed.find_all(exp.Table):
        real_name = table_node.name.lower()
        if real_name in cte_names:
            alias_node = table_node.args.get("alias")
            alias = alias_node.name.lower() if alias_node else real_name
            alias_map[alias] = real_name

    def _resolve_table(col_node: exp.Column) -> str | None:
        """Resolve a column to its table name."""
        table_ref = col_node.table
        if table_ref:
            t = table_ref.lower()
            return alias_map.get(t, t)
        return _resolve_column_table(col_node.name.lower(), all_tables, schema)

    def _add_column(table: str, col: str):
        """Register a column as needed."""
        real_table = alias_map.get(table, table)
        if real_table in schema:
            col_lower = col.lower()
            col_type = schema[real_table].get(col_lower, "?")
            analysis.columns_needed[real_table][col_lower] = col_type

    def _col_ref(col_node: exp.Column) -> str:
        """Get 'table.column' string for a column node."""
        table = _resolve_table(col_node)
        if table:
            _add_column(table, col_node.name)
            return f"{table}.{col_node.name.lower()}"
        return col_node.name.lower()

    def _expr_str(node) -> str:
        """Get a readable string for an expression."""
        if isinstance(node, exp.Column):
            return _col_ref(node)
        if isinstance(node, exp.Literal):
            if node.is_string:
                return f"'{node.this}'"
            return str(node.this)
        if isinstance(node, exp.Neg):
            return f"-{_expr_str(node.this)}"
        return node.sql(dialect='duckdb') if hasattr(node, 'sql') else str(node)

    # --- Walk all predicates ---
    def _process_predicates(where_node):
        """Walk a WHERE/HAVING clause and extract joins + filters."""
        if where_node is None:
            return

        for node in where_node.walk():
            node = node[0] if isinstance(node, tuple) else node

            # EQ: col = col (join) or col = literal (filter)
            if isinstance(node, exp.EQ):
                left, right = node.this, node.expression
                if isinstance(left, exp.Column) and isinstance(right, exp.Column):
                    l_table = _resolve_table(left)
                    r_table = _resolve_table(right)
                    l_ref = _col_ref(left)
                    r_ref = _col_ref(right)
                    if l_table and r_table and l_table != r_table:
                        analysis.joins.append(f"{l_ref} = {r_ref}")
                    else:
                        analysis.filters.append(f"{l_ref} = {r_ref}")
                elif isinstance(left, exp.Column):
                    analysis.filters.append(f"{_col_ref(left)} = {_expr_str(right)}")
                elif isinstance(right, exp.Column):
                    analysis.filters.append(f"{_col_ref(right)} = {_expr_str(left)}")

            # IN: col IN (val1, val2, ...)
            elif isinstance(node, exp.In):
                col = node.this
                if isinstance(col, exp.Column):
                    vals = []
                    exprs = node.expressions
                    if exprs:
                        vals = [_expr_str(v) for v in exprs]
                    analysis.filters.append(f"{_col_ref(col)} IN ({', '.join(vals)})")

            # BETWEEN: col BETWEEN low AND high
            elif isinstance(node, exp.Between):
                col = node.this
                if isinstance(col, exp.Column):
                    low = _expr_str(node.args['low'])
                    high = _expr_str(node.args['high'])
                    analysis.filters.append(f"{_col_ref(col)} BETWEEN {low} AND {high}")
                else:
                    # Expression BETWEEN (e.g. sr_return_amt / sr_return_quantity)
                    expr = _expr_str(col)
                    low = _expr_str(node.args['low'])
                    high = _expr_str(node.args['high'])
                    analysis.filters.append(f"{expr} BETWEEN {low} AND {high}")
                    # Track columns in the expression
                    for sub_col in col.find_all(exp.Column):
                        _col_ref(sub_col)
                    # Note potential division
                    if col.find(exp.Div):
                        analysis.notes.append(
                            f"Division in filter: {expr} — denominator must be > 0"
                        )

            # GT, GTE, LT, LTE with subquery (correlated)
            elif isinstance(node, (exp.GT, exp.GTE, exp.LT, exp.LTE)):
                left, right = node.this, node.expression
                op = {exp.GT: '>', exp.GTE: '>=', exp.LT: '<', exp.LTE: '<='}[type(node)]
                has_subquery = right.find(exp.Select) if right else None
                if has_subquery:
                    # Correlated subquery comparison
                    if isinstance(left, exp.Column):
                        _col_ref(left)
                    agg_desc = _describe_subquery(right)
                    analysis.aggregates.append(
                        f"Correlated: {_expr_str(left)} {op} {agg_desc}"
                    )
                elif isinstance(left, exp.Column):
                    analysis.filters.append(
                        f"{_col_ref(left)} {op} {_expr_str(right)}"
                    )

    def _describe_subquery(node) -> str:
        """Describe a scalar subquery for aggregate section."""
        select_node = node.find(exp.Select)
        if not select_node:
            return _expr_str(node)

        # Get the selected expression
        selects = select_node.expressions
        if selects:
            sel_str = _expr_str(selects[0])
        else:
            sel_str = "?"

        # Find correlation predicate
        where = select_node.find(exp.Where)
        corr_str = ""
        if where:
            for eq in where.find_all(exp.EQ):
                left, right = eq.this, eq.expression
                if isinstance(left, exp.Column) and isinstance(right, exp.Column):
                    corr_str = f" WHERE {_expr_str(left)} = {_expr_str(right)}"
                    break

        return f"(SELECT {sel_str}{corr_str})"

    # --- Process main query ---
    # Walk all SELECT statements (main + subqueries + CTEs)
    for select_node in parsed.find_all(exp.Select):
        where = select_node.find(exp.Where)
        if where:
            _process_predicates(where)

        having = select_node.find(exp.Having)
        if having:
            _process_predicates(having)

    # --- Extract GROUP BY ---
    for group_node in parsed.find_all(exp.Group):
        group_cols = []
        for g_expr in group_node.expressions:
            if isinstance(g_expr, exp.Column):
                group_cols.append(_col_ref(g_expr))
            else:
                group_cols.append(_expr_str(g_expr))
        if group_cols:
            analysis.aggregates.append(f"GROUP BY: {', '.join(group_cols)}")

    # --- Extract aggregate functions ---
    for agg_node in parsed.find_all(exp.AggFunc):
        # Skip if inside a subquery description we already captured
        agg_name = type(agg_node).__name__.upper()
        args = []
        for arg in agg_node.expressions if hasattr(agg_node, 'expressions') else [agg_node.this]:
            if arg:
                if isinstance(arg, exp.Column):
                    args.append(_col_ref(arg))
                else:
                    args.append(_expr_str(arg))
                    for sub_col in arg.find_all(exp.Column):
                        _col_ref(sub_col)
        if not args and agg_node.this:
            if isinstance(agg_node.this, exp.Column):
                args.append(_col_ref(agg_node.this))
            else:
                args.append(_expr_str(agg_node.this))
                for sub_col in agg_node.this.find_all(exp.Column):
                    _col_ref(sub_col)

    # --- Extract SELECT columns (to know what's in the output) ---
    main_select = parsed
    if isinstance(main_select, exp.Select) or main_select.find(exp.Select):
        for sel_expr in (main_select.expressions if isinstance(main_select, exp.Select)
                         else main_select.find(exp.Select).expressions):
            if isinstance(sel_expr, exp.Column):
                _col_ref(sel_expr)
            else:
                for sub_col in sel_expr.find_all(exp.Column):
                    _col_ref(sub_col)

    # --- CTEs ---
    for cte in parsed.find_all(exp.CTE):
        alias = cte.args.get("alias")
        cte_name = alias.name if alias else "?"
        cte_select = cte.this
        if isinstance(cte_select, exp.Select):
            tables_in_cte = [t.name for t in cte_select.find_all(exp.Table)
                             if t.name.lower() in schema]
            # Extract CTE column mappings (alias = source_expr)
            col_mappings = []
            for sel_expr in cte_select.expressions:
                alias_node = sel_expr.args.get("alias")
                if alias_node:
                    out_name = alias_node.name
                    source = _expr_str(sel_expr.this if hasattr(sel_expr, 'this') else sel_expr)
                    col_mappings.append(f"{out_name} = {source}")
                elif isinstance(sel_expr, exp.Column):
                    col_mappings.append(_col_ref(sel_expr))

            group = cte_select.find(exp.Group)
            group_str = ""
            if group:
                gcols = [_expr_str(g) for g in group.expressions]
                group_str = f"\n    GROUP BY: {', '.join(gcols)}"

            # Extract CTE WHERE filters
            cte_where = cte_select.find(exp.Where)
            cte_filters = []
            if cte_where:
                for node_tuple in cte_where.walk():
                    node = node_tuple[0] if isinstance(node_tuple, tuple) else node_tuple
                    if isinstance(node, exp.EQ):
                        left, right = node.this, node.expression
                        if isinstance(left, exp.Column) and isinstance(right, exp.Column):
                            cte_filters.append(f"{_col_ref(left)} = {_col_ref(right)} (JOIN)")
                        elif isinstance(left, exp.Column):
                            cte_filters.append(f"{_col_ref(left)} = {_expr_str(right)}")
                    elif isinstance(node, exp.Between):
                        col = node.this
                        expr = _expr_str(col)
                        low = _expr_str(node.args['low'])
                        high = _expr_str(node.args['high'])
                        cte_filters.append(f"{expr} BETWEEN {low} AND {high}")

            filter_str = ""
            if cte_filters:
                filter_str = "\n    WHERE: " + ", ".join(cte_filters)

            mapping_str = ""
            if col_mappings:
                mapping_str = "\n    OUTPUT: " + ", ".join(col_mappings)

            analysis.ctes.append(
                f"{cte_name}:"
                f"\n    FROM: {', '.join(tables_in_cte)}"
                f"{filter_str}{group_str}{mapping_str}"
            )

    # --- Deduplicate and clean ---
    analysis.joins = list(dict.fromkeys(analysis.joins))
    # Remove self-referential filters (artifacts from correlated subqueries)
    analysis.filters = [f for f in dict.fromkeys(analysis.filters)
                        if not re.match(r'(\S+) = \1$', f)]
    analysis.aggregates = list(dict.fromkeys(analysis.aggregates))

    return analysis


# ---------------------------------------------------------------------------
# Quick test
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    import sys
    from pathlib import Path

    # Load schema DDL from witness_llm.py
    witness_llm_path = Path(__file__).parent / "witness_llm.py"
    schema_ddl = ""
    if witness_llm_path.exists():
        content = witness_llm_path.read_text()
        start = content.find('SCHEMA_DDL = """') + len('SCHEMA_DDL = """')
        end = content.find('""".strip()', start)
        schema_ddl = content[start:end].strip()

    if len(sys.argv) > 1:
        sql = Path(sys.argv[1]).read_text()
    else:
        sql = """
        with customer_total_return as
        (select sr_customer_sk as ctr_customer_sk, sr_store_sk as ctr_store_sk,
        sr_reason_sk as ctr_reason_sk, sum(SR_RETURN_AMT_INC_TAX) as ctr_total_return
        from store_returns, date_dim
        where sr_returned_date_sk = d_date_sk and d_year = 2002
        and sr_return_amt / sr_return_quantity between 108 and 167
        group by sr_customer_sk, sr_store_sk, sr_reason_sk)
        select c_customer_id
        from customer_total_return ctr1, store, customer, customer_demographics
        where ctr1.ctr_total_return > (select avg(ctr_total_return)*1.2
        from customer_total_return ctr2 where ctr1.ctr_store_sk = ctr2.ctr_store_sk)
        and ctr1.ctr_reason_sk BETWEEN 43 AND 46
        and s_store_sk = ctr1.ctr_store_sk and s_state IN ('IL', 'KY', 'TX')
        and ctr1.ctr_customer_sk = c_customer_sk
        and c_current_cdemo_sk = cd_demo_sk
        and cd_marital_status IN ('M', 'M') and cd_education_status IN ('Advanced Degree', 'College')
        and cd_gender = 'F' and c_birth_month = 2 and c_birth_year BETWEEN 1965 AND 1971
        order by c_customer_id limit 100;
        """

    analysis = analyze_query(sql, schema_ddl)
    print(analysis.format())
