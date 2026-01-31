"""Microsoft Fabric/SQL Server specific anti-pattern rules."""

from typing import Iterator

from sqlglot import exp

from ..base import ASTRule, ASTContext, RuleMatch


class TableVariableRule(ASTRule):
    """SQL-FAB-001: Detect table variable usage.

    Table variables have limitations:
        DECLARE @t TABLE (id INT, name VARCHAR(100))
        INSERT INTO @t SELECT * FROM large_table

    Issues:
    - No statistics (always estimates 1 row)
    - Can't add indexes (in older SQL Server)
    - Bad for large datasets

    Detection:
    - Find DECLARE @name TABLE pattern
    """

    rule_id = "SQL-FAB-001"
    name = "Table Variable Usage"
    severity = "medium"
    category = "fabric"
    penalty = 10
    description = "Table variable has no statistics - bad for large data"
    suggestion = "Use temp table (#temp) for large datasets"

    target_node_types = (exp.Command,)

    def check(self, node: exp.Expression, context: ASTContext) -> Iterator[RuleMatch]:
        node_sql = ""
        if hasattr(node, 'sql'):
            node_sql = node.sql().upper()
        elif hasattr(node, 'this'):
            node_sql = str(node.this).upper()

        # Check for table variable declaration
        if 'DECLARE' in node_sql and '@' in node_sql and 'TABLE' in node_sql:
            yield RuleMatch(
                node=node,
                context=context,
                message="Table variable - no statistics, bad for large data",
                matched_text=node_sql[:60],
            )


class TempTableWithoutIndexRule(ASTRule):
    """SQL-FAB-002: Detect temp table creation without indexes.

    Temp tables without indexes can be slow for joins:
        CREATE TABLE #temp (id INT, name VARCHAR(100))
        -- No index on id, then JOIN on id

    Detection:
    - Find CREATE TABLE #name
    - Check if followed by CREATE INDEX (in same batch)

    Note: This is heuristic - can't see full batch context.
    """

    rule_id = "SQL-FAB-002"
    name = "Temp Table Without Index"
    severity = "low"
    category = "fabric"
    penalty = 5
    description = "Temp table may need indexes for efficient joins"
    suggestion = "Add indexes to temp tables used in joins"

    target_node_types = (exp.Create,)

    def check(self, node: exp.Expression, context: ASTContext) -> Iterator[RuleMatch]:
        # Check if this is CREATE TABLE
        kind = node.args.get('kind')
        if str(kind).upper() != 'TABLE':
            return

        # Get table name
        table = node.find(exp.Table)
        if not table:
            return

        table_name = str(table.this) if table.this else ""

        # Check if temp table (starts with # or ##)
        if table_name.startswith('#'):
            yield RuleMatch(
                node=node,
                context=context,
                message=f"Temp table {table_name} - consider adding indexes",
                matched_text=node.sql()[:60],
            )


class MissingOptionRecompileRule(ASTRule):
    """SQL-FAB-003: Detect queries that may benefit from OPTION(RECOMPILE).

    Queries with local variables or skewed data may need recompile:
        SELECT * FROM orders WHERE status = @status
        -- If @status has skewed distribution

    Detection:
    - Find queries with variables in WHERE
    - Suggest OPTION(RECOMPILE) consideration
    """

    rule_id = "SQL-FAB-003"
    name = "Consider OPTION(RECOMPILE)"
    severity = "low"
    category = "fabric"
    penalty = 5
    description = "Query with variables may benefit from OPTION(RECOMPILE)"
    suggestion = "Add OPTION(RECOMPILE) if parameter values vary widely"

    target_node_types = (exp.Select,)

    def check(self, node: exp.Expression, context: ASTContext) -> Iterator[RuleMatch]:
        # Don't flag subqueries
        if context.in_subquery:
            return

        # Find WHERE clause
        where = node.args.get('where')
        if not where:
            return

        # Look for variables (@param) in WHERE
        where_sql = where.sql()
        if '@' in where_sql:
            yield RuleMatch(
                node=node,
                context=context,
                message="Query with variables - consider OPTION(RECOMPILE)",
                matched_text="SELECT ... WHERE @variable ...",
            )
