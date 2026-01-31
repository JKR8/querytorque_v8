"""Cursor and loop anti-pattern detection rules."""

from typing import Iterator

from sqlglot import exp

from ..base import ASTRule, ASTContext, RuleMatch


class CursorUsageRule(ASTRule):
    """SQL-CURSOR-001: Detect cursor usage.

    Cursors process data row-by-row which is very slow:
        DECLARE my_cursor CURSOR FOR SELECT * FROM users
        FETCH NEXT FROM my_cursor INTO @id

    Row-by-row processing should be replaced with set-based operations.

    Detection:
    - Find cursor-related keywords in the AST
    - sqlglot may parse these as Command or specific dialect nodes

    Note: This is somewhat dialect-specific. T-SQL has explicit cursor
    syntax. Other dialects may have different patterns.
    """

    rule_id = "SQL-CURSOR-001"
    name = "Cursor Usage"
    severity = "critical"
    category = "cursors"
    penalty = 20
    description = "Cursor for row-by-row processing is very slow"
    suggestion = "Rewrite as set-based operation"

    # Try to match cursor-related command nodes
    target_node_types = (exp.Command,)

    def check(self, node: exp.Expression, context: ASTContext) -> Iterator[RuleMatch]:
        # Get command text
        cmd_text = ""
        if hasattr(node, 'this'):
            cmd_text = str(node.this).upper()
        if hasattr(node, 'sql'):
            cmd_text = node.sql().upper()

        # Check for cursor keywords
        cursor_keywords = ['CURSOR', 'FETCH', 'DEALLOCATE', 'CLOSE']
        for keyword in cursor_keywords:
            if keyword in cmd_text:
                yield RuleMatch(
                    node=node,
                    context=context,
                    message=f"Cursor operation ({keyword}) - row-by-row processing",
                    matched_text=cmd_text[:60],
                )
                return


class WhileLoopRule(ASTRule):
    """SQL-CURSOR-002: Detect WHILE loop with queries.

    WHILE loops with queries inside process row-by-row:
        WHILE @i < 100 BEGIN
            UPDATE users SET processed = 1 WHERE id = @i
            SET @i = @i + 1
        END

    This should be rewritten as a single set-based UPDATE.

    Detection:
    - Find WHILE loop constructs
    - Check if they contain SELECT/UPDATE/INSERT/DELETE

    Note: This is T-SQL specific. Other dialects have different loop syntax.
    """

    rule_id = "SQL-CURSOR-002"
    name = "WHILE Loop with Queries"
    severity = "critical"
    category = "cursors"
    penalty = 20
    description = "WHILE loop with queries processes row-by-row"
    suggestion = "Rewrite as set-based operation"

    target_node_types = (exp.Command,)

    def check(self, node: exp.Expression, context: ASTContext) -> Iterator[RuleMatch]:
        # Get command text
        cmd_text = ""
        if hasattr(node, 'sql'):
            cmd_text = node.sql().upper()
        elif hasattr(node, 'this'):
            cmd_text = str(node.this).upper()

        # Check for WHILE with query operations
        if 'WHILE' not in cmd_text:
            return

        query_keywords = ['SELECT', 'UPDATE', 'INSERT', 'DELETE']
        for keyword in query_keywords:
            if keyword in cmd_text:
                yield RuleMatch(
                    node=node,
                    context=context,
                    message=f"WHILE loop with {keyword} - row-by-row processing",
                    matched_text=cmd_text[:80],
                )
                return


class DynamicSQLRule(ASTRule):
    """SQL-CURSOR-003: Detect dynamic SQL execution.

    Dynamic SQL prevents plan caching and is injection-prone:
        EXEC sp_executesql @sql
        EXEC(@query)

    Issues:
    - No query plan caching
    - Harder to analyze/optimize
    - SQL injection risk

    Detection:
    - Find EXEC with string variable
    - Find sp_executesql calls
    """

    rule_id = "SQL-CURSOR-003"
    name = "Dynamic SQL Execution"
    severity = "high"
    category = "cursors"
    penalty = 15
    description = "Dynamic SQL prevents plan caching and is injection risk"
    suggestion = "Use parameterized queries or static SQL where possible"

    target_node_types = (exp.Command, exp.Anonymous)

    def check(self, node: exp.Expression, context: ASTContext) -> Iterator[RuleMatch]:
        node_sql = ""
        if hasattr(node, 'sql'):
            node_sql = node.sql().upper()
        elif hasattr(node, 'this'):
            node_sql = str(node.this).upper()

        # Check for dynamic SQL patterns
        dynamic_patterns = ['SP_EXECUTESQL', 'EXEC(', 'EXECUTE(']
        for pattern in dynamic_patterns:
            if pattern in node_sql:
                yield RuleMatch(
                    node=node,
                    context=context,
                    message="Dynamic SQL execution - no plan caching",
                    matched_text=node_sql[:60],
                )
                return
