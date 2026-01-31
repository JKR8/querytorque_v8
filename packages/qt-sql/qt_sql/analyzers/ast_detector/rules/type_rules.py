"""Data type anti-pattern detection rules."""

from typing import Iterator

from sqlglot import exp

from ..base import ASTRule, ASTContext, RuleMatch


class StringNumericComparisonRule(ASTRule):
    """SQL-TYPE-001: Detect string comparison on numeric-looking column.

    Comparing what looks like numeric data with strings:
        WHERE account_number = '12345'  -- If account_number is int

    Forces implicit conversion on every row.

    Detection:
    - Find comparisons where column name suggests numeric
    - But value is quoted string of digits
    """

    rule_id = "SQL-TYPE-001"
    name = "String/Numeric Mismatch"
    severity = "medium"
    category = "data_types"
    penalty = 10
    description = "Possible string/numeric type mismatch in comparison"
    suggestion = "Match literal type to column type"

    target_node_types = (exp.EQ,)

    # Column name patterns that suggest numeric type
    NUMERIC_PATTERNS = ('_id', '_num', '_count', '_qty', '_amount', '_total', '_price')

    def check(self, node: exp.Expression, context: ASTContext) -> Iterator[RuleMatch]:
        if not (context.in_where or context.in_join_condition):
            return

        left = node.args.get('this')
        right = node.args.get('expression')

        mismatch = self._check_mismatch(left, right) or self._check_mismatch(right, left)
        if mismatch:
            yield RuleMatch(
                node=node,
                context=context,
                message=mismatch,
                matched_text=node.sql()[:60],
            )

    def _check_mismatch(self, col_side, val_side) -> str:
        """Check if column name suggests numeric but value is string."""
        if not isinstance(col_side, exp.Column):
            return ""
        if not isinstance(val_side, exp.Literal):
            return ""

        col_name = str(col_side.this).lower() if col_side.this else ""

        # Check if column name suggests numeric
        is_numeric_name = any(col_name.endswith(p) for p in self.NUMERIC_PATTERNS)

        # Check if value is quoted string of digits
        if is_numeric_name and val_side.is_string:
            val = str(val_side.this)
            if val.isdigit():
                return f"Column '{col_name}' looks numeric but compared to quoted string"

        return ""


class DateAsStringRule(ASTRule):
    """SQL-TYPE-002: Detect date stored/compared as string.

    Dates stored as strings prevent proper comparison:
        WHERE date_string > '2024-01-01'  -- String comparison!
        WHERE date_string LIKE '2024%'  -- Can't use date indexes

    Detection:
    - Find LIKE patterns on date-looking columns
    - Find string comparisons on date-named columns
    """

    rule_id = "SQL-TYPE-002"
    name = "Date as String"
    severity = "medium"
    category = "data_types"
    penalty = 10
    description = "Date appears to be stored or compared as string"
    suggestion = "Use proper DATE/DATETIME types for date data"

    target_node_types = (exp.Like,)

    # Column name patterns suggesting date
    DATE_PATTERNS = ('_date', '_dt', '_time', 'date_', 'created', 'updated', 'modified')

    def check(self, node: exp.Expression, context: ASTContext) -> Iterator[RuleMatch]:
        # Get column being LIKEd
        col = node.this
        if not isinstance(col, exp.Column):
            return

        col_name = str(col.this).lower() if col.this else ""

        # Check if column name suggests date
        if any(p in col_name for p in self.DATE_PATTERNS):
            # LIKE on date column suggests string storage
            yield RuleMatch(
                node=node,
                context=context,
                message=f"LIKE on date column '{col_name}' - date stored as string?",
                matched_text=node.sql()[:60],
            )


class UnicodeMismatchRule(ASTRule):
    """SQL-TYPE-003: Detect potential Unicode mismatch.

    Comparing Unicode (NVARCHAR) to non-Unicode (VARCHAR):
        WHERE nvarchar_col = 'ascii string'  -- No N prefix
        WHERE varchar_col = N'unicode'  -- N prefix on non-Unicode

    Detection:
    - Find N'...' literals compared to columns
    - Heuristic based on column naming
    """

    rule_id = "SQL-TYPE-003"
    name = "Unicode Mismatch"
    severity = "low"
    category = "data_types"
    penalty = 5
    description = "Possible Unicode/non-Unicode type mismatch"
    suggestion = "Use N prefix for Unicode columns, omit for VARCHAR"

    target_node_types = (exp.EQ,)

    def check(self, node: exp.Expression, context: ASTContext) -> Iterator[RuleMatch]:
        if not context.in_where:
            return

        left = node.args.get('this')
        right = node.args.get('expression')

        # Check for National string literals (N'...')
        for side in [left, right]:
            if isinstance(side, exp.National):
                yield RuleMatch(
                    node=node,
                    context=context,
                    message="N'...' literal - ensure column is NVARCHAR",
                    matched_text=node.sql()[:60],
                )
                return
