"""UNION anti-pattern detection rules."""

from typing import Iterator

from sqlglot import exp

from ..base import ASTRule, ASTContext, RuleMatch


class UnionWithoutAllRule(ASTRule):
    """SQL-UNION-001: Detect UNION without ALL.

    UNION deduplicates results, which requires:
    - Sorting all rows from both queries
    - Comparing every row for duplicates
    - Significant CPU and memory overhead

    UNION ALL simply concatenates results - much faster.

    Use Cases:
    - UNION: When you need to remove duplicates (less common)
    - UNION ALL: When duplicates are OK or impossible (more common)

    Detection:
    sqlglot parses UNION as exp.Union with distinct=True|False
    - UNION -> distinct=True (default)
    - UNION ALL -> distinct=False

    False Positive Consideration:
    - Sometimes UNION is intentional for deduplication
    - We flag it as medium severity with suggestion to use ALL if possible
    """

    rule_id = "SQL-UNION-001"
    name = "UNION Without ALL"
    severity = "medium"
    category = "union"
    penalty = 10
    description = "UNION deduplicates results - use UNION ALL if not needed"
    suggestion = "Use UNION ALL if duplicate removal is not required"

    target_node_types = (exp.Union,)

    def check(self, node: exp.Expression, context: ASTContext) -> Iterator[RuleMatch]:
        # Check if this is UNION (distinct=True) vs UNION ALL (distinct=False)
        # In sqlglot, Union has a 'distinct' property

        # Method 1: Check 'distinct' attribute
        if hasattr(node, 'distinct') and node.distinct:
            yield RuleMatch(
                node=node,
                context=context,
                message="UNION removes duplicates - consider UNION ALL if not needed",
                matched_text=self._get_union_text(node),
            )
            return

        # Method 2: For older sqlglot versions, check args
        if node.args.get('distinct', True):  # default is True (UNION)
            # But also check if 'all' is explicitly set
            if not node.args.get('all', False):
                yield RuleMatch(
                    node=node,
                    context=context,
                    message="UNION removes duplicates - consider UNION ALL if not needed",
                    matched_text=self._get_union_text(node),
                )

    def _get_union_text(self, node: exp.Union) -> str:
        """Get a representative text snippet for the UNION."""
        try:
            # Get just the UNION part, not the full query
            sql = node.sql()
            # Find the UNION keyword
            upper = sql.upper()
            pos = upper.find('UNION')
            if pos > 0:
                # Return context around UNION
                start = max(0, pos - 20)
                end = min(len(sql), pos + 30)
                return "..." + sql[start:end] + "..."
            return sql[:80]
        except Exception:
            return "UNION"


class LargeUnionChainRule(ASTRule):
    """SQL-UNION-002: Detect large UNION chains.

    Many UNION branches indicate potential issues:
        SELECT * FROM t1 UNION ALL
        SELECT * FROM t2 UNION ALL
        SELECT * FROM t3 UNION ALL
        ... (many more)

    This often indicates:
    - Should be a partitioned table
    - Dynamic SQL might be cleaner
    - Maintenance nightmare

    Detection:
    - Count UNION operators in query
    - Flag if exceeds threshold
    """

    rule_id = "SQL-UNION-002"
    name = "Large UNION Chain"
    severity = "medium"
    category = "union"
    penalty = 10
    description = "Query has many UNION branches"
    suggestion = "Consider partitioned table or dynamic SQL"

    target_node_types = (exp.Select,)

    WARNING_THRESHOLD = 5
    CRITICAL_THRESHOLD = 10

    def check(self, node: exp.Expression, context: ASTContext) -> Iterator[RuleMatch]:
        # Only check top-level, not within unions
        if context.in_subquery:
            return

        # Count UNION nodes
        union_count = len(list(node.find_all(exp.Union)))

        if union_count >= self.CRITICAL_THRESHOLD:
            yield RuleMatch(
                node=node,
                context=context,
                message=f"{union_count} UNION branches - critical complexity",
                matched_text=f"Query with {union_count} UNIONs",
            )
        elif union_count >= self.WARNING_THRESHOLD:
            yield RuleMatch(
                node=node,
                context=context,
                message=f"{union_count} UNION branches - consider refactoring",
                matched_text=f"Query with {union_count} UNIONs",
            )


class UnionTypeMismatchRule(ASTRule):
    """SQL-UNION-003: Detect potential type mismatches in UNION.

    UNION branches should have matching column types:
        SELECT id, name FROM users      -- id is INT, name is VARCHAR
        UNION ALL
        SELECT code, description FROM items  -- code might be VARCHAR

    Type mismatches cause:
    - Implicit type conversion overhead
    - Unexpected result types
    - Potential data truncation

    Detection:
    - Check if UNION branches have different column patterns
    - Flag when column counts differ or obvious type issues
    - Note: Full type checking requires schema information

    This is a heuristic check - we can detect:
    - Different column counts between branches
    - Mixing literals of different types with columns
    """

    rule_id = "SQL-UNION-003"
    name = "UNION Type Mismatch"
    severity = "medium"
    category = "union"
    penalty = 10
    description = "UNION branches may have type mismatches"
    suggestion = "Ensure all UNION branches have matching column types"

    target_node_types = (exp.Union,)

    def check(self, node: exp.Expression, context: ASTContext) -> Iterator[RuleMatch]:
        # Get left and right branches
        left = node.this
        right = node.expression

        if not (left and right):
            return

        # Check column counts
        left_cols = self._count_select_columns(left)
        right_cols = self._count_select_columns(right)

        if left_cols > 0 and right_cols > 0 and left_cols != right_cols:
            yield RuleMatch(
                node=node,
                context=context,
                message=f"UNION branches have different column counts ({left_cols} vs {right_cols})",
                matched_text="UNION with column count mismatch",
            )
            return

        # Check for obvious type mixing (number literals vs string literals)
        left_types = self._get_literal_types(left)
        right_types = self._get_literal_types(right)

        for i, (lt, rt) in enumerate(zip(left_types, right_types)):
            if lt and rt and lt != rt:
                yield RuleMatch(
                    node=node,
                    context=context,
                    message=f"Column {i+1}: {lt} vs {rt} type mismatch in UNION",
                    matched_text="UNION with type mismatch",
                )
                return

    def _count_select_columns(self, node: exp.Expression) -> int:
        """Count columns in SELECT clause."""
        if isinstance(node, exp.Select):
            expressions = node.args.get('expressions', [])
            return len(expressions)
        elif isinstance(node, exp.Union):
            # For nested unions, check the left branch
            return self._count_select_columns(node.this)
        return 0

    def _get_literal_types(self, node: exp.Expression) -> list:
        """Get types of literal values in SELECT (if any)."""
        types = []
        if isinstance(node, exp.Select):
            for expr in node.args.get('expressions', []):
                if isinstance(expr, exp.Literal):
                    if expr.is_number:
                        types.append('number')
                    elif expr.is_string:
                        types.append('string')
                    else:
                        types.append(None)
                else:
                    types.append(None)  # Column or expression - can't determine
        return types
