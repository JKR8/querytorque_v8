"""Core classes for AST-based SQL anti-pattern detection."""

import re
from abc import ABC, abstractmethod
from collections import defaultdict
from dataclasses import dataclass, field
from typing import Iterator, Optional, TYPE_CHECKING

import sqlglot
from sqlglot import exp


def _strip_ansi_codes(text: str) -> str:
    """Remove ANSI escape codes from text (e.g., [4m, [0m for underline)."""
    return re.sub(r'\x1b\[[0-9;]*m|\[\d+m', '', text)

if TYPE_CHECKING:
    from ..sql_antipattern_detector import SQLIssue


@dataclass
class ASTContext:
    """Tracks context during AST traversal for context-aware detection.

    This enables rules to understand WHERE they are in the query structure,
    preventing false positives like flagging SELECT * inside EXISTS clauses.
    """
    # Context flags - where are we in the AST?
    in_exists: bool = False
    in_subquery: bool = False
    in_cte: bool = False
    in_case: bool = False
    in_window: bool = False
    in_where: bool = False
    in_join_condition: bool = False
    in_having: bool = False
    in_select_list: bool = False
    in_order_by: bool = False
    in_group_by: bool = False
    in_from: bool = False

    # Current scope name for location reporting
    current_scope: str = "__main__"

    # Original SQL text for line number extraction
    sql_text: str = ""

    # Nesting depth tracking
    nesting_depth: int = 0
    subquery_depth: int = 0

    def copy(self) -> "ASTContext":
        """Create a shallow copy for child contexts."""
        return ASTContext(
            in_exists=self.in_exists,
            in_subquery=self.in_subquery,
            in_cte=self.in_cte,
            in_case=self.in_case,
            in_window=self.in_window,
            in_where=self.in_where,
            in_join_condition=self.in_join_condition,
            in_having=self.in_having,
            in_select_list=self.in_select_list,
            in_order_by=self.in_order_by,
            in_group_by=self.in_group_by,
            in_from=self.in_from,
            current_scope=self.current_scope,
            sql_text=self.sql_text,
            nesting_depth=self.nesting_depth,
            subquery_depth=self.subquery_depth,
        )


@dataclass
class RuleMatch:
    """A single match from a detection rule."""
    node: exp.Expression
    context: ASTContext
    message: str = ""
    matched_text: str = ""


class ASTRule(ABC):
    """Base class for AST-based detection rules.

    Subclasses must:
    1. Set class attributes (rule_id, name, severity, etc.)
    2. Set target_node_types for efficient filtering
    3. Implement check() to yield RuleMatch for violations

    Example:
        class SelectStarRule(ASTRule):
            rule_id = "SQL-SEL-001"
            name = "SELECT *"
            target_node_types = (exp.Star,)

            def check(self, node, context):
                if not context.in_exists:
                    yield RuleMatch(node=node, context=context)
    """

    # Rule metadata - override in subclasses
    rule_id: str = ""
    name: str = ""
    severity: str = "medium"  # critical, high, medium, low, info
    category: str = ""
    penalty: int = 0
    description: str = ""
    suggestion: str = ""

    # Dialects this rule applies to - empty tuple means all dialects
    # Examples: ("postgres",), ("duckdb",), ("snowflake", "redshift")
    dialects: tuple[str, ...] = ()

    # Node types this rule examines - used for fast filtering
    target_node_types: tuple[type[exp.Expression], ...] = ()

    def applies_to_dialect(self, dialect: str) -> bool:
        """Check if this rule applies to the given dialect.

        Args:
            dialect: The SQL dialect being analyzed

        Returns:
            True if rule applies to this dialect
        """
        # Empty dialects tuple means rule applies to all dialects
        if not self.dialects:
            return True
        # Check if dialect matches any in the tuple
        dialect_lower = dialect.lower()
        return any(d.lower() == dialect_lower for d in self.dialects)

    @abstractmethod
    def check(self, node: exp.Expression, context: ASTContext) -> Iterator[RuleMatch]:
        """Check if this node violates the rule.

        Args:
            node: The AST node to check
            context: Current traversal context

        Yields:
            RuleMatch for each violation found
        """
        ...

    def should_check(self, node: exp.Expression, context: ASTContext) -> bool:
        """Pre-filter hook for additional filtering beyond target_node_types.

        Override in subclasses for complex filtering logic.
        """
        return True

    def to_issue(self, match: RuleMatch) -> "SQLIssue":
        """Convert a RuleMatch to SQLIssue for output."""
        from ..sql_antipattern_detector import SQLIssue

        line_num = self._get_line_number(match.node, match.context)
        location = f"{match.context.current_scope}:{line_num}" if line_num else match.context.current_scope

        # Get matched text, handling potential errors
        try:
            matched_text = match.matched_text or match.node.sql()[:100]
        except Exception:
            matched_text = str(match.node)[:100]

        return SQLIssue(
            rule_id=self.rule_id,
            name=self.name,
            severity=self.severity,
            category=self.category,
            penalty=self.penalty,
            description=match.message or self.description,
            location=location,
            match=matched_text,
            suggestion=self.suggestion,
        )

    def _get_line_number(self, node: exp.Expression, context: ASTContext) -> Optional[int]:
        """Extract approximate line number from node position."""
        if not context.sql_text:
            return None

        try:
            node_sql = node.sql()[:30]
            pos = context.sql_text.upper().find(node_sql.upper())
            if pos >= 0:
                return context.sql_text[:pos].count('\n') + 1
        except Exception:
            pass

        return None


class ASTDetector:
    """AST-based SQL anti-pattern detector.

    Uses sqlglot to parse SQL into an AST, then traverses the tree
    checking each node against registered rules.

    Benefits over regex:
    - Context awareness (knows if inside EXISTS, comments, strings)
    - Dialect agnostic (sqlglot handles T-SQL, Snowflake, etc.)
    - No false positives from patterns in comments/strings
    - Safe rewriting path for future auto-fix capability

    Example:
        detector = ASTDetector(dialect="snowflake")
        issues = detector.detect("SELECT * FROM users")

        # Include noisy style rules (for code review):
        detector = ASTDetector(dialect="duckdb", include_style=True)
    """

    def __init__(
        self,
        dialect: str = "snowflake",
        rules: Optional[list[ASTRule]] = None,
        include_style: bool = False,
    ):
        """Initialize the AST detector.

        Args:
            dialect: SQL dialect (snowflake, postgres, duckdb, etc.)
            rules: Optional list of rules to use (bypasses registry)
            include_style: Include noisy style rules. Default False for cleaner output.
        """
        self.dialect = dialect
        self.include_style = include_style

        # Load rules from registry if not provided
        if rules is None:
            from .registry import get_rules_for_audit
            all_rules = get_rules_for_audit(include_style=include_style)
        else:
            all_rules = rules

        # Filter rules by dialect - only include rules that apply to this dialect
        self.rules = [r for r in all_rules if r.applies_to_dialect(dialect)]

        # Build index of rules by target node type for O(1) lookup
        self._rules_by_type: dict[type, list[ASTRule]] = defaultdict(list)
        for rule in self.rules:
            for node_type in rule.target_node_types:
                self._rules_by_type[node_type].append(rule)

    def detect(self, sql: str) -> list["SQLIssue"]:
        """Detect anti-patterns in SQL using AST analysis.

        Args:
            sql: Raw SQL query string

        Returns:
            List of detected issues (SQLIssue objects)
        """
        from ..sql_antipattern_detector import SQLIssue

        # Check if SQL is empty/whitespace/comment-only before parsing
        if self._is_empty_or_comment_only(sql):
            return []

        # Parse SQL
        # Map "generic" to None since sqlglot doesn't recognize "generic" as a dialect
        parse_dialect = None if self.dialect.lower() == "generic" else self.dialect
        try:
            parsed = sqlglot.parse_one(sql, dialect=parse_dialect)
        except Exception as e:
            # Parse errors are serious - SQL cannot be analyzed without parsing
            # Strip ANSI escape codes from sqlglot error messages
            error_msg = _strip_ansi_codes(str(e))[:200]
            return [SQLIssue(
                rule_id="SQL-PARSE-001",
                name="Parse Error",
                severity="high",
                category="syntax",
                penalty=30,  # Significant penalty - can't analyze unparseable SQL
                description=f"Could not parse SQL: {error_msg}",
                match=sql[:200] if len(sql) > 200 else sql,
                suggestion="Check SQL syntax for your dialect",
            )]

        # Create root context
        context = ASTContext(sql_text=sql)

        # Traverse and collect issues
        issues = self._traverse(parsed, context)

        # Deduplicate by rule_id + location
        seen = set()
        unique_issues = []
        for issue in issues:
            key = (issue.rule_id, issue.location)
            if key not in seen:
                seen.add(key)
                unique_issues.append(issue)

        return unique_issues

    def _traverse(self, node: exp.Expression, context: ASTContext) -> list["SQLIssue"]:
        """Recursively traverse AST and check rules at each node."""
        issues: list["SQLIssue"] = []

        # Update context based on current node type
        child_context = self._update_context(node, context)

        # Get rules that target this node type
        applicable_rules = self._get_applicable_rules(node)

        # Check each applicable rule
        for rule in applicable_rules:
            if rule.should_check(node, child_context):
                try:
                    for match in rule.check(node, child_context):
                        issues.append(rule.to_issue(match))
                except Exception:
                    # One bad rule shouldn't break detection
                    pass

        # Recurse into children
        for child in node.iter_expressions():
            issues.extend(self._traverse(child, child_context))

        return issues

    def _get_applicable_rules(self, node: exp.Expression) -> list[ASTRule]:
        """Get rules that apply to this node type."""
        node_type = type(node)
        rules = list(self._rules_by_type.get(node_type, []))

        # Also check parent types (inheritance)
        for base_type in node_type.__mro__:
            if base_type in self._rules_by_type and base_type != node_type:
                for rule in self._rules_by_type[base_type]:
                    if rule not in rules:
                        rules.append(rule)

        return rules

    def _update_context(self, node: exp.Expression, parent_context: ASTContext) -> ASTContext:
        """Update context based on current node type."""
        ctx = parent_context.copy()

        # Track context switches based on node type
        if isinstance(node, exp.Exists):
            ctx.in_exists = True
        elif isinstance(node, exp.Subquery):
            ctx.in_subquery = True
            ctx.subquery_depth += 1
        elif isinstance(node, exp.CTE):
            ctx.in_cte = True
            if hasattr(node, 'alias') and node.alias:
                ctx.current_scope = str(node.alias)
        elif isinstance(node, exp.Case):
            ctx.in_case = True
        elif isinstance(node, exp.Window):
            ctx.in_window = True
        elif isinstance(node, exp.Where):
            ctx.in_where = True
        elif isinstance(node, exp.Join):
            ctx.in_join_condition = True
        elif isinstance(node, exp.Having):
            ctx.in_having = True
        elif isinstance(node, exp.Order):
            ctx.in_order_by = True
        elif isinstance(node, exp.Group):
            ctx.in_group_by = True
        elif isinstance(node, exp.From):
            ctx.in_from = True

        ctx.nesting_depth += 1

        return ctx

    def _is_empty_or_comment_only(self, sql: str) -> bool:
        """Check if SQL is empty, whitespace-only, or only contains comments.

        Args:
            sql: Raw SQL string

        Returns:
            True if SQL has no actual statements to parse
        """
        import re

        if not sql or not sql.strip():
            return True

        # Remove single-line comments (-- ...)
        sql_no_single = re.sub(r'--[^\n]*', '', sql)

        # Remove block comments (/* ... */)
        sql_no_comments = re.sub(r'/\*.*?\*/', '', sql_no_single, flags=re.DOTALL)

        # Check if anything remains after removing comments and whitespace
        return not sql_no_comments.strip()
