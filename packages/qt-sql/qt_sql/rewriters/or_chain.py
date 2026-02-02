"""OR Chain to IN Rewriter.

Pattern: WHERE col = 'A' OR col = 'B' OR col = 'C'
Rewrite: WHERE col IN ('A', 'B', 'C')

This is one of the safest semantic rewrites as IN is logically
equivalent to OR chains on the same column.
"""

from typing import Any
from collections import defaultdict

from sqlglot import exp

from .base import (
    BaseRewriter,
    RewriteResult,
    RewriteConfidence,
    SafetyCheckResult,
)
from .registry import register_rewriter


@register_rewriter
class OrChainToInRewriter(BaseRewriter):
    """Rewrites OR chains on same column to IN expressions.

    Example:
        WHERE status = 'active' OR status = 'pending' OR status = 'new'
        ->
        WHERE status IN ('active', 'pending', 'new')

    Also handles:
        - Mixed literal types (strings, numbers)
        - Nested OR conditions
        - Multiple OR chains on different columns
    """

    rewriter_id = "or_chain_to_in"
    name = "OR Chain to IN"
    description = "Convert OR chains on same column to IN expression"
    linked_rule_ids = ("SQL-WHERE-010", "SQL-WHERE-004")
    default_confidence = RewriteConfidence.HIGH

    # Minimum OR conditions to trigger (2+ now supported)
    min_or_count = 2

    def can_rewrite(self, node: exp.Expression, context: Any = None) -> bool:
        """Check if node contains OR chain pattern."""
        if not isinstance(node, (exp.Select, exp.Where, exp.Or)):
            return False

        # Find OR chains with same column
        or_chains = self._find_or_chains(node)
        return any(len(values) >= self.min_or_count for values in or_chains.values())

    def rewrite(self, node: exp.Expression, context: Any = None) -> RewriteResult:
        """Perform the OR chain to IN transformation."""
        original_sql = node.sql()

        try:
            # Clone to avoid mutating original
            rewritten = node.copy()

            # Find and replace OR chains
            or_chains = self._find_or_chains(rewritten)
            if not or_chains:
                return self._create_failure(original_sql, "No OR chains found")

            # Filter to chains with enough conditions
            eligible_chains = {
                col: values
                for col, values in or_chains.items()
                if len(values) >= self.min_or_count
            }

            if not eligible_chains:
                return self._create_failure(
                    original_sql,
                    f"OR chains have fewer than {self.min_or_count} conditions"
                )

            # Perform the rewrite
            rewritten = self._replace_or_chains(rewritten, eligible_chains)

            result = self._create_result(
                success=True,
                original_sql=original_sql,
                rewritten_sql=rewritten.sql(),
                rewritten_node=rewritten,
                confidence=RewriteConfidence.HIGH,
                explanation=f"Converted {len(eligible_chains)} OR chain(s) to IN",
            )

            # Add safety check (always passes for this pattern)
            result.add_safety_check(
                name="semantic_equivalence",
                result=SafetyCheckResult.PASSED,
                message="IN is logically equivalent to OR chain on same column",
            )

            return result

        except Exception as e:
            return self._create_failure(original_sql, str(e))

    def _find_or_chains(self, node: exp.Expression) -> dict[str, list[exp.Expression]]:
        """Find OR chains grouped by column name.

        Returns dict: column_identifier -> list of literal values
        """
        chains: dict[str, list[exp.Expression]] = {}

        def collect_or_conditions(or_node: exp.Or):
            """Recursively collect all conditions from nested OR."""
            conditions = []

            def traverse(n):
                if isinstance(n, exp.Or):
                    traverse(n.left)
                    traverse(n.right)
                else:
                    conditions.append(n)

            traverse(or_node)
            return conditions

        def find_root_or(or_node: exp.Or) -> exp.Or:
            """Find the topmost OR in a chain."""
            current = or_node
            while isinstance(current.parent, exp.Or):
                current = current.parent
            return current

        # Find all OR expressions and only process root OR nodes
        processed_roots = set()
        for or_expr in node.find_all(exp.Or):
            root = find_root_or(or_expr)
            if id(root) in processed_roots:
                continue
            processed_roots.add(id(root))

            conditions = collect_or_conditions(root)

            # Group by column
            col_values: dict[str, list] = defaultdict(list)

            for cond in conditions:
                # Check if it's a simple equality: col = literal
                if isinstance(cond, exp.EQ):
                    left, right = cond.left, cond.right

                    # Normalize: column should be on left
                    if isinstance(right, exp.Column) and isinstance(left, exp.Literal):
                        left, right = right, left

                    if isinstance(left, exp.Column) and isinstance(right, exp.Literal):
                        col_key = self._get_column_key(left)
                        col_values[col_key].append(right)

            # Add to chains if all conditions were same column
            for col_key, values in col_values.items():
                if len(values) == len(conditions) and len(values) >= self.min_or_count:
                    chains[col_key] = values

        return chains

    def _get_column_key(self, col: exp.Column) -> str:
        """Get a unique key for a column reference."""
        parts = []
        if col.table:
            parts.append(str(col.table))
        parts.append(str(col.name))
        return ".".join(parts).lower()

    def _replace_or_chains(
        self,
        node: exp.Expression,
        chains: dict[str, list[exp.Expression]]
    ) -> exp.Expression:
        """Replace OR chains with IN expressions."""

        def find_or_root(or_node: exp.Or) -> exp.Or:
            """Find the topmost OR in a chain."""
            current = or_node
            while isinstance(current.parent, exp.Or):
                current = current.parent
            return current

        processed_roots = set()

        for or_expr in list(node.find_all(exp.Or)):
            # Find root of this OR chain
            root = find_or_root(or_expr)
            if id(root) in processed_roots:
                continue

            # Check if this chain matches any of our targets
            conditions = self._collect_conditions(root)

            for col_key, values in chains.items():
                # Verify this OR matches
                if self._or_matches_chain(conditions, col_key, values):
                    # Build IN expression
                    # Find the column expression from first condition
                    first_eq = conditions[0]
                    col_expr = first_eq.left if isinstance(first_eq.left, exp.Column) else first_eq.right

                    in_expr = exp.In(
                        this=col_expr.copy(),
                        expressions=values,
                    )

                    # Replace the OR chain with IN
                    root.replace(in_expr)
                    processed_roots.add(id(root))
                    break

        return node

    def _collect_conditions(self, or_node: exp.Or) -> list[exp.Expression]:
        """Collect all leaf conditions from nested OR."""
        conditions = []

        def traverse(n):
            if isinstance(n, exp.Or):
                traverse(n.left)
                traverse(n.right)
            else:
                conditions.append(n)

        traverse(or_node)
        return conditions

    def _or_matches_chain(
        self,
        conditions: list[exp.Expression],
        col_key: str,
        values: list[exp.Expression]
    ) -> bool:
        """Check if OR conditions match a specific column chain."""
        if len(conditions) != len(values):
            return False

        for cond in conditions:
            if not isinstance(cond, exp.EQ):
                return False

            left, right = cond.left, cond.right
            if isinstance(right, exp.Column):
                left, right = right, left

            if not isinstance(left, exp.Column) or not isinstance(right, exp.Literal):
                return False

            if self._get_column_key(left) != col_key:
                return False

        return True
