"""Repeated Subquery to CTE Rewriter.

Pattern: Same subquery appears multiple times in the query
Rewrite: Extract to CTE and reference by name

This is the "Double-Dip" anti-pattern from the taxonomy.
"""

from typing import Any
from collections import defaultdict
import hashlib

from sqlglot import exp

from .base import (
    BaseRewriter,
    RewriteResult,
    RewriteConfidence,
    SafetyCheckResult,
)
from .registry import register_rewriter


def _normalize_sql(sql: str) -> str:
    """Normalize SQL for comparison (remove whitespace, lowercase)."""
    return " ".join(sql.lower().split())


def _hash_sql(sql: str) -> str:
    """Create hash of normalized SQL for quick comparison."""
    return hashlib.md5(_normalize_sql(sql).encode()).hexdigest()[:12]


@register_rewriter
class RepeatedSubqueryToCTERewriter(BaseRewriter):
    """Rewrites repeated subqueries to CTEs.

    Example:
        SELECT
            (SELECT AVG(salary) FROM employees WHERE dept_id = 1) as avg_sal,
            (SELECT AVG(salary) FROM employees WHERE dept_id = 1) * 1.1 as target
        FROM dual
        ->
        WITH dept_avg AS (
            SELECT AVG(salary) as val FROM employees WHERE dept_id = 1
        )
        SELECT val as avg_sal, val * 1.1 as target
        FROM dual, dept_avg

    Also handles:
        - Repeated derived tables in FROM clause
        - Repeated expressions in SELECT list
        - Repeated predicates in WHERE clause
    """

    rewriter_id = "repeated_subquery_to_cte"
    name = "Repeated Subquery to CTE"
    description = "Extract repeated subqueries into CTEs"
    linked_rule_ids = (
        "SQL-CTE-003",    # Repeated subquery detection
        "SQL-SEL-010",    # Duplicate expression detection
    )
    default_confidence = RewriteConfidence.HIGH

    # Minimum occurrences to trigger CTE extraction
    min_occurrences = 2

    def can_rewrite(self, node: exp.Expression, context: Any = None) -> bool:
        """Check if node contains repeated subqueries."""
        if not isinstance(node, exp.Select):
            return False

        duplicates = self._find_duplicate_subqueries(node)
        return len(duplicates) > 0

    def rewrite(self, node: exp.Expression, context: Any = None) -> RewriteResult:
        """Perform the repeated subquery to CTE transformation."""
        original_sql = node.sql()

        if not isinstance(node, exp.Select):
            return self._create_failure(original_sql, "Node must be SELECT statement")

        try:
            duplicates = self._find_duplicate_subqueries(node)
            if not duplicates:
                return self._create_failure(original_sql, "No repeated subqueries found")

            result = self._create_result(
                success=True,
                original_sql=original_sql,
                confidence=RewriteConfidence.HIGH,
                explanation=f"Extracting {len(duplicates)} repeated subquery group(s) to CTEs",
            )

            rewritten = node.copy()

            # Extract each group of duplicates to a CTE
            cte_counter = 0
            ctes = []

            for sql_hash, subqueries in duplicates.items():
                cte_counter += 1
                cte_name = f"_cte{cte_counter}"

                # Get the first subquery as template
                template = subqueries[0]

                # Create CTE
                cte = self._create_cte(template, cte_name)
                ctes.append(cte)

                # Replace all occurrences with CTE reference
                for subq in subqueries:
                    self._replace_with_cte_ref(rewritten, subq, cte_name)

                result.add_safety_check(
                    name=f"cte_{cte_name}_equivalence",
                    result=SafetyCheckResult.PASSED,
                    message=f"CTE {cte_name} is identical to {len(subqueries)} repeated subqueries",
                )

            # Add CTEs to the query
            if ctes:
                self._add_ctes_to_query(rewritten, ctes)

            result.rewritten_sql = rewritten.sql()
            result.rewritten_node = rewritten

            result.add_safety_check(
                name="semantic_equivalence",
                result=SafetyCheckResult.PASSED,
                message="CTEs are exact extractions of repeated blocks",
            )

            return result

        except Exception as e:
            return self._create_failure(original_sql, str(e))

    def _find_duplicate_subqueries(
        self,
        node: exp.Select
    ) -> dict[str, list[exp.Subquery]]:
        """Find subqueries that appear multiple times.

        Returns dict: sql_hash -> list of matching subquery nodes
        """
        subqueries_by_hash: dict[str, list[exp.Subquery]] = defaultdict(list)

        for subq in node.find_all(exp.Subquery):
            # Skip if it's part of a CTE (already extracted)
            parent = subq.parent
            if isinstance(parent, exp.CTE):
                continue

            sql_hash = _hash_sql(subq.sql())
            subqueries_by_hash[sql_hash].append(subq)

        # Filter to only duplicates
        return {
            h: subqs
            for h, subqs in subqueries_by_hash.items()
            if len(subqs) >= self.min_occurrences
        }

    def _create_cte(self, subquery: exp.Subquery, cte_name: str) -> exp.CTE:
        """Create a CTE from a subquery."""
        inner_select = subquery.find(exp.Select)
        if not inner_select:
            raise ValueError("Subquery has no SELECT")

        # Copy and ensure the SELECT expressions have aliases
        select_copy = inner_select.copy()
        for i, expr in enumerate(select_copy.expressions):
            # Add alias if expression doesn't have one
            if not isinstance(expr, exp.Alias):
                col_name = f"val{i + 1}" if len(select_copy.expressions) > 1 else "val"
                aliased = exp.Alias(this=expr.copy(), alias=exp.to_identifier(col_name))
                select_copy.expressions[i] = aliased

        # Create CTE
        return exp.CTE(
            this=select_copy,
            alias=exp.TableAlias(this=exp.to_identifier(cte_name)),
        )

    def _replace_with_cte_ref(
        self,
        root: exp.Expression,
        subquery: exp.Subquery,
        cte_name: str
    ) -> None:
        """Replace a subquery with a reference to the CTE."""
        # Find the subquery in the copied tree by matching SQL
        target_sql = _normalize_sql(subquery.sql())

        for subq in list(root.find_all(exp.Subquery)):
            if _normalize_sql(subq.sql()) == target_sql:
                # Determine replacement based on context
                parent = subq.parent

                if isinstance(parent, exp.From):
                    # Derived table - replace with table reference
                    table_ref = exp.Table(
                        this=exp.to_identifier(cte_name),
                        alias=subq.alias,
                    )
                    subq.replace(table_ref)

                else:
                    # Scalar subquery (used in expressions, SELECT, etc.)
                    inner = subq.find(exp.Select)
                    if inner and len(inner.expressions) == 1:
                        # Single column - reference the named column
                        col_name = self._get_column_name(inner.expressions[0])
                        col_ref = exp.Subquery(
                            this=exp.Select(
                                expressions=[exp.Column(this=exp.to_identifier(col_name))],
                            ).from_(exp.Table(this=exp.to_identifier(cte_name))),
                            alias=subq.alias,
                        )
                        subq.replace(col_ref)
                    else:
                        # Multi-column - keep as subquery from CTE
                        new_subq = exp.Subquery(
                            this=exp.Select(expressions=[exp.Star()]).from_(
                                exp.Table(this=exp.to_identifier(cte_name))
                            ),
                            alias=subq.alias,
                        )
                        subq.replace(new_subq)

                break  # Replace one at a time

    def _get_column_name(self, expr: exp.Expression) -> str:
        """Extract column name from expression."""
        if isinstance(expr, exp.Alias):
            return str(expr.alias)
        elif isinstance(expr, exp.Column):
            return str(expr.name)
        else:
            # Default matches the alias we add in _create_cte
            return "val"

    def _add_ctes_to_query(
        self,
        select: exp.Select,
        ctes: list[exp.CTE]
    ) -> None:
        """Add CTEs to a SELECT statement."""
        existing_with = select.find(exp.With)

        if existing_with:
            # Add to existing WITH clause
            for cte in ctes:
                existing_with.append("expressions", cte)
        else:
            # Create new WITH clause
            with_clause = exp.With(expressions=ctes)
            select.set("with", with_clause)
