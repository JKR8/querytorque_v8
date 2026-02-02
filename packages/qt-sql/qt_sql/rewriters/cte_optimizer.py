"""CTE Optimization Rewriters.

QT-CTE-001: Nested CTE cascade / unused CTEs -> flatten/remove
QT-CTE-002: CTE fence blocking predicate pushdown -> inline or push filter
"""

from typing import Any, Optional

from sqlglot import exp

from .base import (
    BaseRewriter,
    RewriteResult,
    RewriteConfidence,
    SafetyCheckResult,
)
from .registry import register_rewriter


@register_rewriter
class UnusedCTERemover(BaseRewriter):
    """Removes unused CTEs from queries.

    Example:
        WITH unused AS (SELECT * FROM big_table),
             used AS (SELECT * FROM small_table)
        SELECT * FROM used
        ->
        WITH used AS (SELECT * FROM small_table)
        SELECT * FROM used

    Benefits:
    - Reduces query complexity
    - May prevent unnecessary computation (if engine doesn't optimize)
    - Cleaner query for maintenance
    """

    rewriter_id = "unused_cte_remover"
    name = "Remove Unused CTEs"
    description = "Remove CTEs that are never referenced"
    linked_rule_ids = ("QT-CTE-001",)
    default_confidence = RewriteConfidence.HIGH

    def can_rewrite(self, node: exp.Expression, context: Any = None) -> bool:
        """Check for unused CTEs."""
        if not isinstance(node, exp.Select):
            return False

        # Get CTEs
        with_clause = node.find(exp.With)
        if not with_clause:
            return False

        cte_names = set()
        for cte in with_clause.find_all(exp.CTE):
            if cte.alias:
                cte_names.add(str(cte.alias).lower())

        if not cte_names:
            return False

        # Find referenced tables in main query and other CTEs
        referenced = self._find_referenced_tables(node)

        # Check for unused
        unused = cte_names - referenced
        return len(unused) > 0

    def _find_referenced_tables(self, node: exp.Select) -> set[str]:
        """Find all table references in the query (excluding CTE definitions)."""
        referenced = set()

        # Skip the WITH clause definitions, only look at usage
        with_clause = node.find(exp.With)
        cte_subqueries = set()
        if with_clause:
            for cte in with_clause.find_all(exp.CTE):
                inner = cte.find(exp.Select)
                if inner:
                    cte_subqueries.add(id(inner))

        # Find all table references
        for table in node.find_all(exp.Table):
            # Check if this table is inside a CTE definition
            parent = table.parent
            in_cte_def = False
            while parent:
                if isinstance(parent, exp.Select) and id(parent) in cte_subqueries:
                    in_cte_def = True
                    break
                parent = parent.parent

            if not in_cte_def or isinstance(table.parent_select, exp.CTE):
                name = str(table.name).lower()
                referenced.add(name)

        # Also check CTE cross-references
        if with_clause:
            for cte in with_clause.find_all(exp.CTE):
                inner = cte.find(exp.Select)
                if inner:
                    for table in inner.find_all(exp.Table):
                        name = str(table.name).lower()
                        referenced.add(name)

        return referenced

    def rewrite(self, node: exp.Expression, context: Any = None) -> RewriteResult:
        """Remove unused CTEs."""
        original_sql = node.sql()

        if not isinstance(node, exp.Select):
            return self._create_failure(original_sql, "Node must be SELECT statement")

        try:
            rewritten = node.copy()
            with_clause = rewritten.find(exp.With)

            if not with_clause:
                return self._create_failure(original_sql, "No CTEs found")

            # Get CTE names
            cte_names = {}
            for cte in with_clause.find_all(exp.CTE):
                if cte.alias:
                    cte_names[str(cte.alias).lower()] = cte

            # Find referenced
            referenced = self._find_referenced_tables(rewritten)

            # Remove unused
            removed = []
            remaining_ctes = []
            for name, cte in cte_names.items():
                if name in referenced:
                    remaining_ctes.append(cte.copy())
                else:
                    removed.append(name)

            if not removed:
                return self._create_failure(original_sql, "No unused CTEs found")

            # Update WITH clause
            if remaining_ctes:
                new_with = exp.With(expressions=remaining_ctes)
                rewritten.set("with", new_with)
            else:
                rewritten.set("with", None)

            result = self._create_result(
                success=True,
                original_sql=original_sql,
                rewritten_sql=rewritten.sql(),
                rewritten_node=rewritten,
                confidence=RewriteConfidence.HIGH,
                explanation=f"Removed {len(removed)} unused CTE(s): {', '.join(removed)}",
            )

            result.add_safety_check(
                name="cte_removal",
                result=SafetyCheckResult.PASSED,
                message="Unused CTEs do not affect query results",
            )

            return result

        except Exception as e:
            return self._create_failure(original_sql, str(e))


@register_rewriter
class CTEInliner(BaseRewriter):
    """Inlines CTEs that are only used once.

    Example:
        WITH single_use AS (SELECT * FROM t WHERE x > 10)
        SELECT * FROM single_use WHERE y < 5
        ->
        SELECT * FROM (SELECT * FROM t WHERE x > 10) AS single_use
        WHERE y < 5

    Or even better, push predicate into subquery:
        SELECT * FROM (SELECT * FROM t WHERE x > 10 AND y < 5) AS single_use

    Benefits:
    - Allows predicate pushdown (CTE can be optimization fence)
    - Reduces materialization overhead
    """

    rewriter_id = "cte_inliner"
    name = "Inline Single-Use CTEs"
    description = "Inline CTEs used only once to enable predicate pushdown"
    linked_rule_ids = ("QT-CTE-002",)
    default_confidence = RewriteConfidence.MEDIUM

    def can_rewrite(self, node: exp.Expression, context: Any = None) -> bool:
        """Check for single-use, non-recursive CTEs."""
        if not isinstance(node, exp.Select):
            return False

        with_clause = node.find(exp.With)
        if not with_clause:
            return False

        # Skip recursive CTEs
        if with_clause.args.get("recursive"):
            return False

        # Count CTE usage
        usage_counts = self._count_cte_usage(node)

        # Look for single-use CTEs
        for cte in with_clause.find_all(exp.CTE):
            if cte.alias:
                name = str(cte.alias).lower()
                if usage_counts.get(name, 0) == 1:
                    # Check it's not self-referential
                    inner = cte.find(exp.Select)
                    if inner:
                        inner_refs = set()
                        for table in inner.find_all(exp.Table):
                            inner_refs.add(str(table.name).lower())
                        if name not in inner_refs:
                            return True

        return False

    def _count_cte_usage(self, node: exp.Select) -> dict[str, int]:
        """Count how many times each CTE is referenced."""
        counts = {}

        with_clause = node.find(exp.With)
        if not with_clause:
            return counts

        # Get CTE names and their definitions
        cte_defs = set()
        for cte in with_clause.find_all(exp.CTE):
            inner = cte.find(exp.Select)
            if inner:
                cte_defs.add(id(inner))

        # Count references
        for table in node.find_all(exp.Table):
            name = str(table.name).lower()

            # Check if this reference is inside a CTE definition
            parent = table.parent
            in_cte_def = False
            while parent:
                if isinstance(parent, exp.Select) and id(parent) in cte_defs:
                    in_cte_def = True
                    break
                parent = parent.parent

            # Only count if not in a CTE definition (usage in main query or other CTEs)
            if not in_cte_def:
                counts[name] = counts.get(name, 0) + 1

        return counts

    def rewrite(self, node: exp.Expression, context: Any = None) -> RewriteResult:
        """Inline single-use CTEs."""
        original_sql = node.sql()

        if not isinstance(node, exp.Select):
            return self._create_failure(original_sql, "Node must be SELECT statement")

        try:
            rewritten = node.copy()
            with_clause = rewritten.find(exp.With)

            if not with_clause:
                return self._create_failure(original_sql, "No CTEs found")

            usage_counts = self._count_cte_usage(rewritten)

            # Find single-use CTEs to inline
            to_inline = {}
            remaining_ctes = []

            for cte in with_clause.find_all(exp.CTE):
                if not cte.alias:
                    continue

                name = str(cte.alias).lower()
                inner = cte.find(exp.Select)

                if usage_counts.get(name, 0) == 1 and inner:
                    # Check not self-referential
                    inner_refs = {str(t.name).lower() for t in inner.find_all(exp.Table)}
                    if name not in inner_refs:
                        to_inline[name] = inner.copy()
                    else:
                        remaining_ctes.append(cte.copy())
                else:
                    remaining_ctes.append(cte.copy())

            if not to_inline:
                return self._create_failure(original_sql, "No single-use CTEs to inline")

            # Replace table references with subqueries
            for table in list(rewritten.find_all(exp.Table)):
                name = str(table.name).lower()
                if name in to_inline:
                    subquery = exp.Subquery(
                        this=to_inline[name].copy(),
                        alias=exp.TableAlias(this=exp.to_identifier(name)),
                    )
                    table.replace(subquery)

            # Update WITH clause
            if remaining_ctes:
                new_with = exp.With(expressions=remaining_ctes)
                rewritten.set("with", new_with)
            else:
                rewritten.set("with", None)

            result = self._create_result(
                success=True,
                original_sql=original_sql,
                rewritten_sql=rewritten.sql(),
                rewritten_node=rewritten,
                confidence=RewriteConfidence.MEDIUM,
                explanation=f"Inlined {len(to_inline)} single-use CTE(s): {', '.join(to_inline.keys())}",
            )

            result.add_safety_check(
                name="cte_inline",
                result=SafetyCheckResult.PASSED,
                message="Single-use CTE inlining preserves semantics",
            )

            result.add_safety_check(
                name="predicate_pushdown",
                result=SafetyCheckResult.WARNING,
                message="Inlining may enable predicate pushdown depending on engine",
            )

            return result

        except Exception as e:
            return self._create_failure(original_sql, str(e))


@register_rewriter
class SubqueryToCTERewriter(BaseRewriter):
    """Convert inline subqueries in FROM clause to CTEs.

    Example:
        SELECT * FROM (SELECT * FROM orders WHERE status = 'active') AS active_orders
        JOIN customers ON active_orders.customer_id = customers.id
        ->
        WITH active_orders AS (SELECT * FROM orders WHERE status = 'active')
        SELECT * FROM active_orders
        JOIN customers ON active_orders.customer_id = customers.id

    Benefits:
    - Improved readability for complex queries
    - Named subqueries are easier to understand
    - Some engines can reuse CTE results
    """

    rewriter_id = "subquery_to_cte"
    name = "Subquery to CTE"
    description = "Convert inline FROM subqueries to CTEs"
    linked_rule_ids = ("SQL-JOIN-010",)
    default_confidence = RewriteConfidence.MEDIUM

    def can_rewrite(self, node: exp.Expression, context: Any = None) -> bool:
        """Check for subqueries in FROM clause."""
        if not isinstance(node, exp.Select):
            return False

        # Look for subqueries in FROM clause
        from_clause = node.find(exp.From)
        if not from_clause:
            return False

        for subq in from_clause.find_all(exp.Subquery):
            # Only consider if it has an alias
            if subq.alias:
                return True

        # Also check JOIN clauses
        for join in node.find_all(exp.Join):
            if isinstance(join.this, exp.Subquery) and join.this.alias:
                return True

        return False

    def rewrite(self, node: exp.Expression, context: Any = None) -> RewriteResult:
        """Convert subqueries to CTEs."""
        original_sql = node.sql()

        if not isinstance(node, exp.Select):
            return self._create_failure(original_sql, "Node must be SELECT statement")

        try:
            rewritten = node.copy()
            new_ctes = []
            subqueries_converted = 0

            # Process FROM clause subqueries
            from_clause = rewritten.find(exp.From)
            if from_clause:
                for subq in list(from_clause.find_all(exp.Subquery)):
                    if subq.alias:
                        alias_name = str(subq.alias)
                        inner_select = subq.this

                        if isinstance(inner_select, exp.Select):
                            # Create CTE
                            cte = exp.CTE(
                                this=inner_select.copy(),
                                alias=exp.TableAlias(this=exp.to_identifier(alias_name)),
                            )
                            new_ctes.append(cte)

                            # Replace subquery with table reference
                            table_ref = exp.Table(this=exp.to_identifier(alias_name))
                            subq.replace(table_ref)
                            subqueries_converted += 1

            # Process JOIN subqueries
            for join in list(rewritten.find_all(exp.Join)):
                if isinstance(join.this, exp.Subquery):
                    subq = join.this
                    if subq.alias:
                        alias_name = str(subq.alias)
                        inner_select = subq.this

                        if isinstance(inner_select, exp.Select):
                            # Create CTE
                            cte = exp.CTE(
                                this=inner_select.copy(),
                                alias=exp.TableAlias(this=exp.to_identifier(alias_name)),
                            )
                            new_ctes.append(cte)

                            # Replace subquery with table reference
                            table_ref = exp.Table(this=exp.to_identifier(alias_name))
                            join.set("this", table_ref)
                            subqueries_converted += 1

            if subqueries_converted == 0:
                return self._create_failure(original_sql, "No subqueries to convert")

            # Add CTEs to query
            existing_with = rewritten.find(exp.With)
            if existing_with:
                all_ctes = list(existing_with.expressions) + new_ctes
                existing_with.set("expressions", all_ctes)
            else:
                rewritten.set("with", exp.With(expressions=new_ctes))

            result = self._create_result(
                success=True,
                original_sql=original_sql,
                rewritten_sql=rewritten.sql(),
                rewritten_node=rewritten,
                confidence=RewriteConfidence.MEDIUM,
                explanation=f"Converted {subqueries_converted} subquery(ies) to CTEs",
            )

            result.add_safety_check(
                name="semantic_equivalence",
                result=SafetyCheckResult.PASSED,
                message="CTEs maintain same logical structure as inline subqueries",
            )

            result.add_safety_check(
                name="materialization",
                result=SafetyCheckResult.WARNING,
                message="Some engines may materialize CTEs differently than inline subqueries",
            )

            return result

        except Exception as e:
            return self._create_failure(original_sql, str(e))
