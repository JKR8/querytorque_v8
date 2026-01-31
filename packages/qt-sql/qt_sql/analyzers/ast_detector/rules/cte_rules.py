"""CTE anti-pattern detection rules."""

from typing import Iterator

from sqlglot import exp

from ..base import ASTRule, ASTContext, RuleMatch


class SelectStarInCTERule(ASTRule):
    """SQL-CTE-004: Detect SELECT * in CTE definition.

    SELECT * in CTE passes all columns downstream:
        WITH temp AS (SELECT * FROM users)
        SELECT id FROM temp

    This wastes resources reading unneeded columns and makes
    the query fragile if table schema changes.

    Detection:
    - Find CTE definitions (WITH ... AS (...))
    - Check if the SELECT inside uses *
    """

    rule_id = "SQL-CTE-004"
    name = "SELECT * in CTE"
    severity = "medium"
    category = "cte"
    penalty = 10
    description = "CTE using SELECT * passes all columns downstream"
    suggestion = "Explicitly list needed columns in CTE"

    target_node_types = (exp.CTE,)

    def check(self, node: exp.Expression, context: ASTContext) -> Iterator[RuleMatch]:
        # Get the SELECT inside the CTE
        select = node.this  # CTE.this is the subquery/select
        if not select:
            return

        # Find Star nodes in the SELECT
        for star in select.find_all(exp.Star):
            # Skip if inside COUNT(*) - that's fine in CTEs too
            parent = star.parent
            if isinstance(parent, exp.Count):
                continue

            cte_name = node.alias or "CTE"
            yield RuleMatch(
                node=star,
                context=context,
                message=f"SELECT * in CTE '{cte_name}' - list columns explicitly",
                matched_text=f"WITH {cte_name} AS (SELECT * ...)",
            )
            return  # Only report once per CTE


class MultiRefCTERule(ASTRule):
    """SQL-CTE-001: Detect CTE referenced multiple times.

    CTEs referenced multiple times are re-executed each time:
        WITH order_stats AS (SELECT customer_id, SUM(total) ...)
        SELECT * FROM order_stats o1
        JOIN order_stats o2 ON ...  -- Executed again!

    SQL Server does NOT materialize CTEs.

    Better:
        Insert into temp table and reference that.

    Detection:
    - Get CTE names defined in query
    - Count references to each CTE name in query body
    - Flag if referenced more than once
    """

    rule_id = "SQL-CTE-001"
    name = "CTE Referenced Multiple Times"
    severity = "high"
    category = "cte"
    penalty = 15
    description = "CTE re-executes on each reference - use temp table"
    suggestion = "Materialize to temp table if referenced multiple times"

    target_node_types = (exp.With,)

    def check(self, node: exp.Expression, context: ASTContext) -> Iterator[RuleMatch]:
        # Get CTE names
        cte_names = {}
        for cte in node.find_all(exp.CTE):
            name = str(cte.alias).lower() if cte.alias else ""
            if name:
                cte_names[name] = 0

        if not cte_names:
            return

        # Find parent SELECT (the main query using the CTEs)
        parent = node.parent
        if not parent:
            return

        # Count references to each CTE
        for table in parent.find_all(exp.Table):
            table_name = str(table.this).lower() if table.this else ""
            if table_name in cte_names:
                cte_names[table_name] += 1

        # Flag CTEs referenced more than once
        for name, count in cte_names.items():
            if count > 1:
                yield RuleMatch(
                    node=node,
                    context=context,
                    message=f"CTE '{name}' referenced {count} times - re-executes each time",
                    matched_text=f"WITH {name} AS (...) -- used {count}x",
                )
                return  # Only report once


class RecursiveCTERule(ASTRule):
    """SQL-CTE-002: Detect recursive CTE without clear termination.

    Recursive CTEs without proper termination can cause infinite loops:
        WITH RECURSIVE tree AS (
            SELECT id, parent_id FROM items WHERE parent_id IS NULL
            UNION ALL
            SELECT i.id, i.parent_id FROM items i
            JOIN tree t ON i.parent_id = t.id
            -- No termination condition!
        )

    Detection:
    - Find recursive CTEs (WITH RECURSIVE or self-referencing CTE)
    - Check for termination condition in recursive member
    """

    rule_id = "SQL-CTE-002"
    name = "Recursive CTE"
    severity = "critical"
    category = "cte"
    penalty = 20
    description = "Recursive CTE - verify termination condition"
    suggestion = "Add MAXRECURSION hint and verify termination logic"

    target_node_types = (exp.With,)

    def check(self, node: exp.Expression, context: ASTContext) -> Iterator[RuleMatch]:
        # Check for recursive flag
        if node.args.get('recursive'):
            yield RuleMatch(
                node=node,
                context=context,
                message="Recursive CTE - verify termination condition",
                matched_text="WITH RECURSIVE ...",
            )
            return

        # Check for self-referencing CTE (non-RECURSIVE keyword)
        for cte in node.find_all(exp.CTE):
            cte_name = str(cte.alias).lower() if cte.alias else ""
            if not cte_name:
                continue

            # Check if CTE references itself
            for table in cte.find_all(exp.Table):
                if str(table.this).lower() == cte_name:
                    yield RuleMatch(
                        node=cte,
                        context=context,
                        message=f"CTE '{cte_name}' is self-referencing - verify termination",
                        matched_text=f"WITH {cte_name} AS (... {cte_name} ...)",
                    )
                    return


class DeeplyNestedCTERule(ASTRule):
    """SQL-CTE-003: Detect deeply nested/chained CTEs.

    Many chained CTEs are hard to maintain:
        WITH a AS (...),
             b AS (SELECT * FROM a ...),
             c AS (SELECT * FROM b ...),
             d AS (SELECT * FROM c ...),
             e AS (SELECT * FROM d ...)
        SELECT * FROM e

    Detection:
    - Count CTE definitions in WITH clause
    - Flag if exceeds threshold
    """

    rule_id = "SQL-CTE-003"
    name = "Deeply Nested CTEs"
    severity = "medium"
    category = "cte"
    penalty = 10
    description = "Many chained CTEs - consider materializing"
    suggestion = "Materialize intermediate CTEs to temp tables"

    target_node_types = (exp.With,)

    WARNING_THRESHOLD = 5
    CRITICAL_THRESHOLD = 10

    def check(self, node: exp.Expression, context: ASTContext) -> Iterator[RuleMatch]:
        # Count CTE definitions
        cte_count = len(list(node.find_all(exp.CTE)))

        if cte_count >= self.CRITICAL_THRESHOLD:
            yield RuleMatch(
                node=node,
                context=context,
                message=f"{cte_count} CTEs - critical complexity",
                matched_text=f"WITH {cte_count} CTEs ...",
            )
        elif cte_count >= self.WARNING_THRESHOLD:
            yield RuleMatch(
                node=node,
                context=context,
                message=f"{cte_count} CTEs - consider materializing",
                matched_text=f"WITH {cte_count} CTEs ...",
            )


class CTEWithAggregateReusedRule(ASTRule):
    """SQL-CTE-006: CTE with aggregation referenced multiple times.

    STRUCTURAL REWRITE: Optimizer may re-evaluate CTE each time it's
    referenced. For expensive aggregations, explicit materialization helps.

    Problem - Aggregate CTE referenced multiple times:
        WITH totals AS (
            SELECT customer_id, SUM(amount) as total
            FROM orders GROUP BY customer_id
        )
        SELECT
            (SELECT COUNT(*) FROM totals WHERE total > 1000) as high_value,
            (SELECT COUNT(*) FROM totals WHERE total <= 1000) as low_value,
            (SELECT AVG(total) FROM totals) as avg_total

    The totals CTE might be computed 3 times. Solutions:

    1. PostgreSQL: WITH totals AS MATERIALIZED (...)

    2. Explicit temp table:
        CREATE TEMP TABLE totals AS
        SELECT customer_id, SUM(amount) as total
        FROM orders GROUP BY customer_id;

    Detection:
    - Find CTE with aggregation
    - Check if referenced multiple times
    """

    rule_id = "SQL-CTE-006"
    name = "Aggregate CTE Reused"
    severity = "medium"
    category = "cte"
    penalty = 10
    description = "Aggregate CTE referenced multiple times - may recompute"
    suggestion = "Use MATERIALIZED hint or temp table for expensive CTEs"

    target_node_types = (exp.With,)

    def check(self, node: exp.Expression, context: ASTContext) -> Iterator[RuleMatch]:
        # Get all CTE definitions
        ctes = list(node.find_all(exp.CTE))

        for cte in ctes:
            cte_name = str(cte.alias).lower() if cte.alias else ""
            if not cte_name:
                continue

            # Check if CTE has aggregation
            inner_select = cte.find(exp.Select)
            if not inner_select:
                continue

            has_aggregate = bool(inner_select.find((exp.Sum, exp.Count, exp.Avg, exp.Min, exp.Max)))
            has_group_by = bool(inner_select.args.get('group'))

            if not (has_aggregate or has_group_by):
                continue

            # Count references to this CTE in main query
            # We need to look at the parent SELECT, not within the WITH
            parent_select = node.parent
            if not isinstance(parent_select, exp.Select):
                continue

            ref_count = 0
            for table_ref in parent_select.find_all(exp.Table):
                if str(table_ref.this).lower() == cte_name:
                    ref_count += 1

            if ref_count >= 2:
                yield RuleMatch(
                    node=cte,
                    context=context,
                    message=f"Aggregate CTE '{cte_name}' referenced {ref_count}x - add MATERIALIZED",
                    matched_text=f"WITH {cte_name} AS (aggregate) used {ref_count}x",
                )


class CTEShouldBeSubqueryRule(ASTRule):
    """SQL-CTE-005: Simple CTE used once - could be inline subquery.

    STRUCTURAL REWRITE: While CTEs improve readability, for simple
    single-use cases, inline subqueries may optimize better.

    Problem - Simple CTE used only once:
        WITH filtered AS (
            SELECT * FROM orders WHERE status = 'complete'
        )
        SELECT * FROM filtered WHERE amount > 100

    Could be simpler as:
        SELECT * FROM orders WHERE status = 'complete' AND amount > 100

    Or if subquery needed:
        SELECT * FROM (
            SELECT * FROM orders WHERE status = 'complete'
        ) t WHERE amount > 100

    Detection:
    - Find CTE referenced only once
    - CTE is simple (no aggregation, just filter)
    """

    rule_id = "SQL-CTE-005"
    name = "Simple CTE Used Once"
    severity = "low"
    category = "cte"
    penalty = 5
    description = "Simple CTE used once - could inline for better optimization"
    suggestion = "Consider inlining simple single-use CTEs"

    target_node_types = (exp.With,)

    def check(self, node: exp.Expression, context: ASTContext) -> Iterator[RuleMatch]:
        ctes = list(node.find_all(exp.CTE))

        for cte in ctes:
            cte_name = str(cte.alias).lower() if cte.alias else ""
            if not cte_name:
                continue

            # Check if CTE is simple (no aggregation, no joins)
            inner_select = cte.find(exp.Select)
            if not inner_select:
                continue

            has_aggregate = bool(inner_select.find((exp.Sum, exp.Count, exp.Avg, exp.Min, exp.Max)))
            has_join = bool(inner_select.find(exp.Join))
            has_group_by = bool(inner_select.args.get('group'))

            if has_aggregate or has_join or has_group_by:
                continue  # Not simple

            # Count references
            parent_select = node.parent
            if not isinstance(parent_select, exp.Select):
                continue

            ref_count = sum(1 for t in parent_select.find_all(exp.Table)
                          if str(t.this).lower() == cte_name)

            if ref_count == 1:
                yield RuleMatch(
                    node=cte,
                    context=context,
                    message=f"Simple CTE '{cte_name}' used once - consider inlining",
                    matched_text=f"WITH {cte_name} (simple, 1 use)",
                )
