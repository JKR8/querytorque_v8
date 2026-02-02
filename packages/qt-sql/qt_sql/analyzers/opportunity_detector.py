"""Opportunity Detector - High-precision optimization pattern detection.

This module provides the ONLY static detection logic that matters for identifying
optimization opportunities. These patterns are proven winners from TPC-DS benchmarks.

Key design principles:
- HIGH PRECISION: Only detect patterns with proven optimization value
- Structured output: OpportunityResult with actionable rewrite hints
- Pattern IDs match KNOWLEDGE_BASE_PATTERNS for LLM guidance

Extracted from dspy_optimizer.py detect_knowledge_patterns() for shared use
across audit CLI, API, and optimization pipelines.
"""

from dataclasses import dataclass, field
from typing import Iterator, Optional

import sqlglot
from sqlglot import exp


@dataclass
class OpportunityResult:
    """A detected optimization opportunity with actionable guidance.

    Attributes:
        pattern_id: Unique identifier (e.g., "SQL-DUCK-014")
        pattern_name: Human-readable name (e.g., "Grouped TOPN via LATERAL")
        trigger: What pattern was detected in the query
        rewrite_hint: How to rewrite for better performance
        expected_benefit: Expected improvement (e.g., "O(n^2) -> O(n)")
        example: Optional example of the rewrite
    """
    pattern_id: str
    pattern_name: str
    trigger: str
    rewrite_hint: str
    expected_benefit: str = ""
    example: str = ""

    def to_dict(self) -> dict:
        """Convert to dictionary for JSON serialization."""
        return {
            "pattern_id": self.pattern_id,
            "pattern_name": self.pattern_name,
            "trigger": self.trigger,
            "rewrite_hint": self.rewrite_hint,
            "expected_benefit": self.expected_benefit,
            "example": self.example,
        }


def detect_opportunities(sql: str) -> list[OpportunityResult]:
    """Detect KNOWLEDGE_BASE patterns relevant to this query.

    Scans SQL for structural patterns that match proven optimization
    opportunities. Returns structured results with actionable guidance.

    Only detects patterns with HIGH PRECISION - avoids false positives.

    Args:
        sql: SQL query to analyze

    Returns:
        List of OpportunityResult objects for detected patterns
    """
    try:
        parsed = sqlglot.parse_one(sql, error_level=sqlglot.ErrorLevel.IGNORE)
    except Exception:
        return []

    opportunities = []
    sql_lower = sql.lower()

    # Get CTE names to exclude from self-join detection
    cte_names = set()
    for cte in parsed.find_all(exp.CTE):
        if cte.alias:
            cte_names.add(str(cte.alias).lower())

    # SQL-DUCK-014: Grouped TOPN (ROW_NUMBER/RANK with PARTITION BY + outer filter)
    opportunity = _check_grouped_topn(parsed, sql_lower)
    if opportunity:
        opportunities.append(opportunity)

    # SQL-DUCK-012: Window blocks predicate pushdown
    opportunity = _check_window_blocks_pushdown(parsed, sql_lower)
    if opportunity:
        opportunities.append(opportunity)

    # SQL-WHERE-010: OR in WHERE on DIFFERENT columns
    opportunity = _check_or_to_union(parsed)
    if opportunity:
        opportunities.append(opportunity)

    # SQL-JOIN-008: True self-join (same table JOINed to itself)
    # NOTE: Disabled for now due to high false positive rate on TPC-DS queries.
    # The pattern triggers on dimension tables that appear in UNION branches
    # or in multiple joins, which is often intentional in star schema designs.
    # TODO: Re-enable with more precise detection (check for same aliases)
    # opportunity = _check_self_join(parsed, cte_names)
    # if opportunity:
    #     opportunities.append(opportunity)

    # SQL-ORD-005: OFFSET pagination
    opportunity = _check_offset_pagination(parsed)
    if opportunity:
        opportunities.append(opportunity)

    # SQL-AGG-009: COUNT DISTINCT on high-cardinality column
    opportunity = _check_count_distinct(parsed)
    if opportunity:
        opportunities.append(opportunity)

    # SQL-JOIN-011: Triangular join pattern
    opportunity = _check_triangular_join(parsed)
    if opportunity:
        opportunities.append(opportunity)

    return opportunities


def _check_grouped_topn(parsed: exp.Expression, sql_lower: str) -> Optional[OpportunityResult]:
    """Check for Grouped TOPN pattern (ROW_NUMBER/RANK with PARTITION BY + outer filter).

    Pattern: ROW_NUMBER() OVER (PARTITION BY x ORDER BY y) with outer filter rn <= N
    Rewrite: Use LATERAL subquery with LIMIT for early termination
    """
    # Check for ROW_NUMBER
    has_row_number = bool(parsed.find(exp.RowNumber))

    # Check for RANK/DENSE_RANK (may be Anonymous or specific exp type)
    has_rank = False
    for func in parsed.find_all(exp.Anonymous):
        func_name = str(func.this).upper() if func.this else ""
        if func_name in ('RANK', 'DENSE_RANK'):
            has_rank = True
            break

    has_window_rank = has_row_number or has_rank
    has_partition = 'partition by' in sql_lower

    if has_window_rank and has_partition:
        # Check for outer filter on row number (common patterns)
        rank_filter_patterns = ['rn <=', 'rn <', 'row_num <=', 'row_number <=', 'rnk <=', 'rank <=']
        if any(p in sql_lower for p in rank_filter_patterns):
            return OpportunityResult(
                pattern_id="SQL-DUCK-014",
                pattern_name="Grouped TOPN via LATERAL",
                trigger="ROW_NUMBER/RANK with PARTITION BY and outer filter (rn <= N)",
                rewrite_hint="Use LATERAL subquery with LIMIT for early termination",
                expected_benefit="O(n*k) -> O(n) where k is group count",
                example="SELECT * FROM dim, LATERAL (SELECT ... WHERE fk = dim.pk ORDER BY x LIMIT N) sub",
            )
    return None


def _check_window_blocks_pushdown(parsed: exp.Expression, sql_lower: str) -> Optional[OpportunityResult]:
    """Check for Window function blocking predicate pushdown.

    Pattern: Filter outside subquery that contains window function
    Rewrite: Move filter inside subquery before window computation
    """
    for subq in parsed.find_all(exp.Subquery):
        inner = subq.find(exp.Select)
        if inner and inner.find(exp.Window):
            parent_select = subq.find_ancestor(exp.Select)
            if parent_select:
                outer_where = parent_select.find(exp.Where)
                if outer_where:
                    # Check that filter is on a base column, not window result
                    where_sql = outer_where.sql().lower()
                    pushable_columns = ['_date', '_sk', '_id', 'd_year', 'd_moy', 'd_qoy']
                    if any(c in where_sql for c in pushable_columns):
                        return OpportunityResult(
                            pattern_id="SQL-DUCK-012",
                            pattern_name="Window Blocks Predicate Pushdown",
                            trigger="Date/key filter outside subquery with window function",
                            rewrite_hint="Move filter inside subquery before window computation",
                            expected_benefit="1.5-3x speedup by filtering earlier",
                        )
    return None


def _check_or_to_union(parsed: exp.Expression) -> Optional[OpportunityResult]:
    """Check for OR in WHERE on DIFFERENT columns.

    Pattern: WHERE col_a = X OR col_b = Y (different columns)
    Rewrite: Split into UNION ALL of separate queries
    """
    where = parsed.find(exp.Where)
    if not where:
        return None

    or_node = where.find(exp.Or)
    if not or_node:
        return None

    # Check if OR involves different columns (more likely to benefit from UNION)
    left_cols = set()
    right_cols = set()

    if hasattr(or_node, 'left') and or_node.left:
        for c in or_node.left.find_all(exp.Column):
            if c.name:
                left_cols.add(str(c.name).lower())

    if hasattr(or_node, 'right') and or_node.right:
        for c in or_node.right.find_all(exp.Column):
            if c.name:
                right_cols.add(str(c.name).lower())

    # Use .this and .expression for Or node
    if not left_cols and hasattr(or_node, 'this') and or_node.this:
        for c in or_node.this.find_all(exp.Column):
            if c.name:
                left_cols.add(str(c.name).lower())

    if not right_cols and hasattr(or_node, 'expression') and or_node.expression:
        for c in or_node.expression.find_all(exp.Column):
            if c.name:
                right_cols.add(str(c.name).lower())

    if left_cols and right_cols and not left_cols.intersection(right_cols):
        return OpportunityResult(
            pattern_id="SQL-WHERE-010",
            pattern_name="OR to UNION ALL",
            trigger=f"OR condition on different columns ({', '.join(sorted(left_cols)[:2])} vs {', '.join(sorted(right_cols)[:2])})",
            rewrite_hint="Split into UNION ALL of separate queries for better index usage",
            expected_benefit="2-3x speedup with proper index utilization",
        )
    return None


def _check_self_join(parsed: exp.Expression, cte_names: set) -> Optional[OpportunityResult]:
    """Check for true self-join (same table JOINed to itself in same SELECT).

    Pattern: FROM table t1 JOIN table t2 ON ...
    Rewrite: Use LAG/LEAD window functions instead

    Only detects TRUE self-joins where the same table appears multiple times
    in the same FROM clause with aliases. Tables in EXISTS/subqueries don't count.
    """

    def _is_descendant_of_exists_or_subquery(node: exp.Expression, root: exp.Expression) -> bool:
        """Check if node is inside an EXISTS or Subquery that is a descendant of root."""
        current = node.parent
        while current and current != root:
            if isinstance(current, (exp.Exists, exp.Subquery)):
                return True
            current = current.parent
        return False

    for select in parsed.find_all(exp.Select):
        # Skip if this SELECT is part of a UNION
        if select.find_ancestor(exp.Union):
            continue

        # Skip if this SELECT is inside an EXISTS/subquery
        if select.find_ancestor(exp.Exists):
            continue
        if select.find_ancestor(exp.Subquery):
            continue

        # Collect tables that are direct children (not in nested EXISTS/Subquery)
        select_tables = []

        # Get tables from FROM clause - but only if not inside nested EXISTS/Subquery
        from_clause = select.find(exp.From)
        if from_clause:
            for t in from_clause.find_all(exp.Table):
                if _is_descendant_of_exists_or_subquery(t, select):
                    continue
                if t.name and t.name.lower() not in cte_names:
                    select_tables.append(t.name.lower())

        # Get tables from JOINs - but only if not inside nested EXISTS/Subquery
        for join in select.find_all(exp.Join):
            if _is_descendant_of_exists_or_subquery(join, select):
                continue
            for t in join.find_all(exp.Table):
                if _is_descendant_of_exists_or_subquery(t, select):
                    continue
                if t.name and t.name.lower() not in cte_names:
                    select_tables.append(t.name.lower())

        # Check for duplicates within this SELECT (true self-join)
        table_counts = {}
        for t in select_tables:
            table_counts[t] = table_counts.get(t, 0) + 1

        duplicated = [t for t, c in table_counts.items() if c > 1]
        if duplicated:
            return OpportunityResult(
                pattern_id="SQL-JOIN-008",
                pattern_name="Self-Join to Window Function",
                trigger=f"Table '{duplicated[0]}' self-joined in same query",
                rewrite_hint="Use LAG/LEAD window functions instead of self-join",
                expected_benefit="1.5-2x speedup by avoiding extra table scan",
            )
    return None


def _check_offset_pagination(parsed: exp.Expression) -> Optional[OpportunityResult]:
    """Check for OFFSET pagination pattern.

    Pattern: ORDER BY ... LIMIT N OFFSET M
    Rewrite: Use keyset pagination WHERE (col1, col2) > (@last1, @last2)
    """
    if parsed.find(exp.Offset):
        return OpportunityResult(
            pattern_id="SQL-ORD-005",
            pattern_name="OFFSET to Keyset Pagination",
            trigger="OFFSET clause detected",
            rewrite_hint="Use WHERE (col1, col2) > (@last1, @last2) for O(1) seeking",
            expected_benefit="O(n) -> O(1) for deep pagination",
        )
    return None


def _check_count_distinct(parsed: exp.Expression) -> Optional[OpportunityResult]:
    """Check for COUNT DISTINCT on potentially high-cardinality column.

    Pattern: COUNT(DISTINCT column)
    Rewrite: Use APPROX_COUNT_DISTINCT for ~2% error but much faster
    """
    for func in parsed.find_all(exp.Count):
        # Check for DISTINCT - can be in 'distinct' arg or as Distinct wrapper in 'this'
        has_distinct = func.args.get('distinct')
        if not has_distinct and func.this:
            has_distinct = isinstance(func.this, exp.Distinct)
        if has_distinct:
            return OpportunityResult(
                pattern_id="SQL-AGG-009",
                pattern_name="COUNT DISTINCT to Approximate",
                trigger="COUNT(DISTINCT ...) found",
                rewrite_hint="Use APPROX_COUNT_DISTINCT for ~2% error but much faster",
                expected_benefit="5-10x speedup (if exact count not required)",
            )
    return None


def _check_triangular_join(parsed: exp.Expression) -> Optional[OpportunityResult]:
    """Check for triangular join pattern (scalar subquery with aggregate and <= in WHERE).

    Pattern: SELECT col, (SELECT SUM(x) FROM t2 WHERE t2.id <= t1.id) ...
    Rewrite: Use SUM/COUNT OVER (ORDER BY col ROWS UNBOUNDED PRECEDING)
    """
    for select_expr in parsed.find_all(exp.Select):
        if select_expr.find_ancestor(exp.CTE):
            continue  # Skip CTE definitions

        for col_expr in (select_expr.expressions or []):
            subq = col_expr.find(exp.Subquery)
            if subq:
                inner = subq.find(exp.Select)
                if inner:
                    # Must have aggregate
                    has_agg = bool(inner.find(exp.AggFunc))
                    if not has_agg:
                        continue

                    # Must have <= in WHERE (not just any <=)
                    inner_where = inner.find(exp.Where)
                    if inner_where:
                        has_lte_in_where = bool(inner_where.find(exp.LTE)) or bool(inner_where.find(exp.LT))
                        if has_lte_in_where:
                            return OpportunityResult(
                                pattern_id="SQL-JOIN-011",
                                pattern_name="Triangular Join to Window",
                                trigger="Scalar subquery in SELECT with aggregate and <= in WHERE",
                                rewrite_hint="Use SUM/COUNT OVER (ORDER BY col ROWS UNBOUNDED PRECEDING)",
                                expected_benefit="O(n^2) -> O(n)",
                            )
    return None


def format_opportunities_for_llm(opportunities: list[OpportunityResult]) -> str:
    """Format detected opportunities for LLM prompt injection.

    Args:
        opportunities: List of detected opportunities

    Returns:
        Formatted string for LLM prompt, or empty string if none
    """
    if not opportunities:
        return ""

    lines = ["Optimization opportunities detected:"]
    for opp in opportunities:
        lines.append(f"\n* {opp.pattern_name}")
        lines.append(f"  Trigger: {opp.trigger}")
        lines.append(f"  Rewrite: {opp.rewrite_hint}")
        if opp.expected_benefit:
            lines.append(f"  Benefit: {opp.expected_benefit}")
        if opp.example:
            lines.append(f"  Example: {opp.example}")

    return "\n".join(lines)


# Alias for backward compatibility with dspy_optimizer.py
def detect_knowledge_patterns(sql: str) -> str:
    """Detect KNOWLEDGE_BASE patterns and return formatted string for LLM.

    This is a backward-compatible wrapper around detect_opportunities()
    that returns a formatted string suitable for LLM prompts.

    Args:
        sql: SQL query to analyze

    Returns:
        Formatted string with relevant patterns, or empty string if none
    """
    opportunities = detect_opportunities(sql)
    return format_opportunities_for_llm(opportunities)
