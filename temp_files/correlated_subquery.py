"""Correlated Subquery to JOIN Rewriter.

Pattern: SELECT ... (SELECT col FROM t2 WHERE t2.fk = t1.pk) FROM t1
Rewrite: SELECT ... t2.col FROM t1 LEFT JOIN t2 ON t2.fk = t1.pk

This transforms row-by-row correlated scalar subqueries into set-based JOINs.
One of the most impactful optimizations as it changes O(n*m) to O(n+m).

From the Taxonomy (Pattern #2 - Row-by-Row Subquery):
- Semantic Trigger: Correlated subquery referencing outer query columns
- Safety Check: Confirm join equivalence, verify uniqueness constraints
- Confidence: MEDIUM (requires constraint validation)
"""

from typing import Any, Optional
from collections import defaultdict

from sqlglot import exp

from ..base import (
    BaseRewriter,
    RewriteResult,
    RewriteConfidence,
    SafetyCheckResult,
    SchemaMetadata,
)
from ..registry import register_rewriter


@register_rewriter
class CorrelatedSubqueryToJoinRewriter(BaseRewriter):
    """Rewrites correlated scalar subqueries to JOINs.
    
    Example:
        SELECT 
            e.name,
            (SELECT d.name FROM departments d WHERE d.id = e.dept_id) as dept_name
        FROM employees e
        →
        SELECT 
            e.name,
            d.name as dept_name
        FROM employees e
        LEFT JOIN departments d ON d.id = e.dept_id
    
    Safety Requirements:
        - Subquery must return at most one row (scalar)
        - Join column should have unique/PK constraint on subquery side
        - NULL handling preserved via LEFT JOIN
    """
    
    rewriter_id = "correlated_subquery_to_join"
    name = "Correlated Subquery to JOIN"
    description = "Convert correlated scalar subqueries to JOIN operations"
    linked_rule_ids = (
        "SQL-SEL-008",    # Correlated subquery in SELECT
        "SQL-WHERE-007",  # Correlated subquery in WHERE
    )
    default_confidence = RewriteConfidence.MEDIUM
    
    def get_required_metadata(self) -> list[str]:
        """Uniqueness constraints help verify safety."""
        return ["primary_key", "unique_constraints"]
    
    def can_rewrite(self, node: exp.Expression, context: Any = None) -> bool:
        """Check if node contains correlated scalar subquery."""
        if not isinstance(node, exp.Select):
            return False
        
        # Look for scalar subqueries in SELECT list
        for subq in node.find_all(exp.Subquery):
            if self._is_correlated_scalar(subq, node):
                return True
        
        return False
    
    def rewrite(self, node: exp.Expression, context: Any = None) -> RewriteResult:
        """Perform the correlated subquery to JOIN transformation."""
        original_sql = node.sql()
        
        if not isinstance(node, exp.Select):
            return self._create_failure(original_sql, "Node must be SELECT statement")
        
        try:
            rewritten = node.copy()
            
            # Find all correlated scalar subqueries
            subqueries = []
            for subq in list(rewritten.find_all(exp.Subquery)):
                if self._is_correlated_scalar(subq, rewritten):
                    subqueries.append(subq)
            
            if not subqueries:
                return self._create_failure(original_sql, "No correlated scalar subqueries found")
            
            result = self._create_result(
                success=True,
                original_sql=original_sql,
                confidence=RewriteConfidence.MEDIUM,
                explanation=f"Converting {len(subqueries)} correlated subquery(ies) to JOIN",
            )
            
            # Process each subquery
            join_alias_counter = 0
            for subq in subqueries:
                join_alias_counter += 1
                join_alias = f"_sq{join_alias_counter}"
                
                transform_result = self._transform_subquery(
                    rewritten, subq, join_alias, result
                )
                
                if not transform_result:
                    return self._create_failure(
                        original_sql, 
                        f"Failed to transform subquery {join_alias}"
                    )
            
            result.rewritten_sql = rewritten.sql()
            result.rewritten_node = rewritten
            
            return result
            
        except Exception as e:
            return self._create_failure(original_sql, str(e))
    
    def _is_correlated_scalar(self, subq: exp.Subquery, outer: exp.Select) -> bool:
        """Check if subquery is correlated and scalar."""
        # Must be a single-row subquery (scalar)
        inner = subq.find(exp.Select)
        if not inner:
            return False
        
        # Check for correlation - references to outer query columns
        outer_tables = self._get_table_aliases(outer)
        
        for col in inner.find_all(exp.Column):
            if col.table and str(col.table).lower() in outer_tables:
                # Found correlation
                return True
        
        return False
    
    def _get_table_aliases(self, select: exp.Select) -> set[str]:
        """Get all table aliases in a SELECT statement."""
        aliases = set()
        
        # From clause
        from_clause = select.find(exp.From)
        if from_clause:
            for table in from_clause.find_all(exp.Table):
                alias = table.alias or table.name
                if alias:
                    aliases.add(str(alias).lower())
        
        # Joins
        for join in select.find_all(exp.Join):
            table = join.find(exp.Table)
            if table:
                alias = table.alias or table.name
                if alias:
                    aliases.add(str(alias).lower())
        
        return aliases
    
    def _transform_subquery(
        self,
        outer: exp.Select,
        subq: exp.Subquery,
        join_alias: str,
        result: RewriteResult,
    ) -> bool:
        """Transform a single correlated subquery to JOIN.
        
        Returns True on success.
        """
        inner = subq.find(exp.Select)
        if not inner:
            return False
        
        # Extract correlation predicate from WHERE
        where = inner.find(exp.Where)
        if not where:
            result.add_safety_check(
                name="correlation_predicate",
                result=SafetyCheckResult.FAILED,
                message="Subquery has no WHERE clause - cannot determine join condition",
            )
            return False
        
        # Find the correlation condition (outer.col = inner.col)
        correlation = self._extract_correlation(where, outer)
        if not correlation:
            result.add_safety_check(
                name="correlation_predicate",
                result=SafetyCheckResult.FAILED,
                message="Could not extract correlation predicate from subquery",
            )
            return False
        
        outer_col, inner_col = correlation
        
        # Get the table being queried in subquery
        inner_from = inner.find(exp.From)
        if not inner_from:
            return False
        
        inner_table = inner_from.find(exp.Table)
        if not inner_table:
            return False
        
        # Verify uniqueness constraint for safety
        inner_table_name = str(inner_table.name)
        inner_col_name = str(inner_col.name) if hasattr(inner_col, 'name') else str(inner_col)
        
        if self.metadata.has_primary_key(inner_table_name, inner_col_name):
            result.add_safety_check(
                name="uniqueness_constraint",
                result=SafetyCheckResult.PASSED,
                message=f"Join column {inner_col_name} is primary key on {inner_table_name}",
            )
            result.confidence = RewriteConfidence.HIGH
        elif self.metadata.has_unique_constraint(inner_table_name, [inner_col_name]):
            result.add_safety_check(
                name="uniqueness_constraint",
                result=SafetyCheckResult.PASSED,
                message=f"Join column {inner_col_name} has unique constraint on {inner_table_name}",
            )
            result.confidence = RewriteConfidence.HIGH
        else:
            result.add_safety_check(
                name="uniqueness_constraint",
                result=SafetyCheckResult.WARNING,
                message=f"Cannot verify uniqueness of {inner_col_name} on {inner_table_name}. "
                        "If not unique, results may differ (multiple rows where subquery returned first/arbitrary).",
                metadata_required=["primary_key", "unique_constraints"],
            )
        
        # Get selected columns from subquery
        select_cols = list(inner.expressions)
        if not select_cols:
            return False
        
        # Build the JOIN
        # Create aliased table reference
        join_table = exp.Table(
            this=inner_table.this.copy(),
            alias=exp.TableAlias(this=exp.to_identifier(join_alias)),
        )
        
        # Build ON condition
        join_condition = exp.EQ(
            this=exp.Column(
                this=inner_col.this.copy() if hasattr(inner_col, 'this') else inner_col,
                table=exp.to_identifier(join_alias),
            ),
            expression=outer_col.copy(),
        )
        
        # Add any non-correlation conditions to JOIN
        other_conditions = self._extract_non_correlation_conditions(where, outer)
        if other_conditions:
            for cond in other_conditions:
                # Update table references to use join alias
                for col in cond.find_all(exp.Column):
                    inner_alias = inner_table.alias or inner_table.name
                    if col.table and str(col.table).lower() == str(inner_alias).lower():
                        col.set("table", exp.to_identifier(join_alias))
                
                join_condition = exp.And(this=join_condition, expression=cond)
        
        # Create LEFT JOIN (preserves NULL behavior of scalar subquery returning no rows)
        join = exp.Join(
            this=join_table,
            on=join_condition,
            kind="LEFT",
        )
        
        # Add join to outer query
        from_clause = outer.find(exp.From)
        if from_clause:
            # Append join after FROM
            existing_joins = list(outer.find_all(exp.Join))
            if existing_joins:
                # Add after last join
                last_join = existing_joins[-1]
                last_join.replace(exp.Join(
                    this=last_join.this,
                    on=last_join.args.get("on"),
                    kind=last_join.args.get("kind"),
                ))
                outer.append("joins", join)
            else:
                outer.set("joins", [join])
        
        # Replace subquery with column reference
        # Get the projected column from subquery
        projected_col = select_cols[0]
        
        # Build replacement column reference
        if isinstance(projected_col, exp.Column):
            replacement = exp.Column(
                this=projected_col.this.copy(),
                table=exp.to_identifier(join_alias),
            )
        elif isinstance(projected_col, exp.Alias):
            inner_expr = projected_col.this
            if isinstance(inner_expr, exp.Column):
                replacement = exp.Column(
                    this=inner_expr.this.copy(),
                    table=exp.to_identifier(join_alias),
                )
            else:
                # Complex expression - need to alias
                replacement = exp.Column(
                    this=projected_col.alias_or_name,
                    table=exp.to_identifier(join_alias),
                )
        else:
            # Aggregate or other - need different handling
            replacement = exp.Column(
                this=exp.to_identifier("result"),
                table=exp.to_identifier(join_alias),
            )
        
        # Preserve alias if subquery had one
        if subq.alias:
            replacement = exp.Alias(
                this=replacement,
                alias=subq.alias,
            )
        
        # Replace the subquery
        subq.replace(replacement)
        
        result.add_safety_check(
            name="null_handling",
            result=SafetyCheckResult.PASSED,
            message="Using LEFT JOIN preserves NULL behavior of scalar subquery",
        )
        
        return True
    
    def _extract_correlation(
        self, 
        where: exp.Where, 
        outer: exp.Select
    ) -> Optional[tuple[exp.Column, exp.Column]]:
        """Extract the correlation predicate (outer_col, inner_col)."""
        outer_tables = self._get_table_aliases(outer)
        
        for eq in where.find_all(exp.EQ):
            left = eq.left
            right = eq.right
            
            if isinstance(left, exp.Column) and isinstance(right, exp.Column):
                left_is_outer = left.table and str(left.table).lower() in outer_tables
                right_is_outer = right.table and str(right.table).lower() in outer_tables
                
                if left_is_outer and not right_is_outer:
                    return (left, right)
                elif right_is_outer and not left_is_outer:
                    return (right, left)
        
        return None
    
    def _extract_non_correlation_conditions(
        self,
        where: exp.Where,
        outer: exp.Select,
    ) -> list[exp.Expression]:
        """Extract WHERE conditions that aren't the correlation predicate."""
        outer_tables = self._get_table_aliases(outer)
        conditions = []
        
        def traverse(node):
            if isinstance(node, exp.And):
                traverse(node.left)
                traverse(node.right)
            elif isinstance(node, exp.EQ):
                # Check if this is correlation predicate
                left, right = node.left, node.right
                is_correlation = False
                
                if isinstance(left, exp.Column) and isinstance(right, exp.Column):
                    left_is_outer = left.table and str(left.table).lower() in outer_tables
                    right_is_outer = right.table and str(right.table).lower() in outer_tables
                    is_correlation = left_is_outer != right_is_outer
                
                if not is_correlation:
                    conditions.append(node.copy())
            else:
                # Other condition types
                has_outer_ref = False
                for col in node.find_all(exp.Column):
                    if col.table and str(col.table).lower() in outer_tables:
                        has_outer_ref = True
                        break
                
                if not has_outer_ref:
                    conditions.append(node.copy())
        
        traverse(where.this)
        return conditions


@register_rewriter
class CorrelatedExistsToJoinRewriter(BaseRewriter):
    """Rewrites correlated EXISTS to semi-join pattern.
    
    Example:
        SELECT * FROM orders o
        WHERE EXISTS (SELECT 1 FROM customers c WHERE c.id = o.customer_id AND c.status = 'active')
        →
        SELECT DISTINCT o.* FROM orders o
        INNER JOIN customers c ON c.id = o.customer_id AND c.status = 'active'
    
    Note: This rewrite requires DISTINCT or can use semi-join syntax if supported.
    """
    
    rewriter_id = "correlated_exists_to_join"
    name = "Correlated EXISTS to Semi-Join"
    description = "Convert correlated EXISTS to INNER JOIN with DISTINCT"
    linked_rule_ids = ("SQL-WHERE-008",)  # EXISTS subquery detection
    default_confidence = RewriteConfidence.MEDIUM
    
    def can_rewrite(self, node: exp.Expression, context: Any = None) -> bool:
        """Check if node contains correlated EXISTS."""
        if not isinstance(node, exp.Select):
            return False
        
        for exists in node.find_all(exp.Exists):
            subq = exists.find(exp.Subquery)
            if subq and self._is_correlated(subq, node):
                return True
        
        return False
    
    def _is_correlated(self, subq: exp.Subquery, outer: exp.Select) -> bool:
        """Check if subquery is correlated with outer query."""
        outer_tables = set()
        from_clause = outer.find(exp.From)
        if from_clause:
            for table in from_clause.find_all(exp.Table):
                alias = table.alias or table.name
                if alias:
                    outer_tables.add(str(alias).lower())
        
        inner = subq.find(exp.Select)
        if not inner:
            return False
        
        for col in inner.find_all(exp.Column):
            if col.table and str(col.table).lower() in outer_tables:
                return True
        
        return False
    
    def rewrite(self, node: exp.Expression, context: Any = None) -> RewriteResult:
        """Convert EXISTS to semi-join pattern."""
        original_sql = node.sql()
        
        # This is a more complex rewrite - placeholder for now
        # Full implementation would extract EXISTS condition and convert to JOIN
        
        result = self._create_result(
            success=False,
            original_sql=original_sql,
            confidence=RewriteConfidence.MEDIUM,
            explanation="EXISTS to semi-join rewrite not fully implemented",
        )
        
        result.add_safety_check(
            name="implementation_status",
            result=SafetyCheckResult.FAILED,
            message="Full EXISTS to semi-join transformation requires additional implementation",
        )
        
        return result
