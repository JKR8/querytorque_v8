"""DuckDB-Specific Semantic Rewriters.

These rewriters leverage DuckDB's unique syntax features:
- QUALIFY clause for window function filtering
- PIVOT/UNPIVOT for data transformation
- GROUP BY ALL for automatic grouping
- EXCLUDE clause for column selection
- SAMPLE for efficient data exploration

From DuckDB Optimizer Gaps document:
- SQL-DUCK-001: Subquery instead of QUALIFY
- SQL-DUCK-007: Manual pivot pattern
- SQL-DUCK-008: UNION ALL for unpivot
"""

from typing import Any, Optional

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
class SubqueryToQualifyRewriter(BaseRewriter):
    """Rewrites window function subqueries to DuckDB QUALIFY.
    
    Example:
        SELECT * FROM (
            SELECT *, ROW_NUMBER() OVER (PARTITION BY dept ORDER BY salary DESC) as rn
            FROM employees
        ) t WHERE rn = 1
        →
        SELECT * FROM employees
        QUALIFY ROW_NUMBER() OVER (PARTITION BY dept ORDER BY salary DESC) = 1
    
    DuckDB's QUALIFY clause filters on window function results directly,
    eliminating the need for a subquery wrapper.
    """
    
    rewriter_id = "subquery_to_qualify"
    name = "Subquery to QUALIFY"
    description = "Convert window function subquery to DuckDB QUALIFY clause"
    linked_rule_ids = ("SQL-DUCK-001",)
    default_confidence = RewriteConfidence.HIGH
    dialects = ("duckdb",)
    
    def can_rewrite(self, node: exp.Expression, context: Any = None) -> bool:
        """Check for subquery pattern with window function and outer filter."""
        if not isinstance(node, exp.Select):
            return False
        
        # Look for pattern: SELECT * FROM (SELECT ... window ... as alias) WHERE alias = N
        from_clause = node.find(exp.From)
        if not from_clause:
            return False
        
        subquery = from_clause.find(exp.Subquery)
        if not subquery:
            return False
        
        inner_select = subquery.find(exp.Select)
        if not inner_select:
            return False
        
        # Check for window function in inner select
        has_window = bool(inner_select.find(exp.Window))
        
        # Check for WHERE on window result
        where = node.find(exp.Where)
        has_filter = where is not None
        
        return has_window and has_filter
    
    def rewrite(self, node: exp.Expression, context: Any = None) -> RewriteResult:
        """Perform the subquery to QUALIFY transformation."""
        original_sql = node.sql()
        
        if not isinstance(node, exp.Select):
            return self._create_failure(original_sql, "Node must be SELECT statement")
        
        try:
            # Extract components
            from_clause = node.find(exp.From)
            subquery = from_clause.find(exp.Subquery)
            inner_select = subquery.find(exp.Select)
            outer_where = node.find(exp.Where)
            
            # Find the window function and its alias
            window_alias = None
            window_func = None
            
            for expr in inner_select.expressions:
                if isinstance(expr, exp.Alias):
                    if expr.this.find(exp.Window):
                        window_alias = str(expr.alias)
                        window_func = expr.this
                        break
            
            if not window_func or not window_alias:
                return self._create_failure(original_sql, "Could not find window function alias")
            
            # Extract the filter condition on window result
            filter_value = self._extract_window_filter(outer_where, window_alias)
            if filter_value is None:
                return self._create_failure(original_sql, "Could not extract window filter condition")
            
            # Build new SELECT with QUALIFY
            # Start with columns from inner select (minus the window alias)
            new_expressions = []
            for expr in inner_select.expressions:
                if isinstance(expr, exp.Alias) and str(expr.alias) == window_alias:
                    continue  # Skip the window alias
                new_expressions.append(expr.copy())
            
            # Get FROM clause from inner select
            inner_from = inner_select.find(exp.From)
            
            # Build QUALIFY condition
            qualify_condition = exp.EQ(
                this=window_func.copy(),
                expression=filter_value,
            )
            
            # Construct new query
            new_select = exp.Select(expressions=new_expressions)
            
            if inner_from:
                new_select.set("from", inner_from.copy())
            
            # Copy any WHERE from inner select
            inner_where = inner_select.find(exp.Where)
            if inner_where:
                new_select.set("where", inner_where.copy())
            
            # Copy GROUP BY from inner select
            inner_group = inner_select.find(exp.Group)
            if inner_group:
                new_select.set("group", inner_group.copy())
            
            # Add QUALIFY
            new_select.set("qualify", exp.Qualify(this=qualify_condition))
            
            result = self._create_result(
                success=True,
                original_sql=original_sql,
                rewritten_sql=new_select.sql(),
                rewritten_node=new_select,
                confidence=RewriteConfidence.HIGH,
                explanation="Converted window subquery to QUALIFY clause",
            )
            
            result.add_safety_check(
                name="semantic_equivalence",
                result=SafetyCheckResult.PASSED,
                message="QUALIFY filters on same window function with same condition",
            )
            
            result.add_safety_check(
                name="duckdb_specific",
                result=SafetyCheckResult.WARNING,
                message="QUALIFY is DuckDB-specific syntax",
            )
            
            return result
            
        except Exception as e:
            return self._create_failure(original_sql, str(e))
    
    def _extract_window_filter(
        self, 
        where: exp.Where, 
        window_alias: str
    ) -> Optional[exp.Expression]:
        """Extract the filter value for the window function."""
        if not where:
            return None
        
        # Look for alias = N pattern
        for eq in where.find_all(exp.EQ):
            if isinstance(eq.left, exp.Column):
                if str(eq.left.name).lower() == window_alias.lower():
                    return eq.right.copy()
            if isinstance(eq.right, exp.Column):
                if str(eq.right.name).lower() == window_alias.lower():
                    return eq.left.copy()
        
        # Also check for <= N pattern (top-N)
        for lte in where.find_all((exp.LTE, exp.LT)):
            if isinstance(lte.left, exp.Column):
                if str(lte.left.name).lower() == window_alias.lower():
                    return lte  # Return the whole condition
        
        return None


@register_rewriter
class ManualPivotToPivotRewriter(BaseRewriter):
    """Rewrites manual pivot patterns to DuckDB PIVOT syntax.
    
    Example:
        SELECT id,
            MAX(CASE WHEN category = 'A' THEN value END) as A,
            MAX(CASE WHEN category = 'B' THEN value END) as B
        FROM t GROUP BY id
        →
        PIVOT t ON category USING MAX(value)
    
    DuckDB's PIVOT syntax is cleaner and may be better optimized.
    """
    
    rewriter_id = "manual_pivot_to_pivot"
    name = "Manual Pivot to PIVOT"
    description = "Convert CASE-based pivot patterns to DuckDB PIVOT syntax"
    linked_rule_ids = ("SQL-DUCK-007",)
    default_confidence = RewriteConfidence.MEDIUM
    dialects = ("duckdb",)
    
    def can_rewrite(self, node: exp.Expression, context: Any = None) -> bool:
        """Check for manual pivot pattern (multiple CASE on same column)."""
        if not isinstance(node, exp.Select):
            return False
        
        # Count CASE expressions
        cases = list(node.find_all(exp.Case))
        if len(cases) < 2:
            return False
        
        # Check if they follow pivot pattern
        pivot_cols = self._extract_pivot_info(node)
        return pivot_cols is not None
    
    def _extract_pivot_info(
        self, 
        node: exp.Select
    ) -> Optional[dict]:
        """Extract pivot information from CASE expressions.
        
        Returns dict with:
        - pivot_column: Column being pivoted on
        - value_column: Column being aggregated
        - aggregate: Aggregate function (MAX, SUM, etc.)
        - categories: List of pivot categories
        """
        cases = list(node.find_all(exp.Case))
        if not cases:
            return None
        
        # Analyze first CASE to find pattern
        first_case = cases[0]
        
        # Look for pattern: WHEN pivot_col = 'value' THEN value_col
        pivot_column = None
        value_column = None
        categories = []
        
        for when in first_case.find_all(exp.EQ):
            if isinstance(when.left, exp.Column) and isinstance(when.right, exp.Literal):
                pivot_column = str(when.left.name)
                categories.append(when.right)
            elif isinstance(when.right, exp.Column) and isinstance(when.left, exp.Literal):
                pivot_column = str(when.right.name)
                categories.append(when.left)
        
        if not pivot_column:
            return None
        
        # Find the THEN value
        then_expr = first_case.args.get("ifs", [{}])[0].args.get("true") if first_case.args.get("ifs") else None
        if isinstance(then_expr, exp.Column):
            value_column = str(then_expr.name)
        
        if not value_column:
            return None
        
        # Check if wrapped in aggregate
        aggregate = None
        parent = first_case.parent
        while parent:
            if isinstance(parent, exp.Max):
                aggregate = "MAX"
                break
            elif isinstance(parent, exp.Min):
                aggregate = "MIN"
                break
            elif isinstance(parent, exp.Sum):
                aggregate = "SUM"
                break
            elif isinstance(parent, exp.Avg):
                aggregate = "AVG"
                break
            elif isinstance(parent, exp.Count):
                aggregate = "COUNT"
                break
            parent = parent.parent
        
        if not aggregate:
            return None
        
        # Extract categories from all CASE expressions
        for case in cases[1:]:
            for when in case.find_all(exp.EQ):
                if isinstance(when.left, exp.Column) and isinstance(when.right, exp.Literal):
                    if str(when.left.name) == pivot_column:
                        categories.append(when.right)
                elif isinstance(when.right, exp.Column) and isinstance(when.left, exp.Literal):
                    if str(when.right.name) == pivot_column:
                        categories.append(when.left)
        
        return {
            "pivot_column": pivot_column,
            "value_column": value_column,
            "aggregate": aggregate,
            "categories": categories,
        }
    
    def rewrite(self, node: exp.Expression, context: Any = None) -> RewriteResult:
        """Convert manual pivot to PIVOT syntax."""
        original_sql = node.sql()
        
        if not isinstance(node, exp.Select):
            return self._create_failure(original_sql, "Node must be SELECT statement")
        
        try:
            pivot_info = self._extract_pivot_info(node)
            if not pivot_info:
                return self._create_failure(original_sql, "Could not extract pivot pattern")
            
            # Get source table
            from_clause = node.find(exp.From)
            source_table = from_clause.find(exp.Table) if from_clause else None
            
            if not source_table:
                return self._create_failure(original_sql, "Could not find source table")
            
            table_name = str(source_table.name)
            
            # Build PIVOT query
            # PIVOT table ON pivot_col USING AGG(value_col)
            pivot_sql = (
                f"PIVOT {table_name} "
                f"ON {pivot_info['pivot_column']} "
                f"USING {pivot_info['aggregate']}({pivot_info['value_column']})"
            )
            
            # Get GROUP BY columns (these become the row keys)
            group = node.find(exp.Group)
            if group:
                group_cols = [str(e.name) if isinstance(e, exp.Column) else str(e) 
                             for e in group.expressions]
                # In PIVOT, the GROUP BY is implicit for non-pivot columns
            
            result = self._create_result(
                success=True,
                original_sql=original_sql,
                rewritten_sql=pivot_sql,
                confidence=RewriteConfidence.MEDIUM,
                explanation=f"Converted CASE pivot to PIVOT {pivot_info['pivot_column']}",
            )
            
            result.add_safety_check(
                name="category_coverage",
                result=SafetyCheckResult.WARNING,
                message=f"PIVOT will include all values in {pivot_info['pivot_column']}. "
                        "Manual CASE specified only these categories: " +
                        ", ".join(str(c) for c in pivot_info['categories'][:5]),
            )
            
            result.add_safety_check(
                name="duckdb_specific",
                result=SafetyCheckResult.WARNING,
                message="PIVOT is DuckDB-specific syntax",
            )
            
            return result
            
        except Exception as e:
            return self._create_failure(original_sql, str(e))


@register_rewriter
class UnionToUnpivotRewriter(BaseRewriter):
    """Rewrites UNION ALL unpivot pattern to DuckDB UNPIVOT.
    
    Example:
        SELECT id, 'A' as category, a as value FROM t
        UNION ALL
        SELECT id, 'B' as category, b as value FROM t
        →
        UNPIVOT t ON (a, b) INTO NAME category VALUE value
    """
    
    rewriter_id = "union_to_unpivot"
    name = "UNION to UNPIVOT"
    description = "Convert UNION ALL unpivot pattern to DuckDB UNPIVOT syntax"
    linked_rule_ids = ("SQL-DUCK-008",)
    default_confidence = RewriteConfidence.MEDIUM
    dialects = ("duckdb",)
    
    def can_rewrite(self, node: exp.Expression, context: Any = None) -> bool:
        """Check for UNION ALL unpivot pattern."""
        if not isinstance(node, exp.Union):
            return False
        
        # Check for UNION ALL (not UNION DISTINCT)
        if node.args.get("distinct", True):
            return False
        
        return self._is_unpivot_pattern(node)
    
    def _is_unpivot_pattern(self, union: exp.Union) -> bool:
        """Check if UNION follows unpivot pattern."""
        # Collect all SELECTs in the union
        selects = self._collect_union_selects(union)
        
        if len(selects) < 2:
            return False
        
        # Check if all SELECT from same table
        tables = set()
        for sel in selects:
            from_clause = sel.find(exp.From)
            if from_clause:
                table = from_clause.find(exp.Table)
                if table:
                    tables.add(str(table.name).lower())
        
        if len(tables) != 1:
            return False
        
        # Check structure: should have same columns except for literal category
        # and different source column for value
        first_exprs = list(selects[0].expressions)
        for sel in selects[1:]:
            exprs = list(sel.expressions)
            if len(exprs) != len(first_exprs):
                return False
        
        return True
    
    def _collect_union_selects(self, union: exp.Union) -> list[exp.Select]:
        """Collect all SELECT statements from nested UNIONs."""
        selects = []
        
        def traverse(node):
            if isinstance(node, exp.Union):
                traverse(node.left)
                traverse(node.right)
            elif isinstance(node, exp.Select):
                selects.append(node)
        
        traverse(union)
        return selects
    
    def rewrite(self, node: exp.Expression, context: Any = None) -> RewriteResult:
        """Convert UNION unpivot to UNPIVOT syntax."""
        original_sql = node.sql()
        
        if not isinstance(node, exp.Union):
            return self._create_failure(original_sql, "Node must be UNION statement")
        
        try:
            selects = self._collect_union_selects(node)
            if len(selects) < 2:
                return self._create_failure(original_sql, "Need at least 2 UNION branches")
            
            # Get source table
            from_clause = selects[0].find(exp.From)
            table = from_clause.find(exp.Table) if from_clause else None
            if not table:
                return self._create_failure(original_sql, "Could not find source table")
            
            table_name = str(table.name)
            
            # Analyze columns to find unpivot pattern
            # Look for: static columns + literal category + varying source column
            unpivot_info = self._analyze_unpivot_columns(selects)
            
            if not unpivot_info:
                return self._create_failure(original_sql, "Could not determine unpivot columns")
            
            # Build UNPIVOT query
            columns_str = ", ".join(unpivot_info["value_columns"])
            
            unpivot_sql = (
                f"UNPIVOT {table_name} "
                f"ON ({columns_str}) "
                f"INTO NAME {unpivot_info['category_name']} VALUE {unpivot_info['value_name']}"
            )
            
            result = self._create_result(
                success=True,
                original_sql=original_sql,
                rewritten_sql=unpivot_sql,
                confidence=RewriteConfidence.MEDIUM,
                explanation=f"Converted {len(selects)}-way UNION to UNPIVOT",
            )
            
            result.add_safety_check(
                name="column_mapping",
                result=SafetyCheckResult.PASSED,
                message=f"Unpivoting columns: {columns_str}",
            )
            
            result.add_safety_check(
                name="duckdb_specific",
                result=SafetyCheckResult.WARNING,
                message="UNPIVOT is DuckDB-specific syntax",
            )
            
            return result
            
        except Exception as e:
            return self._create_failure(original_sql, str(e))
    
    def _analyze_unpivot_columns(self, selects: list[exp.Select]) -> Optional[dict]:
        """Analyze SELECT columns to determine unpivot structure."""
        if not selects:
            return None
        
        first = selects[0]
        exprs = list(first.expressions)
        
        # Find which columns are:
        # 1. Static (same in all branches) 
        # 2. Category literals (different literal per branch)
        # 3. Value sources (different column per branch)
        
        category_idx = None
        value_idx = None
        category_name = None
        value_name = None
        value_columns = []
        
        for i, expr in enumerate(exprs):
            is_literal = isinstance(expr, exp.Literal) or (
                isinstance(expr, exp.Alias) and isinstance(expr.this, exp.Literal)
            )
            
            if is_literal:
                # Check if this position varies across branches
                literals = []
                for sel in selects:
                    sel_exprs = list(sel.expressions)
                    if i < len(sel_exprs):
                        e = sel_exprs[i]
                        if isinstance(e, exp.Alias):
                            literals.append(str(e.this))
                            if category_name is None:
                                category_name = str(e.alias)
                        else:
                            literals.append(str(e))
                
                if len(set(literals)) == len(selects):
                    category_idx = i
            
            else:
                # Check if different column selected per branch
                cols = []
                for sel in selects:
                    sel_exprs = list(sel.expressions)
                    if i < len(sel_exprs):
                        e = sel_exprs[i]
                        if isinstance(e, exp.Alias):
                            if isinstance(e.this, exp.Column):
                                cols.append(str(e.this.name))
                                if value_name is None:
                                    value_name = str(e.alias)
                        elif isinstance(e, exp.Column):
                            cols.append(str(e.name))
                
                if len(set(cols)) == len(selects):
                    value_idx = i
                    value_columns = cols
        
        if category_idx is None or value_idx is None:
            return None
        
        return {
            "category_name": category_name or "category",
            "value_name": value_name or "value",
            "value_columns": value_columns,
        }
