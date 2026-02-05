"""SQL rewriting for ADO - applies LLM optimizations to SQL.

This module handles parsing LLM responses and applying SQL rewrites.
Simplified from qt_sql dag_v2 - keeps ONLY what ADO needs.

Key features:
- Parse JSON rewrite_sets from LLM responses
- Apply optimized SQL from rewrite_sets
- Validate transforms against allowlist
- Reassemble SQL from node-level changes
"""

from __future__ import annotations

import json
import re
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

try:
    import sqlglot
    from sqlglot import exp
except ImportError:
    sqlglot = None
    exp = None


# =============================================================================
# Transform Allowlist - Verified transforms only
# =============================================================================

# These transforms have proven speedups on TPC-DS/DSB benchmarks
ALLOWED_TRANSFORMS = frozenset([
    # Core transforms with verified speedups
    "pushdown",             # 2.11x Q9 - Push filters into CTEs/subqueries
    "decorrelate",          # 2.92x Q1 - Correlated subquery -> CTE with GROUP BY
    "or_to_union",          # 3.17x Q15 - OR conditions -> UNION ALL branches
    "early_filter",         # 4.00x Q93 - Filter dimension tables before joining
    "date_cte_isolate",     # 4.00x Q6 - Extract date filtering into early CTE
    "materialize_cte",      # 1.37x Q95 - Extract repeated subqueries into CTE
    "intersect_to_exists",  # 1.83x Q14 - INTERSECT to EXISTS
    "union_cte_split",      # 1.36x Q74 - Year-specialized CTEs

    # Additional valid transforms
    "flatten_subquery",     # Convert EXISTS/IN to JOINs
    "reorder_join",         # Reorder joins for selectivity
    "multi_push_predicate", # Push predicates through multiple CTE layers
    "inline_cte",           # Inline single-use CTEs
    "remove_redundant",     # Remove unnecessary DISTINCT/ORDER BY
    "semantic_rewrite",     # Catch-all for other valid optimizations
])


# =============================================================================
# Data Structures
# =============================================================================

@dataclass
class RewriteSet:
    """Atomic set of coordinated SQL rewrites from LLM."""
    id: str
    transform: str
    nodes: Dict[str, str]  # node_id -> new SQL
    invariants_kept: List[str] = field(default_factory=list)
    expected_speedup: str = "unknown"
    risk: str = "low"
    rationale: str = ""


@dataclass
class RewriteResult:
    """Result of applying a rewrite to SQL."""
    success: bool
    optimized_sql: str
    transform: str = ""
    error: Optional[str] = None
    rewrite_set: Optional[RewriteSet] = None


# =============================================================================
# JSON Response Parser
# =============================================================================

class ResponseParser:
    """Parse LLM responses to extract rewrite_sets."""

    @staticmethod
    def extract_json(response: str) -> Optional[str]:
        """Extract JSON from LLM response (handles markdown code blocks)."""
        # Try markdown code block first
        json_match = re.search(r'```json\s*(.*?)\s*```', response, re.DOTALL)
        if json_match:
            return json_match.group(1).strip()

        # Try raw JSON object with rewrite_sets
        json_match = re.search(r'\{[^{}]*"rewrite_sets"[^{}]*\[.*?\]\s*\}', response, re.DOTALL)
        if json_match:
            return json_match.group(0)

        # Try to find any JSON object
        start = response.find('{')
        end = response.rfind('}')
        if start >= 0 and end > start:
            return response[start:end + 1]

        return None

    @staticmethod
    def parse_rewrite_sets(json_str: str) -> List[RewriteSet]:
        """Parse JSON string to extract RewriteSet objects."""
        try:
            data = json.loads(json_str)
        except json.JSONDecodeError:
            return []

        rewrite_sets = []
        for rs in data.get("rewrite_sets", []):
            # Handle both "nodes" dict and direct "optimized_sql" field
            nodes = rs.get("nodes", {})

            # If no nodes but has optimized_sql, use it as main_query
            if not nodes and rs.get("optimized_sql"):
                nodes = {"main_query": rs["optimized_sql"]}

            rewrite_sets.append(RewriteSet(
                id=rs.get("id", "rs_01"),
                transform=rs.get("transform", "semantic_rewrite"),
                nodes=nodes,
                invariants_kept=rs.get("invariants_kept", []),
                expected_speedup=rs.get("expected_speedup", "unknown"),
                risk=rs.get("risk", "low"),
                rationale=rs.get("rationale", ""),
            ))

        return rewrite_sets

    def parse(self, response: str) -> List[RewriteSet]:
        """Parse LLM response to extract all rewrite_sets."""
        json_str = self.extract_json(response)
        if not json_str:
            return []
        return self.parse_rewrite_sets(json_str)


# =============================================================================
# SQL Assembler - Reassemble SQL from node changes
# =============================================================================

class SQLAssembler:
    """Reassemble full SQL from node-level changes."""

    def __init__(self, dialect: str = "duckdb"):
        self.dialect = dialect

    def assemble(self, original_sql: str, rewrite_set: RewriteSet) -> str:
        """Apply rewrite_set to original SQL and return optimized SQL.

        If the rewrite_set contains a complete main_query with WITH clause,
        uses it directly. Otherwise, reconstructs from individual nodes.
        """
        if not rewrite_set.nodes:
            return original_sql

        # Check if main_query is already a complete statement
        main_sql = rewrite_set.nodes.get("main_query", "")
        if main_sql.strip().upper().startswith("WITH "):
            # main_query is complete, use it directly
            return self._clean_sql(main_sql)

        # Parse original to get existing CTEs
        original_ctes = self._extract_ctes(original_sql)
        original_main = self._extract_main_query(original_sql)

        # Build new nodes dict: start with original, overlay rewrites
        nodes = {}
        for cte_name, cte_sql in original_ctes.items():
            nodes[cte_name] = cte_sql

        # Add original main_query if not present
        if "main_query" not in nodes and original_main:
            nodes["main_query"] = original_main

        # Overlay rewrite_set nodes
        for node_id, node_sql in rewrite_set.nodes.items():
            nodes[node_id] = node_sql

        # Reassemble
        return self._reassemble(nodes)

    def _extract_ctes(self, sql: str) -> Dict[str, str]:
        """Extract CTEs from SQL as {name: body} dict."""
        if sqlglot is None:
            return {}

        try:
            parsed = sqlglot.parse_one(sql, dialect=self.dialect)
            ctes = {}

            for cte in parsed.find_all(exp.CTE):
                name = str(cte.alias) if cte.alias else None
                if name and cte.this:
                    ctes[name] = cte.this.sql(dialect=self.dialect)

            return ctes
        except Exception:
            return {}

    def _extract_main_query(self, sql: str) -> str:
        """Extract the main SELECT (without WITH clause)."""
        if sqlglot is None:
            return sql

        try:
            parsed = sqlglot.parse_one(sql, dialect=self.dialect)

            # If it's a WITH expression, get the final query
            if isinstance(parsed, exp.With):
                main = parsed.this
            else:
                # Find outermost SELECT not in a CTE
                main = None
                for select in parsed.find_all(exp.Select):
                    if not select.find_ancestor(exp.CTE):
                        main = select
                        break

            if main:
                # Strip any WITH clause from the main select
                main_copy = main.copy()
                if main_copy.args.get("with_"):
                    main_copy.set("with_", None)
                if main_copy.args.get("with"):
                    main_copy.set("with", None)
                return main_copy.sql(dialect=self.dialect)

            return sql
        except Exception:
            return sql

    def _build_dependency_graph(self, cte_nodes: Dict[str, str]) -> Dict[str, List[str]]:
        """Build graph of CTE dependencies."""
        deps = {k: [] for k in cte_nodes}

        for node_id, sql in cte_nodes.items():
            sql_lower = sql.lower()
            for other_id in cte_nodes:
                if other_id != node_id:
                    # Check if other_id appears as a word in SQL
                    pattern = r'\b' + re.escape(other_id.lower()) + r'\b'
                    if re.search(pattern, sql_lower):
                        deps[node_id].append(other_id)

        return deps

    def _topological_sort(self, deps: Dict[str, List[str]]) -> List[str]:
        """Topological sort of CTEs by dependencies."""
        result = []
        visited = set()
        temp_visited = set()

        def visit(node: str):
            if node in temp_visited:
                return  # Cycle - skip
            if node in visited:
                return

            temp_visited.add(node)
            for dep in deps.get(node, []):
                visit(dep)
            temp_visited.discard(node)
            visited.add(node)
            result.append(node)

        for node in deps:
            if node not in visited:
                visit(node)

        return result

    def _reassemble(self, nodes: Dict[str, str]) -> str:
        """Reassemble full SQL from nodes dict."""
        main_sql = nodes.get("main_query", "")
        main_sql = self._clean_sql(main_sql)

        # Check if main_query already has WITH
        if main_sql.strip().upper().startswith("WITH "):
            return main_sql

        # Build CTEs
        cte_nodes = {k: v for k, v in nodes.items() if k != "main_query"}
        cte_nodes = {k: self._clean_sql(v) for k, v in cte_nodes.items()}

        if not cte_nodes:
            return main_sql

        # Sort CTEs by dependency
        deps = self._build_dependency_graph(cte_nodes)
        sorted_ctes = self._topological_sort(deps)

        # Build CTE clauses
        cte_clauses = []
        for node_id in sorted_ctes:
            sql = cte_nodes[node_id]

            # Strip any WITH prefix the LLM might have added
            sql_stripped = sql.strip()
            if sql_stripped.upper().startswith("WITH "):
                match = re.match(
                    r'WITH\s+\w+\s+AS\s*\(\s*(.*)\s*\)\s*$',
                    sql_stripped,
                    re.IGNORECASE | re.DOTALL
                )
                if match:
                    sql = match.group(1)

            cte_clauses.append(f"{node_id} AS ({sql})")

        if cte_clauses:
            return f"WITH {', '.join(cte_clauses)}\n{main_sql}"

        return main_sql

    def _clean_sql(self, sql: str) -> str:
        """Clean SQL - strip comments and normalize whitespace."""
        if not sql:
            return sql

        if sqlglot is None:
            return sql.strip()

        try:
            parsed = sqlglot.parse_one(sql, dialect=self.dialect)

            # Clear comments recursively
            def clear_comments(node):
                if hasattr(node, 'comments'):
                    node.comments = None
                for child in node.iter_expressions():
                    clear_comments(child)

            clear_comments(parsed)
            return parsed.sql(dialect=self.dialect)
        except Exception:
            return sql.strip()


# =============================================================================
# Main SQL Rewriter
# =============================================================================

class SQLRewriter:
    """Apply LLM-generated rewrites to SQL."""

    def __init__(self, sql: str, dialect: str = "duckdb"):
        """Initialize with original SQL.

        Args:
            sql: The original SQL query to rewrite
            dialect: SQL dialect (duckdb, postgres, etc.)
        """
        self.original_sql = sql
        self.dialect = dialect
        self.parser = ResponseParser()
        self.assembler = SQLAssembler(dialect=dialect)

    def apply_response(self, llm_response: str) -> RewriteResult:
        """Parse LLM response and apply the first valid rewrite_set.

        Args:
            llm_response: Raw LLM response text (may contain markdown, JSON, etc.)

        Returns:
            RewriteResult with optimized SQL or error
        """
        # Parse rewrite_sets from response
        rewrite_sets = self.parser.parse(llm_response)

        if not rewrite_sets:
            return RewriteResult(
                success=False,
                optimized_sql=self.original_sql,
                error="No rewrite_sets found in LLM response",
            )

        # Try each rewrite_set until one succeeds
        for rs in rewrite_sets:
            # Validate transform
            if rs.transform not in ALLOWED_TRANSFORMS:
                continue  # Skip disallowed transforms

            try:
                optimized_sql = self.assembler.assemble(self.original_sql, rs)

                # Validate the result parses
                if not self._validate_sql(optimized_sql):
                    continue

                return RewriteResult(
                    success=True,
                    optimized_sql=optimized_sql,
                    transform=rs.transform,
                    rewrite_set=rs,
                )
            except Exception as e:
                continue  # Try next rewrite_set

        return RewriteResult(
            success=False,
            optimized_sql=self.original_sql,
            error="No valid rewrite_set could be applied",
        )

    def _validate_sql(self, sql: str) -> bool:
        """Validate SQL parses correctly."""
        if sqlglot is None:
            return True  # Can't validate without sqlglot

        try:
            sqlglot.parse_one(sql, dialect=self.dialect)
            return True
        except Exception:
            return False

    def get_optimized_sql(self, llm_response: str) -> str:
        """Convenience method - returns just the optimized SQL string.

        Args:
            llm_response: Raw LLM response text

        Returns:
            Optimized SQL string (or original if rewrite failed)
        """
        result = self.apply_response(llm_response)
        return result.optimized_sql


# =============================================================================
# Utility Functions
# =============================================================================

def infer_transforms_from_sql_diff(original_sql: str, optimized_sql: str) -> List[str]:
    """Infer transforms by analyzing SQL structure differences.

    Fallback mechanism when LLM response doesn't explicitly name transforms.
    This looks for common optimization patterns in the SQL AST.

    Args:
        original_sql: Original query
        optimized_sql: Optimized query

    Returns:
        List of inferred transform names
    """
    if not sqlglot or original_sql == optimized_sql:
        return []

    inferred = []

    try:
        orig_ast = sqlglot.parse_one(original_sql, dialect="duckdb")
        opt_ast = sqlglot.parse_one(optimized_sql, dialect="duckdb")

        # Count patterns to infer transforms
        orig_subqueries = len(list(orig_ast.find_all(exp.Subquery)))
        opt_subqueries = len(list(opt_ast.find_all(exp.Subquery)))

        orig_ctes = len(list(orig_ast.find_all(exp.CTE)))
        opt_ctes = len(list(opt_ast.find_all(exp.CTE)))

        orig_unions = len(list(orig_ast.find_all(exp.Union)))
        opt_unions = len(list(opt_ast.find_all(exp.Union)))

        orig_conditions = len(list(orig_ast.find_all(exp.Or)))
        opt_or_count = len(list(opt_ast.find_all(exp.Or)))

        # Detect patterns
        if orig_subqueries > opt_subqueries:
            inferred.append("decorrelate")

        if opt_ctes > orig_ctes:
            inferred.append("materialize_cte")

        if opt_unions > orig_unions and opt_or_count < orig_conditions:
            inferred.append("or_to_union")

        # Look for UNION to UNION ALL conversion
        if "UNION ALL" in optimized_sql.upper() and "UNION" in original_sql.upper():
            if "UNION ALL" not in original_sql.upper():
                inferred.append("union_cte_split")

        # Look for window function usage increase (subquery_to_window)
        orig_windows = len(list(orig_ast.find_all(exp.Window)))
        opt_windows = len(list(opt_ast.find_all(exp.Window)))
        if opt_windows > orig_windows:
            inferred.append("subquery_to_window")

    except Exception:
        pass  # Silent fallback

    return inferred


def extract_transforms_from_response(response: str, original_sql: str = None, optimized_sql: str = None) -> List[str]:
    """Extract transform IDs from an LLM response with fallback inference.

    Args:
        response: Raw LLM response text
        original_sql: Original SQL (for fallback inference)
        optimized_sql: Optimized SQL (for fallback inference)

    Returns:
        List of transform IDs found in the response
    """
    parser = ResponseParser()
    rewrite_sets = parser.parse(response)

    transforms = []
    for rs in rewrite_sets:
        if rs.transform and rs.transform not in transforms:
            transforms.append(rs.transform)

    # Fallback: if no transforms found, try to infer from SQL diff
    if not transforms and original_sql and optimized_sql:
        transforms = infer_transforms_from_sql_diff(original_sql, optimized_sql)

    return transforms


def apply_rewrite(original_sql: str, llm_response: str, dialect: str = "duckdb") -> str:
    """Convenience function to apply LLM rewrite to SQL.

    Args:
        original_sql: The original SQL query
        llm_response: Raw LLM response with rewrite_sets
        dialect: SQL dialect

    Returns:
        Optimized SQL string
    """
    rewriter = SQLRewriter(original_sql, dialect=dialect)
    return rewriter.get_optimized_sql(llm_response)
