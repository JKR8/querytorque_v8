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
    # Core transforms with verified speedups (13 gold patterns)
    "pushdown",                 # 2.11x Q9 - Push filters into CTEs/subqueries
    "decorrelate",              # 2.92x Q1 - Correlated subquery -> CTE with GROUP BY
    "or_to_union",              # 3.17x Q15 - OR conditions -> UNION ALL branches
    "early_filter",             # 4.00x Q93 - Filter dimension tables before joining
    "date_cte_isolate",         # 4.00x Q6 - Extract date filtering into early CTE
    "materialize_cte",          # 1.37x Q95 - Extract repeated subqueries into CTE
    "intersect_to_exists",      # 1.83x Q14 - INTERSECT to EXISTS
    "union_cte_split",          # 1.36x Q74 - Year-specialized CTEs
    "single_pass_aggregation",  # 4.47x Q9 - CASE WHEN inside aggregates
    "dimension_cte_isolate",    # 1.93x Q26 - Pre-filter dimension tables into CTEs
    "multi_dimension_prefetch", # 2.71x Q43 - Pre-filter multiple dimension tables
    "prefetch_fact_join",       # 3.77x Q63 - Pre-join fact table with filtered dims
    "multi_date_range_cte",     # 2.35x Q29 - Separate CTEs for date aliases

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
    node_contracts: Dict[str, List[str]] = field(default_factory=dict)  # node_id -> output columns
    set_local: List[str] = field(default_factory=list)  # SET LOCAL commands from JSON


@dataclass
class RewriteResult:
    """Result of applying a rewrite to SQL."""
    success: bool
    optimized_sql: str
    transform: str = ""
    error: Optional[str] = None
    rewrite_set: Optional[RewriteSet] = None
    set_local_commands: List[str] = field(default_factory=list)


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
                node_contracts=rs.get("node_contracts", {}),
                set_local=rs.get("set_local", []),
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

    @staticmethod
    def _extract_raw_sql(response: str) -> Optional[str]:
        """Extract raw SQL from a ```sql code block in the response."""
        # Find SQL code blocks
        matches = re.findall(r'```sql\s*(.*?)\s*```', response, re.DOTALL)
        if not matches:
            return None

        # Take the first SQL block that looks like a real query (not a comment-only block)
        for match in matches:
            stripped = match.strip()
            if stripped == "-- Your rewritten query here":
                continue
            # Must contain SELECT to be a real query
            if "SELECT" in stripped.upper():
                return stripped

        return None

    @staticmethod
    def _split_set_local(sql: str) -> tuple[str, list[str]]:
        """Split SET LOCAL commands from the beginning of extracted SQL.

        Handles formats:
          - SET LOCAL work_mem = '512MB'; SET LOCAL jit = 'off'; SELECT ...
          - BEGIN; SET LOCAL ...; SELECT ...; COMMIT;

        Returns: (clean_sql, [set_local_commands])
        """
        from .pg_tuning import PG_TUNABLE_PARAMS

        if not sql or "SET LOCAL" not in sql.upper():
            return sql, []

        # Strip optional BEGIN/COMMIT wrapper
        stripped = sql.strip()
        if stripped.upper().startswith("BEGIN"):
            stripped = re.sub(r'^BEGIN\s*;\s*', '', stripped, flags=re.IGNORECASE)
        if stripped.rstrip().upper().endswith("COMMIT;") or stripped.rstrip().upper().endswith("COMMIT"):
            stripped = re.sub(r'\s*;\s*COMMIT\s*;?\s*$', '', stripped, flags=re.IGNORECASE)

        # Split by semicolons, preserving content
        parts = [p.strip() for p in stripped.split(';') if p.strip()]

        set_local_cmds: list[str] = []
        sql_parts: list[str] = []

        for part in parts:
            if re.match(r'^SET\s+LOCAL\s+', part, re.IGNORECASE):
                # Validate the param name against whitelist
                param_match = re.match(
                    r'^SET\s+LOCAL\s+(\w+)\s*=', part, re.IGNORECASE
                )
                if param_match and param_match.group(1).lower() in PG_TUNABLE_PARAMS:
                    set_local_cmds.append(part)
                # else: silently skip non-whitelisted params
            elif part.upper().startswith("SET "):
                # Non-LOCAL SET — skip (not safe)
                pass
            else:
                sql_parts.append(part)

        # Rejoin SQL parts with semicolons (multi-statement queries are rare but possible)
        clean_sql = "; ".join(sql_parts)
        if clean_sql and not clean_sql.rstrip().endswith(";"):
            # Don't add trailing semicolon — let caller decide
            pass

        return clean_sql, set_local_cmds

    def apply_response(self, llm_response: str) -> RewriteResult:
        """Parse LLM response and apply the rewrite.

        Supports two response formats (tried in order):
        1. JSON with rewrite_sets (per-node DAG format — preferred)
        2. Raw SQL in a ```sql code block (fallback)

        Args:
            llm_response: Raw LLM response text (may contain markdown, JSON, etc.)

        Returns:
            RewriteResult with optimized SQL or error
        """
        # Try JSON rewrite_sets first (per-node DAG format)
        rewrite_sets = self.parser.parse(llm_response)

        if rewrite_sets:
            for rs in rewrite_sets:
                # Validate transform
                if rs.transform not in ALLOWED_TRANSFORMS:
                    continue

                try:
                    optimized_sql = self.assembler.assemble(self.original_sql, rs)

                    # Use SET LOCAL from JSON field if available, else split from SQL
                    if rs.set_local:
                        from .pg_tuning import PG_TUNABLE_PARAMS
                        set_local_cmds = [
                            cmd for cmd in rs.set_local
                            if any(p in cmd.lower() for p in PG_TUNABLE_PARAMS)
                        ]
                        clean_sql = optimized_sql
                    else:
                        clean_sql, set_local_cmds = self._split_set_local(optimized_sql)
                    if not clean_sql:
                        continue

                    # Validate the result parses
                    if not self._validate_sql(clean_sql):
                        continue

                    return RewriteResult(
                        success=True,
                        optimized_sql=clean_sql,
                        transform=rs.transform,
                        rewrite_set=rs,
                        set_local_commands=set_local_cmds,
                    )
                except Exception:
                    continue

        # Fall back to raw SQL extraction (```sql code block)
        raw_sql = self._extract_raw_sql(llm_response)
        if raw_sql:
            clean_sql, set_local_cmds = self._split_set_local(raw_sql)
            if clean_sql and self._validate_sql(clean_sql):
                transforms = infer_transforms_from_sql_diff(
                    self.original_sql, clean_sql
                )
                return RewriteResult(
                    success=True,
                    optimized_sql=clean_sql,
                    transform=transforms[0] if transforms else "semantic_rewrite",
                    set_local_commands=set_local_cmds,
                )

        return RewriteResult(
            success=False,
            optimized_sql=self.original_sql,
            error="No valid rewrite_sets or SQL found in LLM response",
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

def _get_cte_names_and_bodies(ast) -> Dict[str, Any]:
    """Extract CTE names and their body ASTs from a parsed SQL AST."""
    ctes = {}
    for cte in ast.find_all(exp.CTE):
        name = str(cte.alias).lower() if cte.alias else None
        if name and cte.this:
            ctes[name] = cte.this
    return ctes


def _get_table_refs(ast) -> List[str]:
    """Get all table references (not CTE refs) from an AST node."""
    refs = []
    for table in ast.find_all(exp.Table):
        name = table.name.lower() if table.name else ""
        if name:
            refs.append(name)
    return refs


def _sql_lower(sql: str) -> str:
    """Normalize SQL for text-level pattern matching."""
    return " ".join(sql.lower().split())


# Dimension table names in TPC-DS / DSB
_DIM_TABLES = frozenset([
    "date_dim", "item", "customer", "customer_address", "customer_demographics",
    "store", "store_returns", "household_demographics", "promotion", "time_dim",
    "income_band", "reason", "ship_mode", "warehouse", "web_page", "web_site",
    "call_center", "catalog_page",
])

# Date-specific filter columns
_DATE_FILTER_COLS = frozenset([
    "d_year", "d_moy", "d_qoy", "d_date", "d_month_seq", "d_week_seq",
    "d_date_sk",
])


def infer_transforms_from_sql_diff(
    original_sql: str,
    optimized_sql: str,
    dialect: str = "duckdb",
) -> List[str]:
    """Infer transforms by analyzing SQL structure differences.

    Fallback mechanism when LLM response doesn't explicitly name transforms.
    Uses sqlglot AST parsing for structural detection of all 13 gold transforms.

    Args:
        original_sql: Original query
        optimized_sql: Optimized query
        dialect: SQL dialect for parsing

    Returns:
        List of inferred transform names (ordered by confidence)
    """
    if not sqlglot or not original_sql or not optimized_sql:
        return []

    # Normalize whitespace for comparison
    if original_sql.strip() == optimized_sql.strip():
        return []

    inferred = []
    orig_lower = _sql_lower(original_sql)
    opt_lower = _sql_lower(optimized_sql)

    try:
        orig_ast = sqlglot.parse_one(original_sql, dialect=dialect)
        opt_ast = sqlglot.parse_one(optimized_sql, dialect=dialect)
    except Exception:
        # Fall back to text-only heuristics if AST parsing fails
        return _infer_transforms_text_only(orig_lower, opt_lower)

    try:
        # ── Structural counts ──
        orig_subqueries = len(list(orig_ast.find_all(exp.Subquery)))
        opt_subqueries = len(list(opt_ast.find_all(exp.Subquery)))

        orig_ctes = _get_cte_names_and_bodies(orig_ast)
        opt_ctes = _get_cte_names_and_bodies(opt_ast)
        new_cte_names = set(opt_ctes.keys()) - set(orig_ctes.keys())

        orig_unions = len(list(orig_ast.find_all(exp.Union)))
        opt_unions = len(list(opt_ast.find_all(exp.Union)))

        orig_or_count = len(list(orig_ast.find_all(exp.Or)))
        opt_or_count = len(list(opt_ast.find_all(exp.Or)))

        orig_windows = len(list(orig_ast.find_all(exp.Window)))
        opt_windows = len(list(opt_ast.find_all(exp.Window)))

        # ── 1. decorrelate: correlated subquery → JOIN or window function ──
        # Detected when subquery count drops and JOIN/CTE/window count increases
        if orig_subqueries > opt_subqueries and len(opt_ctes) >= len(orig_ctes):
            inferred.append("decorrelate")
        # Also: correlated subquery replaced by window function
        # (subquery count may stay same if correlated sub becomes derived table)
        elif opt_windows > orig_windows and orig_subqueries >= opt_subqueries:
            inferred.append("decorrelate")

        # ── 2. or_to_union: OR conditions → UNION ALL branches ──
        if opt_unions > orig_unions and opt_or_count < orig_or_count:
            inferred.append("or_to_union")

        # ── 3. union_cte_split: UNION split into separate CTEs ──
        if "union all" in opt_lower and "union" in orig_lower:
            if "union all" not in orig_lower:
                inferred.append("union_cte_split")

        # ── 4. intersect_to_exists: INTERSECT/IN subquery → EXISTS ──
        orig_intersects = orig_lower.count("intersect")
        opt_intersects = opt_lower.count("intersect")
        opt_exists = len(list(opt_ast.find_all(exp.Exists)))
        orig_exists = len(list(orig_ast.find_all(exp.Exists)))
        if (orig_intersects > opt_intersects and opt_exists > orig_exists):
            inferred.append("intersect_to_exists")
        # Also detect IN-subquery → EXISTS conversion
        elif opt_exists > orig_exists and orig_subqueries > opt_subqueries:
            orig_in_count = len(list(orig_ast.find_all(exp.In)))
            opt_in_count = len(list(opt_ast.find_all(exp.In)))
            if orig_in_count > opt_in_count:
                inferred.append("intersect_to_exists")

        # ── 5. single_pass_aggregation: CASE WHEN inside aggregate functions ──
        # Detected when optimized query has CASE inside COUNT/SUM/AVG and
        # original has multiple scans or subqueries of the same table
        opt_case_in_agg = _count_case_in_aggregates(opt_ast)
        orig_case_in_agg = _count_case_in_aggregates(orig_ast)
        if opt_case_in_agg > orig_case_in_agg and opt_case_in_agg >= 2:
            inferred.append("single_pass_aggregation")

        # ── 6-8. CTE-based dimension/date isolation ──
        if new_cte_names:
            date_ctes = 0
            dim_ctes = 0
            fact_join_ctes = 0
            multi_date_ctes = 0

            for cte_name in new_cte_names:
                cte_body = opt_ctes[cte_name]
                table_refs = [t.lower() for t in _get_table_refs(cte_body)]
                cte_sql = cte_body.sql(dialect=dialect).lower() if cte_body else ""

                # Check if this CTE references date_dim with date filters
                has_date_dim = "date_dim" in table_refs
                has_date_filter = any(col in cte_sql for col in _DATE_FILTER_COLS)

                # Check if this CTE references dimension tables (non-date)
                dim_refs = [t for t in table_refs if t in _DIM_TABLES and t != "date_dim"]

                # Check if this CTE joins a fact table with a pre-filtered dim
                fact_tables = {"store_sales", "catalog_sales", "web_sales",
                              "store_returns", "catalog_returns", "web_returns",
                              "inventory"}
                has_fact = any(t in fact_tables for t in table_refs)

                if has_date_dim and has_date_filter and not has_fact:
                    date_ctes += 1
                elif dim_refs and not has_fact:
                    dim_ctes += 1
                elif has_fact and (has_date_dim or dim_refs):
                    fact_join_ctes += 1

            # 6. date_cte_isolate: New CTE referencing date_dim with filters
            if date_ctes >= 1:
                # Check for multiple date CTEs (multi_date_range_cte)
                if date_ctes >= 2:
                    multi_date_ctes = date_ctes

            # 7. dimension_cte_isolate: New CTE referencing dimension tables
            # 8. multi_dimension_prefetch: Multiple dimension CTEs

            if date_ctes >= 1 and "date_cte_isolate" not in inferred:
                inferred.append("date_cte_isolate")

            if dim_ctes >= 2:
                inferred.append("multi_dimension_prefetch")
            elif dim_ctes >= 1:
                inferred.append("dimension_cte_isolate")

            if multi_date_ctes >= 2:
                inferred.append("multi_date_range_cte")

            # 9. prefetch_fact_join: CTE that joins fact table with pre-filtered dim
            if fact_join_ctes >= 1:
                inferred.append("prefetch_fact_join")

        # ── 10. early_filter: WHERE pushed into CTE/subquery ──
        # Detected when new CTEs contain WHERE clauses with selective filters
        # and the outer query has fewer WHERE conditions
        if new_cte_names and not inferred:
            # If we added CTEs but didn't match any specific pattern above,
            # it's likely an early_filter optimization
            orig_where_count = len(list(orig_ast.find_all(exp.Where)))
            opt_where_count = len(list(opt_ast.find_all(exp.Where)))
            if opt_where_count > orig_where_count or new_cte_names:
                inferred.append("early_filter")

        # ── 11. pushdown: Filter predicates moved deeper ──
        # Detected when WHERE clause conditions move from outer to inner queries
        if not new_cte_names and not inferred:
            # Check if existing CTEs got new WHERE clauses
            for cte_name in set(orig_ctes.keys()) & set(opt_ctes.keys()):
                orig_body_sql = orig_ctes[cte_name].sql(dialect=dialect).lower() if orig_ctes[cte_name] else ""
                opt_body_sql = opt_ctes[cte_name].sql(dialect=dialect).lower() if opt_ctes[cte_name] else ""
                if "where" in opt_body_sql and "where" not in orig_body_sql:
                    inferred.append("pushdown")
                    break

        # ── 12. materialize_cte: Subexpression → CTE ──
        # Only add if no more specific CTE pattern was found
        if new_cte_names and "materialize_cte" not in inferred:
            # Check if the new CTEs don't match date/dim/fact patterns
            specific_cte_patterns = {
                "date_cte_isolate", "dimension_cte_isolate",
                "multi_dimension_prefetch", "prefetch_fact_join",
                "multi_date_range_cte",
            }
            if not (set(inferred) & specific_cte_patterns):
                inferred.append("materialize_cte")

        # ── 13. early_filter additional: when existing CTEs get filters ──
        if not inferred:
            # Last resort: if CTEs existed but got modified with more conditions
            for cte_name in set(orig_ctes.keys()) & set(opt_ctes.keys()):
                orig_body = orig_ctes[cte_name].sql(dialect=dialect).lower() if orig_ctes[cte_name] else ""
                opt_body = opt_ctes[cte_name].sql(dialect=dialect).lower() if opt_ctes[cte_name] else ""
                if len(opt_body) > len(orig_body) * 1.1:  # Substantial change
                    inferred.append("early_filter")
                    break

    except Exception:
        pass  # Silent fallback - return whatever we found so far

    # Deduplicate while preserving order
    seen = set()
    result = []
    for t in inferred:
        if t not in seen:
            seen.add(t)
            result.append(t)
    return result


def _count_case_in_aggregates(ast) -> int:
    """Count CASE expressions nested inside aggregate functions (SUM, COUNT, AVG)."""
    count = 0
    agg_types = (exp.Sum, exp.Count, exp.Avg, exp.Min, exp.Max)
    for agg in ast.find_all(*agg_types):
        for case in agg.find_all(exp.Case, exp.If):
            count += 1
    return count


def _infer_transforms_text_only(orig_lower: str, opt_lower: str) -> List[str]:
    """Text-only heuristic fallback when AST parsing fails."""
    inferred = []

    # Check for CASE in aggregate pattern (single_pass_aggregation)
    case_agg_pattern = r'(sum|count|avg)\s*\(\s*case\s+when'
    opt_case_aggs = len(re.findall(case_agg_pattern, opt_lower))
    orig_case_aggs = len(re.findall(case_agg_pattern, orig_lower))
    if opt_case_aggs > orig_case_aggs and opt_case_aggs >= 2:
        inferred.append("single_pass_aggregation")

    # Check for new CTEs with date_dim
    if "date_dim" in opt_lower and "as (" in opt_lower:
        if opt_lower.count("as (") > orig_lower.count("as ("):
            inferred.append("date_cte_isolate")

    # Check for EXISTS increase
    if opt_lower.count("exists") > orig_lower.count("exists"):
        if orig_lower.count("intersect") > opt_lower.count("intersect"):
            inferred.append("intersect_to_exists")

    # Check for UNION ALL increase with OR decrease
    if opt_lower.count("union all") > orig_lower.count("union all"):
        if orig_lower.count(" or ") > opt_lower.count(" or "):
            inferred.append("or_to_union")

    # Check for more CTEs in general
    if opt_lower.count("as (") > orig_lower.count("as ("):
        if not inferred:
            inferred.append("materialize_cte")

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
