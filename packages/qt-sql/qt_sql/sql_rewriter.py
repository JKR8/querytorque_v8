"""SQL rewriting for ADO - applies LLM optimizations to SQL.

This module handles parsing LLM responses and applying SQL rewrites.
Supports two JSON output formats:
  1. DAP (Decomposed Attention Protocol) — structured component payload
  2. Legacy rewrite_sets — per-node DAG format

Key features:
- Parse DAP Component Payload or rewrite_sets JSON from LLM responses
- Assemble SQL from DAP components or rewrite_set nodes
- AST-based equivalence validation (output columns, base tables)
- Validate transforms against allowlist
"""

from __future__ import annotations

import json
import logging
import re
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Set

logger = logging.getLogger(__name__)

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
# DAP (Decomposed Attention Protocol) Data Structures
# =============================================================================

@dataclass
class DAPComponent:
    """A single component in a DAP statement (CTE, main_query, subquery)."""
    component_id: str
    type: str           # cte | main_query | subquery | setup
    change: str         # modified | unchanged | added | removed
    sql: str
    interfaces: Optional[Dict[str, Any]] = None


@dataclass
class DAPStatement:
    """One statement in a DAP payload (maps to CREATE TABLE AS or standalone)."""
    target_table: Optional[str]
    change: str         # modified | unchanged | added | removed
    components: Dict[str, DAPComponent] = field(default_factory=dict)
    reconstruction_order: List[str] = field(default_factory=list)
    assembly_template: str = ""


@dataclass
class DAPPayload:
    """Parsed DAP Component Payload JSON (Part 2 of DAP spec)."""
    spec_version: str
    dialect: str
    rewrite_rules: List[Dict[str, Any]] = field(default_factory=list)
    statements: List[DAPStatement] = field(default_factory=list)
    macros: Dict[str, Dict[str, Any]] = field(default_factory=dict)
    frozen_blocks: List[Dict[str, Any]] = field(default_factory=list)
    runtime_config: List[str] = field(default_factory=list)    # SET LOCAL commands
    validation_checks: List[Dict[str, Any]] = field(default_factory=list)


# =============================================================================
# AST-Based Equivalence Validation
# =============================================================================

@dataclass
class ASTValidationResult:
    """Result of static AST equivalence check between original and rewritten SQL."""
    valid: bool
    errors: List[str] = field(default_factory=list)
    original_columns: List[str] = field(default_factory=list)
    rewritten_columns: List[str] = field(default_factory=list)
    original_tables: Set[str] = field(default_factory=set)
    rewritten_tables: Set[str] = field(default_factory=set)


def validate_ast_equivalence(
    original_sql: str,
    rewritten_sql: str,
    dialect: str = "duckdb",
) -> ASTValidationResult:
    """Static AST equivalence check — runs before any database call.

    Three checks:
    1. Output column check: same count, names, order
    2. Base table check: rewritten must reference same base tables
    3. Parse validity: rewritten SQL must parse cleanly

    Args:
        original_sql: The original SQL query
        rewritten_sql: The assembled rewritten SQL
        dialect: SQL dialect for parsing

    Returns:
        ASTValidationResult with pass/fail and details
    """
    if sqlglot is None:
        return ASTValidationResult(valid=True)  # Can't validate without sqlglot

    errors: List[str] = []
    orig_cols: List[str] = []
    rewr_cols: List[str] = []
    orig_tables: Set[str] = set()
    rewr_tables: Set[str] = set()

    try:
        orig_ast = sqlglot.parse_one(original_sql, dialect=dialect)
    except Exception as e:
        # If original doesn't parse, skip validation
        return ASTValidationResult(valid=True, errors=[f"Original parse skip: {e}"])

    try:
        rewr_ast = sqlglot.parse_one(rewritten_sql, dialect=dialect)
    except Exception as e:
        return ASTValidationResult(
            valid=False, errors=[f"Rewritten SQL parse error: {e}"]
        )

    # ── 1. Output column check ──
    orig_cols = _extract_root_columns(orig_ast, dialect)
    rewr_cols = _extract_root_columns(rewr_ast, dialect)

    if orig_cols and rewr_cols:
        orig_lower = [c.lower() for c in orig_cols]
        rewr_lower = [c.lower() for c in rewr_cols]

        if len(orig_lower) != len(rewr_lower):
            errors.append(
                f"Column count mismatch: original has {len(orig_lower)}, "
                f"rewritten has {len(rewr_lower)}"
            )
        else:
            for i, (oc, rc) in enumerate(zip(orig_lower, rewr_lower)):
                if oc != rc:
                    errors.append(
                        f"Column {i+1} mismatch: original='{orig_cols[i]}', "
                        f"rewritten='{rewr_cols[i]}'"
                    )

    # ── 2. Base table check ──
    orig_tables = _extract_base_tables(orig_ast, dialect)
    rewr_tables = _extract_base_tables(rewr_ast, dialect)

    if orig_tables and not rewr_tables:
        # Original references real tables but rewrite has none — reject
        errors.append(f"Rewrite lost all base tables: original has {orig_tables}")
    elif orig_tables and rewr_tables:
        missing = orig_tables - rewr_tables
        if missing:
            errors.append(f"Missing base tables in rewrite: {missing}")
        # Note: added tables are OK (new CTEs reference them)

    return ASTValidationResult(
        valid=len(errors) == 0,
        errors=errors,
        original_columns=orig_cols,
        rewritten_columns=rewr_cols,
        original_tables=orig_tables,
        rewritten_tables=rewr_tables,
    )


def _extract_root_columns(ast, dialect: str) -> List[str]:
    """Extract output column names from the root SELECT of a parsed AST."""
    try:
        # Navigate to root SELECT (handle WITH wrapper)
        select = ast
        if hasattr(ast, 'this') and not isinstance(ast, exp.Select):
            select = ast.this
        if not isinstance(select, exp.Select):
            select = ast.find(exp.Select)
        if not select:
            return []

        columns = []
        for expr in select.expressions:
            if isinstance(expr, exp.Alias) and expr.alias:
                columns.append(str(expr.alias))
            elif isinstance(expr, exp.Column):
                columns.append(str(expr.name))
            elif isinstance(expr, exp.Star):
                columns.append("*")
            else:
                # Unnamed expression — use SQL fragment
                columns.append(expr.sql(dialect=dialect)[:50])
        return columns
    except Exception:
        return []


def _extract_base_tables(ast, dialect: str) -> Set[str]:
    """Extract base table names (excluding CTE names) from a parsed AST."""
    try:
        # Get CTE names to exclude
        cte_names = set()
        for cte in ast.find_all(exp.CTE):
            if cte.alias:
                cte_names.add(str(cte.alias).lower())

        # Get all table references (schema-qualified)
        tables = set()
        for table in ast.find_all(exp.Table):
            name = table.name.lower() if table.name else ""
            if not name or name in cte_names:
                continue
            # Build schema-qualified identity: schema.name or just name
            parts = []
            if hasattr(table, 'catalog') and table.catalog:
                parts.append(table.catalog.lower())
            if hasattr(table, 'db') and table.db:
                parts.append(table.db.lower())
            parts.append(name)
            tables.add(".".join(parts))
        return tables
    except Exception:
        return set()


# =============================================================================
# JSON Response Parser
# =============================================================================

class ResponseParser:
    """Parse LLM responses — supports DAP and legacy rewrite_sets."""

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
    def detect_format(json_str: str) -> str:
        """Detect JSON format: 'dap' | 'rewrite_sets' | 'unknown'.

        DAP: has spec_version + statements array
        Legacy: has rewrite_sets array
        """
        try:
            data = json.loads(json_str)
        except (json.JSONDecodeError, TypeError):
            return "unknown"

        if isinstance(data, dict):
            if "spec_version" in data and "statements" in data:
                return "dap"
            if "rewrite_sets" in data:
                return "rewrite_sets"
        return "unknown"

    @staticmethod
    def parse_dap_payload(json_str: str) -> Optional[DAPPayload]:
        """Parse DAP Component Payload JSON into DAPPayload dataclass."""
        try:
            data = json.loads(json_str)
        except (json.JSONDecodeError, TypeError):
            return None

        if not isinstance(data, dict) or "spec_version" not in data:
            return None

        # Parse statements
        statements: List[DAPStatement] = []
        for stmt_data in data.get("statements", []):
            components: Dict[str, DAPComponent] = {}
            for comp_id, comp_data in stmt_data.get("components", {}).items():
                if isinstance(comp_data, dict):
                    components[comp_id] = DAPComponent(
                        component_id=comp_id,
                        type=comp_data.get("type", "cte"),
                        change=comp_data.get("change", "modified"),
                        sql=comp_data.get("sql", ""),
                        interfaces=comp_data.get("interfaces"),
                    )

            statements.append(DAPStatement(
                target_table=stmt_data.get("target_table"),
                change=stmt_data.get("change", "modified"),
                components=components,
                reconstruction_order=stmt_data.get("reconstruction_order", []),
                assembly_template=stmt_data.get("assembly_template", ""),
            ))

        # Parse runtime_config (SET LOCAL commands)
        runtime_config = data.get("runtime_config", [])
        if isinstance(runtime_config, list):
            runtime_config = [str(c) for c in runtime_config]
        else:
            runtime_config = []

        return DAPPayload(
            spec_version=data.get("spec_version", "1.0"),
            dialect=data.get("dialect", "duckdb"),
            rewrite_rules=data.get("rewrite_rules", []),
            statements=statements,
            macros=data.get("macros", {}),
            frozen_blocks=data.get("frozen_blocks", []),
            runtime_config=runtime_config,
            validation_checks=data.get("validation_checks", []),
        )

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
        """Parse LLM response to extract all rewrite_sets (legacy path)."""
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
# DAP Assembler - Reassemble SQL from DAP Component Payload
# =============================================================================

class DAPAssembler:
    """Reassemble executable SQL from a DAP Component Payload.

    Implements the reconstruction algorithm from DAP spec section 6:
    1. Expand macros: replace -- [MACRO: x] with macros[x].sql
    2. For each component_id in reconstruction_order:
       a. Get component SQL
       b. If unchanged + no sql: pull from original via sqlglot
    3. Interpolate into assembly_template
    4. Fallback: topo-sort if no template (reuse SQLAssembler logic)
    """

    def __init__(self, dialect: str = "duckdb"):
        self.dialect = dialect
        self._legacy_assembler = SQLAssembler(dialect=dialect)

    def assemble(
        self,
        original_sql: str,
        dap: DAPPayload,
        stmt_idx: int = 0,
    ) -> str:
        """Assemble executable SQL from a DAP payload.

        Args:
            original_sql: The original SQL (for unchanged components)
            dap: Parsed DAPPayload
            stmt_idx: Which statement to assemble (default 0 for single-query)

        Returns:
            Complete, executable SQL string
        """
        if not dap.statements:
            return original_sql

        if stmt_idx >= len(dap.statements):
            return original_sql

        stmt = dap.statements[stmt_idx]

        # Get component SQLs, expanding macros
        component_sqls: Dict[str, str] = {}
        for comp_id, comp in stmt.components.items():
            sql = comp.sql
            if not sql and comp.change == "unchanged":
                # Pull from original via sqlglot
                sql = self._extract_component_from_original(original_sql, comp_id)
            if sql:
                sql = self._expand_macros(sql, dap.macros)
            component_sqls[comp_id] = sql or ""

        # Try assembly_template first
        if stmt.assembly_template:
            try:
                assembled = stmt.assembly_template
                for comp_id, sql in component_sqls.items():
                    placeholder = "{" + comp_id + "}"
                    assembled = assembled.replace(placeholder, sql)
                # Verify no unresolved placeholders remain
                if "{" not in assembled or not re.search(r'\{[a-zA-Z_]\w*\}', assembled):
                    return assembled.strip()
            except Exception:
                pass

        # Fallback: use reconstruction_order or topo-sort
        order = stmt.reconstruction_order
        if not order:
            order = list(component_sqls.keys())

        # Separate CTEs from main_query
        main_id = None
        cte_ids = []
        for comp_id in order:
            comp = stmt.components.get(comp_id)
            if comp and comp.type == "main_query":
                main_id = comp_id
            elif comp_id == "main_query":
                main_id = comp_id
            else:
                cte_ids.append(comp_id)

        # Build as WITH ... SELECT
        main_sql = component_sqls.get(main_id or "main_query", "")
        if not main_sql:
            # Last component in order as main
            if order:
                main_id = order[-1]
                main_sql = component_sqls.get(main_id, "")
                cte_ids = [c for c in order if c != main_id]

        if main_sql.strip().upper().startswith("WITH "):
            return main_sql.strip()

        # Use legacy assembler's topo-sort for CTE ordering
        cte_nodes = {cid: component_sqls[cid] for cid in cte_ids if component_sqls.get(cid)}
        if not cte_nodes:
            return main_sql.strip()

        deps = self._legacy_assembler._build_dependency_graph(cte_nodes)
        sorted_ctes = self._legacy_assembler._topological_sort(deps)

        cte_clauses = []
        for cte_id in sorted_ctes:
            sql = cte_nodes.get(cte_id, "")
            if sql:
                cte_clauses.append(f"{cte_id} AS ({sql})")

        if cte_clauses:
            return f"WITH {', '.join(cte_clauses)}\n{main_sql.strip()}"
        return main_sql.strip()

    def _expand_macros(self, sql: str, macros: Dict[str, Dict[str, Any]]) -> str:
        """Replace -- [MACRO: name] comments with macro SQL."""
        if not macros:
            return sql

        for macro_name, macro_data in macros.items():
            macro_sql = macro_data.get("sql", "")
            if macro_sql:
                # Replace inline macro reference comments
                pattern = r'--\s*\[MACRO:\s*' + re.escape(macro_name) + r'\s*\]'
                sql = re.sub(pattern, macro_sql, sql, flags=re.IGNORECASE)
        return sql

    def _extract_component_from_original(
        self, original_sql: str, component_id: str
    ) -> str:
        """Extract a component's SQL from the original query via sqlglot."""
        if sqlglot is None:
            return ""

        try:
            parsed = sqlglot.parse_one(original_sql, dialect=self.dialect)

            if component_id == "main_query":
                return self._legacy_assembler._extract_main_query(original_sql)

            # Try to find CTE by name
            for cte in parsed.find_all(exp.CTE):
                name = str(cte.alias) if cte.alias else None
                if name and name.lower() == component_id.lower() and cte.this:
                    return cte.this.sql(dialect=self.dialect)
        except Exception:
            pass
        return ""


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
        self.dap_assembler = DAPAssembler(dialect=dialect)

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

        Supports three response formats (tried in priority order):
        1. DAP Component Payload JSON (spec_version + statements)
        2. JSON with rewrite_sets (per-node DAG format — legacy)
        3. Raw SQL in a ```sql code block (fallback)

        Args:
            llm_response: Raw LLM response text (may contain markdown, JSON, etc.)

        Returns:
            RewriteResult with optimized SQL or error
        """
        # Extract JSON from response
        json_str = self.parser.extract_json(llm_response)

        if json_str:
            fmt = self.parser.detect_format(json_str)

            # ── Priority 1: DAP Component Payload ──
            if fmt == "dap":
                dap = self.parser.parse_dap_payload(json_str)
                if dap:
                    result = self._apply_dap(dap)
                    if result.success:
                        return result
                    # DAP parse succeeded but assembly/validation failed —
                    # fall through to legacy paths

            # ── Priority 2: Legacy rewrite_sets ──
            rewrite_sets = self.parser.parse_rewrite_sets(json_str)
            if rewrite_sets:
                for rs in rewrite_sets:
                    if rs.transform not in ALLOWED_TRANSFORMS:
                        continue

                    try:
                        optimized_sql = self.assembler.assemble(self.original_sql, rs)

                        # Use SET LOCAL from JSON field if available, else split from SQL
                        if rs.set_local:
                            from .pg_tuning import PG_TUNABLE_PARAMS
                            set_local_cmds = [
                                cmd for cmd in rs.set_local
                                if _extract_set_local_param(cmd) in PG_TUNABLE_PARAMS
                            ]
                            clean_sql = optimized_sql
                        else:
                            clean_sql, set_local_cmds = self._split_set_local(optimized_sql)
                        if not clean_sql:
                            continue

                        # Validate the result parses
                        if not self._validate_sql(clean_sql):
                            continue

                        # AST equivalence check — hard fail, same as DAP path
                        ast_check = validate_ast_equivalence(
                            self.original_sql, clean_sql, self.dialect
                        )
                        if not ast_check.valid:
                            logger.warning(
                                "AST check failed on rewrite_set %s: %s",
                                rs.id, ast_check.errors,
                            )
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

        # ── Priority 3: Raw SQL extraction (```sql code block) ──
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
            error="No valid DAP, rewrite_sets, or SQL found in LLM response",
        )

    def _apply_dap(self, dap: DAPPayload) -> RewriteResult:
        """Apply a DAP Component Payload to produce a RewriteResult.

        Steps:
        1. Assemble SQL via DAPAssembler
        2. Run AST equivalence check
        3. Extract transforms from rewrite_rules
        4. Extract SET LOCAL from runtime_config
        """
        try:
            assembled = self.dap_assembler.assemble(self.original_sql, dap)
        except Exception as e:
            return RewriteResult(
                success=False,
                optimized_sql=self.original_sql,
                error=f"DAP assembly failed: {e}",
            )

        if not assembled or not assembled.strip():
            return RewriteResult(
                success=False,
                optimized_sql=self.original_sql,
                error="DAP assembly produced empty SQL",
            )

        # Handle SET LOCAL from runtime_config
        set_local_cmds: List[str] = []
        if dap.runtime_config:
            try:
                from .pg_tuning import PG_TUNABLE_PARAMS
                set_local_cmds = [
                    cmd for cmd in dap.runtime_config
                    if re.match(r'^SET\s+LOCAL\s+', cmd, re.IGNORECASE)
                    and _extract_set_local_param(cmd) in PG_TUNABLE_PARAMS
                ]
            except ImportError:
                pass
        clean_sql, split_cmds = self._split_set_local(assembled)
        set_local_cmds.extend(split_cmds)
        if not clean_sql:
            clean_sql = assembled

        # Validate SQL parses
        if not self._validate_sql(clean_sql):
            return RewriteResult(
                success=False,
                optimized_sql=self.original_sql,
                error="DAP assembled SQL does not parse",
            )

        # AST equivalence check
        ast_check = validate_ast_equivalence(
            self.original_sql, clean_sql, self.dialect
        )
        if not ast_check.valid:
            return RewriteResult(
                success=False,
                optimized_sql=self.original_sql,
                error=f"DAP AST validation failed: {ast_check.errors[0]}",
            )

        # Extract transforms from rewrite_rules
        transforms: List[str] = []
        for rule in dap.rewrite_rules:
            rtype = rule.get("type", "")
            if rtype and rtype not in transforms:
                transforms.append(rtype)

        # Map DAP rule types to our transform names
        _DAP_TRANSFORM_MAP = {
            "predicate_pushdown": "pushdown",
            "subquery_decorrelation": "decorrelate",
            "join_elimination": "remove_redundant",
            "cte_extraction": "materialize_cte",
            "macro_dedup": "materialize_cte",
            "materialization": "materialize_cte",
            "type_cast_cleanup": "semantic_rewrite",
            "union_consolidation": "or_to_union",
        }
        mapped = []
        for t in transforms:
            mapped_name = _DAP_TRANSFORM_MAP.get(t, t)
            if mapped_name in ALLOWED_TRANSFORMS and mapped_name not in mapped:
                mapped.append(mapped_name)

        # Fallback: infer from SQL diff
        if not mapped:
            mapped = infer_transforms_from_sql_diff(
                self.original_sql, clean_sql, self.dialect
            )

        transform_name = mapped[0] if mapped else "semantic_rewrite"
        if transform_name not in ALLOWED_TRANSFORMS:
            transform_name = "semantic_rewrite"

        return RewriteResult(
            success=True,
            optimized_sql=clean_sql,
            transform=transform_name,
            set_local_commands=set_local_cmds,
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

def _extract_set_local_param(cmd: str) -> str:
    """Extract the exact parameter name from a SET LOCAL command.

    E.g. 'SET LOCAL work_mem = ...' -> 'work_mem'
    Returns empty string if pattern doesn't match.
    """
    m = re.match(r"^SET\s+LOCAL\s+(\w+)\s*=", cmd, re.IGNORECASE)
    return m.group(1).lower() if m else ""


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
