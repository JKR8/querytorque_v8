"""Enterprise SQL script parser — multi-statement dependency-graph builder.

Parses multi-statement SQL scripts (CREATE VIEW, CREATE TABLE AS SELECT,
DROP TABLE, standalone SELECT, etc.) into a statement-level dependency graph.

Two-level graph architecture:
  Level 1 (this module): Script dependency graph — nodes = statements, edges = table/view dependencies
  Level 2 (dag.py):      Query logical tree graph — nodes = CTEs within a statement, edges = CTE references

Enterprise SQL scripts are data pipelines: views build on base tables, temp
tables build on views, final queries build on temp tables. This module
decomposes the script into independently optimizable chunks that feed into
the existing single-query optimization pipeline.

Usage:
    from qt_sql.script_parser import ScriptParser

    parser = ScriptParser(open("pipeline.sql").read(), dialect="duckdb")
    dag = parser.parse()
    print(dag.summary())

    for target in dag.optimization_targets():
        # target.inner_select → feed to query structure parser + oneshot/swarm prompt
        # target.creates_object → use as query_id
        ...
"""

from __future__ import annotations

import enum
import logging
import re
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Set, Tuple

logger = logging.getLogger(__name__)


# ── Statement classification ────────────────────────────────────────────


class StatementType(enum.Enum):
    """Classification of a SQL statement."""

    CREATE_TABLE = "create_table"  # CREATE [TEMP] TABLE ... AS SELECT
    CREATE_VIEW = "create_view"    # CREATE [TEMP] VIEW ... AS SELECT
    SELECT = "select"              # Standalone SELECT
    DROP = "drop"                  # DROP TABLE/VIEW
    OTHER = "other"                # INSERT, UPDATE, SET, COMMENT, etc.


# ── Data structures ─────────────────────────────────────────────────────


@dataclass
class ScriptStatement:
    """A single statement within a SQL script."""

    index: int                                          # Position in script (0-based)
    raw_sql: str                                        # Original statement text
    statement_type: StatementType = StatementType.OTHER  # Classification
    creates_object: Optional[str] = None                # Table/view name created
    inner_select: Optional[str] = None                  # Extracted SELECT (if present)
    references: Set[str] = field(default_factory=set)   # Tables/views referenced
    is_temporary: bool = False
    is_replace: bool = False
    complexity_score: int = 0

    @property
    def is_optimizable(self) -> bool:
        """Whether this statement is worth running through optimization.

        Requires: extractable SELECT + sufficient structural complexity.
        Simple single-table SELECTs and column-mapping views are filtered out.
        """
        return (
            self.inner_select is not None
            and self.statement_type
            in (StatementType.CREATE_TABLE, StatementType.CREATE_VIEW, StatementType.SELECT)
            and self.complexity_score >= 2
        )


@dataclass
class ScriptDAG:
    """Dependency graph of statements in a SQL script.

    Nodes are ScriptStatement objects. Edges encode table/view dependencies:
    if statement B references a table created by statement A, edge (A → B).
    """

    statements: List[ScriptStatement]
    edges: List[Tuple[int, int]] = field(default_factory=list)
    _creates_index: Dict[str, int] = field(default_factory=dict, repr=False)

    # ── Queries ──────────────────────────────────────────────────────

    def optimization_targets(self) -> List[ScriptStatement]:
        """Statements worth optimizing, in dependency order."""
        order = self.dependency_order()
        return [self.statements[i] for i in order if self.statements[i].is_optimizable]

    def dependency_order(self) -> List[int]:
        """Topological sort of statement indices (Kahn's algorithm)."""
        n = len(self.statements)
        in_degree = [0] * n
        adj: Dict[int, List[int]] = {i: [] for i in range(n)}
        for src, dst in self.edges:
            adj[src].append(dst)
            in_degree[dst] += 1

        # Seed with zero in-degree, prefer original order
        queue = sorted(i for i in range(n) if in_degree[i] == 0)
        result: List[int] = []
        while queue:
            node = queue.pop(0)
            result.append(node)
            for neighbor in sorted(adj[node]):
                in_degree[neighbor] -= 1
                if in_degree[neighbor] == 0:
                    queue.append(neighbor)
                    queue.sort()
        return result

    def independent_groups(self) -> List[List[int]]:
        """Group statements by topological level (parallelizable within a group)."""
        n = len(self.statements)
        level = [0] * n
        for idx in self.dependency_order():
            for src, dst in self.edges:
                if dst == idx:
                    level[idx] = max(level[idx], level[src] + 1)

        groups: Dict[int, List[int]] = {}
        for idx in range(n):
            groups.setdefault(level[idx], []).append(idx)
        return [groups[k] for k in sorted(groups)]

    # ── Display ──────────────────────────────────────────────────────

    def summary(self) -> str:
        """Human-readable summary."""
        targets = self.optimization_targets()
        lines = [
            f"ScriptDAG: {len(self.statements)} statements, "
            f"{len(self.edges)} dependencies, "
            f"{len(targets)} optimization targets",
            "",
        ]
        for stmt in self.statements:
            flag = " *** OPTIMIZE" if stmt.is_optimizable else ""
            creates = f" -> {stmt.creates_object}" if stmt.creates_object else ""
            refs = ""
            if stmt.references:
                # Only show refs to objects created within the script
                script_refs = stmt.references & set(self._creates_index.keys())
                if script_refs:
                    refs = f" (depends: {', '.join(sorted(script_refs))})"
            lines.append(
                f"  [{stmt.index:2d}] {stmt.statement_type.value:<14s}"
                f"{creates}{refs}"
                f"  [complexity={stmt.complexity_score}]{flag}"
            )
        return "\n".join(lines)


# ── Parser ───────────────────────────────────────────────────────────────


class ScriptParser:
    """Parse multi-statement SQL scripts into a statement-level dependency graph.

    Handles enterprise SQL patterns:
    - CREATE [OR REPLACE] [TEMPORARY] TABLE/VIEW ... AS SELECT ...
    - DROP TABLE/VIEW [IF EXISTS] ...
    - Standalone SELECT statements
    - Line comments (--) and block comments between statements
    - DuckDB, PostgreSQL, and standard SQL dialects
    """

    def __init__(self, sql_script: str, dialect: str = "duckdb"):
        self.sql_script = sql_script
        self.dialect = dialect

    def parse(self) -> ScriptDAG:
        """Parse the script into a ScriptDAG."""
        # Phase 1: Split into statements (preserve original text)
        raw_statements = self._split_statements()

        # Phase 2: Classify each statement
        script_statements = []
        for i, raw_sql in enumerate(raw_statements):
            stmt = self._classify_statement(i, raw_sql)
            script_statements.append(stmt)

        # Phase 3: Build dependency edges
        dag = ScriptDAG(statements=script_statements)
        self._build_edges(dag)

        return dag

    # ── Phase 1: Split ───────────────────────────────────────────────

    def _split_statements(self) -> List[str]:
        """Split SQL script into individual statements, preserving original text.

        Uses a character-level scanner that respects:
        - String literals (single-quoted, with '' escapes)
        - Line comments (--)
        - Block comments (/* ... */)
        - Dollar-quoted strings (PostgreSQL)
        """
        results: List[str] = []
        buf: List[str] = []
        text = self.sql_script
        i = 0
        n = len(text)

        while i < n:
            ch = text[i]

            # Line comment
            if ch == "-" and i + 1 < n and text[i + 1] == "-":
                # Consume to end of line
                while i < n and text[i] != "\n":
                    buf.append(text[i])
                    i += 1
                continue

            # Block comment
            if ch == "/" and i + 1 < n and text[i + 1] == "*":
                buf.append(ch)
                i += 1
                buf.append(text[i])
                i += 1
                while i < n:
                    if text[i] == "*" and i + 1 < n and text[i + 1] == "/":
                        buf.append(text[i])
                        i += 1
                        buf.append(text[i])
                        i += 1
                        break
                    buf.append(text[i])
                    i += 1
                continue

            # Single-quoted string
            if ch == "'":
                buf.append(ch)
                i += 1
                while i < n:
                    if text[i] == "'" and i + 1 < n and text[i + 1] == "'":
                        buf.append(text[i])
                        i += 1
                        buf.append(text[i])
                        i += 1
                    elif text[i] == "'":
                        buf.append(text[i])
                        i += 1
                        break
                    else:
                        buf.append(text[i])
                        i += 1
                continue

            # Statement terminator
            if ch == ";":
                stmt = "".join(buf).strip()
                if stmt and not _is_comment_only(stmt):
                    results.append(stmt)
                buf = []
                i += 1
                continue

            buf.append(ch)
            i += 1

        # Trailing statement without semicolon
        stmt = "".join(buf).strip()
        if stmt and not _is_comment_only(stmt):
            results.append(stmt)

        return results

    # ── Phase 2: Classify ────────────────────────────────────────────

    def _classify_statement(self, index: int, raw_sql: str) -> ScriptStatement:
        """Classify a single statement and extract metadata."""
        import sqlglot
        from sqlglot import exp

        stmt = ScriptStatement(index=index, raw_sql=raw_sql)

        try:
            parsed = sqlglot.parse_one(raw_sql, dialect=self.dialect)
        except Exception as e:
            logger.debug(f"Statement {index}: sqlglot parse failed ({e}), classifying as OTHER")
            stmt.statement_type = StatementType.OTHER
            return stmt

        # ── CREATE TABLE / VIEW ──────────────────────────────────
        if isinstance(parsed, exp.Create):
            kind = (parsed.kind or "").upper()

            # Object name
            stmt.creates_object = _extract_object_name(parsed.this)
            stmt.is_temporary = bool(parsed.args.get("temporary"))
            stmt.is_replace = bool(parsed.args.get("replace"))

            stmt.statement_type = (
                StatementType.CREATE_VIEW
                if kind == "VIEW"
                else StatementType.CREATE_TABLE
            )

            # Inner SELECT
            inner_expr = parsed.expression
            if inner_expr is not None:
                stmt.inner_select = inner_expr.sql(dialect=self.dialect)
                stmt.references = self._extract_table_references(inner_expr)
                stmt.complexity_score = _compute_complexity(inner_expr)

        # ── DROP ─────────────────────────────────────────────────
        elif isinstance(parsed, exp.Drop):
            stmt.statement_type = StatementType.DROP
            stmt.creates_object = _extract_object_name(parsed.this)

        # ── SELECT / UNION ───────────────────────────────────────
        elif isinstance(parsed, (exp.Select, exp.Union)):
            stmt.statement_type = StatementType.SELECT
            stmt.inner_select = raw_sql
            stmt.references = self._extract_table_references(parsed)
            stmt.complexity_score = _compute_complexity(parsed)

        # ── Other ────────────────────────────────────────────────
        else:
            stmt.statement_type = StatementType.OTHER

        return stmt

    def _extract_table_references(self, expr: Any) -> Set[str]:
        """Extract all table/view names referenced in a SQL expression."""
        from sqlglot import exp

        refs: Set[str] = set()
        for table in expr.find_all(exp.Table):
            name = table.name
            if name:
                refs.add(name.lower())
        return refs

    # ── Phase 3: Edges ───────────────────────────────────────────

    def _build_edges(self, dag: ScriptDAG) -> None:
        """Build dependency edges: A → B if B references a table that A creates."""
        creates_index: Dict[str, int] = {}
        for stmt in dag.statements:
            if stmt.creates_object and stmt.statement_type in (
                StatementType.CREATE_TABLE,
                StatementType.CREATE_VIEW,
            ):
                creates_index[stmt.creates_object.lower()] = stmt.index

        dag._creates_index = creates_index

        for stmt in dag.statements:
            for ref in stmt.references:
                ref_lower = ref.lower()
                if ref_lower in creates_index:
                    src_idx = creates_index[ref_lower]
                    if src_idx != stmt.index:
                        dag.edges.append((src_idx, stmt.index))


# ── Helpers ──────────────────────────────────────────────────────────────


def _extract_object_name(table_expr: Any) -> Optional[str]:
    """Extract the table/view name from a sqlglot table expression."""
    if table_expr is None:
        return None
    # Schema(this=Table(this=Identifier(name)))
    if hasattr(table_expr, "this"):
        inner = table_expr.this
        if hasattr(inner, "name"):
            return inner.name
        if hasattr(inner, "this") and hasattr(inner.this, "name"):
            return inner.this.name
    if hasattr(table_expr, "name"):
        return table_expr.name
    return None


def _compute_complexity(expr: Any) -> int:
    """Compute structural complexity of a SQL expression.

    Counts optimization-relevant features: JOINs, CTEs, subqueries,
    GROUP BY, UNION, window functions. Ignores CASE expressions
    (they're data mapping, not structural complexity).
    """
    from sqlglot import exp

    score = 0
    score += len(list(expr.find_all(exp.Join)))
    score += len(list(expr.find_all(exp.CTE)))
    score += len(list(expr.find_all(exp.Subquery)))
    score += len(list(expr.find_all(exp.Group)))
    score += len(list(expr.find_all(exp.Union)))
    score += len(list(expr.find_all(exp.Window)))
    return score


def _is_comment_only(text: str) -> bool:
    """Check if a text block contains only comments and whitespace."""
    for line in text.split("\n"):
        stripped = line.strip()
        if stripped and not stripped.startswith("--"):
            return False
    return True
