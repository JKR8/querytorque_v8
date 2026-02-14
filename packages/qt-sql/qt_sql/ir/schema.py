"""IR Schema — Intermediate Representation for multi-statement SQL scripts.

Supports partial rewrites via stable node IDs and structural patching.
Node types: ScriptIR > StatementIR > QueryIR > ExprIR/FromIR/JoinIR/CTEIR
"""
from __future__ import annotations

import hashlib
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Dict, List, Optional


# ── Enums ──────────────────────────────────────────────────────────────


class Dialect(Enum):
    DUCKDB = "duckdb"
    POSTGRES = "postgres"
    SNOWFLAKE = "snowflake"


class StatementKind(Enum):
    CREATE_VIEW = "create_view"
    DROP_TABLE = "drop_table"
    CREATE_TABLE_AS = "create_table_as"
    SELECT = "select"
    CREATE_TABLE = "create_table"
    INSERT = "insert"
    OTHER_DDL = "other_ddl"


class ExprKind(Enum):
    COL = "col"
    LIT = "lit"
    FUNC = "func"
    CASE = "case"
    CAST = "cast"
    BINOP = "binop"
    UNOP = "unop"
    SUBQUERY = "subquery"
    AGG = "agg"
    WINDOW_FUNC = "window_func"
    STAR = "star"
    ALIAS = "alias"
    OTHER = "other"


class FromKind(Enum):
    TABLE = "table"
    SUBQUERY = "subquery"
    JOIN = "join"


class JoinType(Enum):
    INNER = "inner"
    LEFT = "left"
    RIGHT = "right"
    CROSS = "cross"
    FULL = "full"


class MaterializationHint(Enum):
    DEFAULT = "default"
    MATERIALIZE = "materialize"
    INLINE = "inline"


# ── Utility ────────────────────────────────────────────────────────────


def canonical_hash(sql: str) -> str:
    """Canonical hash of SQL text for anchor matching."""
    normalized = " ".join(sql.lower().split())
    return hashlib.sha256(normalized.encode()).hexdigest()[:16]


# ── Expression IR ──────────────────────────────────────────────────────


@dataclass
class ExprIR:
    """A single expression node in the IR tree."""

    id: str
    kind: ExprKind
    sql_text: str
    type: Optional[str] = None
    children: List[ExprIR] = field(default_factory=list)
    props: Dict[str, Any] = field(default_factory=dict)
    labels: List[str] = field(default_factory=list)
    snippet_hash: str = ""
    _ast_node: Any = field(default=None, repr=False)

    def __post_init__(self):
        if not self.snippet_hash and self.sql_text:
            self.snippet_hash = canonical_hash(self.sql_text)


# ── From / Join IR ─────────────────────────────────────────────────────


@dataclass
class TableRefIR:
    name: str
    alias: Optional[str] = None
    schema: Optional[str] = None


@dataclass
class SubqueryRefIR:
    query: QueryIR
    alias: Optional[str] = None


@dataclass
class JoinIR:
    join_type: JoinType
    left: FromIR
    right: FromIR
    condition: Optional[ExprIR] = None
    hints: Dict[str, Any] = field(default_factory=dict)


@dataclass
class FromIR:
    kind: FromKind
    table: Optional[TableRefIR] = None
    subquery: Optional[SubqueryRefIR] = None
    join: Optional[JoinIR] = None


# ── Window / Order IR ─────────────────────────────────────────────────


@dataclass
class WindowSpecIR:
    partition_by: List[ExprIR] = field(default_factory=list)
    order_by: List[OrderItemIR] = field(default_factory=list)


@dataclass
class OrderItemIR:
    expr: ExprIR
    desc: bool = False


# ── CTE IR ─────────────────────────────────────────────────────────────


@dataclass
class CTEIR:
    name: str
    query: QueryIR
    materialization_hint: Optional[MaterializationHint] = None


# ── Dialect Features ───────────────────────────────────────────────────


@dataclass
class DialectFeatures:
    uses_filter_agg: bool = False
    uses_interval: bool = False
    uses_window_row_number: bool = False
    uses_qualify: bool = False
    uses_lateral: bool = False


# ── Query IR ───────────────────────────────────────────────────────────


@dataclass
class QueryIR:
    """IR for a single query (SELECT / UNION / subquery body)."""

    id: str
    with_ctes: List[CTEIR] = field(default_factory=list)
    select_list: List[ExprIR] = field(default_factory=list)
    from_clause: Optional[FromIR] = None
    where: Optional[ExprIR] = None
    group_by: List[ExprIR] = field(default_factory=list)
    having: Optional[ExprIR] = None
    windows: List[WindowSpecIR] = field(default_factory=list)
    order_by: List[OrderItemIR] = field(default_factory=list)
    limit: Optional[ExprIR] = None
    dialect_features: DialectFeatures = field(default_factory=DialectFeatures)
    _ast_node: Any = field(default=None, repr=False)


# ── Cost Hints ─────────────────────────────────────────────────────────


@dataclass
class CostHints:
    estimated_rows: Optional[float] = None
    estimated_cost: Optional[float] = None
    wall_time_ms: Optional[float] = None


# ── Relation Reference ─────────────────────────────────────────────────


@dataclass
class RelationRef:
    name: str
    schema: Optional[str] = None
    alias: Optional[str] = None


# ── Statement IR ───────────────────────────────────────────────────────


@dataclass
class StatementIR:
    """IR for a single SQL statement (DDL / DML / query)."""

    id: str
    kind: StatementKind
    sql_text: str
    ast: Any = field(default=None, repr=False)
    query: Optional[QueryIR] = None
    reads: List[RelationRef] = field(default_factory=list)
    writes: List[RelationRef] = field(default_factory=list)
    tags: List[str] = field(default_factory=list)
    cost_hints: Optional[CostHints] = None
    labels: List[str] = field(default_factory=list)


# ── Symbol Table ───────────────────────────────────────────────────────


@dataclass
class SymbolEntry:
    name: str
    kind: str  # "view", "table", "cte"
    defined_in: str  # statement_id
    columns: List[str] = field(default_factory=list)


@dataclass
class SymbolTable:
    entries: Dict[str, SymbolEntry] = field(default_factory=dict)

    def add(
        self,
        name: str,
        kind: str,
        defined_in: str,
        columns: Optional[List[str]] = None,
    ):
        self.entries[name.lower()] = SymbolEntry(
            name=name, kind=kind, defined_in=defined_in, columns=columns or []
        )

    def lookup(self, name: str) -> Optional[SymbolEntry]:
        return self.entries.get(name.lower())


# ── Reference Index ────────────────────────────────────────────────────


@dataclass
class UseSite:
    """A location in the IR where a pattern was found."""

    statement_id: str
    query_id: Optional[str] = None
    expr_id: Optional[str] = None
    path: str = ""
    snippet_hash: str = ""
    labels: List[str] = field(default_factory=list)


@dataclass
class DuplicateGroup:
    """A group of expression subtrees that are structurally identical."""

    canonical_hash: str
    canonical_sql: str
    sites: List[UseSite] = field(default_factory=list)
    estimated_cost: Optional[str] = None


@dataclass
class FingerprintIndex:
    hashes: Dict[str, str] = field(default_factory=dict)  # node_id -> hash


@dataclass
class ReferenceIndex:
    """Cross-script reference index for safe global rewrites."""

    relation_reads: Dict[str, List[UseSite]] = field(default_factory=dict)
    scalar_subqueries: List[UseSite] = field(default_factory=list)
    function_calls: Dict[str, List[UseSite]] = field(default_factory=dict)
    duplicate_expr_groups: List[DuplicateGroup] = field(default_factory=list)


# ── Script IR (top-level) ─────────────────────────────────────────────


@dataclass
class ScriptIR:
    """Top-level IR for a multi-statement SQL script."""

    script_id: str
    dialect: Dialect
    statements: List[StatementIR] = field(default_factory=list)
    symbols: SymbolTable = field(default_factory=SymbolTable)
    references: ReferenceIndex = field(default_factory=ReferenceIndex)
    fingerprints: FingerprintIndex = field(default_factory=FingerprintIndex)
