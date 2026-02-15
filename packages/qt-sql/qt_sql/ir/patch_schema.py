"""Patch Rewrite Schema — atomic operations against the IR.

Patches target structural nodes (by ID, label, or anchor hash),
not line numbers.  Each step is atomic + gate-validated.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Dict, List, Optional

from .schema import Dialect


class PatchOp(Enum):
    INSERT_STATEMENT_BEFORE = "insert_statement_before"
    INSERT_VIEW_STATEMENT = "insert_view_statement"
    INSERT_CTE = "insert_cte"
    REPLACE_EXPR_SUBTREE = "replace_expr_subtree"
    REPLACE_WHERE_PREDICATE = "replace_where_predicate"
    REPLACE_JOIN_CONDITION = "replace_join_condition"
    REPLACE_BLOCK_WITH_CTE_PAIR = "replace_block_with_cte_pair"
    SPLIT_CTE = "split_cte"
    WRAP_QUERY_WITH_CTE = "wrap_query_with_cte"
    DELETE_EXPR_SUBTREE = "delete_expr_subtree"
    REPLACE_FROM = "replace_from"
    REPLACE_SELECT = "replace_select"
    REPLACE_BODY = "replace_body"


class GateKind(Enum):
    PARSE_OK = "parse_ok"
    BIND_OK = "bind_ok"
    EXPLAIN_OK = "explain_ok"
    ORACLE_EQ = "oracle_eq"
    PLAN_SHAPE = "plan_shape"


@dataclass
class Gate:
    kind: GateKind
    args: Dict[str, Any] = field(default_factory=dict)


@dataclass
class PatchTarget:
    """How to locate the node to patch.  At least one field must be set."""

    by_node_id: Optional[str] = None
    by_label: Optional[str] = None
    by_anchor_hash: Optional[str] = None
    by_path: Optional[str] = None  # fallback


@dataclass
class PatchPayload:
    """What to insert / replace."""

    sql_fragment: Optional[str] = None
    expr_sql: Optional[str] = None
    cte_name: Optional[str] = None
    cte_query_sql: Optional[str] = None
    from_sql: Optional[str] = None


@dataclass
class PatchStep:
    """A single atomic patch operation."""

    step_id: str
    op: PatchOp
    target: PatchTarget
    payload: PatchPayload = field(default_factory=PatchPayload)
    gates: List[Gate] = field(default_factory=list)
    description: Optional[str] = None


@dataclass
class PatchPlan:
    """An ordered sequence of PatchSteps with pre/postconditions."""

    plan_id: str
    dialect: Dialect
    steps: List[PatchStep] = field(default_factory=list)
    preconditions: List[Gate] = field(default_factory=list)
    postconditions: List[Gate] = field(default_factory=list)
    target_script_id: Optional[str] = None
    description: Optional[str] = None


@dataclass
class PatchResult:
    """Result of applying a PatchPlan."""

    plan_id: str
    success: bool
    steps_applied: int
    steps_total: int
    output_sql: Optional[str] = None
    errors: List[str] = field(default_factory=list)
    gate_results: Dict[str, bool] = field(default_factory=dict)
