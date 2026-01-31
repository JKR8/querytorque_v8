"""Base dataclasses and protocols for execution plan handling."""

from dataclasses import dataclass, field
from typing import Any, Optional, Protocol


class DBExecutor(Protocol):
    """Protocol for database executors."""

    def connect(self) -> None:
        """Open connection to database."""
        ...

    def close(self) -> None:
        """Close connection."""
        ...

    def execute(self, sql: str) -> list[dict[str, Any]]:
        """Execute SQL and return results as list of dicts."""
        ...

    def explain(self, sql: str, analyze: bool = True) -> dict[str, Any]:
        """Get execution plan as JSON dict."""
        ...

    def execute_script(self, sql_script: str) -> None:
        """Execute multi-statement SQL script."""
        ...


@dataclass
class PlanNode:
    """A node in the execution plan tree."""

    operator_type: str
    """Operator type (e.g., 'HASH_JOIN', 'SEQ_SCAN', 'PROJECTION')."""

    timing_seconds: float
    """Operator execution time in seconds."""

    cardinality: int
    """Number of rows actually returned by this operator."""

    cost_percentage: float
    """Percentage of total query cost (0-100)."""

    extra_info: dict[str, Any] = field(default_factory=dict)
    """Operator-specific information (join type, filter conditions, etc.)."""

    children: list["PlanNode"] = field(default_factory=list)
    """Child operators in the plan tree."""

    is_bottleneck: bool = False
    """True if this operator is identified as a bottleneck."""

    line_reference: Optional[int] = None
    """Line in SQL that caused this operator (if determinable)."""

    estimated_cardinality: Optional[int] = None
    """Estimated number of rows (from optimizer) - None if not available."""


@dataclass
class ExecutionPlanAnalysis:
    """Complete execution plan analysis for rendering in reports."""

    total_cost: float
    """Total estimated cost of the query."""

    execution_time_ms: float
    """Actual execution time in milliseconds."""

    planning_time_ms: float = 0.0
    """Query planning time in milliseconds."""

    estimated_rows: int = 0
    """Estimated number of rows returned."""

    actual_rows: Optional[int] = None
    """Actual number of rows returned (if ANALYZE was run)."""

    plan_tree: list[dict[str, Any]] = field(default_factory=list)
    """Flattened plan tree for template rendering with indent levels."""

    bottleneck: Optional[dict[str, Any]] = None
    """Information about the primary bottleneck operator."""

    warnings: list[str] = field(default_factory=list)
    """Plan warnings (sequential scans, cross products, etc.)."""

    raw_plan: dict[str, Any] = field(default_factory=dict)
    """Original plan JSON from the database."""

    def to_template_context(self) -> dict[str, Any]:
        """Convert to template context for sql_report.html.j2.

        Returns both the original plan_tree format AND an operators list
        that the execution summary renderer expects.
        """
        # Build operators list in the format renderer expects
        operators = []
        for node in self.plan_tree:
            operators.append({
                "name": node.get("operator", "Unknown"),
                "time_ms": node.get("timing_ms", 0),
                "actual_rows": node.get("rows", 0),
                "cost_pct": node.get("cost_pct", 0),
            })

        return {
            "total_cost": self.total_cost,
            "estimated_rows": self.estimated_rows,
            "actual_rows": self.actual_rows,
            "execution_time_ms": round(self.execution_time_ms, 2),
            "total_time_ms": round(self.execution_time_ms, 2),  # Alias for renderer
            "planning_time_ms": round(self.planning_time_ms, 2),
            "bottleneck": self.bottleneck,
            "plan_tree": self.plan_tree,
            "operators": operators,  # For execution summary
            "warnings": self.warnings,
        }
