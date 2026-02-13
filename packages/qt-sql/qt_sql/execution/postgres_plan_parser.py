"""Parse PostgreSQL EXPLAIN JSON into structured analysis.

Supports both EXPLAIN (FORMAT JSON) and EXPLAIN ANALYZE (FORMAT JSON)
for comprehensive plan analysis including cardinality estimation checks.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Iterator, Optional

from .base import ExecutionPlanAnalysis, PlanNode
from .plan_parser import PlanIssue


# PostgreSQL node types that indicate potential performance issues
PG_OPERATOR_WARNINGS: dict[str, str] = {
    "Seq Scan": "Sequential scan - consider adding an index",
    "Nested Loop": "Nested loop join - can be slow for large tables",
    "Materialize": "Materialization - query results being stored temporarily",
    "Sort": "Sort operation - consider index for ORDER BY",
    "Hash": "Hash build - memory-intensive operation",
    "Bitmap Heap Scan": "Bitmap scan - multiple index conditions combined",
}

# PostgreSQL operators that are typically expensive
PG_EXPENSIVE_OPERATORS = {
    "Hash Join",
    "Merge Join",
    "Sort",
    "Aggregate",
    "GroupAggregate",
    "HashAggregate",
    "WindowAgg",
    "Materialize",
    "CTE Scan",
}

# Node types indicating good index usage
PG_INDEX_SCAN_TYPES = {
    "Index Scan",
    "Index Only Scan",
    "Bitmap Index Scan",
}


@dataclass
class PostgresPlanNode:
    """A node in a PostgreSQL execution plan.

    Attributes:
        node_type: PostgreSQL node type (e.g., "Seq Scan", "Hash Join")
        relation_name: Table name if applicable
        alias: Table alias
        startup_cost: Cost before first row
        total_cost: Total estimated cost
        plan_rows: Estimated row count
        actual_rows: Actual row count (ANALYZE only)
        actual_time: Actual execution time in ms (ANALYZE only)
        actual_loops: Number of loop iterations
        filter_condition: Filter applied to this node
        rows_removed_by_filter: Rows filtered out
        join_type: Type of join (for join nodes)
        index_name: Index used (for index scans)
        sort_key: Sort columns (for sort nodes)
        extra_info: Additional node-specific information
        children: Child nodes
    """

    node_type: str
    relation_name: Optional[str] = None
    alias: Optional[str] = None
    startup_cost: float = 0.0
    total_cost: float = 0.0
    plan_rows: int = 0
    actual_rows: int = 0
    actual_time: float = 0.0
    actual_loops: int = 1
    filter_condition: Optional[str] = None
    rows_removed_by_filter: int = 0
    join_type: Optional[str] = None
    index_name: Optional[str] = None
    sort_key: Optional[list[str]] = None
    extra_info: dict[str, Any] = field(default_factory=dict)
    children: list["PostgresPlanNode"] = field(default_factory=list)

    @property
    def is_seq_scan(self) -> bool:
        """Check if this is a sequential scan."""
        return self.node_type == "Seq Scan"

    @property
    def is_index_scan(self) -> bool:
        """Check if this uses an index."""
        return self.node_type in PG_INDEX_SCAN_TYPES

    @property
    def cardinality_error(self) -> float:
        """Calculate estimation error ratio (actual/estimated)."""
        if self.plan_rows == 0:
            return 1.0
        return self.actual_rows / self.plan_rows if self.actual_rows > 0 else 0.0


class PostgresPlanParser:
    """Parse PostgreSQL EXPLAIN JSON into ExecutionPlanAnalysis.

    PostgreSQL's EXPLAIN JSON format includes:
    - Plan: Root plan node with nested structure
    - Planning Time: Time spent planning (ms)
    - Execution Time: Time spent executing (ms, ANALYZE only)

    Each plan node has:
    - Node Type: e.g., "Seq Scan", "Hash Join", "Index Scan"
    - Relation Name: Table name (for scans)
    - Total Cost: Estimated total cost
    - Plan Rows: Estimated row count
    - Actual Rows: Actual row count (ANALYZE only)
    - Actual Total Time: Actual time in ms (ANALYZE only)
    - Filter: Filter condition if any
    - Plans: Child nodes
    """

    def parse(self, plan_json: dict[str, Any]) -> ExecutionPlanAnalysis:
        """Parse PostgreSQL EXPLAIN JSON into structured analysis.

        Args:
            plan_json: PostgreSQL EXPLAIN JSON output.

        Returns:
            ExecutionPlanAnalysis ready for template rendering.
        """
        # Handle error plans
        if "error" in plan_json:
            return self._error_plan(plan_json)

        # Extract top-level metrics
        planning_time = plan_json.get("Planning Time", 0)
        execution_time = plan_json.get("Execution Time", 0)
        total_time = planning_time + execution_time

        # Parse the plan tree
        root_plan = plan_json.get("Plan", {})
        root_node = self._parse_node(root_plan)

        # Calculate total cost for percentage calculation
        total_cost = root_node.total_cost if root_node else 0

        # Flatten tree for template rendering
        plan_tree = self._flatten_tree(root_node, total_cost)

        # Identify bottleneck (most expensive operator)
        bottleneck = self._identify_bottleneck(plan_tree)

        # Extract warnings
        warnings = self._extract_warnings(root_node)

        # Get row counts
        estimated_rows = root_node.plan_rows if root_node else 0
        actual_rows = root_node.actual_rows if root_node else 0

        return ExecutionPlanAnalysis(
            total_cost=total_cost,
            execution_time_ms=total_time,
            estimated_rows=estimated_rows,
            actual_rows=actual_rows,
            plan_tree=plan_tree,
            bottleneck=bottleneck,
            warnings=warnings,
            raw_plan=plan_json,
        )

    def _error_plan(self, plan_json: dict[str, Any]) -> ExecutionPlanAnalysis:
        """Create analysis for error plans."""
        return ExecutionPlanAnalysis(
            total_cost=0,
            execution_time_ms=0,
            plan_tree=[
                {
                    "indent": 0,
                    "operator": "ERROR",
                    "details": plan_json.get("error", "Unknown error"),
                    "cost_pct": 100,
                    "rows": 0,
                    "is_bottleneck": False,
                }
            ],
            warnings=[f"Plan error: {plan_json.get('error', 'Unknown')}"],
            raw_plan=plan_json,
        )

    def _parse_node(self, plan: dict[str, Any]) -> PostgresPlanNode:
        """Parse a single plan node and its children recursively."""
        if not plan:
            return PostgresPlanNode(node_type="EMPTY")

        # Parse children first
        children = []
        for child_plan in plan.get("Plans", []):
            children.append(self._parse_node(child_plan))

        # Build extra_info with relevant details
        extra_info: dict[str, Any] = {}

        # Capture interesting fields
        for key in ["Output", "Parallel Aware", "Workers Planned", "Workers Launched",
                    "Sort Space Used", "Sort Space Type", "Sort Method",
                    "Join Filter", "Recheck Cond", "Index Cond"]:
            if key in plan:
                extra_info[key] = plan[key]

        return PostgresPlanNode(
            node_type=plan.get("Node Type", "Unknown"),
            relation_name=plan.get("Relation Name"),
            alias=plan.get("Alias"),
            startup_cost=plan.get("Startup Cost", 0),
            total_cost=plan.get("Total Cost", 0),
            plan_rows=plan.get("Plan Rows", 0),
            actual_rows=plan.get("Actual Rows", 0),
            actual_time=plan.get("Actual Total Time", 0),
            actual_loops=plan.get("Actual Loops", 1),
            filter_condition=plan.get("Filter"),
            rows_removed_by_filter=plan.get("Rows Removed by Filter", 0),
            join_type=plan.get("Join Type"),
            index_name=plan.get("Index Name"),
            sort_key=plan.get("Sort Key"),
            extra_info=extra_info,
            children=children,
        )

    def _flatten_tree(
        self, node: PostgresPlanNode, total_cost: float, depth: int = 0
    ) -> list[dict[str, Any]]:
        """Flatten plan tree for template rendering."""
        if not node or node.node_type == "EMPTY":
            return []

        result = []

        # Calculate cost percentage
        cost_pct = 0
        if total_cost > 0:
            cost_pct = round((node.total_cost / total_cost) * 100, 1)

        # Build details string
        details_parts = []
        if node.relation_name:
            details_parts.append(f"on {node.relation_name}")
        if node.index_name:
            details_parts.append(f"using {node.index_name}")
        if node.filter_condition:
            filter_short = node.filter_condition[:50]
            if len(node.filter_condition) > 50:
                filter_short += "..."
            details_parts.append(f"filter: {filter_short}")
        if node.join_type:
            details_parts.append(f"{node.join_type} join")

        details = " | ".join(details_parts) if details_parts else ""

        # Use actual rows if available, otherwise plan rows
        rows = node.actual_rows if node.actual_rows > 0 else node.plan_rows

        result.append({
            "indent": depth,
            "operator": node.node_type,
            "details": details,
            "cost_pct": cost_pct,
            "rows": rows,
            "actual_time_ms": node.actual_time,
            "is_bottleneck": False,  # Set later
        })

        # Recursively process children
        for child in node.children:
            result.extend(self._flatten_tree(child, total_cost, depth + 1))

        return result

    def _identify_bottleneck(self, plan_tree: list[dict[str, Any]]) -> Optional[dict[str, Any]]:
        """Identify the most expensive operator in the plan.

        Returns a dict matching ExecutionPlanAnalysis.bottleneck shape:
        {operator, cost_pct, rows, details, suggestion}
        """
        if not plan_tree:
            return None

        # Find highest cost percentage
        max_cost = 0
        bottleneck_idx = 0

        for i, item in enumerate(plan_tree):
            # Use actual time if available, otherwise cost percentage
            cost = item.get("actual_time_ms", 0) or item.get("cost_pct", 0)
            if cost > max_cost:
                max_cost = cost
                bottleneck_idx = i

        if plan_tree:
            node = plan_tree[bottleneck_idx]
            node["is_bottleneck"] = True

            operator = node.get("operator", "Unknown")
            details = node.get("details", "")
            cost_pct = node.get("cost_pct", 0)
            rows = node.get("rows", 0)

            # Generate suggestion based on operator type
            suggestion = ""
            if operator == "Seq Scan":
                suggestion = "Consider adding an index on filter/join columns"
            elif operator in ("Sort", "IncrementalSort"):
                suggestion = "Consider adding an index for presorted data or increasing work_mem"
            elif operator == "Hash Join":
                suggestion = "Check join selectivity and consider index-based join"
            elif operator in ("Materialize", "CTE Scan"):
                suggestion = "Consider inlining the CTE or reducing materialization"

            return {
                "operator": operator,
                "cost_pct": cost_pct,
                "rows": rows,
                "details": details,
                "suggestion": suggestion,
            }

        return None

    def _extract_warnings(self, node: PostgresPlanNode) -> list[str]:
        """Extract performance warnings from plan."""
        warnings = []
        self._collect_warnings(node, warnings)
        return warnings

    def _collect_warnings(
        self, node: PostgresPlanNode, warnings: list[str]
    ) -> None:
        """Recursively collect warnings from plan nodes."""
        if not node or node.node_type == "EMPTY":
            return

        # Check for sequential scans on tables
        if node.is_seq_scan and node.relation_name:
            if node.actual_rows > 1000 or node.plan_rows > 1000:
                warnings.append(
                    f"Sequential scan on {node.relation_name} "
                    f"({node.actual_rows or node.plan_rows} rows) - consider index"
                )

        # Check for high filter removal ratio
        if node.rows_removed_by_filter > 0:
            total = node.actual_rows + node.rows_removed_by_filter
            if total > 0:
                removal_ratio = node.rows_removed_by_filter / total
                if removal_ratio > 0.9:
                    warnings.append(
                        f"Filter on {node.relation_name or node.node_type} removes "
                        f"{int(removal_ratio * 100)}% of rows - consider index"
                    )

        # Check for cardinality estimation errors
        if node.actual_rows > 0 and node.plan_rows > 0:
            error = node.cardinality_error
            if error > 10 or error < 0.1:
                warnings.append(
                    f"Cardinality estimate error on {node.node_type}: "
                    f"planned {node.plan_rows}, actual {node.actual_rows}"
                )

        # Check for expensive operators
        if node.node_type in PG_EXPENSIVE_OPERATORS:
            if node.actual_time > 100:  # > 100ms
                warnings.append(
                    f"Expensive {node.node_type} ({node.actual_time:.1f}ms)"
                )

        # Recursively check children
        for child in node.children:
            self._collect_warnings(child, warnings)

    def detect_issues(self, plan_json: dict[str, Any]) -> list[PlanIssue]:
        """Detect performance issues from execution plan.

        Args:
            plan_json: PostgreSQL EXPLAIN JSON output.

        Returns:
            List of detected plan issues.
        """
        issues = []
        root_plan = plan_json.get("Plan", {})
        root_node = self._parse_node(root_plan)
        self._detect_issues_recursive(root_node, issues)
        return issues

    def _detect_issues_recursive(
        self, node: PostgresPlanNode, issues: list[PlanIssue]
    ) -> None:
        """Recursively detect issues in plan nodes."""
        if not node or node.node_type == "EMPTY":
            return

        # Issue: Sequential scan on large table
        if node.is_seq_scan and (node.actual_rows > 1000 or node.plan_rows > 1000):
            issues.append(PlanIssue(
                rule_id="SQL-PLAN-001",
                name="Sequential Scan on Large Table",
                severity="high",
                penalty=15,
                description=(
                    f"Sequential scan on {node.relation_name or 'table'} "
                    f"processing {node.actual_rows or node.plan_rows} rows"
                ),
                suggestion=(
                    f"Consider adding an index on columns used in WHERE/JOIN "
                    f"conditions for {node.relation_name or 'this table'}"
                ),
                location=node.relation_name or "Unknown",
                details={
                    "rows": node.actual_rows or node.plan_rows,
                    "filter": node.filter_condition,
                },
            ))

        # Issue: High filter removal ratio
        if node.rows_removed_by_filter > 0 and node.actual_rows > 0:
            total = node.actual_rows + node.rows_removed_by_filter
            removal_ratio = node.rows_removed_by_filter / total
            if removal_ratio > 0.9:
                issues.append(PlanIssue(
                    rule_id="SQL-PLAN-002",
                    name="Late Filter Removal",
                    severity="medium",
                    penalty=10,
                    description=(
                        f"Filter removes {int(removal_ratio * 100)}% of rows late in plan"
                    ),
                    suggestion="Push filter conditions earlier or add supporting index",
                    location=node.relation_name or node.node_type,
                    details={
                        "rows_kept": node.actual_rows,
                        "rows_removed": node.rows_removed_by_filter,
                        "filter": node.filter_condition,
                    },
                ))

        # Issue: Nested loop with large outer table
        if node.node_type == "Nested Loop" and node.actual_loops > 100:
            issues.append(PlanIssue(
                rule_id="SQL-PLAN-003",
                name="Nested Loop with Many Iterations",
                severity="high",
                penalty=15,
                description=f"Nested loop executed {node.actual_loops} times",
                suggestion="Consider hash join or add index to reduce iterations",
                location="Nested Loop Join",
                details={"loops": node.actual_loops},
            ))

        # Issue: Sort operation spilling to disk
        if node.node_type == "Sort":
            sort_method = node.extra_info.get("Sort Method", "")
            if "disk" in sort_method.lower():
                issues.append(PlanIssue(
                    rule_id="SQL-PLAN-004",
                    name="Sort Spills to Disk",
                    severity="high",
                    penalty=15,
                    description="Sort operation exceeds work_mem, spilling to disk",
                    suggestion="Increase work_mem or add index for presorted data",
                    location="Sort",
                    details={
                        "sort_method": sort_method,
                        "sort_key": node.sort_key,
                    },
                ))

        # Issue: Large cardinality estimation error
        if node.actual_rows > 0 and node.plan_rows > 0:
            error = node.cardinality_error
            if error > 10:
                issues.append(PlanIssue(
                    rule_id="SQL-PLAN-005",
                    name="Cardinality Underestimate",
                    severity="medium",
                    penalty=10,
                    description=(
                        f"Planner estimated {node.plan_rows} rows but got "
                        f"{node.actual_rows} ({error:.0f}x underestimate)"
                    ),
                    suggestion="Run ANALYZE on table or update statistics_target",
                    location=node.relation_name or node.node_type,
                    details={
                        "planned": node.plan_rows,
                        "actual": node.actual_rows,
                        "error_ratio": error,
                    },
                ))
            elif error < 0.1:
                issues.append(PlanIssue(
                    rule_id="SQL-PLAN-006",
                    name="Cardinality Overestimate",
                    severity="medium",
                    penalty=10,
                    description=(
                        f"Planner estimated {node.plan_rows} rows but got "
                        f"{node.actual_rows} ({1/error:.0f}x overestimate)"
                    ),
                    suggestion="Run ANALYZE on table or check filter selectivity",
                    location=node.relation_name or node.node_type,
                    details={
                        "planned": node.plan_rows,
                        "actual": node.actual_rows,
                        "error_ratio": error,
                    },
                ))

        # Recursively check children
        for child in node.children:
            self._detect_issues_recursive(child, issues)


def analyze_postgres_plan(plan_json: dict[str, Any]) -> tuple[ExecutionPlanAnalysis, list[PlanIssue]]:
    """Convenience function to analyze a PostgreSQL plan.

    Args:
        plan_json: PostgreSQL EXPLAIN JSON output.

    Returns:
        Tuple of (ExecutionPlanAnalysis, list of PlanIssues).
    """
    parser = PostgresPlanParser()
    analysis = parser.parse(plan_json)
    issues = parser.detect_issues(plan_json)
    return analysis, issues
