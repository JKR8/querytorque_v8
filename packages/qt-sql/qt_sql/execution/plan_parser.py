"""Parse DuckDB execution plan JSON into structured analysis.

Supports both regular EXPLAIN (FORMAT JSON) and EXPLAIN (ANALYZE, FORMAT JSON)
for comprehensive plan analysis including cardinality estimation checks.
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass, field
from typing import Any, Iterator, Optional

from .base import ExecutionPlanAnalysis, PlanNode


@dataclass
class PlanIssue:
    """An issue detected from execution plan analysis.

    Attributes:
        rule_id: Issue identifier (e.g., "SQL-CONN-001")
        name: Short name for the issue
        severity: Issue severity (critical, high, medium, low)
        penalty: Score penalty for this issue
        description: Detailed description of the issue
        suggestion: How to fix the issue
        location: Where in the plan the issue was found
        details: Additional context (e.g., operator name, row counts)
    """
    rule_id: str
    name: str
    severity: str
    penalty: int
    description: str
    suggestion: str
    location: str = ""
    details: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary for JSON serialization."""
        return {
            "rule_id": self.rule_id,
            "name": self.name,
            "severity": self.severity,
            "penalty": self.penalty,
            "description": self.description,
            "suggestion": self.suggestion,
            "location": self.location,
            "details": self.details,
        }

    def to_issue(self) -> "Issue":
        """Convert PlanIssue to the unified Issue model with source=plan.

        This bridges plan-detected performance issues into the main audit
        pipeline so they appear alongside AST-detected issues in reports,
        scoring, and LLM payloads.
        """
        from query_torque.models.audit_result import (
            Issue, Severity, Confidence, IssueSource,
        )

        severity_map = {
            "critical": Severity.CRITICAL,
            "high": Severity.HIGH,
            "medium": Severity.MEDIUM,
            "low": Severity.LOW,
            "info": Severity.INFO,
        }

        # Build evidence from details dict
        evidence = []
        if self.details:
            for key, value in self.details.items():
                evidence.append(f"{key}: {value}")

        # Generate instance ID from rule_id + location
        import hashlib
        hash_suffix = hashlib.md5(
            f"{self.rule_id}:{self.location}".encode()
        ).hexdigest()[:4]
        instance_id = f"{self.rule_id}-{hash_suffix}"

        return Issue(
            id=instance_id,
            rule=self.rule_id,
            severity=severity_map.get(self.severity.lower(), Severity.MEDIUM),
            title=self.name[:100],
            penalty=self.penalty,
            problem=self.description,
            remediation=self.suggestion,
            evidence=evidence,
            confidence=Confidence.HIGH,
            source=IssueSource.PLAN,
        )


# Operator types that indicate potential performance issues
# Keys are normalized (uppercase, stripped) for matching
OPERATOR_WARNINGS: dict[str, str] = {
    "SEQ_SCAN": "Sequential scan - consider adding an index",
    "TABLE_SCAN": "Full table scan - may benefit from filtering earlier",
    "NESTED_LOOP": "Nested loop join - can be slow for large tables",
    "NESTED_LOOP_JOIN": "Nested loop join - can be slow for large tables",
    "CROSS_PRODUCT": "Cross product - likely unintended Cartesian join",
    "BLOCKWISE_NL_JOIN": "Block nested loop - consider hash join instead",
    "FILTER": "Post-filter applied - predicate pushed down after scan",
}

# Operators that are typically expensive
EXPENSIVE_OPERATORS = {
    "HASH_JOIN",
    "MERGE_JOIN",
    "HASH_GROUP_BY",
    "PERFECT_HASH_GROUP_BY",
    "ORDER_BY",
    "WINDOW",
    "AGGREGATE",
}


class DuckDBPlanParser:
    """Parse DuckDB JSON profiling output into ExecutionPlanAnalysis.

    DuckDB's profiling JSON format includes:
    - query_name: The SQL query
    - latency: Total execution time in seconds
    - rows_returned: Number of result rows
    - children: Array of operator nodes

    Each operator node has:
    - operator_type: e.g., "PROJECTION", "HASH_JOIN", "SEQ_SCAN"
    - operator_timing: Time in seconds
    - operator_cardinality: Rows produced
    - extra_info: Operator-specific details
    - children: Child operators
    """

    def parse(self, plan_json: dict[str, Any]) -> ExecutionPlanAnalysis:
        """Parse DuckDB profiling JSON into structured analysis.

        Args:
            plan_json: DuckDB profiling JSON output.

        Returns:
            ExecutionPlanAnalysis ready for template rendering.
        """
        # Handle text-only plan fallback
        if plan_json.get("type") == "text_plan":
            return self._parse_text_plan(plan_json)

        # Extract top-level metrics
        latency = plan_json.get("latency", 0)
        rows_returned = plan_json.get("rows_returned", 0)

        # Parse the operator tree
        children = plan_json.get("children", [])
        nodes = self._parse_nodes(children)

        # Calculate total timing for percentage calculation
        total_timing = self._calculate_total_timing(nodes)

        # Flatten tree for template rendering
        plan_tree = self._flatten_tree(nodes, total_timing)

        # Identify bottleneck (most expensive operator)
        bottleneck = self._identify_bottleneck(plan_tree)

        # Extract warnings
        warnings = self._extract_warnings(nodes)

        return ExecutionPlanAnalysis(
            total_cost=total_timing,
            execution_time_ms=latency * 1000,
            estimated_rows=rows_returned,
            actual_rows=rows_returned,
            plan_tree=plan_tree,
            bottleneck=bottleneck,
            warnings=warnings,
            raw_plan=plan_json,
        )

    def _parse_text_plan(self, plan_json: dict[str, Any]) -> ExecutionPlanAnalysis:
        """Parse text-only EXPLAIN output as fallback."""
        plan_text = plan_json.get("plan_text", "")

        return ExecutionPlanAnalysis(
            total_cost=0,
            execution_time_ms=0,
            plan_tree=[
                {
                    "indent": 0,
                    "operator": "PLAN",
                    "details": plan_text[:200] + "..." if len(plan_text) > 200 else plan_text,
                    "cost_pct": 100,
                    "rows": 0,
                    "is_bottleneck": False,
                }
            ],
            warnings=["Text-only plan - detailed analysis not available"],
            raw_plan=plan_json,
        )

    def _parse_nodes(self, children: list[dict[str, Any]]) -> list[PlanNode]:
        """Recursively parse operator nodes."""
        nodes = []

        for child in children:
            # DuckDB EXPLAIN JSON uses 'name', profiling uses 'operator_type'
            operator_type = child.get("name", child.get("operator_type", "UNKNOWN"))
            # Strip trailing spaces from operator names
            operator_type = operator_type.strip()

            # Get timing (from profiling) or default to 0
            timing = child.get("operator_timing", child.get("timing", 0))

            # Get cardinality from extra_info or direct field
            extra_info = child.get("extra_info", {})
            if isinstance(extra_info, str):
                extra_info = {"details": extra_info}

            # Get actual cardinality (rows actually processed)
            actual_cardinality = child.get("operator_cardinality", child.get("cardinality", 0))

            # Get estimated cardinality from extra_info
            estimated_cardinality = None
            if isinstance(extra_info, dict):
                cardinality_str = extra_info.get("Estimated Cardinality", "")
                if cardinality_str:
                    try:
                        # Remove ~ prefix if present (e.g., "~100")
                        estimated_cardinality = int(str(cardinality_str).lstrip("~"))
                    except (ValueError, TypeError):
                        estimated_cardinality = None

            # Recursively parse children
            child_nodes = self._parse_nodes(child.get("children", []))

            node = PlanNode(
                operator_type=operator_type,
                timing_seconds=timing,
                cardinality=actual_cardinality,
                estimated_cardinality=estimated_cardinality,
                cost_percentage=0,  # Calculated later
                extra_info=extra_info,
                children=child_nodes,
            )
            nodes.append(node)

        return nodes

    def _calculate_total_timing(self, nodes: list[PlanNode]) -> float:
        """Calculate total timing across all nodes."""
        total = 0.0
        for node in nodes:
            total += node.timing_seconds
            total += self._calculate_total_timing(node.children)
        return total

    def _flatten_tree(
        self,
        nodes: list[PlanNode],
        total_timing: float,
        indent: int = 0,
    ) -> list[dict[str, Any]]:
        """Flatten tree for template rendering with indent levels.

        Returns list of dicts with keys matching sql_report.html.j2 expectations:
        - indent: Nesting level (0-based)
        - operator: Operator name
        - details: Extra info string
        - cost_pct: Percentage of total cost
        - rows: Cardinality
        - is_bottleneck: Boolean flag
        """
        result = []

        for node in nodes:
            cost_pct = (
                (node.timing_seconds / total_timing * 100) if total_timing > 0 else 0
            )

            # Format extra_info as string
            details = self._format_extra_info(node.extra_info)

            # Build meta string for template (rows + timing)
            meta_parts = []
            if node.cardinality > 0:
                meta_parts.append(f"{node.cardinality:,} rows")
            if node.timing_seconds > 0:
                meta_parts.append(f"{node.timing_seconds * 1000:.2f}ms")
            meta = " | ".join(meta_parts) if meta_parts else details

            # Detect spill indicators from extra_info
            spill = False
            pruning_ratio = None
            if isinstance(node.extra_info, dict):
                # Check for spill indicators (DuckDB specific)
                if node.extra_info.get("Spill", False) or "spill" in details.lower():
                    spill = True
                # Check for partition pruning info
                partitions_scanned = node.extra_info.get("Partitions Scanned", 0)
                partitions_total = node.extra_info.get("Partitions Total", 0)
                if partitions_total > 0 and partitions_scanned < partitions_total:
                    pruning_ratio = round((1 - partitions_scanned / partitions_total) * 100)

            result.append({
                "indent": indent,
                "operator": node.operator_type,
                "details": details,
                "meta": meta,
                "cost_pct": round(cost_pct, 1),
                "rows": node.cardinality,
                "estimated_rows": node.estimated_cardinality,
                "timing_ms": round(node.timing_seconds * 1000, 2),
                "is_bottleneck": node.is_bottleneck,
                "problem": node.is_bottleneck,  # Template uses 'problem' for styling
                "spill": spill,
                "pruning_ratio": pruning_ratio,
            })

            # Recursively add children
            result.extend(
                self._flatten_tree(node.children, total_timing, indent + 1)
            )

        return result

    def _format_extra_info(self, extra_info: dict[str, Any]) -> str:
        """Format extra_info dict as readable string."""
        if not extra_info:
            return ""

        parts = []
        for key, value in extra_info.items():
            if isinstance(value, (list, tuple)):
                value = ", ".join(str(v) for v in value)
            parts.append(f"{key}: {value}")

        return "; ".join(parts)

    def _identify_bottleneck(
        self, plan_tree: list[dict[str, Any]]
    ) -> Optional[dict[str, Any]]:
        """Find the most expensive operator (bottleneck).

        Returns info about the operator with highest cost percentage.
        """
        if not plan_tree:
            return None

        # Find node with highest cost percentage
        max_node = max(plan_tree, key=lambda n: n.get("cost_pct", 0))

        if max_node.get("cost_pct", 0) < 10:
            # No significant bottleneck
            return None

        # Mark it as bottleneck in the tree
        for node in plan_tree:
            if node is max_node:
                node["is_bottleneck"] = True

        operator = max_node.get("operator", "")
        suggestion = OPERATOR_WARNINGS.get(
            operator,
            "Consider optimizing this operator",
        )
        return {
            "operator": operator,
            "cost_pct": max_node.get("cost_pct"),
            "rows": max_node.get("rows"),
            "details": max_node.get("details"),
            "suggestion": suggestion,
            # Template-compatible fields
            "title": f"{operator} ({max_node.get('cost_pct', 0)}% cost)",
            "detail": suggestion,
        }

    def _extract_warnings(self, nodes: list[PlanNode]) -> list[str]:
        """Extract warnings for problematic operators."""
        warnings = []
        self._collect_warnings(nodes, warnings)
        return list(set(warnings))  # Deduplicate

    def _collect_warnings(
        self, nodes: list[PlanNode], warnings: list[str]
    ) -> None:
        """Recursively collect warnings from nodes."""
        for node in nodes:
            if node.operator_type in OPERATOR_WARNINGS:
                warning = f"{node.operator_type}: {OPERATOR_WARNINGS[node.operator_type]}"
                warnings.append(warning)

            # Check for high-cardinality operations
            if node.cardinality > 100000 and node.operator_type in EXPENSIVE_OPERATORS:
                warnings.append(
                    f"{node.operator_type} processing {node.cardinality:,} rows - "
                    "consider filtering earlier"
                )

            self._collect_warnings(node.children, warnings)


class PlanAnalyzer:
    """Analyze execution plans for performance issues.

    Detects SQL-CONN-* rules from execution plan analysis:
    - SQL-CONN-001: Cardinality estimation mismatch
    - SQL-CONN-004: Sequential scan on large table
    - SQL-CONN-005: Hash match spill
    - SQL-CONN-006: Sort spill

    Also provides optimization target scoring (from LLM SQL Optimizer blueprint).
    """

    # Default thresholds for issue detection
    CARDINALITY_RATIO_THRESHOLD = 10.0  # 10x difference is significant
    LARGE_TABLE_ROW_THRESHOLD = 10000  # 10K rows for "large" table
    EXPENSIVE_SCAN_ROW_THRESHOLD = 100000  # 100K rows is very expensive

    # Blueprint optimization difficulty map (for scoring)
    OPTIMIZATION_DIFFICULTY = {
        "SEQ_SCAN": 1.0,        # Easy - add index
        "TABLE_SCAN": 1.0,      # Easy - add index
        "NESTED_LOOP": 2.0,     # Medium - rewrite join
        "NESTED_LOOP_JOIN": 2.0,
        "HASH_JOIN": 3.0,       # Hard - complex rewrite
        "HASH_GROUP_BY": 2.5,   # Hard - query restructure
        "AGGREGATE": 2.5,       # Hard - query restructure
        "SORT": 1.5,            # Medium - index or limit
        "ORDER_BY": 1.5,        # Medium - index or limit
        "WINDOW": 2.5,          # Hard - query restructure
        "FILTER": 1.0,          # Easy - predicate pushdown
    }

    def __init__(
        self,
        cardinality_threshold: float = CARDINALITY_RATIO_THRESHOLD,
        large_table_threshold: int = LARGE_TABLE_ROW_THRESHOLD,
    ):
        """Initialize the analyzer with custom thresholds.

        Args:
            cardinality_threshold: Ratio threshold for cardinality mismatch
            large_table_threshold: Row threshold for large table scans
        """
        self.cardinality_threshold = cardinality_threshold
        self.large_table_threshold = large_table_threshold

    def analyze(self, plan_json: dict[str, Any]) -> list[PlanIssue]:
        """Analyze an execution plan and return detected issues.

        Args:
            plan_json: DuckDB EXPLAIN (ANALYZE, FORMAT JSON) output

        Returns:
            List of PlanIssue objects for detected problems
        """
        issues: list[PlanIssue] = []

        # Parse plan into nodes
        children = plan_json.get("children", [])
        if not children:
            return issues

        # Traverse and analyze all nodes
        self._analyze_nodes(children, issues)

        return issues

    def _analyze_nodes(
        self,
        nodes: list[dict[str, Any]],
        issues: list[PlanIssue],
    ) -> None:
        """Recursively analyze plan nodes for issues."""
        for node in nodes:
            self._check_cardinality_mismatch(node, issues)
            self._check_sequential_scan(node, issues)
            self._check_hash_spill(node, issues)
            self._check_sort_spill(node, issues)

            # Recurse into children
            children = node.get("children", [])
            self._analyze_nodes(children, issues)

    def _check_cardinality_mismatch(
        self,
        node: dict[str, Any],
        issues: list[PlanIssue],
    ) -> None:
        """Check SQL-CONN-001: Cardinality estimation mismatch.

        Compares estimated cardinality (from extra_info) with actual
        cardinality (from operator_cardinality) to detect estimation errors.
        """
        extra_info = node.get("extra_info", {})
        if not isinstance(extra_info, dict):
            return

        estimated_str = extra_info.get("Estimated Cardinality", "0")
        try:
            estimated = int(str(estimated_str).lstrip("~"))
        except (ValueError, TypeError):
            return

        actual = node.get("operator_cardinality", 0)

        # Skip if both are 0 or very small
        if actual < 10 and estimated < 10:
            return

        # Calculate ratio (max/min to get a symmetric measure)
        if actual == 0:
            if estimated > 100:
                ratio = float('inf')
            else:
                return  # Small estimate, 0 actual is okay
        else:
            ratio = estimated / actual

        # Two-part threshold: ratio must be significant AND the operator
        # must process enough rows to matter. A 15x mismatch on 30 rows
        # is irrelevant; a 10x mismatch on 500K rows is critical.
        symmetric_ratio = max(estimated, actual) / max(min(estimated, actual), 1)
        is_significant = (
            symmetric_ratio >= self.cardinality_threshold
            and max(actual, estimated) >= 10_000
        )
        if not is_significant:
            return

        # Check for significant mismatch (overestimate or underestimate)
        operator_name = node.get("operator_name", node.get("name", "UNKNOWN")).strip()

        if ratio >= self.cardinality_threshold:
            issues.append(PlanIssue(
                rule_id="SQL-CONN-001",
                name="Cardinality Estimation Mismatch",
                severity="high",
                penalty=15,
                description=(
                    f"Optimizer estimated {estimated:,} rows but got {actual:,} "
                    f"({ratio:.1f}x overestimate). This can cause wrong join "
                    "strategies and memory allocation."
                ),
                suggestion=(
                    "Update statistics, check for skewed data, or consider "
                    "query hints like OPTIMIZE FOR."
                ),
                location=operator_name,
                details={
                    "operator": operator_name,
                    "estimated_rows": estimated,
                    "actual_rows": actual,
                    "ratio": round(ratio, 1),
                    "direction": "overestimate",
                },
            ))
        elif ratio > 0 and ratio <= 1.0 / self.cardinality_threshold:
            # Underestimate case (already passed is_significant check above)
            inverse_ratio = 1.0 / ratio
            issues.append(PlanIssue(
                rule_id="SQL-CONN-001",
                name="Cardinality Estimation Mismatch",
                severity="high",
                penalty=15,
                description=(
                    f"Optimizer estimated {estimated:,} rows but got {actual:,} "
                    f"({inverse_ratio:.1f}x underestimate). This can cause "
                    "memory spills and suboptimal join order."
                ),
                suggestion=(
                    "Update statistics on the affected tables. Consider "
                    "using memory hints if spills occur."
                ),
                location=operator_name,
                details={
                    "operator": operator_name,
                    "estimated_rows": estimated,
                    "actual_rows": actual,
                    "ratio": round(inverse_ratio, 1),
                    "direction": "underestimate",
                },
            ))

    def _check_sequential_scan(
        self,
        node: dict[str, Any],
        issues: list[PlanIssue],
    ) -> None:
        """Check SQL-CONN-004: Sequential scan on large table.

        Flags full table scans on tables with many rows.
        """
        operator_name = node.get("operator_name", node.get("name", "")).strip()
        operator_type = node.get("operator_type", operator_name)

        # Check if this is a table scan
        is_scan = (
            operator_type == "TABLE_SCAN" or
            "SEQ_SCAN" in operator_name.upper() or
            "SCAN" in operator_type.upper()
        )
        if not is_scan:
            return

        # Check scan type
        extra_info = node.get("extra_info", {})
        if isinstance(extra_info, dict):
            scan_type = extra_info.get("Type", "")
            table_name = extra_info.get("Table", "unknown")
        else:
            scan_type = ""
            table_name = "unknown"

        # Only flag sequential (full) scans
        if "Sequential" not in scan_type and "Full" not in scan_type:
            # Check if there's no filter (full scan)
            if extra_info.get("Filters"):
                return  # Has filter, might be okay

        # Check row count
        rows_scanned = node.get("operator_rows_scanned", 0)
        if rows_scanned == 0:
            rows_scanned = node.get("operator_cardinality", 0)

        if rows_scanned < self.large_table_threshold:
            return  # Small table, okay

        # Determine severity based on size
        if rows_scanned >= self.EXPENSIVE_SCAN_ROW_THRESHOLD:
            severity = "high"
            penalty = 15
        else:
            severity = "medium"
            penalty = 10

        issues.append(PlanIssue(
            rule_id="SQL-CONN-004",
            name="Sequential Scan on Large Table",
            severity=severity,
            penalty=penalty,
            description=(
                f"Full sequential scan on '{table_name}' processing "
                f"{rows_scanned:,} rows. This may indicate a missing index "
                "or non-sargable predicate."
            ),
            suggestion=(
                "Add an appropriate index for the query predicates, or "
                "review the WHERE clause for non-sargable conditions."
            ),
            location=f"TABLE_SCAN on {table_name}",
            details={
                "table": table_name,
                "rows_scanned": rows_scanned,
                "scan_type": scan_type or "Sequential Scan",
            },
        ))

    def _check_hash_spill(
        self,
        node: dict[str, Any],
        issues: list[PlanIssue],
    ) -> None:
        """Check SQL-CONN-005: Hash match spill.

        Detects when hash operations spill to disk due to memory pressure.
        Note: DuckDB may not expose spill info directly; we check for
        large hash operations that might spill.
        """
        operator_name = node.get("operator_name", node.get("name", "")).strip()

        if "HASH" not in operator_name.upper():
            return

        # Check for spill indicators (DuckDB specific)
        extra_info = node.get("extra_info", {})

        # DuckDB doesn't always expose spill info, but we can check
        # for memory/bytes indicators
        result_set_size = node.get("result_set_size", 0)
        rows = node.get("operator_cardinality", 0)

        # Heuristic: very large hash operations might spill
        if rows > 1_000_000 or result_set_size > 100_000_000:  # 100MB
            issues.append(PlanIssue(
                rule_id="SQL-CONN-005",
                name="Large Hash Operation",
                severity="medium",
                penalty=10,
                description=(
                    f"Large hash operation ({rows:,} rows, "
                    f"{result_set_size / 1_000_000:.1f}MB) may spill to disk "
                    "if memory is constrained."
                ),
                suggestion=(
                    "Consider breaking the query into smaller batches, "
                    "or ensure adequate memory is available."
                ),
                location=operator_name,
                details={
                    "operator": operator_name,
                    "rows": rows,
                    "result_size_bytes": result_set_size,
                },
            ))

    def _check_sort_spill(
        self,
        node: dict[str, Any],
        issues: list[PlanIssue],
    ) -> None:
        """Check SQL-CONN-006: Sort spill.

        Detects when sort operations might spill to disk.
        """
        operator_name = node.get("operator_name", node.get("name", "")).strip()
        operator_type = node.get("operator_type", operator_name)

        is_sort = (
            "SORT" in operator_type.upper() or
            "ORDER" in operator_name.upper()
        )
        if not is_sort:
            return

        rows = node.get("operator_cardinality", 0)
        result_set_size = node.get("result_set_size", 0)

        # Heuristic: very large sort operations might spill
        if rows > 1_000_000 or result_set_size > 100_000_000:  # 100MB
            issues.append(PlanIssue(
                rule_id="SQL-CONN-006",
                name="Large Sort Operation",
                severity="medium",
                penalty=10,
                description=(
                    f"Large sort operation ({rows:,} rows) may spill to disk "
                    "if memory is constrained."
                ),
                suggestion=(
                    "Add an index that provides the required ordering, or "
                    "reduce the result set size before sorting."
                ),
                location=operator_name,
                details={
                    "operator": operator_name,
                    "rows": rows,
                    "result_size_bytes": result_set_size,
                },
            ))

    def score_optimization_target(
        self,
        node: dict[str, Any],
        query_frequency: int = 1,
    ) -> float:
        """Score an optimization target based on impact and difficulty.

        From LLM SQL Optimizer blueprint (State 2: Plan Analysis):
        Score = (actual_time × query_frequency) / optimization_difficulty

        Higher score = higher priority for optimization.

        Args:
            node: A plan node dict
            query_frequency: How often this query runs (default 1)

        Returns:
            Priority score (higher = more urgent)
        """
        # Extract timing (in seconds)
        actual_time = node.get("operator_timing", node.get("timing", 0))
        if actual_time == 0:
            # Try milliseconds from timing_ms
            actual_time = node.get("timing_ms", 0) / 1000

        # Get operator type
        operator_type = node.get("operator_type", node.get("name", "UNKNOWN")).strip().upper()

        # Look up difficulty
        difficulty = self.OPTIMIZATION_DIFFICULTY.get(operator_type, 2.0)

        # Calculate score
        if difficulty == 0:
            difficulty = 1.0  # Prevent division by zero

        score = (actual_time * query_frequency) / difficulty
        return score

    def score_plan_nodes(
        self,
        plan_json: dict[str, Any],
        query_frequency: int = 1,
    ) -> list[dict[str, Any]]:
        """Score all nodes in a plan for optimization priority.

        Args:
            plan_json: DuckDB EXPLAIN (ANALYZE, FORMAT JSON) output
            query_frequency: How often this query runs

        Returns:
            List of dicts with node info and priority score, sorted by score desc
        """
        scored_nodes = []
        children = plan_json.get("children", [])
        self._score_nodes_recursive(children, query_frequency, scored_nodes)

        # Sort by score descending (highest priority first)
        scored_nodes.sort(key=lambda x: x["score"], reverse=True)
        return scored_nodes

    def _score_nodes_recursive(
        self,
        nodes: list[dict[str, Any]],
        query_frequency: int,
        results: list[dict[str, Any]],
    ) -> None:
        """Recursively score all nodes."""
        for node in nodes:
            operator_type = node.get("operator_type", node.get("name", "UNKNOWN")).strip()
            score = self.score_optimization_target(node, query_frequency)

            results.append({
                "operator": operator_type,
                "score": round(score, 4),
                "timing_ms": node.get("operator_timing", 0) * 1000,
                "rows": node.get("operator_cardinality", 0),
                "difficulty": self.OPTIMIZATION_DIFFICULTY.get(operator_type.upper(), 2.0),
            })

            # Recurse
            children = node.get("children", [])
            self._score_nodes_recursive(children, query_frequency, results)

    def analyze_with_scores(
        self,
        plan_json: dict[str, Any],
        query_frequency: int = 1,
    ) -> tuple[list[PlanIssue], list[dict[str, Any]]]:
        """Analyze plan and return both issues and scored targets.

        Combines anti-pattern detection with optimization priority scoring.

        Args:
            plan_json: DuckDB EXPLAIN (ANALYZE, FORMAT JSON) output
            query_frequency: How often this query runs

        Returns:
            Tuple of (issues, scored_nodes)
        """
        issues = self.analyze(plan_json)
        scored_nodes = self.score_plan_nodes(plan_json, query_frequency)
        return issues, scored_nodes


def analyze_plan(plan_json: dict[str, Any]) -> list[PlanIssue]:
    """Convenience function to analyze a plan for issues.

    Args:
        plan_json: DuckDB EXPLAIN (ANALYZE, FORMAT JSON) output

    Returns:
        List of detected PlanIssue objects
    """
    analyzer = PlanAnalyzer()
    return analyzer.analyze(plan_json)


def plan_issues_to_issues(plan_issues: list[PlanIssue]) -> list:
    """Convert a list of PlanIssue objects to unified Issue model objects.

    Args:
        plan_issues: List of PlanIssue from PlanAnalyzer

    Returns:
        List of Issue objects with source=plan
    """
    return [pi.to_issue() for pi in plan_issues]


def build_plan_summary(plan_json: dict[str, Any]) -> dict[str, Any]:
    """Extract a compact plan summary for the LLM payload.

    Returns a small dict (~200-400 tokens when rendered) with the most
    important plan information. Raw plan JSON should never be sent to
    the LLM directly — use this summary instead.

    The summary includes:
    - Top N operators by time (covering >= 80% of total cost)
    - Any spills (hash or sort)
    - Seq scans on tables > 10K rows
    - Cardinality misestimates (10x+ on operators with >= 10K rows)
    - MD5 hash of the full plan for traceability

    Args:
        plan_json: DuckDB EXPLAIN (ANALYZE, FORMAT JSON) output

    Returns:
        Compact summary dict suitable for LLM payload rendering
    """
    plan_hash = hashlib.md5(
        json.dumps(plan_json, sort_keys=True).encode()
    ).hexdigest()[:6]

    # Walk the plan tree to collect operator stats
    operators: list[dict[str, Any]] = []
    spills: list[dict[str, Any]] = []
    scans: list[dict[str, Any]] = []
    misestimates: list[dict[str, Any]] = []

    def _walk(node: dict[str, Any]) -> None:
        name = node.get("operator_name", node.get("name", "")).strip()
        timing = node.get("operator_timing", 0.0)
        cardinality = node.get("operator_cardinality", 0)
        rows_scanned = node.get("operator_rows_scanned", 0)
        extra_info = node.get("extra_info", {})
        if isinstance(extra_info, str):
            extra_info = {}

        table = extra_info.get("Table") if isinstance(extra_info, dict) else None

        if name and name != "EXPLAIN_ANALYZE":
            operators.append({
                "op": name,
                "table": table or "-",
                "time_ms": round(timing * 1000, 1),
                "rows_out": cardinality,
                "rows_scanned": rows_scanned,
            })

            # Check for spill indicators
            result_set_size = node.get("result_set_size", 0)
            is_hash = "HASH" in name.upper()
            is_sort = "SORT" in name.upper() or "ORDER" in name.upper()
            if (is_hash or is_sort) and (cardinality > 1_000_000 or result_set_size > 100_000_000):
                spill_type = "hash" if is_hash else "sort"
                spills.append({"op": name, "type": spill_type, "rows": cardinality})

            # Check for table scans - include ALL scans (small filtered tables are important signals)
            is_scan = "SCAN" in name.upper() and "CTE" not in name.upper() and "COLUMN_DATA" not in name.upper()
            scan_rows = rows_scanned if rows_scanned > 0 else cardinality
            if is_scan and table:
                filters = extra_info.get("Filters", "") if isinstance(extra_info, dict) else ""
                has_filter = bool(filters)
                scans.append({
                    "table": table,
                    "rows": scan_rows,
                    "has_filter": has_filter,
                    "filter_expr": filters if has_filter else None,
                })

            # Check for cardinality misestimates
            if isinstance(extra_info, dict):
                est_str = extra_info.get("Estimated Cardinality", "")
                if est_str:
                    try:
                        estimated = int(str(est_str).lstrip("~"))
                        actual = cardinality
                        if max(estimated, actual) >= 10_000:
                            ratio = max(estimated, actual) / max(min(estimated, actual), 1)
                            if ratio >= 10.0:
                                misestimates.append({
                                    "op": name,
                                    "estimated": estimated,
                                    "actual": actual,
                                    "ratio": round(ratio, 1),
                                })
                    except (ValueError, TypeError):
                        pass

        for child in node.get("children", []):
            _walk(child)

    for child in plan_json.get("children", []):
        _walk(child)

    # Sort operators by time descending
    operators.sort(key=lambda o: o["time_ms"], reverse=True)

    # Select top N operators covering >= 80% of total cost
    total_time = sum(o["time_ms"] for o in operators)
    top_operators = []
    cumulative = 0.0
    for op in operators:
        cost_pct = round(op["time_ms"] / total_time * 100, 0) if total_time > 0 else 0
        top_operators.append({**op, "cost_pct": cost_pct})
        cumulative += op["time_ms"]
        if total_time > 0 and cumulative / total_time >= 0.80 and len(top_operators) >= 5:
            break
    # Ensure at least top 5
    if len(top_operators) < 5:
        for op in operators[len(top_operators):5]:
            cost_pct = round(op["time_ms"] / total_time * 100, 0) if total_time > 0 else 0
            top_operators.append({**op, "cost_pct": cost_pct})

    # Calculate total rows scanned and efficiency ratio
    total_rows_scanned = sum(s["rows"] for s in scans)
    rows_returned = plan_json.get("rows_returned", 0)

    # Efficiency ratio: what fraction of scanned rows made it to results
    # Lower is worse (scanning way more than returning)
    efficiency_ratio = None
    if total_rows_scanned > 0:
        efficiency_ratio = rows_returned / total_rows_scanned

    # Identify bottleneck (operator with highest cost %)
    bottleneck = None
    if top_operators:
        top_op = top_operators[0]
        if top_op.get("cost_pct", 0) >= 10:  # Only flag if >= 10% of cost
            bottleneck = {
                "op": top_op["op"],
                "cost_pct": top_op["cost_pct"],
                "rows": top_op.get("rows_out", 0),
                "table": top_op.get("table"),
                "details": f"{top_op['op']} processing {top_op.get('rows_out', 0):,} rows",
            }

    return {
        "total_time_ms": round(total_time, 1),
        "rows_returned": rows_returned,
        "rows_scanned": total_rows_scanned,
        "efficiency_ratio": efficiency_ratio,
        "top_operators": top_operators,
        "bottleneck": bottleneck,
        "spills": spills,
        "scans": scans,
        "misestimates": misestimates,
        "plan_hash": plan_hash,
    }


def get_execution_summary(plan_json: dict[str, Any]) -> dict[str, Any]:
    """Get a compact execution summary for display.

    This is a higher-level helper that combines plan parsing with
    human-readable formatting for CLI output.

    Args:
        plan_json: DuckDB EXPLAIN (ANALYZE, FORMAT JSON) output

    Returns:
        Dict with execution_time_ms, rows_scanned, rows_returned,
        efficiency_ratio, bottleneck, and efficiency_description
    """
    summary = build_plan_summary(plan_json)

    result = {
        "execution_time_ms": summary.get("total_time_ms", 0),
        "rows_scanned": summary.get("rows_scanned", 0),
        "rows_returned": summary.get("rows_returned", 0),
        "efficiency_ratio": summary.get("efficiency_ratio"),
        "bottleneck": summary.get("bottleneck"),
        "top_operators": summary.get("top_operators", [])[:3],  # Top 3 for display
    }

    # Add human-readable efficiency description
    efficiency = result.get("efficiency_ratio")
    if efficiency is not None:
        if efficiency >= 0.5:
            result["efficiency_description"] = "Good (returning most scanned rows)"
        elif efficiency >= 0.1:
            result["efficiency_description"] = f"Moderate ({1/efficiency:.0f}x scan ratio)"
        elif efficiency >= 0.01:
            result["efficiency_description"] = f"Poor ({1/efficiency:.0f}x more rows scanned than needed)"
        elif efficiency > 0:
            result["efficiency_description"] = f"Very poor ({1/efficiency:,.0f}x scan-to-return ratio)"
        else:
            result["efficiency_description"] = "No rows returned"

    return result
