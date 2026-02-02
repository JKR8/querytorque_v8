"""SQL Anti-Pattern Detector - AST-based analysis.

This module provides the SQLIssue and SQLAnalysisResult dataclasses used throughout
the codebase, and a SQLAntiPatternDetector class that wraps the AST detector.

NOTE: All detection is now performed by the AST detector (ast_detector/).
The regex-based detection has been removed as it was inferior to AST-based analysis.
"""

import logging
import time
from dataclasses import dataclass, field
from typing import Optional

from .ast_detector import ASTDetector

# Module logger
logger = logging.getLogger(__name__)


@dataclass
class SQLIssue:
    """A detected SQL anti-pattern."""
    rule_id: str
    name: str
    severity: str  # critical, high, medium, low
    category: str
    penalty: int
    description: str
    location: Optional[str] = None  # CTE name, line number, etc.
    match: Optional[str] = None  # The matched text
    explanation: str = ""
    suggestion: str = ""
    example_bad: Optional[str] = None
    example_good: Optional[str] = None


@dataclass
class SQLAnalysisResult:
    """Complete SQL analysis result with execution metrics.

    This dataclass supports two modes:
    1. Static analysis only: issues from AST detection
    2. Plan-based analysis: execution metrics from EXPLAIN ANALYZE

    The opportunity_score is the new primary metric, based on:
    - Execution efficiency (rows_returned / rows_scanned)
    - Detected optimization opportunities
    - Bottleneck severity
    """
    sql: str

    # Static analysis issues (from AST detector)
    issues: list[SQLIssue] = field(default_factory=list)
    query_structure: Optional[dict] = None

    # ============================================================
    # NEW: Execution reality (from plan_parser)
    # ============================================================
    execution_time_ms: Optional[float] = None
    rows_scanned: Optional[int] = None
    rows_returned: Optional[int] = None
    efficiency_ratio: Optional[float] = None  # rows_returned / rows_scanned

    # Cost distribution
    top_operators: list[dict] = field(default_factory=list)
    bottleneck: Optional[dict] = None  # Operator with highest cost %

    # ============================================================
    # NEW: Opportunities (from opportunity_detector)
    # ============================================================
    opportunities: list = field(default_factory=list)  # List of OpportunityResult

    # ============================================================
    # DEPRECATED: Old penalty-based scoring (kept for backward compat)
    # ============================================================
    base_score: int = 100
    total_penalty: int = 0
    final_score: int = 100

    # Counts by severity
    critical_count: int = 0
    high_count: int = 0
    medium_count: int = 0
    low_count: int = 0

    def calculate_opportunity_score(self) -> int:
        """Calculate score based on what can be improved, not what's wrong.

        This is the new primary scoring method when execution data is available.

        Returns:
            Score 0-100, where 100 means no obvious optimization opportunities
        """
        score = 100

        # Deduct based on bottleneck severity
        if self.bottleneck:
            cost_pct = self.bottleneck.get("cost_pct", 0)
            if cost_pct >= 80:
                score -= 30  # Single operator is 80%+ of cost
            elif cost_pct >= 50:
                score -= 15  # Single operator is 50%+ of cost

        # Deduct based on efficiency ratio
        if self.efficiency_ratio is not None and self.efficiency_ratio < 0.01:
            score -= 20  # Scanning 100x more rows than needed

        # Deduct for each concrete opportunity
        score -= len(self.opportunities) * 10

        return max(0, score)

    @property
    def has_execution_data(self) -> bool:
        """Check if execution plan data is available."""
        return self.execution_time_ms is not None

    def to_dict(self) -> dict:
        """Export as JSON-serializable dict."""
        result = {
            "score": self.final_score,
            "total_penalty": self.total_penalty,
            "severity_counts": {
                "critical": self.critical_count,
                "high": self.high_count,
                "medium": self.medium_count,
                "low": self.low_count
            },
            "issues": [
                {
                    "rule_id": issue.rule_id,
                    "name": issue.name,
                    "severity": issue.severity,
                    "category": issue.category,
                    "penalty": issue.penalty,
                    "location": issue.location,
                    "match": issue.match[:100] if issue.match and len(issue.match) > 100 else issue.match,
                    "description": issue.description,
                    "explanation": issue.explanation,
                    "suggestion": issue.suggestion
                }
                for issue in self.issues
            ],
            "query_structure": self.query_structure,
        }

        # Add execution metrics if available
        if self.has_execution_data:
            result["execution"] = {
                "time_ms": self.execution_time_ms,
                "rows_scanned": self.rows_scanned,
                "rows_returned": self.rows_returned,
                "efficiency_ratio": self.efficiency_ratio,
            }
            if self.bottleneck:
                result["bottleneck"] = self.bottleneck
            if self.top_operators:
                result["top_operators"] = self.top_operators
            result["opportunity_score"] = self.calculate_opportunity_score()

        # Add opportunities if any
        if self.opportunities:
            result["opportunities"] = [
                opp.to_dict() if hasattr(opp, 'to_dict') else opp
                for opp in self.opportunities
            ]

        return result


class SQLAntiPatternDetector:
    """Detects SQL anti-patterns using AST-based analysis.

    This class wraps the ASTDetector to provide backward-compatible API.
    All detection is performed by the AST detector - no regex patterns are used.
    """

    def __init__(self, dialect: str = "generic", include_style: bool = False):
        """Initialize detector.

        Args:
            dialect: SQL dialect (generic, snowflake, postgres, duckdb, tsql)
            include_style: Include noisy style rules (SELECT *, implicit join, etc.)
                           Default False for cleaner, more actionable output.
        """
        self.dialect = dialect
        self.include_style = include_style
        self._ast_detector = ASTDetector(dialect=dialect, include_style=include_style)

    def analyze(self, sql: str, include_structure: bool = True) -> SQLAnalysisResult:
        """Analyze SQL for anti-patterns using AST-based detection.

        Args:
            sql: SQL query to analyze
            include_structure: Whether to include query structure in result

        Returns:
            SQLAnalysisResult with detected issues and scores
        """
        start_time = time.time()

        # Use AST detector for all detection
        ast_issues = self._ast_detector.detect(sql)

        # Convert AST issues to SQLIssue format
        issues = []
        for ast_issue in ast_issues:
            issue = SQLIssue(
                rule_id=ast_issue.rule_id,
                name=ast_issue.name,
                severity=ast_issue.severity,
                category=ast_issue.category,
                penalty=ast_issue.penalty,
                description=ast_issue.description,
                location=ast_issue.location,
                match=ast_issue.match,
                explanation=ast_issue.explanation,
                suggestion=ast_issue.suggestion,
            )
            issues.append(issue)

        # Calculate scores
        total_penalty = sum(issue.penalty for issue in issues)
        final_score = max(0, 100 - total_penalty)

        # Count by severity
        severity_counts = {"critical": 0, "high": 0, "medium": 0, "low": 0, "info": 0}
        for issue in issues:
            if issue.severity in severity_counts:
                severity_counts[issue.severity] += 1

        # Parse query structure if requested
        query_structure = None
        if include_structure:
            query_structure = self._parse_query_structure(sql)

        elapsed = time.time() - start_time
        logger.debug(f"SQL analysis completed in {elapsed:.3f}s, found {len(issues)} issues")

        return SQLAnalysisResult(
            sql=sql,
            issues=issues,
            query_structure=query_structure,
            base_score=100,
            total_penalty=total_penalty,
            final_score=final_score,
            critical_count=severity_counts["critical"],
            high_count=severity_counts["high"],
            medium_count=severity_counts["medium"],
            low_count=severity_counts["low"],
        )

    def _parse_query_structure(self, sql: str) -> dict:
        """Parse query structure using AST.

        Returns dict with CTE count, table count, join count, etc.
        """
        import sqlglot
        from sqlglot import exp

        structure = {
            "cte_count": 0,
            "cte_names": [],
            "table_count": 0,
            "tables": [],
            "join_count": 0,
            "subquery_count": 0,
            "line_count": len(sql.strip().split("\n")),
        }

        try:
            parsed = sqlglot.parse_one(sql, error_level=sqlglot.ErrorLevel.IGNORE)

            # Count CTEs
            for cte in parsed.find_all(exp.CTE):
                if cte.alias:
                    structure["cte_names"].append(cte.alias)
            structure["cte_count"] = len(structure["cte_names"])

            # Count tables
            tables = set()
            for table in parsed.find_all(exp.Table):
                if table.name:
                    tables.add(table.name)
            structure["tables"] = sorted(tables)
            structure["table_count"] = len(tables)

            # Count joins
            structure["join_count"] = len(list(parsed.find_all(exp.Join)))

            # Count subqueries
            structure["subquery_count"] = len(list(parsed.find_all(exp.Subquery)))

        except Exception:
            pass

        return structure


def analyze_sql(sql: str, dialect: str = "generic") -> dict:
    """Convenience function to analyze SQL and return dict.

    Args:
        sql: SQL query to analyze
        dialect: SQL dialect (generic, snowflake, postgres, duckdb, tsql)

    Returns:
        Dictionary with analysis results
    """
    detector = SQLAntiPatternDetector(dialect=dialect)
    result = detector.analyze(sql)
    return result.to_dict()
