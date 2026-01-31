#!/usr/bin/env python3
"""
VPAX Differ - DAX-006
=====================
Compare two VPAX analyses to track model changes over time.
Useful for tracking model evolution, regression detection, and audit trails.

Author: QueryTorque / Dialect Labs
Version: 1.0.0
"""

import logging
from dataclasses import dataclass, field
from typing import Dict, List, Set, Optional, Any
from datetime import datetime
from enum import Enum

logger = logging.getLogger(__name__)


# =============================================================================
# ENUMS & CONSTANTS
# =============================================================================

class ChangeType(Enum):
    """Type of change detected between versions."""
    ADDED = "added"
    REMOVED = "removed"
    MODIFIED = "modified"
    UNCHANGED = "unchanged"


class ChangeCategory(Enum):
    """Category of the changed object."""
    MEASURE = "measure"
    TABLE = "table"
    COLUMN = "column"
    RELATIONSHIP = "relationship"
    ISSUE = "issue"
    METRIC = "metric"


class ChangeSeverity(Enum):
    """Severity of the change."""
    CRITICAL = "critical"   # Breaking change, major regression
    HIGH = "high"           # Significant change requiring attention
    MEDIUM = "medium"       # Notable change
    LOW = "low"             # Minor change
    INFO = "info"           # Informational change


# =============================================================================
# DATA CLASSES
# =============================================================================

@dataclass
class ObjectChange:
    """A single change to a model object."""
    change_type: ChangeType
    category: ChangeCategory
    severity: ChangeSeverity
    object_name: str
    table_name: Optional[str] = None

    # For modifications
    field_changed: Optional[str] = None
    old_value: Any = None
    new_value: Any = None

    # Context
    description: str = ""
    impact: str = ""


@dataclass
class MeasureChange:
    """Detailed change information for a measure."""
    name: str
    table: str
    change_type: ChangeType

    # Expression changes
    old_expression: Optional[str] = None
    new_expression: Optional[str] = None
    expression_changed: bool = False

    # Issue changes
    issues_added: List[str] = field(default_factory=list)
    issues_removed: List[str] = field(default_factory=list)
    issues_changed: bool = False

    # Metrics
    old_severity_score: int = 0
    new_severity_score: int = 0
    score_delta: int = 0


@dataclass
class TableChange:
    """Change information for a table."""
    name: str
    change_type: ChangeType

    # Size changes
    old_row_count: int = 0
    new_row_count: int = 0
    old_size_bytes: int = 0
    new_size_bytes: int = 0

    # Column changes
    columns_added: List[str] = field(default_factory=list)
    columns_removed: List[str] = field(default_factory=list)


@dataclass
class ScoreDelta:
    """Change in model scores."""
    old_torque_score: int
    new_torque_score: int
    delta: int
    direction: str  # improved, degraded, stable

    old_penalty: int = 0
    new_penalty: int = 0


@dataclass
class DiffSummary:
    """Summary of all changes between versions."""
    # Version info
    old_file: str
    new_file: str
    old_timestamp: str
    new_timestamp: str

    # Overall score change
    score_delta: ScoreDelta

    # Change counts by category
    measures_added: int = 0
    measures_removed: int = 0
    measures_modified: int = 0

    tables_added: int = 0
    tables_removed: int = 0
    tables_modified: int = 0

    columns_added: int = 0
    columns_removed: int = 0

    relationships_added: int = 0
    relationships_removed: int = 0

    # Issue changes
    issues_added: int = 0
    issues_removed: int = 0
    issues_changed: int = 0

    # Breaking changes
    breaking_changes: List[str] = field(default_factory=list)

    # Recommendations
    recommendations: List[str] = field(default_factory=list)


@dataclass
class VPAXDiffResult:
    """Complete diff result between two VPAX analyses."""
    summary: DiffSummary

    # Detailed changes
    measure_changes: List[MeasureChange] = field(default_factory=list)
    table_changes: List[TableChange] = field(default_factory=list)
    all_changes: List[ObjectChange] = field(default_factory=list)

    # Groupings for easier consumption
    critical_changes: List[ObjectChange] = field(default_factory=list)
    high_changes: List[ObjectChange] = field(default_factory=list)
    medium_changes: List[ObjectChange] = field(default_factory=list)
    low_changes: List[ObjectChange] = field(default_factory=list)


# =============================================================================
# VPAX DIFFER
# =============================================================================

class VPAXDiffer:
    """
    Compare two VPAX analysis results to detect changes.

    Usage:
        differ = VPAXDiffer()
        diff = differ.compare(old_report, new_report)
    """

    def compare(
        self,
        old_report: Dict,  # DiagnosticReport as dict
        new_report: Dict,  # DiagnosticReport as dict
        old_file: str = "baseline",
        new_file: str = "current"
    ) -> VPAXDiffResult:
        """
        Compare two VPAX diagnostic reports.

        Args:
            old_report: Baseline DiagnosticReport (as dict from asdict())
            new_report: Current DiagnosticReport (as dict from asdict())
            old_file: Name/identifier for baseline
            new_file: Name/identifier for current

        Returns:
            VPAXDiffResult with complete change analysis
        """
        all_changes: List[ObjectChange] = []
        measure_changes: List[MeasureChange] = []
        table_changes: List[TableChange] = []

        # Extract summaries
        old_summary = old_report.get('summary', {})
        new_summary = new_report.get('summary', {})

        # Calculate score delta
        old_score = old_summary.get('torque_score', 100)
        new_score = new_summary.get('torque_score', 100)
        score_delta = ScoreDelta(
            old_torque_score=old_score,
            new_torque_score=new_score,
            delta=new_score - old_score,
            direction=self._get_direction(old_score, new_score),
            old_penalty=old_summary.get('total_penalty', 0),
            new_penalty=new_summary.get('total_penalty', 0)
        )

        # Compare measures
        measure_changes, measure_all = self._compare_measures(
            old_report.get('measures', []),
            new_report.get('measures', [])
        )
        all_changes.extend(measure_all)

        # Compare tables
        table_changes, table_all = self._compare_tables(
            old_report.get('tables', []),
            new_report.get('tables', [])
        )
        all_changes.extend(table_all)

        # Compare relationships
        rel_changes = self._compare_relationships(
            old_report.get('relationships', []),
            new_report.get('relationships', [])
        )
        all_changes.extend(rel_changes)

        # Compare issues
        issue_changes = self._compare_issues(
            old_report.get('all_issues', []),
            new_report.get('all_issues', [])
        )
        all_changes.extend(issue_changes)

        # Build summary
        summary = self._build_summary(
            old_file, new_file,
            old_summary, new_summary,
            score_delta,
            measure_changes, table_changes,
            all_changes
        )

        # Group by severity
        result = VPAXDiffResult(
            summary=summary,
            measure_changes=measure_changes,
            table_changes=table_changes,
            all_changes=all_changes
        )

        for change in all_changes:
            if change.severity == ChangeSeverity.CRITICAL:
                result.critical_changes.append(change)
            elif change.severity == ChangeSeverity.HIGH:
                result.high_changes.append(change)
            elif change.severity == ChangeSeverity.MEDIUM:
                result.medium_changes.append(change)
            else:
                result.low_changes.append(change)

        return result

    def _get_direction(self, old: int, new: int) -> str:
        """Determine direction of score change."""
        if new > old:
            return "improved"
        elif new < old:
            return "degraded"
        return "stable"

    def _compare_measures(
        self,
        old_measures: List[Dict],
        new_measures: List[Dict]
    ) -> tuple:
        """Compare measures between versions."""
        changes: List[MeasureChange] = []
        all_changes: List[ObjectChange] = []

        old_map = {m.get('name', ''): m for m in old_measures}
        new_map = {m.get('name', ''): m for m in new_measures}

        old_names = set(old_map.keys())
        new_names = set(new_map.keys())

        # Added measures
        for name in (new_names - old_names):
            m = new_map[name]
            changes.append(MeasureChange(
                name=name,
                table=m.get('table', ''),
                change_type=ChangeType.ADDED,
                new_expression=m.get('expression', ''),
                new_severity_score=m.get('severity_score', 0)
            ))
            all_changes.append(ObjectChange(
                change_type=ChangeType.ADDED,
                category=ChangeCategory.MEASURE,
                severity=ChangeSeverity.MEDIUM,
                object_name=name,
                table_name=m.get('table'),
                description=f"New measure added: {name}"
            ))

        # Removed measures
        for name in (old_names - new_names):
            m = old_map[name]
            changes.append(MeasureChange(
                name=name,
                table=m.get('table', ''),
                change_type=ChangeType.REMOVED,
                old_expression=m.get('expression', ''),
                old_severity_score=m.get('severity_score', 0)
            ))
            all_changes.append(ObjectChange(
                change_type=ChangeType.REMOVED,
                category=ChangeCategory.MEASURE,
                severity=ChangeSeverity.HIGH,  # Removals are significant
                object_name=name,
                table_name=m.get('table'),
                description=f"Measure removed: {name}",
                impact="Any visuals using this measure will break"
            ))

        # Modified measures
        for name in (old_names & new_names):
            old_m = old_map[name]
            new_m = new_map[name]

            old_expr = old_m.get('expression', '')
            new_expr = new_m.get('expression', '')

            old_issues = {i.get('rule_id', '') for i in old_m.get('issues', [])}
            new_issues = {i.get('rule_id', '') for i in new_m.get('issues', [])}

            if old_expr != new_expr or old_issues != new_issues:
                old_score = old_m.get('severity_score', 0)
                new_score = new_m.get('severity_score', 0)

                mc = MeasureChange(
                    name=name,
                    table=new_m.get('table', ''),
                    change_type=ChangeType.MODIFIED,
                    old_expression=old_expr,
                    new_expression=new_expr,
                    expression_changed=(old_expr != new_expr),
                    issues_added=list(new_issues - old_issues),
                    issues_removed=list(old_issues - new_issues),
                    issues_changed=(old_issues != new_issues),
                    old_severity_score=old_score,
                    new_severity_score=new_score,
                    score_delta=new_score - old_score
                )
                changes.append(mc)

                # Determine severity based on what changed
                severity = ChangeSeverity.LOW
                if mc.expression_changed:
                    severity = ChangeSeverity.MEDIUM
                if mc.issues_added:
                    severity = ChangeSeverity.HIGH  # New issues = regression

                all_changes.append(ObjectChange(
                    change_type=ChangeType.MODIFIED,
                    category=ChangeCategory.MEASURE,
                    severity=severity,
                    object_name=name,
                    table_name=new_m.get('table'),
                    description=self._describe_measure_change(mc)
                ))

        return changes, all_changes

    def _describe_measure_change(self, mc: MeasureChange) -> str:
        """Generate description for a measure change."""
        parts = []
        if mc.expression_changed:
            parts.append("expression modified")
        if mc.issues_added:
            parts.append(f"new issues: {', '.join(mc.issues_added)}")
        if mc.issues_removed:
            parts.append(f"issues fixed: {', '.join(mc.issues_removed)}")
        if mc.score_delta != 0:
            direction = "improved" if mc.score_delta < 0 else "degraded"
            parts.append(f"score {direction} by {abs(mc.score_delta)}")
        return f"Measure {mc.name}: " + "; ".join(parts)

    def _compare_tables(
        self,
        old_tables: List[Dict],
        new_tables: List[Dict]
    ) -> tuple:
        """Compare tables between versions."""
        changes: List[TableChange] = []
        all_changes: List[ObjectChange] = []

        old_map = {t.get('name', ''): t for t in old_tables}
        new_map = {t.get('name', ''): t for t in new_tables}

        old_names = set(old_map.keys())
        new_names = set(new_map.keys())

        # Added tables
        for name in (new_names - old_names):
            t = new_map[name]
            changes.append(TableChange(
                name=name,
                change_type=ChangeType.ADDED,
                new_row_count=t.get('row_count', 0),
                new_size_bytes=t.get('size_bytes', 0)
            ))
            all_changes.append(ObjectChange(
                change_type=ChangeType.ADDED,
                category=ChangeCategory.TABLE,
                severity=ChangeSeverity.MEDIUM,
                object_name=name,
                description=f"New table added: {name}"
            ))

        # Removed tables
        for name in (old_names - new_names):
            t = old_map[name]
            changes.append(TableChange(
                name=name,
                change_type=ChangeType.REMOVED,
                old_row_count=t.get('row_count', 0),
                old_size_bytes=t.get('size_bytes', 0)
            ))
            all_changes.append(ObjectChange(
                change_type=ChangeType.REMOVED,
                category=ChangeCategory.TABLE,
                severity=ChangeSeverity.HIGH,
                object_name=name,
                description=f"Table removed: {name}",
                impact="Measures referencing this table will break"
            ))

        # Modified tables
        for name in (old_names & new_names):
            old_t = old_map[name]
            new_t = new_map[name]

            old_rows = old_t.get('row_count', 0)
            new_rows = new_t.get('row_count', 0)
            old_size = old_t.get('size_bytes', 0)
            new_size = new_t.get('size_bytes', 0)

            # Check for significant changes
            row_change = abs(new_rows - old_rows) / max(old_rows, 1) > 0.1
            size_change = abs(new_size - old_size) / max(old_size, 1) > 0.1

            if row_change or size_change:
                tc = TableChange(
                    name=name,
                    change_type=ChangeType.MODIFIED,
                    old_row_count=old_rows,
                    new_row_count=new_rows,
                    old_size_bytes=old_size,
                    new_size_bytes=new_size
                )
                changes.append(tc)

                all_changes.append(ObjectChange(
                    change_type=ChangeType.MODIFIED,
                    category=ChangeCategory.TABLE,
                    severity=ChangeSeverity.LOW,
                    object_name=name,
                    description=f"Table {name}: rows {old_rows:,} -> {new_rows:,}, size changed"
                ))

        return changes, all_changes

    def _compare_relationships(
        self,
        old_rels: List[Dict],
        new_rels: List[Dict]
    ) -> List[ObjectChange]:
        """Compare relationships between versions."""
        changes: List[ObjectChange] = []

        def rel_key(r: Dict) -> str:
            return f"{r.get('from_table', '')}.{r.get('from_column', '')} -> {r.get('to_table', '')}.{r.get('to_column', '')}"

        old_map = {rel_key(r): r for r in old_rels}
        new_map = {rel_key(r): r for r in new_rels}

        old_keys = set(old_map.keys())
        new_keys = set(new_map.keys())

        for key in (new_keys - old_keys):
            changes.append(ObjectChange(
                change_type=ChangeType.ADDED,
                category=ChangeCategory.RELATIONSHIP,
                severity=ChangeSeverity.MEDIUM,
                object_name=key,
                description=f"New relationship: {key}"
            ))

        for key in (old_keys - new_keys):
            changes.append(ObjectChange(
                change_type=ChangeType.REMOVED,
                category=ChangeCategory.RELATIONSHIP,
                severity=ChangeSeverity.HIGH,
                object_name=key,
                description=f"Relationship removed: {key}",
                impact="Queries may return incorrect results"
            ))

        return changes

    def _compare_issues(
        self,
        old_issues: List[Dict],
        new_issues: List[Dict]
    ) -> List[ObjectChange]:
        """Compare issues between versions."""
        changes: List[ObjectChange] = []

        def issue_key(i: Dict) -> str:
            return f"{i.get('rule_id', '')}:{i.get('object_name', '')}"

        old_map = {issue_key(i): i for i in old_issues}
        new_map = {issue_key(i): i for i in new_issues}

        old_keys = set(old_map.keys())
        new_keys = set(new_map.keys())

        # New issues (regression)
        for key in (new_keys - old_keys):
            issue = new_map[key]
            severity = ChangeSeverity.MEDIUM
            if issue.get('severity') in ('critical', 'high'):
                severity = ChangeSeverity.HIGH

            changes.append(ObjectChange(
                change_type=ChangeType.ADDED,
                category=ChangeCategory.ISSUE,
                severity=severity,
                object_name=issue.get('object_name', ''),
                description=f"New issue: {issue.get('rule_id', '')} - {issue.get('description', '')}"
            ))

        # Fixed issues (improvement)
        for key in (old_keys - new_keys):
            issue = old_map[key]
            changes.append(ObjectChange(
                change_type=ChangeType.REMOVED,
                category=ChangeCategory.ISSUE,
                severity=ChangeSeverity.INFO,  # Fixes are positive
                object_name=issue.get('object_name', ''),
                description=f"Issue fixed: {issue.get('rule_id', '')} - {issue.get('description', '')}"
            ))

        return changes

    def _build_summary(
        self,
        old_file: str,
        new_file: str,
        old_summary: Dict,
        new_summary: Dict,
        score_delta: ScoreDelta,
        measure_changes: List[MeasureChange],
        table_changes: List[TableChange],
        all_changes: List[ObjectChange]
    ) -> DiffSummary:
        """Build the diff summary."""
        summary = DiffSummary(
            old_file=old_file,
            new_file=new_file,
            old_timestamp=old_summary.get('analysis_timestamp', ''),
            new_timestamp=new_summary.get('analysis_timestamp', ''),
            score_delta=score_delta
        )

        # Count measure changes
        for mc in measure_changes:
            if mc.change_type == ChangeType.ADDED:
                summary.measures_added += 1
            elif mc.change_type == ChangeType.REMOVED:
                summary.measures_removed += 1
            elif mc.change_type == ChangeType.MODIFIED:
                summary.measures_modified += 1

        # Count table changes
        for tc in table_changes:
            if tc.change_type == ChangeType.ADDED:
                summary.tables_added += 1
            elif tc.change_type == ChangeType.REMOVED:
                summary.tables_removed += 1
            elif tc.change_type == ChangeType.MODIFIED:
                summary.tables_modified += 1

        # Count by category
        for change in all_changes:
            if change.category == ChangeCategory.RELATIONSHIP:
                if change.change_type == ChangeType.ADDED:
                    summary.relationships_added += 1
                elif change.change_type == ChangeType.REMOVED:
                    summary.relationships_removed += 1
            elif change.category == ChangeCategory.ISSUE:
                if change.change_type == ChangeType.ADDED:
                    summary.issues_added += 1
                elif change.change_type == ChangeType.REMOVED:
                    summary.issues_removed += 1

        # Identify breaking changes
        for change in all_changes:
            if change.severity in (ChangeSeverity.CRITICAL, ChangeSeverity.HIGH):
                if change.change_type == ChangeType.REMOVED:
                    summary.breaking_changes.append(change.description)

        # Generate recommendations
        summary.recommendations = self._generate_recommendations(summary, all_changes)

        return summary

    def _generate_recommendations(
        self,
        summary: DiffSummary,
        all_changes: List[ObjectChange]
    ) -> List[str]:
        """Generate recommendations based on changes."""
        recs = []

        if summary.score_delta.direction == "degraded":
            recs.append(
                f"Model health degraded from {summary.score_delta.old_torque_score} to "
                f"{summary.score_delta.new_torque_score}. Review new issues."
            )

        if summary.measures_removed > 0:
            recs.append(
                f"{summary.measures_removed} measures were removed. "
                "Verify no reports are broken."
            )

        if summary.issues_added > 5:
            recs.append(
                f"{summary.issues_added} new issues detected. "
                "Consider reverting recent changes or addressing issues."
            )

        if summary.breaking_changes:
            recs.append(
                f"{len(summary.breaking_changes)} breaking changes detected. "
                "Test all dependent reports before deployment."
            )

        if not recs:
            if summary.score_delta.direction == "improved":
                recs.append("Model health improved. Good job!")
            else:
                recs.append("No significant issues detected in this change.")

        return recs


# =============================================================================
# REPORT GENERATOR
# =============================================================================

class DiffReportGenerator:
    """Generate human-readable diff reports."""

    def generate_markdown(self, diff: VPAXDiffResult) -> str:
        """Generate a Markdown diff report."""
        lines = [
            "# VPAX Model Change Report",
            "",
            f"**Baseline:** {diff.summary.old_file}  ",
            f"**Current:** {diff.summary.new_file}  ",
            f"**Generated:** {datetime.now().isoformat()}",
            "",
            "## Executive Summary",
            "",
        ]

        # Score change
        sd = diff.summary.score_delta
        direction_marker = "+" if sd.direction == "improved" else "-" if sd.direction == "degraded" else "="
        lines.append(f"**Torque Score:** {sd.old_torque_score} -> {sd.new_torque_score} ({sd.delta:+d}) {direction_marker}")
        lines.append("")

        # Change counts
        lines.append("### Changes Overview")
        lines.append("")
        lines.append("| Category | Added | Removed | Modified |")
        lines.append("|----------|-------|---------|----------|")
        lines.append(f"| Measures | {diff.summary.measures_added} | {diff.summary.measures_removed} | {diff.summary.measures_modified} |")
        lines.append(f"| Tables | {diff.summary.tables_added} | {diff.summary.tables_removed} | {diff.summary.tables_modified} |")
        lines.append(f"| Relationships | {diff.summary.relationships_added} | {diff.summary.relationships_removed} | - |")
        lines.append(f"| Issues | {diff.summary.issues_added} | {diff.summary.issues_removed} | - |")
        lines.append("")

        # Breaking changes
        if diff.summary.breaking_changes:
            lines.append("## Breaking Changes")
            lines.append("")
            for bc in diff.summary.breaking_changes:
                lines.append(f"- {bc}")
            lines.append("")

        # Critical/High changes
        if diff.critical_changes or diff.high_changes:
            lines.append("## High Priority Changes")
            lines.append("")
            for change in diff.critical_changes + diff.high_changes:
                lines.append(f"- **{change.severity.value.upper()}** [{change.category.value}] {change.description}")
            lines.append("")

        # Recommendations
        if diff.summary.recommendations:
            lines.append("## Recommendations")
            lines.append("")
            for rec in diff.summary.recommendations:
                lines.append(f"- {rec}")
            lines.append("")

        # Detailed measure changes
        if diff.measure_changes:
            lines.append("## Measure Changes")
            lines.append("")
            for mc in diff.measure_changes:
                if mc.change_type == ChangeType.ADDED:
                    lines.append(f"### + {mc.name}")
                    lines.append(f"*New measure in table `{mc.table}`*")
                elif mc.change_type == ChangeType.REMOVED:
                    lines.append(f"### - {mc.name}")
                    lines.append("*Measure removed*")
                elif mc.change_type == ChangeType.MODIFIED:
                    lines.append(f"### ~ {mc.name}")
                    if mc.expression_changed:
                        lines.append("*Expression modified*")
                    if mc.issues_added:
                        lines.append(f"*New issues:* {', '.join(mc.issues_added)}")
                    if mc.issues_removed:
                        lines.append(f"*Issues fixed:* {', '.join(mc.issues_removed)}")
                lines.append("")

        return "\n".join(lines)

    def generate_json(self, diff: VPAXDiffResult) -> Dict:
        """Generate a JSON-serializable diff report."""
        return {
            "summary": {
                "old_file": diff.summary.old_file,
                "new_file": diff.summary.new_file,
                "old_timestamp": diff.summary.old_timestamp,
                "new_timestamp": diff.summary.new_timestamp,
                "score_delta": {
                    "old": diff.summary.score_delta.old_torque_score,
                    "new": diff.summary.score_delta.new_torque_score,
                    "delta": diff.summary.score_delta.delta,
                    "direction": diff.summary.score_delta.direction
                },
                "changes": {
                    "measures": {
                        "added": diff.summary.measures_added,
                        "removed": diff.summary.measures_removed,
                        "modified": diff.summary.measures_modified
                    },
                    "tables": {
                        "added": diff.summary.tables_added,
                        "removed": diff.summary.tables_removed,
                        "modified": diff.summary.tables_modified
                    },
                    "relationships": {
                        "added": diff.summary.relationships_added,
                        "removed": diff.summary.relationships_removed
                    },
                    "issues": {
                        "added": diff.summary.issues_added,
                        "removed": diff.summary.issues_removed
                    }
                },
                "breaking_changes": diff.summary.breaking_changes,
                "recommendations": diff.summary.recommendations
            },
            "changes": {
                "critical": [
                    {"category": c.category.value, "type": c.change_type.value, "description": c.description}
                    for c in diff.critical_changes
                ],
                "high": [
                    {"category": c.category.value, "type": c.change_type.value, "description": c.description}
                    for c in diff.high_changes
                ],
                "medium": [
                    {"category": c.category.value, "type": c.change_type.value, "description": c.description}
                    for c in diff.medium_changes
                ],
                "low": [
                    {"category": c.category.value, "type": c.change_type.value, "description": c.description}
                    for c in diff.low_changes
                ]
            },
            "measure_changes": [
                {
                    "name": mc.name,
                    "table": mc.table,
                    "change_type": mc.change_type.value,
                    "expression_changed": mc.expression_changed,
                    "issues_added": mc.issues_added,
                    "issues_removed": mc.issues_removed,
                    "score_delta": mc.score_delta
                }
                for mc in diff.measure_changes
            ]
        }


# =============================================================================
# FACTORY FUNCTIONS
# =============================================================================

def create_vpax_differ() -> VPAXDiffer:
    """Factory function to create a VPAXDiffer."""
    return VPAXDiffer()


def create_diff_report_generator() -> DiffReportGenerator:
    """Factory function to create a DiffReportGenerator."""
    return DiffReportGenerator()


def compare_vpax_reports(
    old_report: Dict,
    new_report: Dict,
    old_file: str = "baseline",
    new_file: str = "current"
) -> VPAXDiffResult:
    """
    Convenience function to compare two VPAX reports.

    Args:
        old_report: Baseline DiagnosticReport as dict
        new_report: Current DiagnosticReport as dict
        old_file: Name for baseline
        new_file: Name for current

    Returns:
        VPAXDiffResult with complete analysis
    """
    differ = create_vpax_differ()
    return differ.compare(old_report, new_report, old_file, new_file)
