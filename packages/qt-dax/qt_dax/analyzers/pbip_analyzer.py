"""PBIP semantic model analysis using TMDL inputs."""

from __future__ import annotations

from dataclasses import asdict
from datetime import datetime
from pathlib import Path

from .vpax_analyzer import (
    DAXAnalyzer,
    DiagnosticReport,
    ModelSummary,
    QualityGate,
    Severity,
    DAX_RULES,
    VPAXIssue,
    MeasureAnalysis,
    TableAnalysis,
    ColumnAnalysis,
    RelationshipAnalysis,
)
from .measure_dependencies import MeasureDependencyAnalyzer
from ..parsers.tmdl_parser import TMDLParser


class PBIPReportGenerator:
    """Generate diagnostic reports from PBIP semantic model (TMDL)."""

    def __init__(self, semantic_model_dir: str):
        self.semantic_model_dir = Path(semantic_model_dir)
        self.parser = TMDLParser(self.semantic_model_dir)
        self.dax_analyzer = DAXAnalyzer()

    def generate(self) -> DiagnosticReport:
        parsed = self.parser.parse()

        measures_data = [
            {
                "name": m.name,
                "table": m.table,
                "expression": m.expression,
            }
            for m in parsed["measures"]
        ]

        dep_analyzer = MeasureDependencyAnalyzer()
        dependency_result = dep_analyzer.analyze(measures_data)
        self.dax_analyzer = DAXAnalyzer(dependency_result=dependency_result)

        measure_analyses: list[MeasureAnalysis] = []
        for measure in measures_data:
            analysis = self.dax_analyzer.analyze_measure(
                name=measure["name"],
                table=measure["table"],
                expression=measure["expression"],
            )
            measure_analyses.append(analysis)

        table_analyses: list[TableAnalysis] = []
        for table in parsed["tables"]:
            name = table["name"]
            is_local = table.get("is_local_date_table", False)
            table_analyses.append(TableAnalysis(
                name=name,
                row_count=0,
                column_count=0,
                size_bytes=0,
                is_hidden=False,
                is_date_table=name == "Date" or is_local,
                is_local_date_table=is_local,
            ))

        column_analyses: list[ColumnAnalysis] = []
        for col in parsed["columns"]:
            column_analyses.append(ColumnAnalysis(
                name=col.name,
                table=col.table,
                data_type=col.data_type,
                encoding="",
                cardinality=0,
                total_size=0,
                dictionary_size=0,
                dictionary_ratio=0.0,
                is_key=False,
            ))

        relationship_analyses: list[RelationshipAnalysis] = []
        for rel in parsed["relationships"]:
            relationship_analyses.append(RelationshipAnalysis(
                from_table=rel.from_table,
                from_column=rel.from_column,
                to_table=rel.to_table,
                to_column=rel.to_column,
                is_active=rel.is_active,
                cross_filter=rel.cross_filter,
                missing_keys=0,
            ))

        all_issues: list[VPAXIssue] = []
        for m in measure_analyses:
            all_issues.extend(m.issues)

        severity_counts = {s.value: 0 for s in Severity}
        for issue in all_issues:
            severity_counts[issue.severity] += 1

        penalty_by_rule: dict[str, int] = {}
        for issue in all_issues:
            rule_id = issue.rule_id
            if rule_id not in penalty_by_rule:
                penalty_by_rule[rule_id] = 0
            rule_def = DAX_RULES.get(rule_id)
            penalty = rule_def.get("penalty", 5) if rule_def else 5
            penalty_by_rule[rule_id] = min(40, penalty_by_rule[rule_id] + penalty)

        total_penalty = sum(penalty_by_rule.values())
        torque_score = max(0, 100 - total_penalty)
        quality_gate = QualityGate.from_score(torque_score)

        tech_debt_hours_raw = (
            severity_counts["critical"] * 2.0 +
            severity_counts["high"] * 1.0 +
            severity_counts["medium"] * 0.5 +
            severity_counts["low"] * 0.25
        )
        tech_debt_hours = round(tech_debt_hours_raw)

        local_date_tables = [t for t in table_analyses if t.is_local_date_table]

        summary = ModelSummary(
            file_name=self.semantic_model_dir.name,
            analysis_timestamp=datetime.now().isoformat(),
            total_tables=len([t for t in table_analyses if not t.is_local_date_table]),
            total_columns=len(column_analyses),
            total_measures=len(measure_analyses),
            total_relationships=len(relationship_analyses),
            total_size_bytes=0,
            local_date_table_count=len(local_date_tables),
            local_date_table_size_bytes=0,
            actual_data_size_bytes=0,
            critical_count=severity_counts["critical"],
            high_count=severity_counts["high"],
            medium_count=severity_counts["medium"],
            low_count=severity_counts["low"],
            info_count=severity_counts["info"],
            torque_score=torque_score,
            total_penalty=total_penalty,
            quality_gate=quality_gate,
            tech_debt_hours=tech_debt_hours,
        )

        report = DiagnosticReport(
            summary=summary,
            tables=[asdict(t) for t in table_analyses],
            columns=[asdict(c) for c in column_analyses if c.issues],
            measures=[asdict(m) for m in measure_analyses],
            relationships=[asdict(r) for r in relationship_analyses],
            all_issues=[asdict(i) for i in all_issues],
            critical_issues=[asdict(i) for i in all_issues if i.severity == "critical"],
            high_issues=[asdict(i) for i in all_issues if i.severity == "high"],
            medium_issues=[asdict(i) for i in all_issues if i.severity == "medium"],
            low_issues=[asdict(i) for i in all_issues if i.severity == "low"],
            info_issues=[asdict(i) for i in all_issues if i.severity == "info"],
            worst_measures=[asdict(m) for m in sorted(measure_analyses, key=lambda x: x.severity_score, reverse=True)[:20]],
            largest_tables=[asdict(t) for t in sorted(table_analyses, key=lambda x: x.size_bytes, reverse=True)[:20]],
            highest_cardinality_columns=[asdict(c) for c in sorted(column_analyses, key=lambda x: x.cardinality, reverse=True)[:20]],
        )

        return report
