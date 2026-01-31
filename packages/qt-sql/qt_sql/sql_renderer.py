"""SQL report renderer for Query Torque.

Transforms SQL analysis output into self-contained HTML reports using
Jinja2 templating with sql_report.html.j2.
"""

import json
import re
from pathlib import Path
from datetime import datetime
from typing import Any, Optional
import hashlib

from jinja2 import Environment, FileSystemLoader

from ..parsers.sql_parser import SQLParser, QueryGraph, parse_sql
from ..analyzers.sql_antipattern_detector import (
    SQLAntiPatternDetector,
    SQLAnalysisResult,
)
from ..analyzers.join_profiler import JoinProfiler
from ..execution.base import ExecutionPlanAnalysis
from ..utils import get_quality_gate_label
from ..optimization.payload_builder_v2 import build_optimization_payload_v2


class SQLRenderer:
    """Render SQL efficiency audit reports.

    Transforms SQL analysis output into HTML reports using
    the sql_report.html.j2 Jinja2 template.
    """

    def __init__(self, dialect: str = "snowflake"):
        # Templates are in query_torque/templates/, not renderers/templates/
        template_dir = Path(__file__).parent.parent / 'templates'
        self.env = Environment(
            loader=FileSystemLoader(template_dir),
            autoescape=True
        )
        # Add custom filters
        self.env.filters['number_format'] = self._number_format
        self.dialect = dialect
        self.parser = SQLParser(dialect=dialect)
        self.detector = SQLAntiPatternDetector(dialect=dialect)

    def _number_format(self, value: int | float | None) -> str:
        """Format number with thousand separators."""
        if value is None:
            return "—"
        if isinstance(value, float):
            return f"{value:,.2f}"
        return f"{value:,}"

    def render(self, analysis_data: dict[str, Any]) -> str:
        """Render analysis data to HTML string.

        Args:
            analysis_data: Analysis output from SQL analyzer

        Returns:
            Complete HTML string
        """
        template = self.env.get_template('sql_report.html.j2')
        context = self._build_template_context(analysis_data)
        return template.render(**context)

    def _build_template_context(self, analysis_data: dict[str, Any]) -> dict[str, Any]:
        """Build template context from analyzer output.

        Args:
            analysis_data: Raw analysis output from SQL analyzer

        Returns:
            Context dict for Jinja2 template
        """
        summary = analysis_data.get('summary', {})
        sql_content = analysis_data.get('sql_content', '')
        file_name = analysis_data.get('file_name', 'query.sql')
        platform = analysis_data.get('platform', self.dialect)

        # Get detector result if available
        detector_result = analysis_data.get('detector_result')
        query_graph = analysis_data.get('query_graph')

        # Calculate score
        if detector_result:
            score = detector_result.get('score', 100)
            severity_counts = detector_result.get('severity_counts', {})
        else:
            score = summary.get('torque_score', self._calculate_score(analysis_data))
            severity_counts = {
                'critical': summary.get('critical_issues', 0),
                'high': summary.get('high_issues', 0),
                'medium': summary.get('medium_issues', 0),
                'low': summary.get('low_issues', 0),
            }

        # Determine quality gate (handle None score)
        if score is None:
            score = 0  # Default to 0 for calculations
            quality_gate_label = 'Unknown'
            quality_gate = {'status': 'deny', 'label': quality_gate_label}
            score_class = 'critical'
        else:
            quality_gate_label = get_quality_gate_label(score)
            if score >= 90:
                quality_gate = {'status': 'pass', 'label': quality_gate_label}
                score_class = 'excellent'
            elif score >= 70:
                quality_gate = {'status': 'warn', 'label': quality_gate_label}
                score_class = 'good'
            elif score >= 50:
                quality_gate = {'status': 'fail', 'label': quality_gate_label}
                score_class = 'warning'
            else:
                quality_gate = {'status': 'deny', 'label': quality_gate_label}
                score_class = 'critical'

        # Build issues list
        issues = self._build_template_issues(analysis_data, detector_result)

        # Count issues
        issue_count = len(issues)

        # Build headline
        if issue_count == 0:
            headline = "Query looks clean!"
        elif severity_counts.get('critical', 0) > 0:
            headline = f"Found {severity_counts['critical']} critical issue(s) requiring immediate attention"
        elif severity_counts.get('high', 0) > 0:
            headline = f"Found {severity_counts['high']} high priority issue(s)"
        else:
            headline = f"Found {issue_count} issue(s) to review"

        # Build code lines with annotations (legacy format)
        code_lines = self._build_code_lines(sql_content, issues)

        # Build schema context
        schema_context = self._build_schema_context(analysis_data, query_graph)

        # Build tasks (remediation checklist)
        tasks = self._build_tasks(issues)

        # Get LLM summary if available
        llm_summary = summary.get('llm_summary', '')

        # Build source lines for new annotated source section
        source_lines, source_annotations = self._build_source_lines(sql_content, issues)

        # Build schema map for new template format
        schema_map = self._build_template_schema_map(analysis_data, query_graph, sql_content)

        # Build execution reality for new template format
        execution_reality = self._build_template_execution_reality(analysis_data)

        # Build constraints from query structure
        constraints = self._build_constraints(query_graph, sql_content, execution_reality)

        # Generate report ID
        report_id = f"qt-{hashlib.md5((file_name + sql_content[:100]).encode()).hexdigest()[:8]}"
        generated_date = datetime.now().strftime("%Y-%m-%d %H:%M")

        # Build verdict for template
        verdict = {
            'primary_issue': issues[0]['title'] if issues else None,
            'detail': issues[0]['explanation'] if issues else None,
        }

        # Build summary section
        summary_data = {
            'torque_score': score,
            'quality_gate': quality_gate,
            'headline': headline,
            'llm_summary': llm_summary,
            'issue_count': issue_count,
            'severity_counts': severity_counts,
            'improvement_estimate': f"{min(50, issue_count * 5)}% faster" if issue_count > 0 else "Optimized",
            'tech_debt_hours': sum(i.get('estimates', {}).get('effort', 15) for i in issues) // 60,
        }

        # Build JSON data for embedded script tag
        json_data = self._build_json_data(
            report_id=report_id,
            generated_date=generated_date,
            file_name=file_name,
            platform=platform,
            score=score,
            quality_gate=quality_gate,
            severity_counts=severity_counts,
            issues=issues,
            execution_reality=execution_reality,
            schema_map=schema_map,
            sql_content=sql_content,
            source_annotations=source_annotations,
            constraints=constraints,
        )

        # Build LLM optimization payload (single source of truth for CLI and web UI)
        # Using v2 structured YAML format for better token efficiency
        payload_result = build_optimization_payload_v2(
            code=sql_content,
            query_type="sql",
            file_name=file_name,
            issues=issues,
            schema_context=schema_context,
            execution_plan=execution_reality,
            constraints=constraints,
        )
        llm_payload = payload_result.payload_yaml

        return {
            # Core identifiers
            'filename': file_name,
            'file_path': file_name,
            'report_id': report_id,
            'platform': platform,
            'generated_date': generated_date,
            'client_name': 'Query Torque',
            'score_class': score_class,
            'quality_gate_label': quality_gate_label,

            # Summary and verdict
            'summary': summary_data,
            'verdict': verdict,

            # JSON data for embedded script
            'json_data': json_data,

            # Pre-generated LLM payload (single source of truth)
            'llm_payload': llm_payload,

            # Issues and tasks
            'issues': issues,
            'tasks': tasks,

            # New template sections
            'source_lines': source_lines,
            'source_annotations': source_annotations,
            'original_sql': sql_content,
            'schema_map': schema_map,
            'execution_reality': execution_reality,

            # Legacy support
            'query_lines': code_lines,
            'schema_context': schema_context,
            'warnings': [],
            'index_recommendations': self._build_index_recommendations(issues, schema_context),
            'execution_plan_analysis': self._build_execution_plan_context(
                analysis_data.get('execution_plan')
            ),
            'structural_observations': self._build_structural_observations(issues, schema_context),

            # Database schema (from live connection)
            'database_schema': analysis_data.get('database_schema'),

            # Server timings and query plan operators (from execution profiling)
            'server_timings': analysis_data.get('server_timings'),
            'query_plan_operators': analysis_data.get('query_plan_operators'),
        }

    def _build_json_data(
        self,
        report_id: str,
        generated_date: str,
        file_name: str,
        platform: str,
        score: int,
        quality_gate: dict,
        severity_counts: dict,
        issues: list,
        execution_reality: Optional[dict],
        schema_map: Optional[dict],
        sql_content: str,
        source_annotations: dict,
        constraints: Optional[dict] = None,
    ) -> str:
        """Build JSON data for embedded script tag.

        Returns JSON string for the qt-data script block.
        """
        # Build issues for JSON
        json_issues = []
        for issue in issues:
            json_issues.append({
                "id": issue.get('id', ''),
                "rule": issue.get('rule_id', ''),
                "title": issue.get('title', ''),
                "severity": issue.get('severity', 'medium'),
                "category": issue.get('category', 'general'),
                "line": issue.get('location', {}).get('line', 0),
                "snippet": issue.get('location', {}).get('expression'),
                "problem": issue.get('explanation', ''),
                "evidence": issue.get('evidence', []),
                "fix": issue.get('fix', {}),
                "suggestion": issue.get('pattern', {}).get('example', ''),
                "penalty": issue.get('penalty', 10),
                "impact_pct": issue.get('impact_pct', 0),
                "confidence": issue.get('confidence', 'HIGH'),
                "effort_minutes": issue.get('estimates', {}).get('effort', 15),
            })

        data = {
            "report_id": report_id,
            "generated_at": generated_date,
            "anti_pattern_library": "sql-patterns-v3.yaml",
            "query": {
                "name": Path(file_name).stem,
                "path": file_name,
                "platform": platform,
                "materialization": "query",
            },
            "verdict": {
                "status": quality_gate.get('status', 'unknown').upper(),
                "label": quality_gate.get('label', 'Unknown'),
                "score": score,
                "blocking": quality_gate.get('status') in ['deny', 'fail'],
                "primary_issue": issues[0]['rule_id'] if issues else None,
                "summary": issues[0]['title'] if issues else "No issues found",
                "severity_counts": severity_counts,
                "total_penalty": 100 - score,
            },
            "constraints": constraints or {},
            "execution_reality": execution_reality,
            "schema_map": schema_map or {},
            "issues": json_issues,
            "source": {
                "path": file_name,
                "lines": len(sql_content.splitlines()) if sql_content else 0,
                "annotations": source_annotations,
                "sql": sql_content,
            },
        }

        # Escape </script> to prevent XSS when embedding in HTML script tags
        json_str = json.dumps(data, indent=2)
        # Replace </script> with escaped version to prevent early tag closure
        json_str = json_str.replace("</script>", "<\\/script>")
        json_str = json_str.replace("<script>", "<\\script>")
        return json_str

    def _build_constraints(
        self,
        query_graph: Optional[dict],
        sql_content: str,
        execution_reality: Optional[dict],
    ) -> dict[str, Any]:
        """Build optimization constraints from parsed query structure.

        Extracts actual constraints from the query:
        - Output columns from SELECT clause
        - Sort order from ORDER BY
        - Row count from execution (if available)
        - Permitted/forbidden based on query structure

        Args:
            query_graph: Parsed query graph from SQLParser
            sql_content: Raw SQL content
            execution_reality: Execution plan data (for row counts)

        Returns:
            Constraints dict with preserve, permitted, forbidden sections
        """
        # Extract output columns and analysis from query graph
        output_columns = []
        output_grain = []
        sort_order = "none"
        order_by_columns = []
        has_aggregation = False
        has_window = False
        has_distinct = False
        has_union = False
        cte_names = []

        if query_graph:
            nodes = query_graph.get('nodes', {})
            output_node = nodes.get('__output__', {})
            output_columns = [c for c in output_node.get('columns_output', []) if c != '*']

            # Get analysis from output node
            output_analysis = output_node.get('analysis', {})
            has_distinct = output_analysis.get('has_distinct', False)

            # Get ORDER BY from AST (already extracted by parser)
            order_by_columns = output_analysis.get('order_by', [])
            if order_by_columns:
                sort_order = ', '.join(order_by_columns)

            # Check for aggregation/window in any node
            for node_name, node in nodes.items():
                analysis = node.get('analysis', {})
                if analysis.get('has_aggregation'):
                    has_aggregation = True
                if analysis.get('has_window'):
                    has_window = True
                if node.get('type') == 'cte':
                    cte_names.append(node_name)

            # Extract GROUP BY columns as grain
            data_flow = query_graph.get('data_flow', {})
            for node_name in reversed(data_flow.get('execution_order', [])):
                if node_name == '__output__':
                    continue
                node = nodes.get(node_name, {})
                analysis = node.get('analysis', {})
                if analysis.get('has_aggregation'):
                    output_grain = output_columns[:3]
                    break

        # Check for UNION (not tracked in AST yet, use simple check)
        sql_upper = sql_content.upper()
        has_union = ' UNION ' in sql_upper

        # Get row count from execution if available
        row_count = None
        if execution_reality:
            # Try to get from cardinality data
            cardinality = execution_reality.get('cardinality', [])
            if cardinality:
                # Last operator usually has final row count
                row_count = cardinality[-1].get('rows') if cardinality else None

        # Build permitted transformations based on query structure
        permitted = []
        permitted.append("Reorder JOINs for efficiency")
        if not cte_names:
            permitted.append("Convert subqueries to CTEs")
        else:
            permitted.append("Refactor CTE structure")
        permitted.append("Push predicates closer to data sources")
        permitted.append("Add index hints if beneficial")
        if has_aggregation:
            permitted.append("Pre-aggregate in subqueries")

        # Build forbidden transformations based on query structure
        forbidden = []
        # CRITICAL: These are the most common LLM mistakes
        forbidden.append("NEVER introduce SELECT * - always preserve explicit column lists")
        forbidden.append("NEVER introduce CROSS JOIN or Cartesian products")
        forbidden.append("Change output column names or order")
        if output_columns:
            forbidden.append(f"Remove any of the {len(output_columns)} output columns")
        else:
            forbidden.append("Remove columns from SELECT")
        forbidden.append("Modify literal filter values")
        if has_aggregation:
            forbidden.append("Change aggregation granularity")
        if has_distinct:
            forbidden.append("Remove DISTINCT (required for deduplication)")
        if has_union:
            forbidden.append("Change UNION semantics")
        if sort_order != "none":
            forbidden.append("Remove or change ORDER BY clause")

        return {
            "preserve": {
                "output_columns": output_columns if output_columns else ["*"],
                "output_grain": output_grain,
                "sort_order": sort_order,
                "row_count": row_count,
                "row_tolerance": 0,  # Exact match required
                "checksum_columns": [],
            },
            "permitted": permitted,
            "forbidden": forbidden,
            "query_features": {
                "has_aggregation": has_aggregation,
                "has_window": has_window,
                "has_distinct": has_distinct,
                "has_union": has_union,
                "cte_count": len(cte_names),
            }
        }

    def _build_execution_plan_context(
        self, execution_plan: Optional[ExecutionPlanAnalysis | dict[str, Any]]
    ) -> Optional[dict[str, Any]]:
        """Build execution plan section for template.

        Args:
            execution_plan: ExecutionPlanAnalysis from DuckDB or dict (already converted)

        Returns:
            Template context for execution plan section, or None if no plan
        """
        if execution_plan is None:
            return None

        # Handle both object and dict formats
        if isinstance(execution_plan, dict):
            return execution_plan

        return execution_plan.to_template_context()

    def _build_template_issues(
        self, data: dict[str, Any], detector_result: Optional[dict] = None
    ) -> list[dict[str, Any]]:
        """Build issues list formatted for the template.

        Populates ALL fields for demo-quality output. Every issue will have:
        - fix.before/after code blocks
        - evidence list with 2-3 items
        - scope string
        - expected_improvement percentage
        - confidence level
        """
        issues = []

        if detector_result and detector_result.get('issues'):
            for i, issue in enumerate(detector_result['issues'], 1):
                rule_id = issue.get('rule_id', f'SQL{i:03d}')
                severity = issue.get('severity', 'medium').lower()
                penalty = issue.get('penalty', 10)  # Default to 10 if missing
                match_text = issue.get('match', '')
                suggestion = issue.get('suggestion', 'Review and refactor this pattern')
                location = issue.get('location')

                # Build fix block - ALWAYS populated
                fix = {
                    'before': match_text if match_text else 'Current pattern',
                    'after': suggestion,
                    'label': 'Recommended',
                    'also_replace': None,
                }

                # Build evidence list - ALWAYS 2-3 items
                evidence = []
                if match_text:
                    evidence.append(f"Pattern: <code>{match_text}</code>")
                evidence.append(f"Severity penalty: {penalty} points")
                if issue.get('description'):
                    evidence.append(issue['description'])
                # Ensure at least 2 items
                if len(evidence) < 2:
                    evidence.append(f"Category: {issue.get('category', 'general')}")

                # Build scope string - ALWAYS populated
                if location:
                    scope = f"<code>{location}</code> CTE"
                else:
                    scope = "Query scope"

                # Calculate improvement % (not time)
                improvement_pct = penalty * 2  # Rough estimate: penalty × 2 = improvement %
                expected_improvement = f"−{improvement_pct}% estimated"

                issues.append({
                    'id': f"{rule_id}-{i}",
                    'rule_id': rule_id,
                    'severity': severity,
                    'title': issue.get('name', 'Issue'),
                    'explanation': issue.get('description', ''),
                    'location': {
                        'line': issue.get('line', 0),
                        'expression': match_text,
                        'scope': location,
                    },
                    # All sections populated for demo-quality output
                    'fix': fix,
                    'evidence': evidence,
                    'scope': scope,
                    'expected_improvement': expected_improvement,
                    'impact_pct': self._penalty_to_impact(penalty) if severity == 'critical' else None,
                    'risk': 'Risk' if severity in ('high', 'medium') else None,
                    'risk_label': 'performance' if severity == 'high' else 'code quality' if severity == 'medium' else None,
                    'confidence': 'HIGH',
                    # Existing fields for compatibility
                    'pattern': {
                        'description': issue.get('explanation', ''),
                        'example': suggestion,
                        'note': '',
                    },
                    'estimates': {
                        'effort': self._severity_to_effort(severity),
                        'improvement': f"{improvement_pct}%",
                    },
                    'llm_suggestion': issue.get('llm_suggestion'),
                    'reference_link': '',
                    'plan_operator': '',
                })
        else:
            # Legacy format from LLM analyzer
            for i, issue in enumerate(data.get('issues', data.get('all_issues', [])), 1):
                rule_id = issue.get('rule_id', f'SQL{i:03d}')
                severity = issue.get('severity', 'medium').lower()
                code_snippet = issue.get('code_snippet', '')
                recommendation = issue.get('recommendation', 'Review and refactor')
                confidence = issue.get('confidence', 75)

                # Build fix block - ALWAYS populated
                fix = {
                    'before': code_snippet if code_snippet else 'Current pattern',
                    'after': issue.get('what_good_looks_like', recommendation),
                    'label': 'Recommended',
                    'also_replace': None,
                }

                # Build evidence list - ALWAYS 2-3 items
                evidence = []
                if code_snippet:
                    evidence.append(f"Pattern: <code>{code_snippet}</code>")
                if issue.get('why_bad'):
                    evidence.append(issue['why_bad'])
                evidence.append(f"Confidence: {confidence}%")

                # Build scope
                scope = "Query scope"

                # Calculate improvement
                improvement_pct = min(50, confidence // 2)
                expected_improvement = f"−{improvement_pct}% estimated"

                issues.append({
                    'id': f"{rule_id}-{i}",
                    'rule_id': rule_id,
                    'severity': severity,
                    'title': issue.get('rule_name', issue.get('title', 'Issue')),
                    'explanation': issue.get('description', ''),
                    'location': {
                        'line': issue.get('line', 0),
                        'expression': code_snippet,
                    },
                    # All sections populated for demo-quality output
                    'fix': fix,
                    'evidence': evidence,
                    'scope': scope,
                    'expected_improvement': expected_improvement,
                    'impact_pct': self._penalty_to_impact(20) if severity == 'critical' else None,
                    'risk': 'Risk' if severity in ('high', 'medium') else None,
                    'risk_label': 'performance' if severity == 'high' else 'code quality' if severity == 'medium' else None,
                    'confidence': 'HIGH' if confidence >= 80 else 'MEDIUM',
                    # Existing fields for compatibility
                    'pattern': {
                        'description': issue.get('why_bad', ''),
                        'example': issue.get('what_good_looks_like', ''),
                        'note': recommendation,
                    },
                    'estimates': {
                        'effort': issue.get('effort_minutes', 15),
                        'improvement': f"{improvement_pct}%",
                    },
                    'llm_suggestion': issue.get('llm_suggestion'),
                    'reference_link': '',
                    'plan_operator': '',
                })

        # Sort by severity
        severity_order = {'critical': 0, 'high': 1, 'medium': 2, 'low': 3, 'info': 4}
        issues.sort(key=lambda x: severity_order.get(x['severity'], 4))

        return issues

    def _build_code_lines(
        self, sql_content: str, issues: list[dict]
    ) -> list[dict[str, Any]]:
        """Build code lines with syntax highlighting and issue markers."""
        lines = sql_content.splitlines() if sql_content else []
        code_lines = []

        # Build line -> issue map
        line_issues = {}
        for issue in issues:
            line = issue.get('location', {}).get('line') if isinstance(issue.get('location'), dict) else issue.get('line', 0)
            if line and line > 0:
                if line not in line_issues:
                    line_issues[line] = []
                line_issues[line].append(issue)

        for i, line in enumerate(lines, 1):
            issue_info = None
            if i in line_issues:
                first_issue = line_issues[i][0]
                issue_info = {
                    'id': first_issue['id'],
                    'label': first_issue['rule_id'],
                    'severity': first_issue['severity'],
                }

            code_lines.append({
                'number': i,
                'highlighted': self._highlight_sql(line),
                'issue': issue_info,
            })

        return code_lines

    def _highlight_sql(self, line: str) -> str:
        """Simple SQL syntax highlighting."""
        keywords = [
            'SELECT', 'FROM', 'WHERE', 'JOIN', 'LEFT', 'RIGHT', 'INNER', 'OUTER',
            'ON', 'AND', 'OR', 'NOT', 'IN', 'AS', 'GROUP', 'BY', 'ORDER', 'HAVING',
            'LIMIT', 'OFFSET', 'UNION', 'ALL', 'DISTINCT', 'WITH', 'CTE', 'INSERT',
            'UPDATE', 'DELETE', 'CREATE', 'DROP', 'ALTER', 'TABLE', 'INDEX', 'VIEW',
            'NULL', 'IS', 'LIKE', 'BETWEEN', 'CASE', 'WHEN', 'THEN', 'ELSE', 'END',
            'SUM', 'COUNT', 'AVG', 'MIN', 'MAX', 'COALESCE', 'CAST', 'OVER', 'PARTITION',
        ]

        result = line
        for kw in keywords:
            pattern = rf'\b({kw})\b'
            result = re.sub(pattern, r'<span class="kw">\1</span>', result, flags=re.IGNORECASE)

        return result

    def _build_schema_context(
        self, data: dict[str, Any], query_graph: Optional[dict] = None
    ) -> dict[str, Any]:
        """Build schema context for the template."""
        tables = []
        columns = []
        joins = []
        indexes = []

        if query_graph:
            # Extract from query graph
            nodes = query_graph.get('nodes', {})
            for name, node in nodes.items():
                if name != '__output__':
                    tables.append({
                        'name': name,
                        'type': node.get('type', 'table'),
                    })

            for join in query_graph.get('joins', []):
                joins.append({
                    'left': join.get('left', ''),
                    'right': join.get('right', ''),
                    'type': join.get('type', 'INNER'),
                })

            # Extract columns from lineage
            for lineage in query_graph.get('column_lineage', []):
                source = lineage.get('source') or ''
                if source and '.' in source:
                    table, col = source.rsplit('.', 1)
                    columns.append({
                        'name': col,
                        'table': table,
                        'data_type': 'unknown',
                        'is_indexed': False,
                    })

        return {
            'mode': 'extracted',
            'tables': tables,
            'columns': columns,
            'joins': joins,
            'indexes': indexes,
        }

    def _build_tasks(self, issues: list[dict]) -> list[dict[str, Any]]:
        """Build remediation task checklist."""
        tasks = []
        for issue in issues[:10]:  # Top 10 issues
            tasks.append({
                'rule_id': issue.get('rule_id', ''),
                'severity': issue.get('severity', 'medium'),
                'title': issue.get('title', 'Fix issue'),
                'detail': issue.get('pattern', {}).get('note', '') or issue.get('explanation', ''),
                'effort': f"{issue.get('estimates', {}).get('effort', 15)} min",
            })
        return tasks

    def _build_index_recommendations(
        self, issues: list[dict], schema_context: dict[str, Any]
    ) -> list[dict[str, Any]]:
        """Build index recommendations based on detected issues.

        Analyzes issues related to table scans, missing indexes, and join
        performance to suggest appropriate indexes.
        """
        recommendations = []
        seen_tables = set()
        related_issues = []

        # Keywords that suggest index would help
        index_indicators = ['scan', 'index', 'filter', 'join', 'sort', 'order']

        for issue in issues:
            rule_id = issue.get('rule_id', '')
            title = (issue.get('title', '') or '').lower()
            explanation = (issue.get('explanation', '') or '').lower()
            location = issue.get('location', {})
            expression = (location.get('expression', '') if isinstance(location, dict) else '') or ''

            # Check if this issue suggests an index could help
            suggests_index = any(kw in title or kw in explanation for kw in index_indicators)

            if suggests_index or 'SCAN' in rule_id.upper() or 'IDX' in rule_id.upper():
                # Try to extract table name from the issue
                table_name = None

                # Look in schema context tables
                for table in schema_context.get('tables', []):
                    tbl_name = table.get('name', '')
                    if tbl_name.lower() in expression.lower() or tbl_name.lower() in explanation:
                        table_name = tbl_name
                        break

                # Skip if we already recommended for this table
                if table_name and table_name in seen_tables:
                    continue
                if table_name:
                    seen_tables.add(table_name)

                # Extract column names from expression
                columns = []
                if expression:
                    words = re.findall(r'\b([a-z_][a-z0-9_]*)\b', expression.lower())
                    for word in words:
                        if word not in ['select', 'from', 'where', 'and', 'or', 'on', 'join', 'as']:
                            if word not in columns:
                                columns.append(word)

                if columns or table_name:
                    cols = columns[:3] if columns else ['id']  # Limit to 3 columns
                    tbl = table_name or 'table_name'
                    idx_name = f"idx_{tbl}_{'_'.join(cols[:2])}"

                    recommendations.append({
                        'sql': f"CREATE INDEX {idx_name} ON {tbl} ({', '.join(cols)});",
                        'rationale': f"Based on {rule_id}: {issue.get('title', 'detected issue')}",
                        'priority': issue.get('severity', 'medium'),
                        'addresses_issues': [rule_id],
                        'tradeoff': 'Additional storage and write overhead for improved query performance.',
                    })

        # Also check for joins without indexes
        for join in schema_context.get('joins', []):
            left_table = join.get('left', '')
            right_table = join.get('right', '')

            for tbl in [left_table, right_table]:
                if tbl and tbl not in seen_tables:
                    seen_tables.add(tbl)
                    recommendations.append({
                        'sql': f"CREATE INDEX idx_{tbl}_id ON {tbl} (id);",
                        'rationale': f"Join key column on {tbl} should be indexed for faster joins",
                        'priority': 'medium',
                        'addresses_issues': [],
                        'tradeoff': 'Primary key indexes are typically created automatically, verify your table definition.',
                    })

        return recommendations[:5]  # Limit to top 5 recommendations

    def _build_structural_observations(
        self, issues: list[dict], schema_context: dict[str, Any]
    ) -> list[dict[str, Any]]:
        """Build structural observations about the query.

        Provides high-level observations about query structure, complexity,
        and potential optimization opportunities.
        """
        observations = []

        # Count tables and joins
        num_tables = len(schema_context.get('tables', []))
        num_joins = len(schema_context.get('joins', []))
        num_columns = len(schema_context.get('columns', []))

        # Table complexity observation
        if num_tables > 5:
            observations.append({
                'type': 'complexity',
                'title': 'High Table Count',
                'description': f"Query involves {num_tables} tables. Consider breaking into smaller queries or using CTEs for clarity.",
                'severity': 'medium',
            })
        elif num_tables > 0:
            observations.append({
                'type': 'structure',
                'title': 'Table References',
                'description': f"Query references {num_tables} table(s) with {num_columns} column(s) in the output.",
                'severity': 'info',
            })

        # Join complexity observation
        if num_joins > 3:
            observations.append({
                'type': 'complexity',
                'title': 'Multiple Joins',
                'description': f"Query contains {num_joins} joins. Verify join order and consider using covering indexes.",
                'severity': 'medium',
            })
        elif num_joins > 0:
            observations.append({
                'type': 'structure',
                'title': 'Join Operations',
                'description': f"Query performs {num_joins} join operation(s).",
                'severity': 'info',
            })

        # Issue-based observations
        critical_count = sum(1 for i in issues if i.get('severity') == 'critical')
        high_count = sum(1 for i in issues if i.get('severity') == 'high')

        if critical_count > 0:
            observations.append({
                'type': 'risk',
                'title': 'Critical Issues Detected',
                'description': f"Found {critical_count} critical issue(s) that may cause significant performance degradation or incorrect results.",
                'severity': 'critical',
            })

        if high_count > 0:
            observations.append({
                'type': 'optimization',
                'title': 'High Priority Optimizations',
                'description': f"Found {high_count} high-priority optimization(s) that could improve performance by 20-50%.",
                'severity': 'high',
            })

        # Pattern observations based on rule types
        rule_categories = {}
        for issue in issues:
            rule_id = issue.get('rule_id', '')
            if '-' in rule_id:
                category = rule_id.split('-')[1] if len(rule_id.split('-')) > 1 else 'general'
            else:
                category = 'general'
            rule_categories[category] = rule_categories.get(category, 0) + 1

        for category, count in rule_categories.items():
            if count >= 2:
                observations.append({
                    'type': 'pattern',
                    'title': f'Recurring {category.upper()} Pattern',
                    'description': f"Multiple issues ({count}) detected in the {category} category. This may indicate a systematic pattern to address.",
                    'severity': 'medium',
                })

        return observations

    def render_from_sql(
        self,
        sql: str,
        file_name: str = "query.sql",
        llm_analysis: Optional[dict] = None,
        execution_plan: Optional[ExecutionPlanAnalysis] = None,
    ) -> str:
        """Render report directly from raw SQL with full analysis.

        This method runs the SQL parser and anti-pattern detector,
        then renders the combined output.

        Args:
            sql: Raw SQL query string
            file_name: Name of the source file
            llm_analysis: Optional LLM analysis to merge (for recommendations)
            execution_plan: Optional execution plan analysis from DuckDB or other database

        Returns:
            Complete HTML string with embedded JSON data
        """
        # Parse SQL structure
        query_graph = self.parser.parse(sql)

        # Detect anti-patterns
        detector_result = self.detector.analyze(sql, include_structure=False)

        # Build combined analysis data
        analysis_data = {
            "sql_content": sql,
            "file_name": file_name,
            "platform": self.dialect,
            # Parser outputs
            "query_graph": query_graph.to_dict(),
            # Detector outputs
            "detector_result": detector_result.to_dict(),
        }

        # Add execution plan if provided
        if execution_plan:
            analysis_data["execution_plan"] = execution_plan

        # Merge LLM analysis if provided
        if llm_analysis:
            analysis_data["llm_analysis"] = llm_analysis

        return self.render(analysis_data)

    def render_to_file(self, analysis_data: dict[str, Any], output_path: Path) -> None:
        """Render analysis data and write to file.

        Args:
            analysis_data: Analysis output from SQL analyzer
            output_path: Path to write HTML file
        """
        html = self.render(analysis_data)
        output_path = Path(output_path)
        output_path.write_text(html, encoding='utf-8')

    def transform_data(self, analysis_data: dict[str, Any]) -> dict[str, Any]:
        """Transform SQL analysis output to report JSON schema.

        Args:
            analysis_data: Output from SQL LLM analyzer, or combined parser/detector output

        Returns:
            Data matching the sql_audit.html JSON schema
        """
        summary = analysis_data.get('summary', {})
        sql_content = analysis_data.get('sql_content', '')
        file_name = analysis_data.get('file_name', 'query.sql')

        # Check for new parser/detector outputs
        query_graph = analysis_data.get('query_graph')
        detector_result = analysis_data.get('detector_result')

        # Get score: prefer detector result, then LLM, then calculate
        if detector_result:
            score = detector_result.get('score', 100)
        else:
            score = summary.get('torque_score', self._calculate_score(analysis_data))

        # Build report structure
        return {
            "report_id": f"qt-{hashlib.md5(file_name.encode()).hexdigest()[:8]}",
            "generated_at": datetime.now().strftime("%Y-%m-%dT%H:%M:%SZ"),
            "anti_pattern_library": "sql-patterns-v2.yaml",
            "query": {
                "name": Path(file_name).stem,
                "path": file_name,
                "platform": analysis_data.get('platform', 'sql'),
                "materialization": analysis_data.get('materialization', 'query')
            },
            "verdict": self._build_verdict(score, analysis_data),
            "execution_reality": self._build_execution_reality(analysis_data),
            "schema_map": self._build_schema_map(analysis_data, query_graph),
            "issues": self._build_issues(analysis_data, detector_result),
            "projected_improvement": self._calculate_projected_improvement(score),
            "source": self._build_source(sql_content, analysis_data, detector_result),
            # Include raw structured data for JSON export
            "structured_data": {
                "query_graph": query_graph,
                "anti_patterns": detector_result,
            } if query_graph or detector_result else None
        }

    def _calculate_score(self, data: dict[str, Any]) -> int:
        """Calculate Torque Score from issues."""
        score = 100

        all_issues = data.get('all_issues', [])
        for issue in all_issues:
            severity = issue.get('severity', 'medium').lower()
            if severity == 'critical':
                score -= 20
            elif severity == 'high':
                score -= 10
            elif severity == 'medium':
                score -= 5
            elif severity == 'low':
                score -= 2

        return max(0, min(100, score))

    def _build_verdict(self, score: int, data: dict[str, Any]) -> dict[str, Any]:
        """Build verdict section."""
        summary = data.get('summary', {})
        detector_result = data.get('detector_result')

        if score is None:
            status = "DENY"
            label = "Unknown"
        elif score >= 90:
            status = "PASS"
            label = "Peak Torque"
        elif score >= 70:
            status = "WARN"
            label = "Power Band"
        elif score >= 50:
            status = "FAIL"
            label = "Stall Zone"
        else:
            status = "DENY"
            label = "Redline"

        # Get primary issue from detector or legacy
        primary_issue = None
        primary_description = None

        if detector_result and detector_result.get('issues'):
            issues = detector_result['issues']
            # Get highest severity issue
            for issue in issues:
                if issue.get('severity', '').lower() == 'critical':
                    primary_issue = issue.get('rule_id')
                    primary_description = issue.get('description')
                    break
            if not primary_issue and issues:
                primary_issue = issues[0].get('rule_id')
                primary_description = issues[0].get('description')
        else:
            # Fallback to legacy format
            all_issues = data.get('all_issues', [])
            if all_issues:
                for issue in all_issues:
                    if issue.get('severity', '').lower() == 'critical':
                        primary_issue = issue.get('rule_id', 'UNKNOWN')
                        primary_description = issue.get('description')
                        break
                if not primary_issue and all_issues:
                    primary_issue = all_issues[0].get('rule_id', 'UNKNOWN')
                    primary_description = all_issues[0].get('description')

        # Get LLM summary or generate from detector
        llm_summary = summary.get('llm_summary', '')
        if not llm_summary and primary_description:
            llm_summary = primary_description
        if not llm_summary:
            llm_summary = "Analysis complete"

        # Build severity counts
        severity_counts = None
        if detector_result:
            severity_counts = detector_result.get('severity_counts')

        return {
            "status": status,
            "label": label,
            "score": score,
            "blocking": score is None or score < 50,
            "primary_issue": primary_issue,
            "summary": llm_summary,
            "severity_counts": severity_counts,
            "total_penalty": detector_result.get('total_penalty') if detector_result else None
        }

    def _build_execution_reality(self, data: dict[str, Any]) -> Optional[dict[str, Any]]:
        """Build execution reality section from execution plan data."""
        execution_plan = data.get('execution_plan')
        if not execution_plan:
            return None

        # Extract data from execution plan
        return {
            "duration_p50_ms": execution_plan.get('execution_time_ms'),
            "logical_reads": None,
            "bottleneck": execution_plan.get('bottleneck'),
            "cardinality": [],
            "wait_stats": [],
            "plan_tree": execution_plan.get('plan_tree', []),
        }

    def _build_schema_map(
        self, data: dict[str, Any], query_graph: Optional[dict] = None
    ) -> dict[str, Any]:
        """Build schema map section from parser output.

        Args:
            data: Analysis data (fallback)
            query_graph: Parsed query graph from SQLParser

        Returns:
            Schema map with CTE chain, columns, joins, data flow
        """
        if not query_graph:
            # Fallback to legacy format
            return {
                "cte_chain": data.get('cte_chain', []),
                "columns": data.get('columns', {}),
                "joins": data.get('joins', []),
                "cte_outputs": data.get('cte_outputs', {}),
                "data_flow": None,
            }

        nodes = query_graph.get('nodes', {})
        data_flow = query_graph.get('data_flow', {})
        column_lineage = query_graph.get('column_lineage', [])

        # Build CTE chain from execution order
        cte_chain = []
        execution_order = data_flow.get('execution_order', [])
        for node_name in execution_order:
            node = nodes.get(node_name, {})
            if node.get('type') == 'cte':
                analysis = node.get('analysis', {})
                cte_chain.append({
                    "name": node_name,
                    "columns_output": node.get('columns_output', []),
                    "source_tables": node.get('source_tables', []),
                    "analysis": {
                        "has_aggregation": analysis.get('has_aggregation', False),
                        "has_window": analysis.get('has_window', False),
                        "has_filter": analysis.get('has_filter', False),
                        "join_count": analysis.get('join_count', 0),
                        "row_multiplier": analysis.get('row_multiplier', 'unknown'),
                    }
                })

        # Build column lineage map (output column -> source columns)
        columns = {}
        for lineage in column_lineage:
            output_col = lineage.get('output', '')
            if output_col:
                col_name = output_col.split('.')[-1]  # Get just column name
                if col_name not in columns:
                    columns[col_name] = {
                        "sources": [],
                        "transformations": [],
                    }
                source = lineage.get('source')
                if source:
                    columns[col_name]["sources"].append(source)
                expr = lineage.get('expression')
                if expr and expr not in columns[col_name]["transformations"]:
                    columns[col_name]["transformations"].append(expr)

        # Build CTE outputs map
        cte_outputs = {}
        for node_name, node in nodes.items():
            if node.get('type') == 'cte':
                cte_outputs[node_name] = node.get('columns_output', [])

        # Build joins list
        joins = query_graph.get('joins', [])

        return {
            "cte_chain": cte_chain,
            "columns": columns,
            "joins": joins,
            "cte_outputs": cte_outputs,
            "data_flow": {
                "edges": data_flow.get('edges', []),
                "base_tables": data_flow.get('base_tables', []),
                "execution_order": execution_order,
            },
            "summary": query_graph.get('summary', {}),
        }

    def _build_issues(
        self, data: dict[str, Any], detector_result: Optional[dict] = None
    ) -> list[dict[str, Any]]:
        """Build issues list from detector output or legacy format.

        Args:
            data: Analysis data (fallback)
            detector_result: Output from SQLAntiPatternDetector

        Returns:
            List of issues formatted for the report
        """
        issues = []

        # Use detector result if available
        if detector_result and detector_result.get('issues'):
            for i, issue in enumerate(detector_result['issues'], 1):
                rule_id = issue.get('rule_id', f'SQL{i:03d}')
                location = issue.get('location', '')

                issues.append({
                    "id": f"{rule_id}-{i}",
                    "rule": rule_id,
                    "title": issue.get('name', 'Issue'),
                    "severity": issue.get('severity', 'medium').lower(),
                    "category": issue.get('category', 'general'),
                    "line": 0,  # Would need line tracking in detector
                    "snippet": issue.get('match', ''),
                    "problem": issue.get('description', ''),
                    "explanation": issue.get('explanation', ''),
                    "evidence": [issue.get('match')] if issue.get('match') else [],
                    "fix": {
                        "before": issue.get('match', ''),
                        "after": issue.get('suggestion', '')
                    },
                    "suggestion": issue.get('suggestion', ''),
                    "scope": {
                        "location": location,
                        "lines": []
                    },
                    "penalty": issue.get('penalty', 0),
                    "impact_pct": self._penalty_to_impact(issue.get('penalty', 0)),
                    "confidence": "HIGH",
                    "effort_minutes": self._severity_to_effort(issue.get('severity', 'medium'))
                })

            # Add severity counts as summary
            severity_counts = detector_result.get('severity_counts', {})
            issues_summary = {
                "total": len(issues),
                "critical": severity_counts.get('critical', 0),
                "high": severity_counts.get('high', 0),
                "medium": severity_counts.get('medium', 0),
                "low": severity_counts.get('low', 0),
                "total_penalty": detector_result.get('total_penalty', 0),
            }

        else:
            # Fallback to legacy format
            all_issues = data.get('all_issues', [])
            for i, issue in enumerate(all_issues, 1):
                rule_id = issue.get('rule_id', f'SQL{i:03d}')
                line = issue.get('line', 0)

                issues.append({
                    "id": f"{rule_id}-L{line}" if line else rule_id,
                    "rule": rule_id,
                    "title": issue.get('rule_name', issue.get('title', 'Issue')),
                    "severity": issue.get('severity', 'medium').lower(),
                    "line": line,
                    "snippet": issue.get('code_snippet', ''),
                    "problem": issue.get('description', ''),
                    "evidence": issue.get('evidence', []),
                    "fix": {
                        "before": issue.get('code_snippet', ''),
                        "after": issue.get('recommendation', '')
                    },
                    "scope": {
                        "lines": [line] if line else []
                    },
                    "impact_pct": issue.get('impact_pct', 0),
                    "confidence": "HIGH",
                    "effort_minutes": issue.get('effort_minutes', 15)
                })

        # Sort by severity
        severity_order = {'critical': 0, 'high': 1, 'medium': 2, 'low': 3, 'info': 4}
        issues.sort(key=lambda x: severity_order.get(x['severity'], 4))

        return issues

    def _penalty_to_impact(self, penalty: int) -> int:
        """Convert penalty points to estimated impact percentage."""
        if penalty >= 20:
            return 40
        elif penalty >= 15:
            return 30
        elif penalty >= 10:
            return 20
        elif penalty >= 5:
            return 10
        return 5

    def _severity_to_effort(self, severity: str) -> int:
        """Estimate fix effort in minutes based on severity."""
        effort_map = {
            'critical': 60,
            'high': 30,
            'medium': 15,
            'low': 5
        }
        return effort_map.get(severity.lower(), 15)

    def _calculate_projected_improvement(self, score: int) -> dict[str, Any]:
        """Calculate projected improvement."""
        if score is None or score < 30:
            return {"duration_reduction_pct": 80, "score_after": 85}
        elif score < 50:
            return {"duration_reduction_pct": 60, "score_after": 80}
        elif score < 70:
            return {"duration_reduction_pct": 40, "score_after": 85}
        else:
            return {"duration_reduction_pct": 20, "score_after": 95}

    def _build_source(
        self,
        sql_content: str,
        data: dict[str, Any],
        detector_result: Optional[dict] = None
    ) -> dict[str, Any]:
        """Build source section with SQL and annotations.

        Args:
            sql_content: Raw SQL string
            data: Analysis data
            detector_result: Output from anti-pattern detector

        Returns:
            Source section with SQL and issue annotations
        """
        lines = sql_content.splitlines() if sql_content else []

        # Build annotations from issues
        annotations = {}

        # Add detector issues (they don't have line numbers yet, but we can
        # try to find them in the SQL)
        if detector_result:
            for issue in detector_result.get('issues', []):
                match_text = issue.get('match', '')
                rule_id = issue.get('rule_id', 'SQL')
                if match_text:
                    # Try to find the line number
                    for i, line in enumerate(lines, 1):
                        if match_text.upper() in line.upper():
                            line_str = str(i)
                            if line_str not in annotations:
                                annotations[line_str] = []
                            if rule_id not in annotations[line_str]:
                                annotations[line_str].append(rule_id)
                            break  # Only annotate first occurrence

        # Also check legacy format
        for issue in data.get('all_issues', []):
            line = issue.get('line')
            if line:
                line_str = str(line)
                if line_str not in annotations:
                    annotations[line_str] = []
                rule_id = issue.get('rule_id', 'SQL')
                if rule_id not in annotations[line_str]:
                    annotations[line_str].append(rule_id)

        return {
            "path": data.get('file_name', 'query.sql'),
            "lines": len(lines),
            "annotations": annotations,
            "sql": sql_content
        }

    def _build_source_lines(
        self, sql_content: str, issues: list[dict]
    ) -> tuple[list[dict[str, Any]], dict[str, list[str]]]:
        """Build source lines with syntax highlighting for new template format.

        Args:
            sql_content: Raw SQL string
            issues: List of detected issues

        Returns:
            Tuple of (source_lines list, source_annotations dict)

        Note: If issues don't have line numbers, this method will attempt
        to find them by matching the issue expression text in the SQL.
        """
        lines = sql_content.splitlines() if sql_content else []
        source_lines = []
        source_annotations = {}

        # Pre-process: find line numbers for issues that don't have them
        for issue in issues:
            match_text = issue.get('location', {}).get('expression', '')
            current_line = issue.get('location', {}).get('line', 0)

            # If no line number but we have match text, search for it
            if match_text and current_line == 0:
                # Normalize match text for comparison
                match_upper = match_text.upper().strip()
                for i, line in enumerate(lines, 1):
                    line_upper = line.upper()
                    if match_upper in line_upper:
                        issue['location']['line'] = i
                        break

        # Build line -> issue map with severity
        line_issues = {}
        for issue in issues:
            line = issue.get('location', {}).get('line') if isinstance(issue.get('location'), dict) else issue.get('line', 0)
            if line and line > 0:
                line_str = str(line)
                if line not in line_issues:
                    line_issues[line] = []
                line_issues[line].append(issue)
                # Track annotations
                rule_id = issue.get('rule_id', '')
                if rule_id:
                    if line_str not in source_annotations:
                        source_annotations[line_str] = []
                    if rule_id not in source_annotations[line_str]:
                        source_annotations[line_str].append(rule_id)

        for i, line in enumerate(lines, 1):
            issue_info = None
            if i in line_issues:
                first_issue = line_issues[i][0]
                severity = first_issue.get('severity', 'medium').lower()
                # Map severity to CSS class
                severity_class = 'crit' if severity == 'critical' else 'high' if severity == 'high' else 'med'
                issue_info = {
                    'rule_id': first_issue.get('rule_id', ''),
                    'severity': severity,
                    'severity_class': severity_class,
                }

            source_lines.append({
                'number': i,
                'highlighted': self._highlight_sql_full(line),
                'issue': issue_info,
            })

        return source_lines, source_annotations

    def _highlight_sql_full(self, line: str) -> str:
        """Full SQL syntax highlighting with all token types.

        Provides highlighting for keywords, functions, strings, numbers,
        operators, comments, and dbt references.
        """
        import html

        # Escape HTML first
        escaped = html.escape(line)

        # Comments first (preserve them)
        if escaped.strip().startswith('--'):
            return f'<span class="cmt">{escaped}</span>'

        # Use placeholders to avoid nested replacements
        placeholders = []

        def save_placeholder(match):
            placeholders.append(match.group(0))
            return f'\x00{len(placeholders) - 1}\x00'

        # Check for dbt references {{ ref(...) }} or {{ config(...) }}
        dbt_pattern = r'(\{\{[^}]+\}\})'
        escaped = re.sub(dbt_pattern, lambda m: f'<span class="dbt">{m.group(1)}</span>', escaped)

        # Strings (single quotes) - protect from further processing
        string_pattern = r"('(?:[^'\\]|\\.)*')"
        escaped = re.sub(string_pattern, lambda m: f'<span class="str">{m.group(1)}</span>', escaped)

        # Numbers - only outside of already highlighted spans
        num_pattern = r'\b(\d+(?:\.\d+)?)\b'
        escaped = re.sub(num_pattern, r'<span class="num">\1</span>', escaped)

        # SQL keywords (case insensitive)
        keywords = [
            'SELECT', 'FROM', 'WHERE', 'JOIN', 'LEFT', 'RIGHT', 'INNER', 'OUTER',
            'CROSS', 'ON', 'AND', 'OR', 'NOT', 'IN', 'AS', 'GROUP', 'BY', 'ORDER',
            'HAVING', 'LIMIT', 'OFFSET', 'UNION', 'ALL', 'DISTINCT', 'WITH', 'INSERT',
            'UPDATE', 'DELETE', 'CREATE', 'DROP', 'ALTER', 'TABLE', 'INDEX', 'VIEW',
            'NULL', 'IS', 'LIKE', 'BETWEEN', 'CASE', 'WHEN', 'THEN', 'ELSE', 'END',
            'TRUE', 'FALSE', 'ASC', 'DESC', 'OVER', 'PARTITION', 'ROWS', 'RANGE',
            'PRECEDING', 'FOLLOWING', 'UNBOUNDED', 'CURRENT', 'ROW',
        ]
        for kw in keywords:
            # Only match if not inside a span tag
            pattern = rf'(?<!["\w])({kw})(?!["\w])'
            escaped = re.sub(pattern, r'<span class="kw">\1</span>', escaped, flags=re.IGNORECASE)

        # SQL functions (before opening paren)
        functions = [
            'SUM', 'COUNT', 'AVG', 'MIN', 'MAX', 'COALESCE', 'NULLIF', 'CAST',
            'CONVERT', 'ROUND', 'FLOOR', 'CEILING', 'ABS', 'UPPER', 'LOWER',
            'TRIM', 'SUBSTRING', 'LENGTH', 'CONCAT', 'DATE_TRUNC', 'DATE_PART',
            'DATEADD', 'DATEDIFF', 'CURRENT_DATE', 'CURRENT_TIMESTAMP', 'NOW',
            'ROW_NUMBER', 'RANK', 'DENSE_RANK', 'LAG', 'LEAD', 'FIRST_VALUE',
            'LAST_VALUE', 'NTH_VALUE', 'MEDIAN', 'PERCENTILE', 'LISTAGG',
        ]
        for fn in functions:
            pattern = rf'\b({fn})\s*\('
            escaped = re.sub(pattern, r'<span class="fn">\1</span>(', escaped, flags=re.IGNORECASE)

        return escaped

    def _build_template_schema_map(
        self,
        data: dict[str, Any],
        query_graph: Optional[dict] = None,
        sql_content: str = "",
    ) -> Optional[dict[str, Any]]:
        """Build schema map for new template format.

        Returns a structure compatible with the new template sections:
        - cte_chain: list of {name, rows, columns} with column NAMES not counts
        - columns: list of {name, type, note, problem} with inferred types
        - joins: list of {left, right, type, ok, problem}
        - focus_cte: name of the CTE to focus on
        """
        graph_obj = None
        if sql_content:
            graph_obj = self.parser.parse(sql_content)

        graph_dict = query_graph or (graph_obj.to_dict() if graph_obj else None)
        if not graph_dict:
            return None

        nodes = graph_dict.get('nodes', {})
        data_flow = graph_dict.get('data_flow', {})
        schema_stats = data.get("schema_stats") or {}
        row_counts = schema_stats.get("row_counts", {})
        column_cardinality = schema_stats.get("column_cardinality", {})
        join_profiles = {}
        if graph_obj:
            profile_report = JoinProfiler(schema_stats=schema_stats).profile(graph_obj)
            for profile in profile_report.joins:
                key = self._join_profile_key(
                    profile.left_table,
                    profile.left_column,
                    profile.right_table,
                    profile.right_column,
                    profile.join_type,
                    profile.operator,
                )
                join_profiles[key] = profile

        # Build CTE chain with column NAMES not counts
        cte_chain = []
        execution_order = data_flow.get('execution_order', [])
        for node_name in execution_order:
            if node_name == '__output__':
                cte_chain.append({
                    'name': 'final',
                    'rows': '',
                    'columns': '',
                })
            else:
                node = nodes.get(node_name, {})
                if node.get('type') in ('cte', 'base_table'):
                    cols = node.get('columns_output', [])
                    # Show column names like "[stage_bk, probability]" not "5 cols"
                    col_display = ''
                    if cols:
                        col_names = [c for c in cols if c != '*'][:3]
                        if col_names:
                            suffix = '...' if len(cols) > 3 else ''
                            col_display = f"[{', '.join(col_names)}{suffix}]"
                        elif '*' in cols:
                            col_display = '[*]'
                    cte_chain.append({
                        'name': node_name,
                        'rows': '',  # From execution plan if available
                        'columns': col_display,
                    })

        # Build columns list with inferred types
        columns = []
        output_node = nodes.get('__output__', {})
        output_columns = output_node.get('columns_output', [])
        if not output_columns or all(col == '*' for col in output_columns):
            join_columns: list[str] = []
            seen = set()
            for join in graph_dict.get('joins', []):
                left_col = self._split_join_ref(join.get('left', ''))[1]
                right_col = self._split_join_ref(join.get('right', ''))[1]
                for col_name in (left_col, right_col):
                    if col_name and col_name not in seen:
                        seen.add(col_name)
                        join_columns.append(col_name)
            if join_columns:
                output_columns = join_columns
            else:
                output_columns = [col for col in output_columns if col != '*']

        for col in output_columns:
            columns.append({
                'name': col,
                'type': self._infer_column_type(col),
                'note': '',
                'problem': False,
            })

        # Build joins list with OK/problem markers and cardinality
        joins = []
        for join in graph_dict.get('joins', []):
            join_type = join.get('type', 'JOIN').upper()
            left = join.get('left', '—')
            right = join.get('right', '')
            operator = join.get('operator', '')
            left_table, left_column = self._split_join_ref(left)
            right_table, right_column = self._split_join_ref(right)

            # Infer cardinality from column naming conventions
            cardinality = self._infer_join_cardinality(left, right)
            profile = join_profiles.get(
                self._join_profile_key_from_strings(left, right, join_type, operator)
            )

            joins.append({
                'left': left,
                'right': right,
                'type': join_type,
                'cardinality': cardinality,
                'ok': join_type not in ('CROSS',),
                'problem': join_type == 'CROSS',
                'line_range': join.get('line_range'),
                'profile': profile.to_dict() if profile else None,
                'stats': {
                    'left_rows': row_counts.get(left_table),
                    'right_rows': row_counts.get(right_table),
                    'left_key_cardinality': self._get_column_cardinality(
                        column_cardinality, left_table, left_column
                    ),
                    'right_key_cardinality': self._get_column_cardinality(
                        column_cardinality, right_table, right_column
                    ),
                } if left_table or right_table else None,
            })

        table_stats = self._build_table_stats(graph_dict, row_counts, column_cardinality)

        cte_dependencies = []
        for edge in data_flow.get('cte_edges', []):
            from_node = nodes.get(edge.get('from', ''), {})
            to_node = nodes.get(edge.get('to', ''), {})
            cte_dependencies.append({
                'from': edge.get('from'),
                'to': edge.get('to'),
                'from_line': (from_node.get('line_range') or {}).get('start_line'),
                'to_line': (to_node.get('line_range') or {}).get('start_line'),
            })

        return {
            'cte_chain': cte_chain if cte_chain else None,
            'columns': columns if columns else None,
            'joins': joins if joins else None,
            'focus_cte': 'final',
            'cte_dependencies': cte_dependencies if cte_dependencies else None,
            'tables': table_stats if table_stats else None,
        }

    def _join_profile_key(
        self,
        left_table: str,
        left_column: str,
        right_table: str,
        right_column: str,
        join_type: str,
        operator: str,
    ) -> str:
        return f"{left_table}.{left_column}|{right_table}.{right_column}|{join_type}|{operator}"

    def _join_profile_key_from_strings(
        self,
        left: str,
        right: str,
        join_type: str,
        operator: str,
    ) -> str:
        return f"{left}|{right}|{join_type}|{operator}"

    def _split_join_ref(self, value: str) -> tuple[str, str]:
        if not value or "." not in value:
            return "", ""
        table, column = value.split(".", 1)
        return table, column

    def _get_column_cardinality(
        self,
        column_cardinality: dict[str, dict[str, int]],
        table: str,
        column: str,
    ) -> Optional[int]:
        if not table or not column:
            return None
        table_stats = column_cardinality.get(table) or column_cardinality.get(table.lower())
        if not table_stats:
            return None
        value = table_stats.get(column) or table_stats.get(column.lower())
        return int(value) if value is not None else None

    def _build_table_stats(
        self,
        graph_dict: dict[str, Any],
        row_counts: dict[str, int],
        column_cardinality: dict[str, dict[str, int]],
    ) -> list[dict[str, Any]]:
        tables = []
        join_keys = {}
        for join in graph_dict.get("joins", []):
            left_table, left_column = self._split_join_ref(join.get("left", ""))
            right_table, right_column = self._split_join_ref(join.get("right", ""))
            if left_table and left_column:
                join_keys.setdefault(left_table, set()).add(left_column)
            if right_table and right_column:
                join_keys.setdefault(right_table, set()).add(right_column)

        for table in graph_dict.get("data_flow", {}).get("base_tables", []):
            keys = sorted(join_keys.get(table, set()))
            tables.append({
                "name": table,
                "row_count": row_counts.get(table),
                "join_keys": [
                    {
                        "name": key,
                        "cardinality": self._get_column_cardinality(
                            column_cardinality, table, key
                        ),
                    }
                    for key in keys
                ],
            })

        return tables

    def _infer_column_type(self, col_name: str) -> str:
        """Infer column type from naming conventions.

        Used when schema data is not available to provide best-effort type hints.
        """
        if not col_name:
            return ''
        name = col_name.lower()

        # Primary/foreign keys
        if name == 'id':
            return 'INT · PK'
        if name.endswith('_id') or name.endswith('_sid'):
            return 'INT · FK'

        # Dates and timestamps
        if name.endswith('_at') or name.endswith('_date') or name in ('created', 'updated', 'deleted'):
            return 'TIMESTAMP'

        # Numeric types
        if name.endswith('_amount') or name.endswith('_total') or name.endswith('_price'):
            return 'DECIMAL'
        if name.endswith('_pct') or name.endswith('_rate') or name.endswith('_ratio'):
            return 'FLOAT'
        if name.endswith('_count') or name.endswith('_qty') or name == 'quantity':
            return 'INT'

        # String types
        if name.endswith('_name') or name.endswith('_bk') or name in ('name', 'description', 'title'):
            return 'VARCHAR'
        if name.endswith('_status') or name.endswith('_type') or name == 'status':
            return 'VARCHAR'

        # Boolean types
        if name.startswith('is_') or name.startswith('has_') or name.startswith('can_'):
            return 'BOOLEAN'

        # Rank/row_number common names
        if name in ('rank', 'row_number', 'rn', 'dense_rank'):
            return 'INT'

        return ''

    def _infer_join_cardinality(self, left: str, right: str) -> str:
        """Infer join cardinality from column naming conventions.

        Args:
            left: Left side of join (e.g., "customers.id")
            right: Right side of join (e.g., "orders.customer_id")

        Returns:
            Cardinality string: "1:1", "1:N", "N:1", or "N:M"
        """
        # Extract column names
        left_col = left.split('.')[-1].lower() if '.' in left else left.lower()
        right_col = right.split('.')[-1].lower() if '.' in right else right.lower()

        left_is_pk = left_col == 'id' or left_col.endswith('_pk')
        right_is_pk = right_col == 'id' or right_col.endswith('_pk')
        left_is_fk = left_col.endswith('_id') and left_col != 'id'
        right_is_fk = right_col.endswith('_id') and right_col != 'id'

        # Determine cardinality
        if left_is_pk and right_is_fk:
            return '1:N'  # One left row matches many right rows
        elif left_is_fk and right_is_pk:
            return 'N:1'  # Many left rows match one right row
        elif left_is_pk and right_is_pk:
            return '1:1'  # One-to-one relationship
        else:
            return 'N:M'  # Many-to-many or unknown

    def _build_template_execution_reality(
        self, data: dict[str, Any]
    ) -> Optional[dict[str, Any]]:
        """Build execution summary for template.

        Returns a focused summary with actionable insights:
        - top_operators: operators that make up 80% of execution time
        - full_scans: list of table names being fully scanned
        - duration_ms: total execution time
        - cardinality: row counts per operator
        - duration_after_ms: optimized query time (if available)
        - speedup_ratio: improvement factor (if available)
        """
        execution_plan = data.get('execution_plan')
        if not execution_plan:
            return None

        if not isinstance(execution_plan, dict):
            # ExecutionPlanAnalysis object - convert to dict
            if hasattr(execution_plan, 'to_template_context'):
                execution_plan = execution_plan.to_template_context()
            else:
                return None

        # Handle both 'operators' and 'plan_tree' formats
        operators = execution_plan.get('operators', [])
        if not operators:
            # Try plan_tree format from DuckDB explain
            plan_tree = execution_plan.get('plan_tree', [])
            if plan_tree:
                import re
                operators = []
                for op in plan_tree:
                    # Extract table name from details field
                    # e.g., "Table: customers; Type: Sequential Scan; Projections: id, name"
                    details = op.get('details', '')
                    table_name = None
                    if details and 'Table:' in details:
                        match = re.search(r'Table:\s*(\w+)', details)
                        if match:
                            table_name = match.group(1)

                    op_name = op.get('operator', 'Unknown')
                    display_name = f"{op_name}: {table_name}" if table_name else op_name

                    operators.append({
                        'name': display_name,
                        'table': table_name,
                        'time_ms': op.get('timing_ms', 0),
                        'actual_rows': op.get('rows', 0),
                    })

        if not operators:
            return None

        # Get total time from various possible fields
        total_time = (
            execution_plan.get('total_time_ms') or
            execution_plan.get('execution_time_ms') or
            (execution_plan.get('total_cost', 0) * 1000)  # Convert seconds to ms
        )

        # If no timing data, don't show execution reality (it would be meaningless)
        if total_time <= 0:
            return None

        # Build list of operators with timing, sorted by time desc
        ops_with_time = []
        for op in operators:
            op_time = op.get('time_ms', 0)
            pct = round(op_time / total_time * 100) if total_time > 0 else 0
            ops_with_time.append({
                'operator': op.get('name', 'Unknown'),
                'pct': pct,
                'time_ms': op_time,
            })
        ops_with_time.sort(key=lambda x: -x['time_ms'])

        # Get top operators that make up 80% of time
        top_operators = []
        cumulative = 0
        for op in ops_with_time:
            if cumulative >= 80:
                break
            top_operators.append(op)
            cumulative += op['pct']

        # Find full table scans
        full_scans = []
        for op in operators:
            op_name = op.get('name', '')
            if any(scan in op_name.upper() for scan in ['SEQ SCAN', 'TABLE_SCAN', 'FULL SCAN', 'TABLE SCAN']):
                # Use extracted table name if available
                table = op.get('table')
                if table:
                    full_scans.append(table)
                elif ':' in op_name:
                    # Fallback to parsing from name
                    table = op_name.split(':')[1].strip()
                    if table:
                        full_scans.append(table)
                # Don't add operator name as fallback - it's useless

        # Build cardinality (row counts per operator)
        cardinality = []
        for op in operators:
            actual = op.get('actual_rows', 0)
            if actual > 0:
                cardinality.append({
                    'operator': op.get('name', ''),
                    'rows': actual,
                })

        # Get performance comparison data if available
        # Only show comparison data when has_comparison flag is True
        # This prevents showing performance section on first audit (before optimization)
        performance_comparison = data.get('performance_comparison')
        duration_after_ms = None
        speedup_ratio = None
        improvement_percent = None
        original_time_ms = None
        performance_dashboard = None

        if performance_comparison and performance_comparison.get('has_comparison'):
            duration_after_ms = performance_comparison.get('optimized_time_ms')
            speedup_ratio = performance_comparison.get('speedup_ratio')
            improvement_percent = performance_comparison.get('improvement_percent')
            original_time_ms = performance_comparison.get('original_time_ms')
            performance_dashboard = self._build_performance_dashboard(
                performance_comparison=performance_comparison,
                fallback_original_ms=total_time,
                fallback_optimized_ms=duration_after_ms,
            )

        if performance_dashboard and performance_dashboard.get('speedup_ratio') is not None:
            speedup_ratio = performance_dashboard['speedup_ratio']

        # Extract additional plan details
        plan_tree = execution_plan.get('plan_tree', [])
        bottleneck = execution_plan.get('bottleneck')
        warnings = execution_plan.get('warnings', [])

        return {
            'top_operators': top_operators if top_operators else None,
            'full_scans': full_scans if full_scans else None,
            'duration_ms': total_time if total_time else None,
            'cardinality': cardinality if cardinality else None,
            'duration_after_ms': duration_after_ms,
            'speedup_ratio': speedup_ratio,
            'improvement_percent': improvement_percent,
            'original_time_ms': original_time_ms,
            'has_comparison': bool(performance_dashboard),
            'performance_dashboard': performance_dashboard,
            # Additional plan details for Section 3
            'plan_tree': plan_tree if plan_tree else None,
            'bottleneck': bottleneck,
            'warnings': warnings if warnings else None,
        }

    def _build_performance_dashboard(
        self,
        performance_comparison: dict[str, Any],
        fallback_original_ms: Optional[float] = None,
        fallback_optimized_ms: Optional[float] = None,
    ) -> Optional[dict[str, Any]]:
        """Build performance dashboard metrics with safe defaults."""
        original_ms = performance_comparison.get('original_time_ms')
        optimized_ms = performance_comparison.get('optimized_time_ms')
        original_rows_examined = performance_comparison.get('original_rows_examined')
        optimized_rows_examined = performance_comparison.get('optimized_rows_examined')

        if original_ms is None:
            original_ms = fallback_original_ms
        if optimized_ms is None:
            optimized_ms = fallback_optimized_ms

        has_row_data = original_rows_examined is not None or optimized_rows_examined is not None
        if original_ms is None and optimized_ms is None and not has_row_data:
            return None

        time_saved_ms = None
        time_change_pct = None
        speedup_ratio = None

        if original_ms is not None and optimized_ms is not None:
            time_saved_ms = original_ms - optimized_ms
            if original_ms > 0:
                time_change_pct = ((optimized_ms - original_ms) / original_ms) * 100
            if optimized_ms > 0:
                speedup_ratio = original_ms / optimized_ms

        max_time = max(
            [value for value in (original_ms, optimized_ms) if value is not None and value > 0],
            default=0,
        )
        original_bar_pct = (original_ms / max_time * 100) if original_ms is not None and max_time > 0 else 0
        optimized_bar_pct = (optimized_ms / max_time * 100) if optimized_ms is not None and max_time > 0 else 0

        is_improvement = time_change_pct is not None and time_change_pct < 0
        is_regression = time_change_pct is not None and time_change_pct > 0
        speedup_class = "positive" if is_improvement else "negative" if is_regression else "neutral"
        time_saved_class = (
            "positive" if time_saved_ms is not None and time_saved_ms > 0
            else "negative" if time_saved_ms is not None and time_saved_ms < 0
            else "neutral"
        )

        return {
            "original_ms": original_ms,
            "optimized_ms": optimized_ms,
            "original_rows_examined": original_rows_examined,
            "optimized_rows_examined": optimized_rows_examined,
            "speedup_ratio": speedup_ratio,
            "time_saved_ms": time_saved_ms,
            "time_change_pct": time_change_pct,
            "original_bar_pct": original_bar_pct,
            "optimized_bar_pct": optimized_bar_pct,
            "is_improvement": is_improvement,
            "is_regression": is_regression,
            "speedup_class": speedup_class,
            "time_saved_class": time_saved_class,
        }
