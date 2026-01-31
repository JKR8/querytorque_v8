"""DAX report renderer for QueryTorque.

Transforms vpax_analyzer output into self-contained HTML reports using
Jinja2 templating with dax_report.html.j2.
"""

from pathlib import Path
from datetime import datetime
from typing import Any
from jinja2 import Environment, FileSystemLoader


def get_quality_gate_label(score: int) -> str:
    """Get quality gate label from score."""
    if score is None:
        return "Unknown"
    if score >= 90:
        return "Peak Torque"
    elif score >= 70:
        return "Power Band"
    elif score >= 50:
        return "Stall Zone"
    else:
        return "Redline"


class DAXRenderer:
    """Render DAX efficiency audit reports.

    Transforms vpax_analyzer.py output (DiagnosticReport) into HTML using
    the dax_report.html.j2 Jinja2 template.
    """

    def __init__(self):
        # Templates are in qt_dax/templates/
        template_dir = Path(__file__).parent.parent / 'templates'
        self.env = Environment(
            loader=FileSystemLoader(template_dir),
            autoescape=True
        )
        # Add custom filters
        self.env.filters['number_format'] = self._number_format

    def _number_format(self, value: int | float) -> str:
        """Format number with thousand separators."""
        if value is None:
            return "-"
        return f"{value:,.0f}"

    def render(self, analyzer_output: dict[str, Any]) -> str:
        """Render complete HTML report from analyzer output.

        Args:
            analyzer_output: Output from vpax_analyzer.py (DiagnosticReport as dict)

        Returns:
            Complete HTML string
        """
        template = self.env.get_template('dax_report.html.j2')
        context = self._build_template_context(analyzer_output)
        return template.render(**context)

    def render_to_file(self, analyzer_output: dict[str, Any], output_path: Path) -> Path:
        """Render and write to file.

        Args:
            analyzer_output: Raw output from the analyzer
            output_path: Path to write the HTML report

        Returns:
            The output path
        """
        html = self.render(analyzer_output)
        output_path = Path(output_path)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.write_text(html, encoding='utf-8')
        return output_path

    def _build_template_context(self, data: dict[str, Any]) -> dict[str, Any]:
        """Build template context from analyzer output.

        Args:
            data: DiagnosticReport as dict

        Returns:
            Context dict for Jinja2 template
        """
        summary = data.get('summary') or {}
        tables = data.get('tables') or []
        measures = data.get('measures') or []
        relationships = data.get('relationships') or []
        all_issues = data.get('all_issues') or []
        largest_tables = data.get('largest_tables') or []
        highest_cardinality_columns = data.get('highest_cardinality_columns') or []

        # Get Torque Score
        torque_score = summary.get('torque_score', 0)

        # Build quality gate
        quality_gate = summary.get('quality_gate')
        if quality_gate:
            if hasattr(quality_gate, 'status'):
                qg = {'status': quality_gate.status, 'label': quality_gate.label}
            else:
                qg = quality_gate
        else:
            qg = self._calculate_quality_gate(torque_score)

        quality_gate_label = "Unknown" if torque_score is None else get_quality_gate_label(torque_score)

        # Determine quality band class for styling
        quality_band_class = self._get_quality_band_class(torque_score)

        # score_class for backwards compatibility (critical/high/medium/good)
        score_class = self._get_score_class(torque_score)

        # Build severity counts
        severity_counts = {
            'critical': summary.get('critical_count', 0),
            'high': summary.get('high_count', 0),
            'medium': summary.get('medium_count', 0),
            'low': summary.get('low_count', 0),
            'info': summary.get('info_count', 0),
        }

        # Build headline from issues
        headline = self._build_headline(all_issues, severity_counts)

        # Model stats
        total_rows = sum(t.get('row_count', 0) for t in tables)
        total_size_bytes = summary.get('total_size_bytes', 0)
        size_mb = round(total_size_bytes / (1024 * 1024), 1) if total_size_bytes else 0

        bidir_count = len([r for r in relationships
                          if r.get('cross_filter') == 'Both' or r.get('cross_filter_direction') == 'Both'])

        inactive_count = len([r for r in relationships if not r.get('is_active', True)])

        high_card_cols = self._get_high_cardinality_columns(tables)
        if not high_card_cols and highest_cardinality_columns:
            high_card_cols = self._normalize_high_cardinality_columns(highest_cardinality_columns)

        # Calculate wasted space
        local_date_size = summary.get('local_date_table_size_bytes', 0)
        wasted_pct = round((local_date_size / total_size_bytes) * 100, 1) if total_size_bytes > 0 else 0

        model_stats = {
            'size_mb': size_mb,
            'table_count': summary.get('total_tables', len([t for t in tables if not t.get('is_local_date_table')])),
            'total_rows': total_rows,
            'measure_count': summary.get('total_measures', len(measures)),
            'relationship_count': summary.get('total_relationships', len(relationships)),
            'bidir_count': bidir_count,
            'inactive_count': inactive_count,
            'inactive_unused': inactive_count,
            'column_count': summary.get('total_columns', 0),
            'high_cardinality_count': len(high_card_cols),
            'wasted_pct': wasted_pct,
        }

        # Build model size breakdown
        model_size_breakdown = self._build_model_size_breakdown(tables, summary)

        # Build table sizes
        table_sizes = self._build_table_sizes(tables, total_size_bytes)

        # Build relationship issues
        relationship_issues = self._build_relationship_issues(relationships)

        # Build Mermaid diagram for relationships
        relationships_mermaid = self._build_mermaid_diagram(tables, relationships)

        # Build measure issues
        measure_issues = self._build_measure_issues(measures, all_issues)

        # Build architecture issues (model-level)
        architecture_issues = self._build_architecture_issues(all_issues, summary)

        # Build calc group issues
        calc_group_issues = self._build_calc_group_issues(all_issues)

        # Categorize issues for tabs
        dax_issues = [i for i in measure_issues]
        model_issues = [i for i in architecture_issues]

        # Build tasks (remediation checklist)
        tasks = self._build_tasks(all_issues)

        # Total issue count
        issue_count = sum(severity_counts.values())

        # Estimate improvement based on score
        if torque_score is None:
            improvement_estimate = "Unknown"
        elif torque_score < 50:
            improvement_estimate = "3-5x"
        elif torque_score < 70:
            improvement_estimate = "2-3x"
        elif torque_score < 90:
            improvement_estimate = "1.5-2x"
        else:
            improvement_estimate = "Optimal"

        optimization_context = self._build_optimization_context(
            tables=tables,
            largest_tables=largest_tables,
            highest_cardinality_columns=highest_cardinality_columns,
            relationships=relationships,
            total_size_bytes=total_size_bytes,
            total_rows=total_rows,
        )

        return {
            'model_name': summary.get('file_name', 'Unknown Model'),
            'generated_date': datetime.now().strftime("%Y-%m-%d %H:%M"),
            'client_name': None,
            'version': '2.0',
            'model_stats': model_stats,
            'quality_band_class': quality_band_class,
            'score_class': score_class,
            'summary': {
                'torque_score': torque_score,
                'quality_gate': qg,
                'quality_gate_label': quality_gate_label,
                'headline': headline,
                'issue_count': issue_count,
                'severity_counts': severity_counts,
                'improvement_estimate': improvement_estimate,
                'tech_debt_hours': summary.get('tech_debt_hours'),
            },
            'optimization_context': optimization_context,
            'model_size_breakdown': model_size_breakdown,
            'engine_distribution': None,
            'relationships_mermaid': relationships_mermaid,
            'table_sizes': table_sizes,
            'relationship_issues': relationship_issues,
            'high_cardinality_columns': high_card_cols,
            'measure_issues': measure_issues,
            'architecture_issues': architecture_issues,
            'calc_group_issues': calc_group_issues,
            'dax_issues': dax_issues,
            'model_issues': model_issues,
            'tasks': tasks,
            'quality_gate_label': quality_gate_label,
        }

    def _get_quality_band_class(self, score: int) -> str:
        """Get CSS class for quality band styling."""
        if score is None:
            return 'redline'
        if score >= 90:
            return 'peak'
        elif score >= 70:
            return 'power'
        elif score >= 50:
            return 'stall'
        else:
            return 'redline'

    def _get_score_class(self, score: int) -> str:
        """Get backwards-compatible score class."""
        if score is None:
            return 'critical'
        if score >= 90:
            return 'good'
        elif score >= 70:
            return 'medium'
        elif score >= 50:
            return 'high'
        else:
            return 'critical'

    def _calculate_quality_gate(self, score: int) -> dict[str, str]:
        """Calculate quality gate from score."""
        if score is None:
            return {'status': 'deny', 'label': 'Unknown'}
        label = get_quality_gate_label(score)
        if score >= 90:
            return {'status': 'pass', 'label': label}
        elif score >= 70:
            return {'status': 'warn', 'label': label}
        elif score >= 50:
            return {'status': 'fail', 'label': label}
        else:
            return {'status': 'deny', 'label': label}

    def _build_headline(self, all_issues: list, severity_counts: dict) -> str:
        """Build headline text from issues."""
        critical = severity_counts.get('critical', 0)
        high = severity_counts.get('high', 0)
        total = sum(severity_counts.values())

        if critical > 0:
            critical_issues = [i for i in all_issues if i.get('severity') == 'critical']
            if critical_issues:
                categories = {}
                for issue in critical_issues:
                    cat = issue.get('category', 'unknown')
                    categories[cat] = categories.get(cat, 0) + 1

                top_cat = max(categories.items(), key=lambda x: x[1])

                if top_cat[0] == 'date_table' or any(i.get('rule_id') == 'MDL001' for i in critical_issues):
                    date_count = len([i for i in critical_issues if i.get('rule_id') == 'MDL001' or i.get('category') == 'date_table'])
                    if date_count > 0:
                        return f"{date_count} auto date/time tables consuming unnecessary space"
                elif top_cat[0] == 'dax_anti_pattern':
                    return f"{critical} critical DAX anti-patterns detected"
                elif top_cat[0] == 'model_structure' or top_cat[0] == 'cardinality':
                    return f"{critical} critical model structure issues found"
                else:
                    return f"{critical} critical issues require immediate attention"

        if high > 0:
            return f"{high} high-severity optimization opportunities identified"

        if total > 0:
            return f"Model analysis complete: {total} improvement opportunities"

        return "Model analysis complete: No significant issues found"

    def _build_model_size_breakdown(self, tables: list, summary: dict) -> dict | None:
        """Build model size breakdown for visualization."""
        total_size = summary.get('total_size_bytes', 0)
        if not total_size:
            return None

        columns = []
        for table in tables:
            columns.extend(table.get('columns', []))

        total_data_size = sum(c.get('data_size', 0) or c.get('TotalSize', 0) - c.get('dictionary_size', 0) or c.get('DictionarySize', 0) for c in columns)
        total_dict_size = sum(c.get('dictionary_size', 0) or c.get('DictionarySize', 0) for c in columns)

        wasted_size = summary.get('local_date_table_size_bytes', 0)
        auto_datetime_count = summary.get('local_date_table_count', 0)

        rel_size = total_size - total_data_size - total_dict_size - wasted_size
        if rel_size < 0:
            rel_size = 0

        is_estimated = False
        if total_data_size == 0 and total_dict_size == 0:
            actual_data = total_size - wasted_size
            total_data_size = int(actual_data * 0.6)
            total_dict_size = int(actual_data * 0.4)
            rel_size = 0
            is_estimated = True

        return {
            'is_estimated': is_estimated,
            'data_size': self._format_bytes(total_data_size),
            'data_pct': round((total_data_size / total_size) * 100, 1) if total_size else 0,
            'dict_size': self._format_bytes(total_dict_size),
            'dict_pct': round((total_dict_size / total_size) * 100, 1) if total_size else 0,
            'rel_size': self._format_bytes(rel_size) if rel_size > 1024 else None,
            'rel_pct': round((rel_size / total_size) * 100, 1) if total_size and rel_size > 1024 else 0,
            'wasted_size': self._format_bytes(wasted_size),
            'wasted_pct': round((wasted_size / total_size) * 100, 1) if total_size else 0,
            'auto_datetime_count': auto_datetime_count,
        }

    def _format_bytes(self, bytes_val: int) -> str:
        """Format bytes as human-readable string."""
        if bytes_val is None or bytes_val == 0:
            return "0 B"
        if bytes_val < 1024:
            return f"{bytes_val} B"
        elif bytes_val < 1024 * 1024:
            return f"{bytes_val / 1024:.1f} KB"
        elif bytes_val < 1024 * 1024 * 1024:
            return f"{bytes_val / (1024 * 1024):.1f} MB"
        else:
            return f"{bytes_val / (1024 * 1024 * 1024):.2f} GB"

    def _build_mermaid_diagram(self, tables: list, relationships: list) -> str | None:
        """Build Mermaid ER diagram from relationships."""
        if not relationships:
            return None

        visible_tables = set()
        for table in tables:
            if not table.get('is_local_date_table') and not table.get('is_hidden'):
                name = table.get('name', '')
                if name and 'LocalDateTable' not in name:
                    visible_tables.add(name)

        lines = ['erDiagram']

        seen_rels = set()
        for rel in relationships:
            from_table = rel.get('from_table', '')
            to_table = rel.get('to_table', '')

            if from_table not in visible_tables or to_table not in visible_tables:
                continue

            if 'LocalDateTable' in from_table or 'LocalDateTable' in to_table:
                continue

            rel_key = f"{from_table}-{to_table}"
            if rel_key in seen_rels:
                continue
            seen_rels.add(rel_key)

            cross_filter = rel.get('cross_filter', rel.get('cross_filter_direction', 'Single'))

            if cross_filter == 'Both':
                symbol = '}o--o{'
            else:
                symbol = '||--o{'

            from_clean = self._clean_mermaid_name(from_table)
            to_clean = self._clean_mermaid_name(to_table)

            from_col = rel.get('from_column', '')
            label = f"{from_col}" if from_col else "key"

            line = f'    {from_clean} {symbol} {to_clean} : "{label}"'
            lines.append(line)

        if len(lines) <= 1:
            return None

        return '\n'.join(lines)

    def _clean_mermaid_name(self, name: str) -> str:
        """Clean table name for Mermaid diagram."""
        clean = name.replace(' ', '_').replace('-', '_').replace("'", "")
        clean = ''.join(c if c.isalnum() or c == '_' else '' for c in clean)
        return clean

    def _get_high_cardinality_columns(self, tables: list) -> list[dict]:
        """Extract high cardinality columns from tables."""
        high_card = []
        for table in tables:
            if table.get('is_local_date_table'):
                continue

            columns = table.get('columns', [])
            for col in columns:
                cardinality = col.get('cardinality', 0) or 0
                if cardinality > 100_000:
                    size_bytes = col.get('total_size', 0) or col.get('TotalSize', 0) or 0
                    high_card.append({
                        'table': table.get('name', ''),
                        'column': col.get('name', ''),
                        'cardinality': cardinality,
                        'size_mb': round(size_bytes / (1024 * 1024), 1) if size_bytes else 0,
                    })
        high_card.sort(key=lambda x: x['cardinality'], reverse=True)
        return high_card[:15]

    def _normalize_high_cardinality_columns(self, columns: list[dict]) -> list[dict]:
        """Normalize high-cardinality columns from report into template format."""
        normalized = []
        for col in columns:
            cardinality = col.get('cardinality', 0) or 0
            size_bytes = col.get('total_size', 0) or 0
            normalized.append({
                'table': col.get('table', ''),
                'column': col.get('name', ''),
                'cardinality': cardinality,
                'size_mb': round(size_bytes / (1024 * 1024), 1) if size_bytes else 0,
            })
        normalized.sort(key=lambda x: x['cardinality'], reverse=True)
        return normalized[:15]

    def _build_optimization_context(
        self,
        tables: list,
        largest_tables: list,
        highest_cardinality_columns: list,
        relationships: list,
        total_size_bytes: int,
        total_rows: int,
    ) -> dict:
        """Build condensed VPAX context useful for LLM optimization."""
        def _mb(value: int) -> float:
            return round(value / (1024 * 1024), 1) if value else 0

        filtered_tables = [t for t in tables if not t.get('is_local_date_table')]
        top_tables_by_rows = sorted(
            filtered_tables, key=lambda t: t.get('row_count', 0), reverse=True
        )[:10]
        top_tables_by_size = sorted(
            filtered_tables, key=lambda t: t.get('size_bytes', 0), reverse=True
        )[:10]

        def _format_tables(items: list[dict]) -> list[dict]:
            formatted = []
            for item in items:
                size_bytes = item.get('size_bytes', 0)
                pct = round((size_bytes / total_size_bytes) * 100, 1) if total_size_bytes else 0
                formatted.append({
                    'name': item.get('name', ''),
                    'rows': item.get('row_count', 0),
                    'size_mb': _mb(size_bytes),
                    'pct_of_model': pct,
                    'hidden': item.get('is_hidden', False),
                })
            return formatted

        top_rows = _format_tables(top_tables_by_rows)
        top_sizes = _format_tables(largest_tables or top_tables_by_size)

        top_columns = []
        for col in (highest_cardinality_columns or []):
            top_columns.append({
                'table': col.get('table', ''),
                'name': col.get('name', ''),
                'cardinality': col.get('cardinality', 0) or 0,
                'size_mb': _mb(col.get('total_size', 0) or 0),
                'encoding': col.get('encoding', ''),
                'data_type': col.get('data_type', ''),
                'dictionary_ratio': col.get('dictionary_ratio'),
            })

        bidir_count = len([r for r in relationships if r.get('cross_filter') == 'Both'])
        inactive_count = len([r for r in relationships if not r.get('is_active', True)])
        missing_keys_count = len([r for r in relationships if (r.get('missing_keys', 0) or 0) > 0])

        max_table_rows = max((t.get('row_count', 0) for t in filtered_tables), default=0)
        max_column_cardinality = max((c.get('cardinality', 0) for c in top_columns), default=0)

        return {
            'has_context': bool(top_rows or top_sizes or top_columns),
            'top_tables_by_rows': top_rows,
            'top_tables_by_size': top_sizes,
            'top_columns_by_cardinality': top_columns[:10],
            'relationship_flags': {
                'bidirectional': bidir_count,
                'inactive': inactive_count,
                'missing_keys': missing_keys_count,
            },
            'model_totals': {
                'total_rows': total_rows,
                'total_size_mb': _mb(total_size_bytes),
                'max_table_rows': max_table_rows,
                'max_column_cardinality': max_column_cardinality,
            },
        }

    def _build_table_sizes(self, tables: list, total_size: int) -> list[dict]:
        """Build table size list for template."""
        result = []
        for table in tables:
            if table.get('is_local_date_table'):
                continue

            size_bytes = table.get('size_bytes', 0)
            size_mb = round(size_bytes / (1024 * 1024), 1) if size_bytes else 0
            pct = round((size_bytes / total_size) * 100, 1) if total_size else 0

            result.append({
                'name': table.get('name', ''),
                'size_mb': size_mb,
                'rows': table.get('row_count', 0),
                'pct_of_model': pct,
                'warning': pct > 30,
                'hidden': table.get('is_hidden', False),
            })

        result.sort(key=lambda x: x['size_mb'], reverse=True)
        return result[:15]

    def _build_relationship_issues(self, relationships: list) -> list[dict]:
        """Build relationship issues for template."""
        issues = []
        for rel in relationships:
            rel_issues = rel.get('issues', [])
            if not rel_issues:
                continue

            for issue in rel_issues:
                issue_type = 'unknown'
                rule_id = issue.get('rule_id', '').lower()
                desc = issue.get('description', '').lower()

                if 'bidirectional' in rule_id or 'bidir' in desc:
                    issue_type = 'bidirectional'
                elif 'missing' in desc:
                    issue_type = 'missing_keys'
                elif 'orphan' in desc:
                    issue_type = 'orphan_rows'
                elif 'inactive' in desc:
                    issue_type = 'inactive'

                issues.append({
                    'from_table': rel.get('from_table', ''),
                    'to_table': rel.get('to_table', ''),
                    'issue_type': issue_type,
                    'explanation': issue.get('description', 'Relationship issue detected'),
                })

        return issues

    def _build_measure_issues(self, measures: list, all_issues: list) -> list[dict]:
        """Build measure issues for template."""
        result = []
        severity_order = {'critical': 0, 'high': 1, 'medium': 2, 'low': 3, 'info': 4}

        measure_issue_map = {}
        for issue in all_issues:
            if issue.get('object_type') != 'measure':
                continue
            measure_name = issue.get('object_name', '')
            if measure_name not in measure_issue_map:
                measure_issue_map[measure_name] = []
            measure_issue_map[measure_name].append(issue)

        for measure in measures:
            name = measure.get('name', '')
            issues = measure_issue_map.get(name, [])
            if not issues:
                continue

            severities = [i.get('severity', 'low') for i in issues]
            severities.sort(key=lambda s: severity_order.get(s, 5))
            top_severity = severities[0] if severities else 'medium'

            rule_id = issues[0].get('rule_id') if issues else None
            total_penalty = sum(self._get_penalty_for_rule(i.get('rule_id')) for i in issues)

            issue_tags = []
            for issue in issues:
                tag = 'medium'
                if issue.get('severity') == 'critical':
                    tag = 'critical'
                elif issue.get('severity') == 'high':
                    tag = 'high'

                issue_tags.append({
                    'type': issue.get('rule_name', issue.get('rule_id', 'Issue')),
                    'tag': tag,
                    'explanation': issue.get('description', ''),
                })

            result.append({
                'severity': top_severity,
                'rule_id': rule_id,
                'name': name,
                'title': issues[0].get('rule_name', name) if issues else name,
                'table': measure.get('table', ''),
                'expression': measure.get('expression', ''),
                'issues': issue_tags,
                'penalty': total_penalty,
                'pattern': self._get_pattern_for_rule(rule_id),
                'llm_suggestion': None,
                'estimates': {
                    'improvement': self._get_improvement_estimate(rule_id),
                    'effort': self._get_effort_estimate(rule_id),
                },
                'reference_link': self._get_reference_link(rule_id),
            })

        result.sort(key=lambda x: severity_order.get(x['severity'], 5))
        return result[:25]

    def _build_architecture_issues(self, all_issues: list, summary: dict) -> list[dict]:
        """Build architecture/model-level issues for template."""
        grouped = {}

        for issue in all_issues:
            if issue.get('object_type') == 'measure':
                continue
            if issue.get('object_type') == 'calculation_item':
                continue

            rule_id = issue.get('rule_id', 'UNKNOWN')
            if rule_id not in grouped:
                grouped[rule_id] = {
                    'severity': issue.get('severity', 'medium'),
                    'rule_id': rule_id,
                    'title': issue.get('rule_name', 'Model Issue'),
                    'description': issue.get('description', ''),
                    'recommendation': issue.get('recommendation', ''),
                    'impact': None,
                    'reference_link': self._get_reference_link(rule_id),
                    'count': 0,
                    'affected_objects': [],
                }
            grouped[rule_id]['count'] += 1
            obj_name = issue.get('object_name', '')
            if obj_name and obj_name not in grouped[rule_id]['affected_objects']:
                grouped[rule_id]['affected_objects'].append(obj_name)

        local_date_count = summary.get('local_date_table_count', 0)
        if local_date_count > 0:
            local_date_size = summary.get('local_date_table_size_bytes', 0)
            size_mb = round(local_date_size / (1024 * 1024), 1) if local_date_size else 0
            total_size = max(summary.get("total_size_bytes", 1), 1)

            grouped.pop('MDL001', None)

            pct = round((local_date_size / total_size) * 100)
            grouped['MDL001'] = {
                'severity': 'critical',
                'rule_id': 'MDL001',
                'title': 'Auto Date/Time Tables Enabled',
                'description': f'{local_date_count} hidden auto-generated date tables consuming {size_mb} MB ({pct}% of model).',
                'recommendation': "Disable: File -> Options -> Data Load -> Uncheck 'Auto date/time for new files'. Then delete existing auto date tables.",
                'impact': f'-{pct}% model size',
                'reference_link': None,
                'count': local_date_count,
                'affected_objects': [],
            }

        result = list(grouped.values())
        severity_order = {'critical': 0, 'high': 1, 'medium': 2, 'low': 3, 'info': 4}
        result.sort(key=lambda x: (severity_order.get(x['severity'], 5), -x['count']))
        return result

    def _build_calc_group_issues(self, all_issues: list) -> list[dict]:
        """Build calculation group issues for template."""
        result = []

        for issue in all_issues:
            if issue.get('object_type') not in ('calculation_item', 'calculation_group'):
                continue

            result.append({
                'severity': issue.get('severity', 'medium'),
                'rule_id': issue.get('rule_id'),
                'title': issue.get('rule_name', 'Calculation Group Issue'),
                'description': issue.get('description', ''),
                'recommendation': issue.get('recommendation', ''),
                'object_name': issue.get('object_name', ''),
                'table_name': issue.get('table_name', ''),
                'code_snippet': issue.get('code_snippet', ''),
                'details': issue.get('details', {}),
            })

        severity_order = {'critical': 0, 'high': 1, 'medium': 2, 'low': 3, 'info': 4}
        result.sort(key=lambda x: severity_order.get(x['severity'], 5))
        return result

    def _build_tasks(self, all_issues: list) -> list[dict]:
        """Build remediation task list grouped by rule_id."""
        severity_order = {'critical': 0, 'high': 1, 'medium': 2, 'low': 3, 'info': 4}

        rule_tasks = {}
        for issue in all_issues:
            rule_id = issue.get('rule_id', 'UNKNOWN')
            if rule_id not in rule_tasks:
                rule_tasks[rule_id] = {
                    'rule_id': rule_id,
                    'title': issue.get('rule_name', 'Fix issue'),
                    'severity': issue.get('severity', 'medium'),
                    'count': 0,
                    'recommendation': issue.get('recommendation', ''),
                    'affected_objects': [],
                }
            rule_tasks[rule_id]['count'] += 1
            obj_name = issue.get('object_name', '')
            if obj_name and obj_name not in rule_tasks[rule_id]['affected_objects']:
                rule_tasks[rule_id]['affected_objects'].append(obj_name)

        tasks = []
        for rule_id, task_data in rule_tasks.items():
            tasks.append({
                'title': task_data['title'],
                'detail': task_data['recommendation'],
                'rule_id': rule_id,
                'effort': self._get_effort_estimate(rule_id),
                'severity': task_data['severity'],
                'count': task_data['count'],
                'affected_objects': task_data['affected_objects'][:10],
            })

        tasks.sort(key=lambda x: (severity_order.get(x['severity'], 5), -x['count']))
        return tasks

    def _get_penalty_for_rule(self, rule_id: str | None) -> int:
        """Get penalty points for a rule."""
        if not rule_id:
            return 5

        penalties = {
            'MDL001': 15, 'MDL002': 5, 'MDL003': 12, 'MDL004': 5,
            'MDL005': 5, 'MDL006': 10, 'DAX001': 20, 'DAX002': 25,
            'DAX003': 15, 'DAX004': 8, 'DAX005': 10, 'DAX006': 10,
            'DAX007': 5, 'CG001': 15, 'CG002': 8, 'CG003': 5,
        }
        return penalties.get(rule_id, 5)

    def _get_pattern_for_rule(self, rule_id: str | None) -> dict | None:
        """Get pattern information for a rule."""
        if not rule_id:
            return None

        patterns = {
            'DAX001': {
                'description': 'Replace FILTER(table) with a Boolean predicate for better performance.',
                'example': "-- Instead of:\nCALCULATE([Measure], FILTER('Table', [Col] = \"Val\"))\n\n-- Use:\nCALCULATE([Measure], 'Table'[Col] = \"Val\")",
                'note': 'Boolean predicates are evaluated in the Storage Engine, while FILTER() forces Formula Engine evaluation.',
            },
            'DAX002': {
                'description': 'Replace SUMX/AVERAGEX with base aggregation when iterating over measures.',
                'example': "-- Instead of:\nSUMX(FILTER(Table, [Cond]), [Measure])\n\n-- Use:\nCALCULATE([Measure], 'Table'[Cond] = TRUE())",
                'note': 'Combining iterators with FILTER creates row-by-row Formula Engine evaluation.',
            },
            'DAX003': {
                'description': 'Flatten deeply nested CALCULATE using VAR and SWITCH patterns.',
                'example': "-- Instead of nested CALCULATE:\nCALCULATE(\n    CALCULATE(\n        CALCULATE([M], ...),\n        ...\n    ),\n    ...\n)\n\n-- Use:\nVAR _val1 = CALCULATE([M], ...)\nVAR _val2 = CALCULATE(_val1, ...)\nRETURN _val2",
                'note': 'Each CALCULATE creates a context transition. Nesting beyond 2-3 levels indicates a pattern problem.',
            },
            'DAX004': {
                'description': 'Use DIVIDE() instead of the division operator to handle division by zero.',
                'example': "-- Instead of:\n[Numerator] / [Denominator]\n\n-- Use:\nDIVIDE([Numerator], [Denominator], 0)",
                'note': 'DIVIDE() provides safe division with a configurable alternate result for zero/blank denominators.',
            },
            'DAX006': {
                'description': 'Use VAR to store intermediate results and prevent re-evaluation.',
                'example': "-- Instead of repeating:\nIF([Complex] > 0, [Complex] * 2, [Complex] / 2)\n\n-- Use:\nVAR _val = [Complex]\nRETURN IF(_val > 0, _val * 2, _val / 2)",
                'note': 'Variables are evaluated once and cached, avoiding repeated calculation.',
            },
        }
        return patterns.get(rule_id)

    def _get_improvement_estimate(self, rule_id: str | None) -> str:
        """Get estimated improvement for a rule."""
        if not rule_id:
            return "Varies"

        estimates = {
            'MDL001': "-15% model size", 'MDL002': "Correct aggregations",
            'MDL003': "Reduced memory", 'MDL006': "Reduced ambiguity",
            'DAX001': "50-100x faster", 'DAX002': "25-50x faster",
            'DAX003': "10-25x faster", 'DAX004': "Error prevention",
            'DAX005': "10-20x faster", 'DAX006': "5-15x faster",
            'DAX007': "Better maintainability", 'CG001': "10-20x faster",
            'CG002': "Predictable behavior",
        }
        return estimates.get(rule_id, "Performance improvement")

    def _get_effort_estimate(self, rule_id: str | None) -> str:
        """Get estimated effort for a rule."""
        if not rule_id:
            return "15 min"

        efforts = {
            'MDL001': "5 min", 'MDL002': "30 min", 'MDL003': "20 min",
            'MDL006': "15 min", 'DAX001': "15 min", 'DAX002': "20 min",
            'DAX003': "30 min", 'DAX004': "5 min", 'DAX005': "15 min",
            'DAX006': "10 min", 'DAX007': "15 min", 'CG001': "45 min",
            'CG002': "10 min",
        }
        return efforts.get(rule_id, "15 min")

    def _get_reference_link(self, rule_id: str | None) -> str | None:
        """Get reference link for a rule."""
        return None
