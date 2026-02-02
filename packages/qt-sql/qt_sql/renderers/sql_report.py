"""SQL Report Renderer - generates HTML audit reports."""

import json
import uuid
from datetime import datetime
from pathlib import Path
from typing import Any

from jinja2 import Environment, FileSystemLoader


def render_sql_report(
    analysis_result: Any,
    sql: str,
    filename: str = "query.sql",
    dialect: str = "generic",
) -> str:
    """Render SQL analysis result as HTML report.

    Args:
        analysis_result: The AnalysisResult from SQLAntiPatternDetector
        sql: The original SQL query
        filename: Name of the SQL file
        dialect: SQL dialect (generic, snowflake, postgres, etc.)

    Returns:
        HTML string of the rendered report
    """
    # Set up Jinja2 environment
    template_dir = Path(__file__).parent.parent / "templates"
    env = Environment(loader=FileSystemLoader(str(template_dir)))
    template = env.get_template("sql_report.html.j2")

    # Build the data structure expected by the template
    report_id = str(uuid.uuid4())[:8]

    # Convert issues to template format
    issues = []
    for issue in analysis_result.issues:
        issues.append({
            "rule": issue.rule_id,
            "rule_id": issue.rule_id,
            "title": issue.name,
            "severity": issue.severity.lower(),
            "category": issue.category,
            "description": issue.description,
            "line": _extract_line_number(issue.location),
            "location": {"line": _extract_line_number(issue.location)},
            "suggestion": issue.suggestion,
            "fix": {"after": issue.suggestion} if issue.suggestion else None,
        })

    # Build summary
    summary = {
        "torque_score": analysis_result.final_score,
        "headline": _generate_headline(analysis_result),
        "improvement_estimate": _estimate_improvement(issues),
    }

    # Build source info
    source = {
        "sql": sql,
    }

    # Build constraints (default values for now)
    constraints = {
        "preserve": {
            "output_columns": [],
            "row_count": None,
            "sort_order": "none",
        },
        "permitted": ["Standard query rewrites", "Index hints", "Join reordering"],
        "forbidden": ["Changes affecting output columns", "Removing WHERE conditions"],
        "query_features": analysis_result.query_structure or {},
    }

    # Build schema map from query structure
    schema_map = {
        "columns": [],
        "joins": [],
        "tables": [],
        "cte_chain": [],
        "cte_dependencies": [],
    }

    if analysis_result.query_structure:
        qs = analysis_result.query_structure
        # Extract tables
        if "tables" in qs:
            schema_map["tables"] = [{"name": t, "row_count": None} for t in qs["tables"]]
        # Extract CTEs
        if "ctes" in qs:
            schema_map["cte_chain"] = [{"name": c, "columns": ""} for c in qs["ctes"]]
        # Extract joins
        if "joins" in qs:
            for join in qs["joins"]:
                schema_map["joins"].append({
                    "left": join.get("left", "?"),
                    "right": join.get("right", "?"),
                    "type": join.get("type", "INNER"),
                    "cardinality": "1:N",
                    "ok": True,
                })

    # Build the full data structure
    json_data = {
        "report_id": report_id,
        "filename": filename,
        "generated_date": datetime.now().strftime("%Y-%m-%d %H:%M"),
        "platform": dialect.upper(),
        "query": {
            "name": filename,
            "platform": dialect,
            "raw_sql": sql,
        },
        "summary": summary,
        "issues": issues,
        "source": source,
        "constraints": constraints,
        "schema_map": schema_map,
        "execution_reality": {},
        "cost_impact": {},
        "cost_analysis": {},
        "metrics": {},
    }

    # Generate LLM payload
    llm_payload = _generate_llm_payload(sql, issues, filename, dialect)

    # Render template
    html = template.render(
        json_data=json.dumps(json_data),
        llm_payload=llm_payload,
        filename=filename,
    )

    return html


def _extract_line_number(location: str | None) -> int | None:
    """Extract line number from location string like 'Line 5'."""
    if not location:
        return None
    try:
        if location.lower().startswith("line"):
            return int(location.split()[1])
        return int(location)
    except (ValueError, IndexError):
        return None


def _generate_headline(result: Any) -> str:
    """Generate a headline based on analysis results."""
    score = result.final_score
    issue_count = len(result.issues)

    if score >= 90:
        return f"Excellent query quality - {issue_count} minor suggestions"
    elif score >= 70:
        return f"Good query with {issue_count} optimization opportunities"
    elif score >= 50:
        return f"Query needs attention - {issue_count} issues detected"
    else:
        return f"Critical issues found - {issue_count} anti-patterns detected"


def _estimate_improvement(issues: list) -> str:
    """Estimate potential improvement from fixing issues."""
    critical = sum(1 for i in issues if i["severity"] == "critical")
    high = sum(1 for i in issues if i["severity"] == "high")

    if critical > 0:
        return "50-80% potential"
    elif high > 0:
        return "20-50% potential"
    elif issues:
        return "5-20% potential"
    return "Already optimized"


def _escape_for_js(text: str) -> str:
    """Escape text for safe embedding in JavaScript."""
    # Escape backslashes first, then other special chars
    text = text.replace("\\", "\\\\")
    text = text.replace("`", "\\`")
    text = text.replace("${", "\\${")
    text = text.replace("</script>", "<\\/script>")
    return text


def _generate_llm_payload(sql: str, issues: list, filename: str, dialect: str) -> str:
    """Generate the LLM payload for optimization."""
    lines = [
        "# SQL Optimization Request",
        "",
        f"**File:** {filename}",
        f"**Dialect:** {dialect}",
        "",
        "## Original SQL",
        "```sql",
        sql,
        "```",
        "",
    ]

    if issues:
        lines.extend([
            "## Detected Anti-Patterns",
            "",
        ])
        for i, issue in enumerate(issues, 1):
            lines.append(f"{i}. **{issue['rule']}** ({issue['severity']}): {issue['title']}")
            if issue.get("description"):
                lines.append(f"   - {issue['description']}")
            if issue.get("suggestion"):
                lines.append(f"   - Suggestion: {issue['suggestion']}")
            lines.append("")

    lines.extend([
        "## Instructions",
        "",
        "Please provide an optimized version of this SQL that:",
        "1. Fixes the detected anti-patterns",
        "2. Preserves the same output columns and row count",
        "3. Uses explicit JOIN syntax instead of comma joins",
        "4. Avoids functions on indexed columns in WHERE clauses",
        "",
        "Return the optimized SQL in a code block.",
    ])

    return "\n".join(lines)
