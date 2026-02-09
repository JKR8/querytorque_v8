"""Probe prompt builder — constructs frontier probe prompts from PROBE.txt template.

Substitutes {{engine}}, {{exploit_profile}}, {{original_sql}}, {{explain_plan}}
into the probe template and returns the complete prompt.
"""

from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import Optional

logger = logging.getLogger(__name__)

# Load the PROBE.txt template once
_TEMPLATE_PATH = Path(__file__).resolve().parent.parent.parent / "PROBE.txt"


def _load_template() -> str:
    """Load the PROBE.txt template text."""
    if _TEMPLATE_PATH.exists():
        return _TEMPLATE_PATH.read_text()
    raise FileNotFoundError(f"PROBE.txt template not found at {_TEMPLATE_PATH}")


def _add_line_numbers(sql: str) -> str:
    """Add line numbers to SQL for analyst reference."""
    lines = sql.split("\n")
    width = len(str(len(lines)))
    return "\n".join(f"{i+1:>{width}} | {line}" for i, line in enumerate(lines))


def _format_explain_text(
    explain_data: dict,
    dialect: str = "duckdb",
) -> str:
    """Format EXPLAIN plan data into readable text.

    For DuckDB: renders JSON plan tree via format_duckdb_explain_tree.
    For PG: renders plan_json via format_pg_explain_tree.
    Falls back to raw text if already formatted.
    """
    from ..prompts.analyst_briefing import (
        format_duckdb_explain_tree,
        format_pg_explain_tree,
    )

    # DuckDB path: plan_text field contains JSON
    plan_text = explain_data.get("plan_text")
    if plan_text:
        return format_duckdb_explain_tree(plan_text)

    # PG path: plan_json field
    plan_json = explain_data.get("plan_json")
    if plan_json:
        return format_pg_explain_tree(plan_json)

    return ""


def _load_explain_plan(
    benchmark_dir: Path,
    query_id: str,
    dialect: str = "duckdb",
) -> Optional[str]:
    """Load and format EXPLAIN plan from cached explains.

    Searches: explains/ (flat) -> explains/sf10/ -> explains/sf5/.
    """
    search_paths = [
        benchmark_dir / "explains" / f"{query_id}.json",
        benchmark_dir / "explains" / "sf10" / f"{query_id}.json",
        benchmark_dir / "explains" / "sf5" / f"{query_id}.json",
    ]

    for cache_path in search_paths:
        if cache_path.exists():
            try:
                data = json.loads(cache_path.read_text())
                formatted = _format_explain_text(data, dialect)
                if formatted:
                    return formatted
            except Exception:
                pass

    return None


def build_probe_prompt(
    sql: str,
    explain_plan_text: Optional[str],
    exploit_profile_text: Optional[str],
    dialect: str = "duckdb",
    dialect_version: Optional[str] = None,
) -> str:
    """Build the frontier probe prompt with variable substitution.

    Args:
        sql: Original SQL query.
        explain_plan_text: Formatted EXPLAIN ANALYZE tree text (or None).
        exploit_profile_text: Current exploit algorithm YAML text (or None).
        dialect: SQL dialect (duckdb, postgresql).
        dialect_version: Engine version string (e.g., '1.4.3').

    Returns:
        Complete probe prompt string.
    """
    template = _load_template()

    # Engine name
    engine_names = {
        "duckdb": "DuckDB",
        "postgres": "PostgreSQL",
        "postgresql": "PostgreSQL",
    }
    engine = engine_names.get(dialect, dialect)
    version = f" v{dialect_version}" if dialect_version else ""

    # Substitute template variables
    prompt = template.replace("{{engine}}", engine)
    prompt = prompt.replace("{{version}}", version)

    # Exploit profile
    if exploit_profile_text:
        prompt = prompt.replace("{{exploit_profile}}", exploit_profile_text)
    else:
        prompt = prompt.replace(
            "{{exploit_profile}}", "No gaps discovered yet. This is the first probe round."
        )

    # SQL with line numbers
    prompt = prompt.replace("{{original_sql}}", f"```sql\n{_add_line_numbers(sql)}\n```")

    # EXPLAIN plan
    if explain_plan_text:
        prompt = prompt.replace(
            "{{explain_plan}}",
            f"```\n{explain_plan_text}\n```",
        )
    else:
        prompt = prompt.replace(
            "{{explain_plan}}",
            "*EXPLAIN plan not available for this query.*",
        )

    return prompt
