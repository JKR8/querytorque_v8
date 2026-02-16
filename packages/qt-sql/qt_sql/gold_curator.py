"""Gold example curator — convert beam promotion candidates into gold example JSON.

Takes PromotionCandidate objects (from beam_analyzer) and generates gold example
JSON files matching the format in qt_sql/examples/{dialect}/.

The human provides key_insight, when_not_to_use, and input_slice.
The curator handles all the structural formatting.
"""

from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import Any, Dict, Optional

from qt_sql.beam_analyzer import PromotionCandidate

logger = logging.getLogger(__name__)

# Engine name → examples subdirectory
_ENGINE_TO_DIR = {
    "duckdb": "duckdb",
    "postgresql": "postgres",
    "postgres": "postgres",
    "snowflake": "snowflake",
}

# Family code → family name
_FAMILY_NAMES = {
    "A": "Early Filtering",
    "B": "Decorrelation",
    "C": "Aggregation",
    "D": "Set Ops",
    "E": "Materialization",
    "F": "Join Transform",
}


def _summarize_explain(explain_text: str, max_lines: int = 10) -> str:
    """Summarize EXPLAIN output to a compact form for gold examples."""
    if not explain_text:
        return ""
    lines = explain_text.strip().splitlines()
    if len(lines) <= max_lines:
        return explain_text.strip()
    return "\n".join(lines[:max_lines]) + f"\n  ... ({len(lines) - max_lines} more lines)"


def create_gold_example(
    candidate: PromotionCandidate,
    key_insight: str,
    when_not_to_use: str,
    input_slice: str,
) -> Dict[str, Any]:
    """Generate gold example JSON from promotion candidate + human insights.

    Args:
        candidate: The beam optimization result to promote.
        key_insight: Human-written explanation of WHY it's faster.
        when_not_to_use: Human-written failure mode description.
        input_slice: Human-written pattern description (1-2 sentences).

    Returns:
        Gold example dict matching the format in qt_sql/examples/{dialect}/.
    """
    is_win = candidate.speedup >= 1.0
    example_type = "gold" if is_win else "regression"

    gold = {
        "id": candidate.transform,
        "name": candidate.transform.replace("_", " ").title(),
        "description": key_insight[:200] if len(key_insight) > 200 else key_insight,
        "type": example_type,
        "verified_speedup": f"{candidate.speedup:.2f}x",
        "engine": candidate.engine,
        "benchmark": candidate.benchmark.upper(),
        "benchmark_queries": [candidate.query_id],
        "family": candidate.family,
        "principle": key_insight,
        "example": {
            "opportunity": f"{candidate.transform.upper()} (Family {candidate.family})",
            "input_slice": input_slice,
            "output": {
                "rewrite_sets": [
                    {
                        "id": "rs_01",
                        "transform": candidate.transform,
                        "nodes": _extract_output_nodes(candidate.optimized_sql),
                        "invariants_kept": [
                            "same result rows",
                            "same ordering",
                            "same column output",
                        ],
                        "expected_speedup": f"{candidate.speedup:.2f}x",
                        "risk": "low" if candidate.speedup >= 1.5 else "medium",
                    }
                ]
            },
            "key_insight": key_insight,
            "when_not_to_use": when_not_to_use,
        },
        "original_sql": candidate.original_sql,
        "optimized_sql": candidate.optimized_sql,
    }

    # Add EXPLAIN summaries if available
    if candidate.explain_before:
        gold["explain_before_summary"] = _summarize_explain(candidate.explain_before)
    if candidate.explain_after:
        gold["explain_after_summary"] = _summarize_explain(candidate.explain_after)

    # Regression-specific fields
    if not is_win:
        gold["transform_attempted"] = candidate.transform
        gold["regression_mechanism"] = when_not_to_use

    return gold


def _extract_output_nodes(optimized_sql: str) -> Dict[str, str]:
    """Extract CTE names and their definitions from optimized SQL.

    Parses WITH ... AS (...) patterns to create a node map.
    Falls back to a single 'main_query' node if no CTEs found.
    """
    import re

    nodes: Dict[str, str] = {}

    if not optimized_sql:
        return {"main_query": ""}

    # Simple CTE extraction: match "name AS (" patterns
    # This is a best-effort extraction — not a full SQL parser
    cte_pattern = re.compile(
        r'\b(\w+)\s+AS\s*\(',
        re.IGNORECASE,
    )

    # Find all CTE names
    matches = list(cte_pattern.finditer(optimized_sql))

    if not matches:
        nodes["main_query"] = optimized_sql.strip()
        return nodes

    # For each CTE, extract the name and note it exists
    # (Full CTE body extraction requires balanced-paren parsing)
    for m in matches:
        cte_name = m.group(1).lower()
        # Skip SQL keywords that look like CTEs
        if cte_name in ("select", "where", "from", "join", "and", "or", "not",
                        "in", "exists", "case", "when", "then", "else", "end",
                        "having", "group", "order", "limit", "union", "intersect",
                        "except", "insert", "update", "delete", "create", "with"):
            continue
        nodes[cte_name] = f"(CTE defined in optimized SQL)"

    nodes["main_query"] = "(final SELECT from optimized SQL)"

    return nodes


def write_gold_example(
    gold: Dict[str, Any],
    examples_dir: Path,
    engine: str,
    overwrite_if_lower: bool = True,
) -> Optional[Path]:
    """Write gold example JSON to the appropriate directory.

    Args:
        gold: Gold example dict from create_gold_example().
        examples_dir: Root examples directory (qt_sql/examples/).
        engine: Engine name (duckdb/postgresql/snowflake).
        overwrite_if_lower: If True, overwrite existing if new speedup is higher.

    Returns:
        Path to written file, or None if skipped.
    """
    subdir = _ENGINE_TO_DIR.get(engine, engine)
    target_dir = examples_dir / subdir

    # Handle regressions → put in regressions/ subdirectory
    is_regression = gold.get("type") == "regression"
    if is_regression:
        target_dir = target_dir / "regressions"

    target_dir.mkdir(parents=True, exist_ok=True)

    # File name from transform ID
    transform_id = gold["id"]
    filename = f"{transform_id}.json"

    # For regressions, prefix with "regression_"
    if is_regression and not filename.startswith("regression_"):
        filename = f"regression_{filename}"

    target_path = target_dir / filename

    # Check existing file
    if target_path.exists():
        try:
            existing = json.loads(target_path.read_text(encoding="utf-8"))
            existing_speedup_str = existing.get("verified_speedup", "0x")
            existing_speedup = float(existing_speedup_str.rstrip("x"))
            new_speedup_str = gold.get("verified_speedup", "0x")
            new_speedup = float(new_speedup_str.rstrip("x"))

            if not is_regression and new_speedup <= existing_speedup and not overwrite_if_lower:
                logger.info(
                    "Skipping %s — existing has higher speedup (%.2fx vs %.2fx)",
                    filename,
                    existing_speedup,
                    new_speedup,
                )
                return None

            # Merge benchmark_queries from existing
            existing_queries = existing.get("benchmark_queries", [])
            new_queries = gold.get("benchmark_queries", [])
            merged = sorted(set(existing_queries + new_queries))
            gold["benchmark_queries"] = merged

        except (json.JSONDecodeError, ValueError, KeyError):
            pass  # Overwrite broken file

    target_path.write_text(
        json.dumps(gold, indent=2, ensure_ascii=False) + "\n",
        encoding="utf-8",
    )
    logger.info("Wrote gold example: %s", target_path)
    return target_path


def examples_root() -> Path:
    """Return the path to qt_sql/examples/."""
    return Path(__file__).parent / "examples"
