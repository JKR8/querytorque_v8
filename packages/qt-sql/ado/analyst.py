"""LLM-guided query analysis — deep structural/performance reasoning.

This module implements the iterative analysis methodology:
1. Structural breakdown: decompose query into logical blocks, explain what each does
2. Profile analysis: map costs to blocks, identify dominant cost center
3. Root cause: explain the MECHANISM — not just "it's slow" but WHY (sorting, scanning, etc.)
4. Propose specific structural changes with reasoning about correctness risks
5. Incorporate failure history: each failed attempt teaches something that constrains the next

This analysis is generated BEFORE the rewrite prompt, so the rewrite LLM
has specific, concrete guidance instead of just pattern names.
"""

from __future__ import annotations

import json
import logging
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)


def build_analysis_prompt(
    query_id: str,
    sql: str,
    dag: Any,
    costs: Dict[str, Any],
    history: Optional[Dict[str, Any]] = None,
    effective_patterns: Optional[Dict[str, Any]] = None,
    known_regressions: Optional[Dict[str, str]] = None,
    faiss_picks: Optional[List[str]] = None,
    available_examples: Optional[List[Dict[str, str]]] = None,
    dialect: str = "duckdb",
) -> str:
    """Build the LLM-guided analysis prompt.

    This prompt asks the LLM to do deep structural analysis of the query
    following the methodology from successful manual optimization sessions:

    1. STRUCTURAL BREAKDOWN — what does each CTE/subquery/block do in plain language
    2. PROFILE ANALYSIS — where is time being spent and why (map DAG costs to blocks)
    3. ROOT CAUSE — the mechanism: what operation is expensive and why
       (e.g., "3 separate sorts of 5.5M rows" not just "window functions are slow")
    4. PROPOSED CHANGES — specific structural changes with:
       - What to change (concrete: "move the date filter from X to Y")
       - Why it should be faster (mechanism: "reduces hash join probe from 73K to 365 rows")
       - Semantic risk (what could break: "NULL handling changes if...")
    5. FAILURE ANALYSIS — if previous attempts exist, explain why they failed
       and what constraint that teaches us

    Args:
        query_id: Query identifier
        sql: The SQL query to analyze
        dag: Parsed DAG from Phase 1
        costs: Per-node cost analysis
        history: Previous attempts and promotion context
        effective_patterns: Known effective patterns from history.json
        known_regressions: Known regression patterns to avoid
        dialect: SQL dialect

    Returns:
        Analysis prompt string
    """
    lines = []

    # Role
    lines.append(
        "You are an expert database performance analyst. Your job is to deeply "
        "analyze a slow SQL query, identify the root cause of its performance "
        "problems, and propose specific structural changes."
    )
    lines.append("")
    lines.append(
        "You follow a rigorous methodology: understand the structure, profile "
        "the costs, identify the mechanism (not just the symptom), propose "
        "changes with correctness reasoning, and learn from past failures."
    )
    lines.append("")

    # The query
    lines.append(f"## Query: {query_id}")
    lines.append(f"## Dialect: {dialect}")
    lines.append("")

    # Pretty-print SQL
    clean_sql = sql
    try:
        import sqlglot
        clean_sql = sqlglot.transpile(sql, read=dialect, write=dialect, pretty=True)[0]
    except Exception:
        pass
    lines.append("```sql")
    lines.append(clean_sql)
    lines.append("```")
    lines.append("")

    # DAG topology + costs
    lines.append("## Query Structure (DAG)")
    lines.append("")
    _append_dag_analysis(lines, dag, costs)
    lines.append("")

    # Previous attempts
    if history:
        lines.append("## Previous Optimization Attempts")
        lines.append("")
        _append_history_analysis(lines, history)
        lines.append("")

    # Known patterns
    if effective_patterns:
        lines.append("## Known Effective Patterns (from benchmark history)")
        lines.append("")
        for pat, info in effective_patterns.items():
            wins = info.get("wins", 0)
            avg = info.get("avg_speedup", 0)
            notes = info.get("notes", "")
            lines.append(f"- **{pat}**: {wins} wins, {avg:.2f}x avg. {notes}")
        lines.append("")

    # Known regressions
    if known_regressions:
        lines.append("## Known Regressions (DO NOT repeat these)")
        lines.append("")
        for name, desc in known_regressions.items():
            lines.append(f"- **{name}**: {desc}")
        lines.append("")

    # FAISS picks + available examples for override
    if faiss_picks or available_examples:
        lines.append("## Reference Examples")
        lines.append("")
        if faiss_picks:
            lines.append(f"**FAISS selected (by structural similarity):** {', '.join(faiss_picks)}")
            lines.append("")
        if available_examples:
            lines.append("**All available gold examples:**")
            lines.append("")
            for ex in available_examples:
                lines.append(
                    f"- **{ex['id']}** ({ex.get('speedup', '?')}x) — {ex.get('description', '')}"
                )
            lines.append("")

    # The task
    lines.append("## Your Task")
    lines.append("")
    lines.append("Analyze this query following these steps IN ORDER:")
    lines.append("")
    lines.append("### 1. STRUCTURAL BREAKDOWN")
    lines.append("For each CTE/subquery/block, explain in 1-2 sentences:")
    lines.append("- What it computes (in plain language)")
    lines.append("- What tables it reads and approximately how many rows")
    lines.append("- What it outputs (cardinality estimate)")
    lines.append("")
    lines.append("### 2. BOTTLENECK IDENTIFICATION")
    lines.append("Using the DAG costs above, identify the dominant cost center.")
    lines.append("Don't just name it — explain the MECHANISM:")
    lines.append("- Is it a full table scan that could be filtered?")
    lines.append("- Is it a sort for a window function that could be deferred?")
    lines.append("- Is it a hash join on a large build side that could be pre-filtered?")
    lines.append("- Is it scanning the same table multiple times when once would suffice?")
    lines.append("")
    lines.append("### 3. PROPOSED OPTIMIZATION")
    lines.append("Propose 1-3 specific structural changes. For EACH one:")
    lines.append("- **What**: Exactly what to change (e.g., 'merge CTEs X and Y into one scan')")
    lines.append("- **Why**: The performance mechanism (e.g., 'eliminates a 28M-row rescan of store_sales')")
    lines.append("- **Risk**: What semantic constraint could break (e.g., 'the HAVING filter must be preserved')")
    lines.append("- **Estimated impact**: minor / moderate / significant")
    lines.append("")

    if history and history.get("attempts"):
        lines.append("### 4. FAILURE ANALYSIS")
        lines.append("For each previous failed/regressed attempt, explain:")
        lines.append("- WHY it failed (the specific mechanism)")
        lines.append("- What constraint that teaches us for the next attempt")
        lines.append("")

    lines.append("### 5. RECOMMENDED STRATEGY")
    lines.append("Synthesize everything into a single recommended optimization approach.")
    lines.append("Be specific enough that another engineer could implement it from your description.")
    lines.append("")

    if available_examples:
        lines.append("### 6. EXAMPLE SELECTION")
        lines.append(f"FAISS selected these examples: {', '.join(faiss_picks or [])}")
        lines.append("Review the FAISS picks against the available examples above.")
        lines.append("If you think different examples would be more relevant for this query,")
        lines.append("list your preferred examples. Otherwise confirm the FAISS picks are good.")
        lines.append("")
        lines.append("```")
        lines.append("EXAMPLES: example_id_1, example_id_2, example_id_3")
        lines.append("```")
        lines.append("")
        lines.append("Use exact IDs from the available examples list above.")
        lines.append("")

    return "\n".join(lines)


def _append_dag_analysis(
    lines: List[str],
    dag: Any,
    costs: Dict[str, Any],
) -> None:
    """Append DAG structure with costs to the analysis prompt."""
    from .node_prompter import compute_depths
    depths = compute_depths(dag)

    max_depth = max(depths.values()) if depths else 0
    for depth in range(max_depth + 1):
        nodes_at_depth = [nid for nid, d in depths.items() if d == depth]
        if not nodes_at_depth:
            continue

        for nid in nodes_at_depth:
            node = dag.nodes[nid]
            cost = costs.get(nid)
            cost_pct = cost.cost_pct if cost and hasattr(cost, "cost_pct") else 0
            flags = node.flags if hasattr(node, "flags") and node.flags else []
            refs = list(node.refs) if hasattr(node, "refs") else []
            tables = list(node.tables) if hasattr(node, "tables") else []

            flag_str = f" [{', '.join(flags)}]" if flags else ""
            ref_str = f" ← reads [{', '.join(refs)}]" if refs else ""
            table_str = f" tables: {', '.join(tables)}" if tables else ""

            lines.append(
                f"- **{nid}** ({node.node_type}, depth {depth}, "
                f"**{cost_pct:.0f}%** cost){flag_str}{ref_str}"
            )
            if table_str:
                lines.append(f"  {table_str}")

            # Show operators if available
            if cost and hasattr(cost, "operators") and cost.operators:
                ops = ", ".join(cost.operators[:5])
                lines.append(f"  operators: {ops}")

            # Show SQL snippet (first 200 chars)
            if hasattr(node, "sql") and node.sql:
                snippet = node.sql[:200].replace("\n", " ")
                if len(node.sql) > 200:
                    snippet += "..."
                lines.append(f"  sql: `{snippet}`")


def _append_history_analysis(
    lines: List[str],
    history: Dict[str, Any],
) -> None:
    """Append previous attempt history for failure analysis."""
    # Promotion context
    promotion = history.get("promotion")
    if promotion:
        lines.append(f"**Best previous result: {promotion.speedup:.2f}x** "
                      f"(transforms: {', '.join(promotion.transforms)})")
        lines.append("")
        if promotion.analysis:
            lines.append(f"Previous analysis: {promotion.analysis}")
            lines.append("")
        if promotion.suggestions:
            lines.append(f"Previous suggestions: {promotion.suggestions}")
            lines.append("")

    # All attempts
    attempts = history.get("attempts", [])
    if attempts:
        for i, attempt in enumerate(attempts):
            status = attempt.get("status", "unknown")
            speedup = attempt.get("speedup", 0)
            transforms = attempt.get("transforms", [])
            error = attempt.get("error", "")
            t_str = ", ".join(transforms) if transforms else "unknown"

            if status in ("error", "ERROR"):
                lines.append(f"- Attempt {i+1}: **{t_str}** → ERROR: {error}")
            elif speedup < 0.95:
                lines.append(
                    f"- Attempt {i+1}: **{t_str}** → REGRESSION ({speedup:.2f}x)"
                )
            elif speedup >= 1.10:
                lines.append(
                    f"- Attempt {i+1}: **{t_str}** → WIN ({speedup:.2f}x)"
                )
            else:
                lines.append(
                    f"- Attempt {i+1}: **{t_str}** → NEUTRAL ({speedup:.2f}x)"
                )

            # Include attempted SQL if available (for failure analysis)
            opt_sql = attempt.get("optimized_sql", "")
            if opt_sql and status not in ("WIN", "IMPROVED") and len(opt_sql) < 2000:
                lines.append(f"  Attempted SQL:")
                lines.append(f"  ```sql\n  {opt_sql}\n  ```")


def parse_analysis_response(response: str) -> Dict[str, str]:
    """Parse the LLM analysis response into structured sections.

    Returns dict with keys:
    - structural_breakdown
    - bottleneck
    - proposed_changes
    - failure_analysis (if present)
    - recommended_strategy
    - raw (full response)
    """
    import re

    result = {"raw": response}

    patterns = {
        "structural_breakdown": r"###?\s*1\.?\s*STRUCTURAL\s+BREAKDOWN\s*\n(.*?)(?=###?\s*2\.|$)",
        "bottleneck": r"###?\s*2\.?\s*BOTTLENECK\s+IDENTIFICATION\s*\n(.*?)(?=###?\s*3\.|$)",
        "proposed_changes": r"###?\s*3\.?\s*PROPOSED\s+OPTIMIZATION\s*\n(.*?)(?=###?\s*4\.|###?\s*5\.|$)",
        "failure_analysis": r"###?\s*4\.?\s*FAILURE\s+ANALYSIS\s*\n(.*?)(?=###?\s*5\.|$)",
        "recommended_strategy": r"###?\s*5\.?\s*RECOMMENDED\s+STRATEGY\s*\n(.*?)$",
    }

    for key, pattern in patterns.items():
        match = re.search(pattern, response, re.DOTALL | re.IGNORECASE)
        if match:
            result[key] = match.group(1).strip()

    return result


def parse_example_overrides(response: str) -> Optional[List[str]]:
    """Parse the analyst response for example override recommendations.

    Looks for a line like: EXAMPLES: decorrelate, early_filter, pushdown

    Returns:
        List of example IDs if the analyst recommended overrides, None otherwise.
    """
    import re

    match = re.search(r'EXAMPLES:\s*(.+)', response)
    if not match:
        return None

    raw = match.group(1).strip()
    # Split on comma, clean whitespace
    ids = [x.strip() for x in raw.split(",") if x.strip()]

    if not ids:
        return None

    return ids


def format_analysis_for_prompt(analysis: Dict[str, str]) -> str:
    """Format the parsed analysis into a prompt section for the rewrite LLM.

    This goes into Section 5 (History/Context) of the rewrite prompt,
    giving the rewrite LLM concrete guidance.
    """
    lines = ["## Expert Analysis", ""]

    if analysis.get("structural_breakdown"):
        lines.append("### Query Structure")
        lines.append(analysis["structural_breakdown"])
        lines.append("")

    if analysis.get("bottleneck"):
        lines.append("### Performance Bottleneck")
        lines.append(analysis["bottleneck"])
        lines.append("")

    if analysis.get("proposed_changes"):
        lines.append("### Proposed Optimization Strategy")
        lines.append(analysis["proposed_changes"])
        lines.append("")

    if analysis.get("failure_analysis"):
        lines.append("### Lessons from Previous Failures")
        lines.append(analysis["failure_analysis"])
        lines.append("")

    if analysis.get("recommended_strategy"):
        lines.append("### Recommended Approach")
        lines.append(analysis["recommended_strategy"])
        lines.append("")

    lines.append(
        "Apply the recommended strategy above. The analysis has already "
        "identified the bottleneck and the specific structural change needed. "
        "Focus on implementing it correctly while preserving semantic equivalence."
    )

    return "\n".join(lines)
