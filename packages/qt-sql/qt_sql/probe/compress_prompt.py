"""Compression prompt builder — constructs prompt for exploit algorithm generation.

Takes probe results (evidence table) and asks the LLM to produce a structured
exploit algorithm in YAML format following the EX_ALGO.txt template.
"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import List, Optional

from .schemas import ProbeResult

logger = logging.getLogger(__name__)

# Load EX_ALGO.txt template for format reference
_EX_ALGO_PATH = Path(__file__).resolve().parent.parent.parent / "EX_ALGO.txt"


def _load_ex_algo_template() -> str:
    """Load the EX_ALGO.txt template text."""
    if _EX_ALGO_PATH.exists():
        return _EX_ALGO_PATH.read_text()
    return "(EX_ALGO.txt template not found)"


def _build_evidence_table(probe_results: List[ProbeResult]) -> str:
    """Build compact evidence table from probe results.

    Format: query_id | attack_id | gap_hypothesis | structural_preconditions | status | speedup
    """
    lines = [
        "| query_id | attack | gap_hypothesis | structural_preconditions | status | speedup |",
        "|----------|--------|----------------|--------------------------|--------|---------|",
    ]

    for result in sorted(probe_results, key=lambda r: r.query_id):
        for attack in result.attacks:
            gap = attack.gap_hypothesis[:60]
            precond = attack.structural_preconditions[:60]
            lines.append(
                f"| {result.query_id} | A{attack.attack_id} | {gap} | "
                f"{precond} | {attack.status} | {attack.speedup:.2f}x |"
            )

    return "\n".join(lines)


def _build_gap_summary(probe_results: List[ProbeResult]) -> str:
    """Build gap frequency summary across all probes."""
    gap_stats: dict[str, dict] = {}

    for result in probe_results:
        for attack in result.attacks:
            gap = attack.gap_hypothesis.strip()
            if not gap:
                continue

            if gap not in gap_stats:
                gap_stats[gap] = {
                    "attempts": 0,
                    "wins": 0,
                    "improved": 0,
                    "speedups": [],
                    "queries": [],
                }

            gap_stats[gap]["attempts"] += 1
            gap_stats[gap]["queries"].append(result.query_id)
            if attack.status == "WIN":
                gap_stats[gap]["wins"] += 1
                gap_stats[gap]["speedups"].append(attack.speedup)
            elif attack.status == "IMPROVED":
                gap_stats[gap]["improved"] += 1
                gap_stats[gap]["speedups"].append(attack.speedup)

    if not gap_stats:
        return "No gap data available."

    lines = ["### Gap Frequency Summary", ""]

    # Sort by win count descending
    ranked = sorted(
        gap_stats.items(),
        key=lambda x: (-x[1]["wins"], -x[1]["improved"]),
    )

    for gap, stats in ranked:
        win_rate = (
            (stats["wins"] + stats["improved"]) / stats["attempts"]
            if stats["attempts"] > 0
            else 0
        )
        median_speedup = 0.0
        if stats["speedups"]:
            sorted_sp = sorted(stats["speedups"])
            mid = len(sorted_sp) // 2
            median_speedup = sorted_sp[mid]

        lines.append(
            f"- **{gap}**: {stats['attempts']} attempts, "
            f"{stats['wins']} WIN, {stats['improved']} IMPROVED "
            f"(win rate {win_rate:.0%}, median {median_speedup:.2f}x)"
        )
        # Show which queries
        unique_qs = list(set(stats["queries"]))[:5]
        lines.append(f"  Queries: {', '.join(unique_qs)}")

    return "\n".join(lines)


def build_compression_prompt(
    probe_results: List[ProbeResult],
    previous_algorithm_text: Optional[str],
    engine: str = "duckdb",
    engine_version: Optional[str] = None,
) -> str:
    """Build the compression prompt for exploit algorithm generation.

    Args:
        probe_results: All probe results from this round.
        previous_algorithm_text: Previous round's exploit algorithm YAML (or None).
        engine: Target engine name.
        engine_version: Engine version string.

    Returns:
        Complete compression prompt string.
    """
    engine_names = {
        "duckdb": "DuckDB",
        "postgres": "PostgreSQL",
        "postgresql": "PostgreSQL",
    }
    engine_display = engine_names.get(engine, engine)
    ver = f" v{engine_version}" if engine_version else ""

    # Statistics
    total_attacks = sum(len(r.attacks) for r in probe_results)
    total_wins = sum(r.n_wins for r in probe_results)
    total_improved = sum(r.n_improved for r in probe_results)
    n_queries = len(probe_results)

    lines = [
        f"You are a query optimization researcher analyzing probe results for {engine_display}{ver}.",
        "",
        "Your task: compress the evidence from frontier probing into a structured "
        "exploit algorithm that production workers can use to optimize queries.",
        "",
        f"## Evidence Summary",
        f"",
        f"- **Queries probed**: {n_queries}",
        f"- **Total attacks**: {total_attacks}",
        f"- **Wins (>=1.5x)**: {total_wins}",
        f"- **Improved (1.1-1.5x)**: {total_improved}",
        "",
    ]

    # Gap frequency summary
    lines.append(_build_gap_summary(probe_results))
    lines.append("")

    # Evidence table
    lines.append("## Evidence Table")
    lines.append("")
    lines.append(_build_evidence_table(probe_results))
    lines.append("")

    # Previous algorithm (if updating)
    if previous_algorithm_text:
        lines.append("## Previous Exploit Algorithm (to update/extend)")
        lines.append("")
        lines.append("```yaml")
        lines.append(previous_algorithm_text)
        lines.append("```")
        lines.append("")
        lines.append(
            "UPDATE this algorithm with the new evidence above. "
            "Add new exploits, update success rates, add new evidence lines, "
            "and promote open_questions to exploits when sufficient evidence exists."
        )
        lines.append("")
    else:
        lines.append(
            "This is the FIRST round. Create the exploit algorithm from scratch."
        )
        lines.append("")

    # Format reference
    lines.append("## Output Format Reference")
    lines.append("")
    lines.append(
        "Output the exploit algorithm as YAML following this structure exactly:"
    )
    lines.append("")
    lines.append("```yaml")
    lines.append(_load_ex_algo_template())
    lines.append("```")
    lines.append("")

    # Instructions
    lines.append("## Instructions")
    lines.append("")
    lines.append(
        "1. **Cluster**: Group similar attacks into single exploits. "
        "Attacks with the same gap_hypothesis but different queries are evidence "
        "for ONE exploit, not separate exploits."
    )
    lines.append(
        "2. **Compute statistics**: For each exploit, compute success rate "
        "(wins+improved / attempts) and median speedup from the evidence table."
    )
    lines.append(
        "3. **Order by expected value**: Rank exploits by (success_rate × median_speedup). "
        "Higher expected value = earlier in the list."
    )
    lines.append(
        "4. **Write detection rules**: For each exploit, write DETECT rules using "
        "the FEATURE_VOCABULARY. Detection rules must be structural (checkable from "
        "SQL text or EXPLAIN plan), not semantic."
    )
    lines.append(
        "5. **Write exploit steps**: Procedural steps a worker can follow. "
        "Be specific about CTE structure, join rewriting, filter placement."
    )
    lines.append(
        "6. **Document critical rules**: What breaks if done wrong. "
        "Include observed regressions from REGRESSION/FAIL evidence."
    )
    lines.append(
        "7. **Non-exploitable**: List areas where probing confirmed the optimizer "
        "handles well (from NEGATIVE_RESULTS). These save future workers from "
        "wasting time."
    )
    lines.append(
        "8. **Open questions**: Gaps with <3 data points that need more probing."
    )
    lines.append("")
    lines.append(
        "Replace template placeholders ({{engine}}, {{date}}, etc.) with actual values."
    )
    lines.append("")
    lines.append("Output ONLY the YAML exploit algorithm inside ```yaml ... ``` markers.")

    return "\n".join(lines)
