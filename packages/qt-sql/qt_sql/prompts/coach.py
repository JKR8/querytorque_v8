"""Coach prompt — replaces snipe for multi-round optimization.

The Coach reviews race results and vital signs from round N, then produces
per-worker refinement directives for round N+1. All 4 workers are refined
in parallel, reusing the shared prefix from round 1 for cache efficiency.

Flow:
  Round 1: Analyst → 4 workers (shared prefix) → Race validate
  Round 2+: Coach (sees race results + vital signs) → 4 refined workers → Race validate
"""

from __future__ import annotations

import logging
from typing import Any, Dict, List, Optional

from ..schemas import WorkerResult

logger = logging.getLogger(__name__)


def build_coach_prompt(
    original_sql: str,
    worker_results: List[WorkerResult],
    vital_signs: Dict[int, str],
    race_timings: Optional[Dict[str, Any]],
    engine_profile: Optional[Dict[str, Any]],
    dialect: str,
    target_speedup: float = 2.0,
    original_vital_signs: Optional[str] = None,
    round_num: int = 1,
) -> str:
    """Build the coach prompt for post-mortem analysis and refinement directives.

    The coach sees:
    - Race results (compact table)
    - Original query vital signs (baseline EXPLAIN condensed)
    - Per-worker vital signs (5-10 lines each, extracted from EXPLAIN)
    - Per-worker SQL + SET LOCAL commands
    - Engine profile (optimizer strengths/gaps)

    And produces:
    - Diagnosis of winner/losers
    - Per-worker refinement directives for round 2+

    Args:
        original_sql: The original SQL query
        worker_results: All results from the previous round (typically 4)
        vital_signs: worker_id -> condensed EXPLAIN signals (from extract_vital_signs)
        race_timings: Race timing data (original_ms, candidate timings, etc.)
        engine_profile: Engine profile JSON with optimizer strengths/gaps
        dialect: SQL dialect
        target_speedup: Target speedup ratio
        original_vital_signs: Condensed EXPLAIN of the ORIGINAL query (baseline)
        round_num: Which coach round this is (1-based)

    Returns:
        Complete coach prompt string
    """
    target = f"{target_speedup:.1f}".rstrip("0").rstrip(".")
    sections: list[str] = []

    # ── 1. Role ──────────────────────────────────────────────────────────
    engine_names = {
        "duckdb": "DuckDB",
        "postgres": "PostgreSQL",
        "postgresql": "PostgreSQL",
        "snowflake": "Snowflake",
    }
    engine = engine_names.get(dialect, dialect)

    sections.append(
        f"You are the Lead Database Performance Engineer (Post-Mortem Specialist) "
        f"for {engine}. You have just observed 4 parallel optimization workers race "
        f"against the original query (round {round_num}). Your job:\n\n"
        f"1. Diagnose WHY the winner won (or why no one won)\n"
        f"2. Diagnose WHY each loser lost — structural mechanisms, not surface descriptions\n"
        f"3. Synthesize refinement directives for each worker's next attempt\n\n"
        f"Target: >={target}x speedup. Anything below is a miss."
    )

    # ── 2. Race results table ────────────────────────────────────────────
    race_lines = ["## Race Results"]
    race_lines.append("")

    if race_timings:
        orig_ms = race_timings.get("original_ms", 0)
        has_winner = race_timings.get("has_clear_winner", False)
        race_lines.append(f"**Original**: {orig_ms:.0f}ms")
        race_lines.append("")
        race_lines.append("| Worker | Strategy | Time (ms) | Speedup | Status |")
        race_lines.append("|--------|----------|-----------|---------|--------|")

        sorted_results = sorted(worker_results, key=lambda w: w.speedup, reverse=True)
        for wr in sorted_results:
            cand_info = race_timings.get("candidates", {}).get(wr.worker_id, {})
            ms = cand_info.get("elapsed_ms", 0)
            finished = cand_info.get("finished", True)
            if not finished:
                race_lines.append(
                    f"| W{wr.worker_id} | {wr.strategy} | DNF | — | DID_NOT_FINISH |"
                )
            elif wr.error_message:
                race_lines.append(
                    f"| W{wr.worker_id} | {wr.strategy} | — | — | ERROR |"
                )
            else:
                spd = f"{wr.speedup:.2f}x"
                status = wr.status
                race_lines.append(
                    f"| W{wr.worker_id} | {wr.strategy} | {ms:.0f} | {spd} | {status} |"
                )
        race_lines.append("")
        if not has_winner:
            race_lines.append(
                "**No clear winner** — all candidates within margin or slower."
            )
    else:
        # Sequential validation fallback — no race data
        race_lines.append("(Sequential validation — no race timing data)")
        race_lines.append("")
        race_lines.append("| Worker | Strategy | Speedup | Status |")
        race_lines.append("|--------|----------|---------|--------|")
        sorted_results = sorted(worker_results, key=lambda w: w.speedup, reverse=True)
        for wr in sorted_results:
            spd = f"{wr.speedup:.2f}x"
            race_lines.append(
                f"| W{wr.worker_id} | {wr.strategy} | {spd} | {wr.status} |"
            )
    sections.append("\n".join(race_lines))

    # ── 2b. Original query vital signs (baseline) ────────────────────────
    if original_vital_signs:
        sections.append(
            "## Original Query Vital Signs (baseline)\n\n"
            "This is WHY the original query takes the time it does.\n"
            "```\n" + original_vital_signs + "\n```"
        )

    # ── 3. Per-worker vital signs ────────────────────────────────────────
    if vital_signs:
        vs_lines = ["## Per-Worker Vital Signs (condensed EXPLAIN)"]
        vs_lines.append("")
        for wid in sorted(vital_signs.keys()):
            vs_text = vital_signs[wid]
            vs_lines.append(f"### W{wid}")
            vs_lines.append("```")
            vs_lines.append(vs_text)
            vs_lines.append("```")
            vs_lines.append("")
        sections.append("\n".join(vs_lines))

    # ── 4. Per-worker SQL + config (compact) ─────────────────────────────
    sql_lines = ["## Per-Worker Optimized SQL"]
    sql_lines.append("")
    for wr in sorted(worker_results, key=lambda w: w.worker_id):
        sql_lines.append(f"### W{wr.worker_id}: {wr.strategy}")
        if wr.transforms:
            sql_lines.append(f"Transforms: {', '.join(wr.transforms)}")
        # SET LOCAL commands (critical for PG — coach must see config changes)
        set_local = wr.set_local_commands
        if set_local:
            sql_lines.append(f"SET LOCAL: {'; '.join(set_local)}")
        if wr.error_message:
            sql_lines.append(f"Error: {wr.error_message[:120]}")
        sql_text = wr.optimized_sql.strip()
        if sql_text:
            sql_lines.append("```sql")
            sql_lines.append(sql_text)
            sql_lines.append("```")
        sql_lines.append("")
    sections.append("\n".join(sql_lines))

    # ── 5. Engine profile (compact) ──────────────────────────────────────
    if engine_profile:
        ep_lines = ["## Engine Profile (reference)"]
        gaps = engine_profile.get("gaps", [])
        if gaps:
            ep_lines.append("")
            ep_lines.append("### Optimizer Gaps (opportunities)")
            for g in gaps:
                gid = g.get("id", "")
                what = g.get("what", "")
                ep_lines.append(f"- **{gid}**: {what}")
        sections.append("\n".join(ep_lines))

    # ── 6. Original SQL ──────────────────────────────────────────────────
    sections.append(
        "## Original SQL\n\n"
        "```sql\n"
        + original_sql.strip() + "\n"
        "```"
    )

    # ── 7. Analysis protocol + output format ─────────────────────────────
    sections.append(
        "## Your Task\n\n"
        "Analyze the race results and vital signs above, then produce refinement "
        "directives for each worker.\n\n"
        "### Analysis Protocol\n\n"
        "1. **Compare against baseline**: Using the Original Query Vital Signs, "
        "identify which bottleneck operators each worker addressed (or failed to address).\n"
        "2. **Identify winner**: Which worker had the best result? Why did its "
        "strategy succeed at the execution level?\n"
        "3. **Diagnose losers**: For each non-winning worker, identify the specific "
        "execution bottleneck (from vital signs) that prevented a win.\n"
        "4. **Synthesize hybrid**: Can elements from multiple workers be combined? "
        "Did any worker's approach reveal an opportunity the winner missed?\n"
        "5. **Produce directives**: For each worker, produce a specific refinement "
        "directive for the next round.\n\n"
        "### Output Format\n\n"
        "Produce exactly 4 refinement directives, one per worker:\n\n"
        "```\n"
        "=== REFINEMENT DIRECTIVE FOR WORKER 1 ===\n"
        "DIAGNOSIS: <why this worker's approach performed as it did — reference "
        "specific operators from vital signs>\n"
        "KEEP: <what to preserve from this worker's approach>\n"
        "CHANGE: <specific structural SQL changes for the next round — be concrete "
        "about CTE structure, join order, predicate placement>\n"
        "INCORPORATE: <elements from other workers to adopt, with rationale>\n"
        "PRIORITY: high|medium|low (likelihood of reaching target)\n\n"
        "=== REFINEMENT DIRECTIVE FOR WORKER 2 ===\n"
        "...\n\n"
        "=== REFINEMENT DIRECTIVE FOR WORKER 3 ===\n"
        "...\n\n"
        "=== REFINEMENT DIRECTIVE FOR WORKER 4 ===\n"
        "...\n"
        "```\n\n"
        "Each directive must be actionable — the worker will use it as its primary "
        "guidance for the next rewrite attempt. The CHANGE field must contain enough "
        "structural detail that the worker can produce a complete rewrite from it."
    )

    return "\n\n".join(sections)


def build_coach_refinement_prefix(
    base_prefix: str,
    coach_directives: str,
    round_results_summary: str,
    round_num: int = 1,
) -> str:
    """Build the round 2+ shared prefix by appending coach output to the base prefix.

    For round 1 coach, base_prefix is the original shared prefix from fan-out.
    For round 2+ coach, base_prefix is the PREVIOUS round's refinement prefix,
    so context accumulates across rounds.

    This preserves cache hits on the earlier prefix portions.

    Args:
        base_prefix: The prefix from the previous round (shared prefix or prior refinement)
        coach_directives: Full coach output (all 4 refinement directives)
        round_results_summary: Compact summary of race results + vital signs
        round_num: Which coach round produced these directives (1-based)

    Returns:
        Extended shared prefix for round 2+ workers
    """
    refinement_section = (
        "\n\n"
        f"## Round {round_num} Results\n\n"
        + round_results_summary
        + "\n\n"
        f"## Coach Round {round_num} Refinement Directives\n\n"
        "The lead performance engineer has reviewed all 4 workers' results and "
        "produced specific refinement directives for each. These directives are "
        "AUTHORITATIVE — where they conflict with the original Worker Briefings, "
        "follow the directive.\n\n"
        + coach_directives
    )

    return base_prefix + refinement_section
