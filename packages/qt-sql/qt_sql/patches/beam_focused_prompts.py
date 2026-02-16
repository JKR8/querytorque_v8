"""Beam Focused prompt builders — full strike package with R1 workers.

For heavy queries (top 20% by workload = 80% of runtime). R1 workers
get the full 9-section swarm-style briefing with creative latitude.
Multiple sorties (up to 5) with V4 sniper protocol.

Functions:
    build_focused_analyst_prompt()  — reuses existing tiered analyst
    build_focused_strike_prompt()   — R1 worker with full briefing
    build_focused_sniper_prompt()   — V4 protocol with sortie history
"""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)


# ── Data classes ──────────────────────────────────────────────────────────────

@dataclass
class FocusedTarget:
    """A deep optimization target from the focused analyst."""
    target_id: str
    family: str
    transform: str
    relevance_score: float
    hypothesis: str         # EXPLAIN-evidenced bottleneck diagnosis
    target_ir: str          # structural shape of optimized query
    recommended_examples: List[str]

    def to_dict(self) -> Dict[str, Any]:
        return {
            "target_id": self.target_id,
            "family": self.family,
            "transform": self.transform,
            "relevance_score": self.relevance_score,
            "hypothesis": self.hypothesis,
            "target_ir": self.target_ir,
            "recommended_examples": self.recommended_examples,
        }


@dataclass
class SortieResult:
    """Results from one sortie (iteration) for sniper context."""
    sortie: int
    strikes: List[Dict[str, Any]]  # [{strike_id, family, transform, speedup, status, error}]
    explains: Dict[str, str]       # {strike_id: explain_text}


# ── Focused Analyst Prompt ────────────────────────────────────────────────────

def build_focused_analyst_prompt(
    query_id: str,
    original_sql: str,
    explain_text: str,
    ir_node_map: str,
    gold_examples: Dict[str, Dict[str, Any]],
    dialect: str,
    intelligence_brief: str = "",
) -> str:
    """Build focused analyst prompt — delegates to existing tiered builder.

    The focused analyst is the same as the current beam tiered analyst:
    deep EXPLAIN diagnosis, 4 targets with structural IR maps.

    Args:
        query_id: Query identifier.
        original_sql: Full original SQL.
        explain_text: EXPLAIN ANALYZE output.
        ir_node_map: IR node map.
        gold_examples: Dict mapping family ID to gold example.
        dialect: SQL dialect.
        intelligence_brief: Engine profile + pathology tree.

    Returns:
        Complete analyst prompt.
    """
    from .beam_prompt_builder import build_beam_prompt_tiered

    return build_beam_prompt_tiered(
        query_id=query_id,
        original_sql=original_sql,
        explain_text=explain_text,
        ir_node_map=ir_node_map,
        all_5_examples=gold_examples,
        dialect=dialect,
        intelligence_brief=intelligence_brief,
    )


# ── Focused Strike Prompt (R1 worker, swarm-style briefing) ──────────────────

def build_focused_strike_prompt(
    original_sql: str,
    explain_text: str,
    target: FocusedTarget,
    gold_examples: List[Dict[str, Any]],
    dialect: str,
    engine_version: Optional[str] = None,
    output_columns: Optional[List[str]] = None,
    regression_warnings: str = "",
    active_constraints: str = "",
) -> str:
    """Build a focused strike prompt — R1 worker with full 9-section briefing.

    This is the swarm-style worker prompt adapted for beam focused mode.
    R1 workers get creative latitude within the structural blueprint.

    Args:
        original_sql: Full original SQL.
        explain_text: EXPLAIN ANALYZE output.
        target: The optimization target from the analyst.
        gold_examples: Loaded gold examples (full before/after SQL).
        dialect: SQL dialect.
        engine_version: Engine version string.
        output_columns: Expected output columns.
        regression_warnings: Known regression patterns to avoid.
        active_constraints: Engine-specific rules.

    Returns:
        Complete R1 worker prompt.
    """
    engine_names = {
        "duckdb": "DuckDB",
        "postgres": "PostgreSQL",
        "postgresql": "PostgreSQL",
        "snowflake": "Snowflake",
    }
    engine = engine_names.get(dialect, dialect)
    ver = f" v{engine_version}" if engine_version else ""

    sections = []

    # ── [1] Role + Assignment ─────────────────────────────────────────
    sections.append(
        f"## Role\n\n"
        f"You are a SQL rewrite engine for {engine}{ver}. "
        f"Preserve exact semantic equivalence (same rows, same columns, same ordering).\n\n"
        f"**Assignment:** Strategy: {target.transform} | "
        f"Family: {target.family} | "
        f"Confidence: {target.relevance_score:.0%}"
    )

    # ── [2] Semantic Contract ─────────────────────────────────────────
    sections.append(
        "## Semantic Contract (MUST preserve)\n\n"
        "- All WHERE/HAVING/ON conditions preserved exactly\n"
        "- Same column names, types, ordering in output\n"
        "- Same row count as original query\n"
        "- All literal values unchanged (35*0.01 stays as 35*0.01, NOT 0.35)\n"
        "- ORDER BY and LIMIT preserved exactly\n"
        "- No new filter conditions added"
    )

    # ── [3] Plan Gap (bottleneck diagnosis) ───────────────────────────
    sections.append(
        f"## Bottleneck Diagnosis\n\n"
        f"{target.hypothesis}"
    )

    # ── [4] Target Query Map ──────────────────────────────────────────
    if target.target_ir:
        sections.append(
            "## Target Query Map\n\n"
            "Build your rewrite following this structural blueprint. "
            "Each CTE and the main query should match this shape:\n\n"
            f"```\n{target.target_ir}\n```"
        )

    # ── [5] Hazard Flags + Regression Warnings ───────────────────────
    hazards = [
        "- Do NOT simplify arithmetic (35*0.01 stays as 35*0.01)",
        "- Do NOT convert INNER JOIN to LEFT JOIN or vice versa unless the target IR specifies it",
        "- Keep ORDER BY + LIMIT exactly as original",
    ]
    if regression_warnings:
        hazards.append(f"\n**Known regressions on similar queries:**\n{regression_warnings}")

    sections.append(
        "## Hazard Flags\n\n" + "\n".join(hazards)
    )

    # ── [6] Active Constraints ────────────────────────────────────────
    if active_constraints:
        sections.append(
            f"## Engine Constraints ({engine})\n\n{active_constraints}"
        )

    # ── [7] Example Adaptation ────────────────────────────────────────
    if gold_examples:
        example_lines = ["## Reference Examples\n"]
        example_lines.append(
            "Adapt these patterns to the query below. "
            "Do not copy table/column names or literal values.\n"
        )

        for i, ex in enumerate(gold_examples[:2]):  # Max 2 examples
            ex_id = ex.get("id", f"example_{i+1}")
            speedup = ex.get("verified_speedup", "")
            principle = ex.get("principle", "")

            example_lines.append(f"### {i+1}. {ex_id} ({speedup})")
            if principle:
                example_lines.append(f"**Principle:** {principle[:300]}\n")

            before_sql = ex.get("original_sql", "")
            after_sql = ex.get("optimized_sql", "")

            if before_sql:
                before_display = "\n".join(before_sql.strip().split("\n")[:20])
                example_lines.append(f"**BEFORE:**\n```sql\n{before_display}\n```\n")

            if after_sql:
                after_display = "\n".join(after_sql.strip().split("\n")[:20])
                example_lines.append(f"**AFTER:**\n```sql\n{after_display}\n```\n")

            # Key insight / adaptation notes
            key_insight = ex.get("key_insight", "")
            if key_insight:
                example_lines.append(f"**Key insight:** {key_insight}\n")

        sections.append("\n".join(example_lines))

    # ── [8] Original SQL + EXPLAIN ────────────────────────────────────
    sections.append(
        f"## Original SQL\n\n```sql\n{original_sql}\n```"
    )

    if explain_text:
        explain_lines = explain_text.strip().split("\n")[:60]
        explain_display = "\n".join(explain_lines)
        sections.append(
            f"## EXPLAIN Plan\n\n```\n{explain_display}\n```"
        )

    # ── [9] Output Format ─────────────────────────────────────────────
    output_section = [
        "## Output Format\n",
        "Output a single rewritten SQL query. No explanation, no markdown fences around it.",
        "The query must be complete and executable.",
    ]

    if output_columns:
        cols_str = ", ".join(f"`{c}`" for c in output_columns)
        output_section.append(
            f"\n**Column Contract:** Must produce exactly: {cols_str}"
        )

    output_section.append(
        "\nYou have latitude to make complementary changes that support the "
        "primary transform (e.g., explicit JOIN syntax, additional CTE "
        "materialization) as long as they don't violate the semantic contract."
    )

    sections.append("\n".join(output_section))

    return "\n\n".join(sections)


# ── Focused Sniper Prompt (V4 Protocol) ───────────────────────────────────────

def build_focused_sniper_prompt(
    query_id: str,
    original_sql: str,
    original_explain: str,
    sortie_history: List[SortieResult],
    gold_examples: Dict[str, Dict[str, Any]],
    dialect: str,
    intelligence_brief: str = "",
) -> str:
    """Build the focused sniper prompt — V4 protocol with full sortie history.

    The sniper sees ALL prior sorties in a compact table, plus detailed
    EXPLAIN plans from the latest sortie.

    Args:
        query_id: Query identifier.
        original_sql: Full original SQL.
        original_explain: EXPLAIN ANALYZE of original.
        sortie_history: Results from all prior sorties.
        gold_examples: Gold examples by family ID.
        dialect: SQL dialect.
        intelligence_brief: Engine profile summary.

    Returns:
        Complete sniper prompt.
    """
    sections = []

    current_sortie = len(sortie_history)
    total_strikes = sum(len(s.strikes) for s in sortie_history)
    total_wins = sum(
        1 for s in sortie_history for st in s.strikes
        if st.get("status") == "WIN"
    )

    # ── Role ──────────────────────────────────────────────────────────
    sections.append(
        "## Role\n\n"
        f"You are a strike commander for query {query_id} on "
        f"{dialect.upper().replace('POSTGRES', 'PostgreSQL')}. "
        f"This is sortie {current_sortie + 1}. "
        f"{total_strikes} strikes fired so far, {total_wins} hits. "
        "Design the next round of strikes based on BDA from all prior sorties."
    )

    # ── Original ──────────────────────────────────────────────────────
    sections.append(
        f"## Original SQL\n\n```sql\n{original_sql}\n```"
    )

    if original_explain:
        explain_lines = original_explain.strip().split("\n")[:40]
        sections.append(
            f"## EXPLAIN (Original)\n\n```\n" +
            "\n".join(explain_lines) + "\n```"
        )

    # ── Engine Intelligence ───────────────────────────────────────────
    if intelligence_brief:
        # Truncate to avoid bloat
        brief_lines = intelligence_brief.strip().split("\n")[:30]
        sections.append(
            "## Engine Intelligence\n\n" + "\n".join(brief_lines)
        )

    # ── Sortie History Table ──────────────────────────────────────────
    history_lines = [
        "## Sortie History\n",
        "| Sortie | Strike | Family | Transform | Speedup | Status |",
        "|--------|--------|--------|-----------|---------|--------|",
    ]
    for sr in sortie_history:
        for st in sr.strikes:
            speedup = st.get("speedup")
            speedup_str = f"{speedup:.2f}x" if speedup else "-"
            status = st.get("status", "?")
            error = st.get("error", "")
            if status == "FAIL" and error:
                status = f"FAIL: {error[:40]}"
            history_lines.append(
                f"| {sr.sortie} | {st.get('strike_id', '?')} | "
                f"{st.get('family', '?')} | {st.get('transform', '?')} | "
                f"{speedup_str} | {status} |"
            )
    sections.append("\n".join(history_lines))

    # ── Latest Sortie EXPLAIN Detail ──────────────────────────────────
    if sortie_history:
        latest = sortie_history[-1]
        if latest.explains:
            explain_detail = ["## EXPLAIN Plans (latest sortie)\n"]
            for strike_id, exp_text in latest.explains.items():
                # Find strike metadata
                strike_meta = next(
                    (st for st in latest.strikes if st.get("strike_id") == strike_id),
                    {}
                )
                speedup = strike_meta.get("speedup")
                transform = strike_meta.get("transform", "?")
                speedup_str = f"{speedup:.2f}x" if speedup else "?"

                exp_lines = exp_text.strip().split("\n")[:40]
                explain_detail.append(
                    f"### {strike_id}: {transform} ({speedup_str})\n"
                    f"```\n" + "\n".join(exp_lines) + "\n```\n"
                )
            sections.append("\n".join(explain_detail))

    # ── V4 Protocol Task ──────────────────────────────────────────────
    sections.append("""## V4 Strike Protocol

### Step 1: COMPARE EXPLAIN PLANS
- What operators changed between original and best strike?
- Where did row counts drop most significantly?
- What NEW bottleneck appeared after the optimization?
- Which strikes caused regressions and why?

### Step 2: DESIGN COMPOUND TARGETS
- **Build on wins**: Identify the best strike(s) and what made them work
- **Fix regressions**: If a family caused regression, understand why
  (wrong join order? missing filter? literal change?)
- **Compound strategies**: Combine winning transforms — e.g., if
  decorrelation (B) removed re-scans and early filtering (A) reduced
  input rows, try B+A together

### Step 3: OUTPUT 2 NEW TARGETS

```json
[
  {
    "target_id": "t1",
    "family": "B+A",
    "transform": "decorrelate_with_early_filter",
    "relevance_score": 0.95,
    "hypothesis": "Combine sortie 0's B decorrelation (removed 4810 re-scans) with A early filter (reduced hash join input by 40%)",
    "target_ir": "S0 [SELECT]\\n  CTE: filtered_items ...\\n  CTE: thresholds ...\\n  MAIN: ...",
    "recommended_examples": ["shared_scan_decorrelate", "early_filter_decorrelate"]
  }
]
```

Rules:
- Each target should combine insights from prior sorties
- Don't repeat strategies that already failed
- If a strategy worked at 1.3x, try to push it further
- Rank by expected impact""")

    return "\n\n".join(sections)
