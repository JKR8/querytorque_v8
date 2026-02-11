"""Distill transforms.json + trials.jsonl into a compact LLM system prompt.

Assembles structured evidence from SQL optimization experiments and sends
it to an LLM to produce a distilled system prompt for rewrite workers.

Usage:
    PYTHONPATH=packages/qt-shared:packages/qt-sql:. python research/detection/distill.py [--engine duckdb|postgresql] [--output PATH]
"""

from __future__ import annotations

import argparse
import json
import sys
from collections import defaultdict
from pathlib import Path

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------
_HERE = Path(__file__).resolve().parent
_TRANSFORMS_PATH = _HERE / "transforms.json"
_TRIALS_PATH = _HERE / "trials.jsonl"
_PROJECT_ROOT = _HERE.parent.parent
_PROFILES_DIR = _PROJECT_ROOT / "packages" / "qt-sql" / "qt_sql" / "constraints"

# Gap-based worker routing: each worker owns optimizer gaps.
# Workers read the distilled prompt and focus on transforms tagged for them.
#
# DuckDB:
#   W1: CTE predicate injection — CROSS_CTE_PREDICATE_BLINDNESS (highest win rate)
#   W2: Scan consolidation     — REDUNDANT_SCAN_ELIMINATION + CROSS_COLUMN_OR_DECOMPOSITION
#   W3: Decorrelation          — CORRELATED_SUBQUERY_PARALYSIS + standalones (needs EXPLAIN check)
#   W4: Exploration            — UNION_CTE_SELF_JOIN_DECOMPOSITION + novel combos + guard rail QA
#
# PostgreSQL:
#   W1: CTE predicate injection — CROSS_CTE_PREDICATE_BLINDNESS + COMMA_JOIN_WEAKNESS
#   W2: Scan reduction          — NON_EQUI_JOIN_INPUT_BLINDNESS
#   W3: Decorrelation           — CORRELATED_SUBQUERY_PARALYSIS
#   W4: Exploration             — novel combos + guard rail QA

GAP_WORKER_MAP: dict[str, dict[str | None, str]] = {
    "duckdb": {
        "CROSS_CTE_PREDICATE_BLINDNESS": "W1",
        "REDUNDANT_SCAN_ELIMINATION": "W2",
        "CROSS_COLUMN_OR_DECOMPOSITION": "W2",
        "CORRELATED_SUBQUERY_PARALYSIS": "W3",
        "UNION_CTE_SELF_JOIN_DECOMPOSITION": "W4",
        None: "W3",  # standalones → W3 (decorrelation + misc)
    },
    "postgresql": {
        "CROSS_CTE_PREDICATE_BLINDNESS": "W1",
        "COMMA_JOIN_WEAKNESS": "W1",
        "NON_EQUI_JOIN_INPUT_BLINDNESS": "W2",
        "CORRELATED_SUBQUERY_PARALYSIS": "W3",
        None: "W3",  # standalones → W3
    },
}

WORKER_LABELS: dict[str, dict[str, str]] = {
    "duckdb": {
        "W1": "W1: CTE predicate injection",
        "W2": "W2: Scan consolidation",
        "W3": "W3: Decorrelation",
        "W4": "W4: Exploration",
    },
    "postgresql": {
        "W1": "W1: CTE predicate injection",
        "W2": "W2: Scan reduction",
        "W3": "W3: Decorrelation",
        "W4": "W4: Exploration",
    },
}


def _load_transforms(engine: str) -> list[dict]:
    """Load transforms.json, filtered by engine."""
    with open(_TRANSFORMS_PATH) as f:
        all_transforms = json.load(f)
    return [t for t in all_transforms if engine in t.get("engines", [])]


def _load_trials(engine: str) -> list[dict]:
    """Load trials.jsonl (all lines), filtered by engine."""
    trials: list[dict] = []
    with open(_TRIALS_PATH) as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            trial = json.loads(line)
            if trial.get("engine") == engine:
                trials.append(trial)
    return trials


def _load_engine_strengths(engine: str) -> list[dict]:
    """Load strengths from engine profile JSON."""
    profile_path = _PROFILES_DIR / f"engine_profile_{engine}.json"
    with open(profile_path) as f:
        profile = json.load(f)
    return profile.get("strengths", [])


# ---------------------------------------------------------------------------
# Data assembly
# ---------------------------------------------------------------------------

def _assemble_distillation_context(engine: str) -> dict:
    """Assemble all data needed for distillation, grouped by gap."""
    transforms = _load_transforms(engine)
    trials = _load_trials(engine)
    strengths = _load_engine_strengths(engine)

    # Index trials by transform id
    trials_by_transform: dict[str, list[dict]] = defaultdict(list)
    for trial in trials:
        trials_by_transform[trial["transform"]].append(trial)

    # Build enriched transforms with trial data
    enriched: list[dict] = []
    for t in transforms:
        tid = t["id"]
        matching_trials = trials_by_transform.get(tid, [])
        ratios = [tr["ratio"] for tr in matching_trials]
        best_ratio = max(ratios) if ratios else None
        avg_ratio = sum(ratios) / len(ratios) if ratios else None
        win_count = len(ratios)
        all_queries: list[str] = []
        for tr in matching_trials:
            all_queries.extend(tr.get("queries") or [])

        # Assign worker from gap
        gap = t.get("gap")
        engine_map = GAP_WORKER_MAP.get(engine, {})
        worker = engine_map.get(gap, engine_map.get(None, "W4"))

        enriched.append({
            "id": tid,
            "principle": t["principle"],
            "notes": t.get("notes", ""),
            "contraindications": t.get("contraindications", []),
            "min_baseline_ms": t.get("min_baseline_ms"),
            "confirm_with_explain": t.get("confirm_with_explain", False),
            "gap": gap,
            "trial_best": best_ratio,
            "trial_avg": avg_ratio,
            "trial_count": win_count,
            "trial_queries": sorted(set(all_queries)),
            "worker": worker,
        })

    # Group by gap
    gaps: dict[str, list[dict]] = defaultdict(list)
    standalone: list[dict] = []
    for entry in enriched:
        gap = entry["gap"]
        if gap:
            gaps[gap].append(entry)
        else:
            standalone.append(entry)

    # Sort transforms within each gap by trial_avg descending (most reliable first)
    for gap_id in gaps:
        gaps[gap_id].sort(key=lambda x: x["trial_avg"] or 0, reverse=True)
    standalone.sort(key=lambda x: x["trial_avg"] or 0, reverse=True)

    # Count transforms per worker for summary
    worker_counts: dict[str, int] = defaultdict(int)
    for entry in enriched:
        worker_counts[entry["worker"]] += 1

    return {
        "engine": engine,
        "strengths": strengths,
        "gaps": dict(gaps),
        "standalone": standalone,
        "transform_count": len(enriched),
        "trial_count": len(trials),
        "worker_counts": dict(worker_counts),
    }


# ---------------------------------------------------------------------------
# Prompt builder
# ---------------------------------------------------------------------------

def _render_strengths(strengths: list[dict]) -> str:
    lines: list[str] = []
    for i, s in enumerate(strengths, 1):
        lines.append(f"{i}. **{s['id']}**: {s['summary']}")
        if s.get("field_note"):
            lines.append(f"   > {s['field_note']}")
    return "\n".join(lines)


def _render_contraindications(contras: list[dict]) -> str:
    if not contras:
        return "  (none)"
    lines: list[str] = []
    for c in contras:
        ratio_str = f" (worst: {c['worst_ratio']}x)" if c.get("worst_ratio") else ""
        lines.append(f"  - [{c['severity']}] {c['instruction']}{ratio_str}")
    return "\n".join(lines)


def _render_transform(t: dict) -> str:
    best_str = f"{t['trial_best']:.2f}x" if t["trial_best"] else "N/A"
    avg_str = f"{t['trial_avg']:.2f}x" if t["trial_avg"] else "N/A"
    count = t["trial_count"]
    queries_str = ", ".join(t["trial_queries"]) if t["trial_queries"] else "no trials"

    parts = [
        f"### {t['id']}  [{t['worker']}]",
        f"- **Stats**: {count} win{'s' if count != 1 else ''}, avg {avg_str}, best {best_str} (queries: {queries_str})",
        f"- **Principle**: {t['principle']}",
    ]
    if t["min_baseline_ms"]:
        parts.append(f"- min_baseline_ms: {t['min_baseline_ms']}")
    if t["confirm_with_explain"]:
        parts.append(f"- confirm_with_explain: yes")
    parts.append(f"- **Notes**: {t['notes']}")
    parts.append(f"- **Contraindications**:")
    parts.append(_render_contraindications(t["contraindications"]))
    return "\n".join(parts)


def _build_distillation_prompt(context: dict) -> str:
    engine = context["engine"]
    n_transforms = context["transform_count"]
    n_trials = context["trial_count"]

    # Render strengths
    strengths_text = _render_strengths(context["strengths"])

    # Render gaps
    gaps_text_parts: list[str] = []
    for gap_id, transforms in sorted(context["gaps"].items()):
        gaps_text_parts.append(f"## Gap: {gap_id}")
        for t in transforms:
            gaps_text_parts.append(_render_transform(t))
        gaps_text_parts.append("")
    gaps_text = "\n".join(gaps_text_parts)

    # Render standalone
    standalone_text_parts: list[str] = []
    for t in context["standalone"]:
        standalone_text_parts.append(_render_transform(t))
    standalone_text = "\n".join(standalone_text_parts) if standalone_text_parts else "(none)"

    # Build worker routing table for the prompt
    labels = WORKER_LABELS.get(engine, {})
    routing_lines = "\n".join(f"- {labels[w]}" for w in sorted(labels))

    prompt = f"""\
You are distilling field evidence from {n_trials} SQL optimization trials
covering {n_transforms} transforms on {engine} into a compact system prompt
for an LLM-based SQL rewrite worker.

This prompt will be injected into a 4-worker swarm. Each worker owns
specific optimizer gaps. Each transform below has a [WN] tag showing
which worker owns it. A worker reads this document and focuses on
transforms tagged for it.

### Worker Routing
{routing_lines}

Each transform already has a [WN] tag in the input data below.
Preserve these tags EXACTLY in the output so workers can scan for their assignments.

## TARGET FORMAT

Produce a Markdown document with these sections:

1. **ENGINE STRENGTHS** — one bullet per strength: name, what it does, the "do NOT" rule.
   Use this format exactly:
   N. **NAME**: Summary sentence. **Do NOT** anti-pattern sentence.

2. **CORRECTNESS RULES** — 4 bullet points: row count, NULL handling, ORDER BY, LIMIT.

3. **OPTIMIZER GAPS** — grouped by gap ID. For each gap:
   - 2-line header: what the gap is, what the opportunity is
   - Transforms ordered by avg reliability (win count and avg ratio provided)

   For each transform, use this EXACT compact format:
   **transform_name** [WN] — RELIABILITY tag, N wins, avg X.XXx
   1-line principle.
   - checkmark wins: X.XXx on QN — what was done
   - cross losses: X.XXx on QN — what went wrong (extract from Notes, look for "Caused X.XXx" patterns)
   - Guard: specific constraint (from contraindications)

   Reliability tags: HIGH if avg >= 2.0x, MEDIUM if avg >= 1.3x, LOW otherwise.

4. **STANDALONE TRANSFORMS** — same compact format for gap=null transforms.

5. **GLOBAL GUARD RAILS** — numbered list, max 10 rules. Each rule:
   "Never/Always X — caused Y.YYx on QN."
   Consolidate duplicates across transforms.

## RULES

- CONCISENESS IS CRITICAL. Target 120-150 lines total. Each transform = 3-5 lines max.
- Use inline checkmark/cross notation: "- ✓ Q6/Q11: 4.00x" and "- ✗ Q31: 0.49x"
- Include win count AND avg ratio (not just best) — workers need reliability signal
- Preserve the [WN] worker tag on every transform header line EXACTLY as given in input
- Extract regression data from "Notes" field — look for "Caused X.XXx on QNN" patterns
- Do NOT include original/rewritten SQL
- Do NOT include full notes text — distill into compact guidance

## REFERENCE STYLE

Here is the target style for a transform entry (from the hand-crafted reference):

**date_cte_isolate** [W1] — HIGH reliability, 2 wins, avg 4.00x
Extract date dimension lookups into CTE. Join instead of scalar subquery. Materializes once → tiny hash table.
- ✓ Q6/Q11: 4.00x — date filter into CTE
- ✗ Q25: 0.50x — 31ms baseline, CTE overhead exceeded savings
- ✗ Q67: 0.85x — CTE prevented ROLLUP/window pushdown
- Guard: Skip if baseline <100ms. Don't decompose efficient existing CTEs.

## INPUT DATA

### Engine: {engine}

### Engine Strengths
{strengths_text}

### Transforms by Gap
{gaps_text}

### Standalone Transforms
{standalone_text}
"""
    return prompt


# ---------------------------------------------------------------------------
# Distillation
# ---------------------------------------------------------------------------

def distill(engine: str, output_path: Path | None = None) -> Path:
    """Run the full distillation pipeline for one engine."""
    from qt_shared.llm.factory import create_llm_client

    print(f"[distill] Assembling context for {engine}...")
    context = _assemble_distillation_context(engine)
    print(f"  {context['transform_count']} transforms, {context['trial_count']} trials")
    print(f"  {len(context['gaps'])} gaps + {len(context['standalone'])} standalone")
    labels = WORKER_LABELS.get(engine, {})
    for w in sorted(context["worker_counts"]):
        label = labels.get(w, w)
        print(f"  {label}: {context['worker_counts'][w]} transforms")

    prompt = _build_distillation_prompt(context)
    prompt_chars = len(prompt)
    prompt_tokens_est = prompt_chars // 4
    print(f"  Prompt: ~{prompt_chars} chars (~{prompt_tokens_est} tokens)")

    client = create_llm_client()
    if client is None:
        print("[distill] ERROR: No LLM provider configured. Set QT_LLM_PROVIDER in .env")
        sys.exit(1)

    print(f"[distill] Calling LLM ({type(client).__name__})...")
    response = client.analyze(prompt)

    if output_path is None:
        output_path = _HERE / f"distilled_{engine}.md"

    output_path.write_text(response, encoding="utf-8")
    output_chars = len(response)
    output_tokens_est = output_chars // 4
    print(f"[distill] Saved to {output_path}")
    print(f"  Output: ~{output_chars} chars (~{output_tokens_est} tokens)")
    return output_path


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(
        description="Distill SQL optimization evidence into LLM system prompts"
    )
    parser.add_argument(
        "--engine",
        choices=["duckdb", "postgresql"],
        default=None,
        help="Engine to distill (default: both)",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=None,
        help="Output path (default: distilled_{engine}.md)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print the prompt instead of calling the LLM",
    )
    args = parser.parse_args()

    engines = [args.engine] if args.engine else ["duckdb", "postgresql"]

    for engine in engines:
        if args.dry_run:
            context = _assemble_distillation_context(engine)
            prompt = _build_distillation_prompt(context)
            print(f"=== {engine} prompt ({len(prompt)} chars) ===")
            print(prompt)
            print()
        else:
            distill(engine, output_path=args.output)


if __name__ == "__main__":
    main()
