#!/usr/bin/env python3
"""
Generate DSR2 recommendations by merging ALL optimization data sources.

Data sources:
1. State YAMLs (baseline, kimi, v2_standard, retry3w, dsr1)
2. Master CSV (Kimi, V2, Evolutionary, Retry3W SF5/SF10, DSR1)
3. Gold examples (15 patterns with verified speedups)
4. Feature-to-pattern mappings (query structure → applicable patterns)

Output: research/state/state_1/DSR2_RECOMMENDATIONS.md
"""

import csv
import json
import yaml
import sys
from pathlib import Path
from dataclasses import dataclass, field
from typing import List, Dict, Optional, Set, Tuple

PROJECT = Path("/mnt/c/Users/jakc9/Documents/QueryTorque_V8")
sys.path.insert(0, str(PROJECT / "packages" / "qt-sql"))

STATE_DIR = PROJECT / "research" / "state_histories_all_99"
MASTER_CSV = PROJECT / "research" / "CONSOLIDATED_BENCHMARKS" / "DuckDB_TPC-DS_Master_v3_20260206.csv"
EXAMPLES_DIR = PROJECT / "packages" / "qt-sql" / "qt_sql" / "optimization" / "examples"
OUTPUT = PROJECT / "research" / "state" / "state_1" / "DSR2_RECOMMENDATIONS.md"


# ============================================================================
# Feature-to-pattern mapping (same as generate_prompts_v2.py)
# ============================================================================

TPCDS_QUERY_FEATURES = {
    1:  ["correlated_subquery", "date_filter", "dim_fact_chain"],
    2:  ["date_filter", "complex_multi_join"],
    3:  ["date_filter", "dim_fact_chain"],
    4:  ["multi_date_alias", "complex_multi_join", "correlated_subquery"],
    5:  ["date_filter", "union_year", "multi_dim_filter"],
    6:  ["correlated_subquery", "date_filter"],
    7:  ["date_filter", "multi_dim_filter", "dim_fact_chain"],
    8:  ["correlated_subquery", "date_filter"],
    9:  ["repeated_scan"],
    10: ["date_filter", "multi_dim_filter", "exists_repeat"],
    11: ["correlated_subquery", "date_filter", "multi_date_alias"],
    12: ["date_filter", "dim_fact_chain"],
    13: ["multi_dim_filter", "dim_fact_chain"],
    14: ["intersect", "date_filter", "complex_multi_join"],
    15: ["or_condition", "date_filter"],
    16: ["date_filter", "exists_repeat", "complex_multi_join"],
    17: ["multi_date_alias", "dim_fact_chain"],
    18: ["date_filter", "multi_dim_filter"],
    19: ["date_filter", "multi_dim_filter", "dim_fact_chain"],
    20: ["date_filter", "dim_fact_chain"],
    21: ["date_filter", "dim_fact_chain"],
    22: ["date_filter", "window_fn"],
    23: ["correlated_subquery", "date_filter", "complex_multi_join"],
    24: ["correlated_subquery", "complex_multi_join"],
    25: ["multi_date_alias", "dim_fact_chain"],
    26: ["multi_dim_filter", "dim_fact_chain"],
    27: ["multi_dim_filter", "dim_fact_chain"],
    28: ["repeated_scan"],
    29: ["multi_date_alias", "dim_fact_chain"],
    30: ["correlated_subquery", "date_filter"],
    31: ["date_filter", "complex_multi_join"],
    32: ["correlated_subquery", "date_filter"],
    33: ["date_filter", "multi_dim_filter", "union_year"],
    34: ["multi_dim_filter", "dim_fact_chain"],
    35: ["date_filter", "exists_repeat", "multi_dim_filter"],
    36: ["date_filter", "multi_dim_filter", "dim_fact_chain"],
    37: ["date_filter", "multi_dim_filter", "dim_fact_chain"],
    38: ["date_filter", "intersect", "complex_multi_join"],
    39: ["date_filter", "dim_fact_chain"],
    40: ["date_filter", "dim_fact_chain"],
    41: ["correlated_subquery"],
    42: ["date_filter", "dim_fact_chain"],
    43: ["date_filter", "multi_dim_filter", "dim_fact_chain"],
    44: ["repeated_scan", "window_fn"],
    45: ["or_condition", "date_filter", "correlated_subquery"],
    46: ["multi_dim_filter", "dim_fact_chain"],
    47: ["date_filter", "dim_fact_chain", "window_fn"],
    48: ["multi_dim_filter", "or_condition"],
    49: ["date_filter", "union_year", "window_fn"],
    50: ["date_filter", "dim_fact_chain"],
    51: ["date_filter", "window_fn"],
    52: ["date_filter", "dim_fact_chain"],
    53: ["date_filter", "dim_fact_chain"],
    54: ["date_filter", "complex_multi_join"],
    55: ["date_filter", "dim_fact_chain"],
    56: ["multi_dim_filter", "union_year"],
    57: ["date_filter", "dim_fact_chain", "window_fn"],
    58: ["date_filter", "multi_date_alias"],
    59: ["date_filter", "complex_multi_join"],
    60: ["date_filter", "multi_dim_filter", "union_year"],
    61: ["date_filter", "multi_dim_filter"],
    62: ["date_filter", "dim_fact_chain"],
    63: ["date_filter", "dim_fact_chain"],
    64: ["complex_multi_join", "multi_date_alias"],
    65: ["date_filter", "dim_fact_chain"],
    66: ["date_filter", "multi_dim_filter", "union_year"],
    67: ["date_filter", "dim_fact_chain", "window_fn"],
    68: ["multi_dim_filter", "dim_fact_chain"],
    69: ["multi_dim_filter", "dim_fact_chain"],
    70: ["date_filter", "window_fn", "correlated_subquery"],
    71: ["date_filter", "multi_dim_filter", "dim_fact_chain"],
    72: ["multi_date_alias", "multi_dim_filter", "complex_multi_join"],
    73: ["multi_dim_filter", "dim_fact_chain"],
    74: ["union_year", "date_filter"],
    75: ["date_filter", "union_year"],
    76: ["date_filter", "or_condition", "union_year"],
    77: ["date_filter", "union_year"],
    78: ["date_filter", "complex_multi_join"],
    79: ["multi_dim_filter", "dim_fact_chain"],
    80: ["date_filter", "multi_date_alias", "union_year"],
    81: ["correlated_subquery", "date_filter"],
    82: ["multi_dim_filter", "dim_fact_chain"],
    83: ["multi_dim_filter", "dim_fact_chain"],
    84: ["multi_dim_filter", "dim_fact_chain"],
    85: ["multi_dim_filter", "dim_fact_chain"],
    86: ["multi_dim_filter", "dim_fact_chain"],
    87: ["date_filter", "multi_dim_filter", "complex_multi_join"],
    88: ["repeated_scan", "or_condition"],
    89: ["date_filter", "dim_fact_chain"],
    90: ["date_filter", "dim_fact_chain"],
    91: ["date_filter", "dim_fact_chain"],
    92: ["date_filter", "dim_fact_chain"],
    93: ["dim_fact_chain"],
    94: ["date_filter", "exists_repeat", "complex_multi_join"],
    95: ["exists_repeat", "date_filter"],
    96: ["multi_dim_filter", "dim_fact_chain"],
    97: ["date_filter", "union_year"],
    98: ["date_filter", "dim_fact_chain"],
    99: ["date_filter", "multi_dim_filter"],
}

FEATURE_TO_PATTERN = {
    "correlated_subquery": ["decorrelate", "composite_decorrelate_union", "date_cte_isolate"],
    "date_filter":         ["date_cte_isolate", "prefetch_fact_join"],
    "multi_date_alias":    ["multi_date_range_cte"],
    "or_condition":        ["or_to_union", "composite_decorrelate_union"],
    "multi_dim_filter":    ["dimension_cte_isolate", "multi_dimension_prefetch", "early_filter", "shared_dimension_multi_channel"],
    "dim_fact_chain":      ["prefetch_fact_join", "early_filter", "multi_dimension_prefetch", "shared_dimension_multi_channel"],
    "repeated_scan":       ["single_pass_aggregation", "pushdown"],
    "intersect":           ["intersect_to_exists"],
    "union_year":          ["union_cte_split"],
    "exists_repeat":       ["materialize_cte", "composite_decorrelate_union"],
    "complex_multi_join":  [],
    "window_fn":           [],
}

# Gold example verified speedups
GOLD_SPEEDUPS = {
    "single_pass_aggregation": 4.47,
    "date_cte_isolate": 4.00,
    "early_filter": 4.00,
    "prefetch_fact_join": 3.77,
    "or_to_union": 3.17,
    "decorrelate": 2.92,
    "multi_dimension_prefetch": 2.71,
    "composite_decorrelate_union": 2.42,
    "multi_date_range_cte": 2.35,
    "pushdown": 2.11,
    "dimension_cte_isolate": 1.93,
    "intersect_to_exists": 1.83,
    "materialize_cte": 1.37,
    "union_cte_split": 1.36,
    "shared_dimension_multi_channel": 1.30,
}


@dataclass
class Attempt:
    """A single optimization attempt."""
    state_id: str
    speedup: float
    model: str
    transforms: List[str]
    status: str  # success, neutral, regression, error
    error: Optional[str] = None


@dataclass
class QueryState:
    """Complete state for one query."""
    query_num: int
    baseline_ms: float
    attempts: List[Attempt]
    best_speedup: float
    best_attempt: str
    best_transforms: List[str]
    transforms_tried: Set[str]
    transforms_succeeded: Set[str]  # speedup >= 1.1
    transforms_failed: Set[str]    # speedup < 0.95 or error
    features: List[str]
    applicable_patterns: List[str]
    untried_patterns: List[str]


def load_master_csv() -> Dict[int, dict]:
    """Load the master CSV with all results."""
    rows = {}
    with open(MASTER_CSV) as f:
        reader = csv.DictReader(f)
        for row in reader:
            q = int(row["Query_Num"])
            rows[q] = row
    return rows


def load_state_yamls() -> Dict[int, list]:
    """Load all state history YAMLs."""
    states = {}
    for q in range(1, 100):
        path = STATE_DIR / f"q{q}_state_history.yaml"
        if not path.exists():
            continue
        with open(path) as f:
            data = yaml.safe_load(f)
        states[q] = data.get("states", [])
    return states


def build_query_state(q: int, yaml_states: list, csv_row: dict) -> QueryState:
    """Build complete query state from all sources."""
    # Build attempts from YAML (has the most complete chain)
    attempts = []
    for s in yaml_states:
        sid = s.get("state_id", "unknown")
        if sid == "baseline":
            continue
        speedup = float(s.get("speedup", 1.0))
        transforms = [t for t in s.get("transforms", []) if t and t.strip()]
        status = s.get("status", "unknown")
        model = s.get("model", "unknown")

        attempts.append(Attempt(
            state_id=sid,
            speedup=speedup,
            model=model or "unknown",
            transforms=transforms,
            status=status,
            error=s.get("error"),
        ))

    # Add retry3w data from CSV if not in YAML
    yaml_ids = {a.state_id for a in attempts}
    if "retry3w" not in "_".join(yaml_ids):
        sf10 = csv_row.get("Retry3W_SF10_Speedup", "")
        if sf10 and sf10 not in ("", "0", "0.0"):
            try:
                sf10_val = float(sf10)
                if sf10_val > 0:
                    best_w = csv_row.get("Retry3W_Best_Worker", "?")
                    w1 = csv_row.get("Retry3W_W1", "")
                    w2 = csv_row.get("Retry3W_W2", "")
                    w3 = csv_row.get("Retry3W_W3", "")
                    status_3w = csv_row.get("Retry3W_Status", "unknown")
                    if status_3w == "pass" or sf10_val >= 1.0:
                        attempts.append(Attempt(
                            state_id=f"retry3w_w{best_w}",
                            speedup=sf10_val,
                            model="v2_evolutionary",
                            transforms=[],  # Not tracked in CSV
                            status="success" if sf10_val >= 1.1 else "neutral" if sf10_val >= 0.95 else "regression",
                        ))
            except ValueError:
                pass

    # Baseline runtime from DSR1 validation (most recent measurement)
    dsr1_orig = csv_row.get("DSR1_Original_ms", "0")
    kimi_orig = csv_row.get("Kimi_Original_ms", "0")
    try:
        baseline_ms = float(dsr1_orig) if dsr1_orig else float(kimi_orig) if kimi_orig else 0
    except ValueError:
        baseline_ms = 0

    # Collect transforms tried/succeeded/failed
    transforms_tried = set()
    transforms_succeeded = set()
    transforms_failed = set()
    best_speedup = 1.0
    best_attempt = "baseline"
    best_transforms = []

    for a in attempts:
        for t in a.transforms:
            if t and t not in ("unknown", ""):
                transforms_tried.add(t)
                if a.speedup >= 1.1:
                    transforms_succeeded.add(t)
                elif a.status == "error" or a.speedup < 0.95:
                    transforms_failed.add(t)
        if a.speedup > best_speedup:
            best_speedup = a.speedup
            best_attempt = a.state_id
            best_transforms = a.transforms

    # Get applicable patterns from features
    features = TPCDS_QUERY_FEATURES.get(q, [])
    applicable = []
    seen = set()
    for feat in features:
        for pat in FEATURE_TO_PATTERN.get(feat, []):
            if pat not in seen:
                applicable.append(pat)
                seen.add(pat)

    # Untried = applicable and not in transforms_tried
    untried = [p for p in applicable if p not in transforms_tried]

    return QueryState(
        query_num=q,
        baseline_ms=baseline_ms,
        attempts=attempts,
        best_speedup=best_speedup,
        best_attempt=best_attempt,
        best_transforms=best_transforms,
        transforms_tried=transforms_tried,
        transforms_succeeded=transforms_succeeded,
        transforms_failed=transforms_failed,
        features=features,
        applicable_patterns=applicable,
        untried_patterns=untried,
    )


def classify(speedup: float) -> str:
    if speedup >= 1.5:
        return "WIN"
    elif speedup >= 1.1:
        return "IMPROVED"
    elif speedup >= 0.95:
        return "NEUTRAL"
    else:
        return "REGRESSION"


def format_attempt_chain(qs: QueryState) -> str:
    """Format the optimization chain: base -> attempt -> attempt."""
    parts = ["1.00x (baseline)"]
    for a in qs.attempts:
        t_str = "+".join(a.transforms) if a.transforms else "unknown"
        status_icon = {"success": "+", "neutral": "=", "regression": "-", "error": "X"}.get(a.status, "?")
        parts.append(f"{a.speedup:.2f}x ({a.state_id}/{t_str}) [{status_icon}]")
    return " -> ".join(parts)


def get_recommendations(qs: QueryState) -> List[Tuple[str, float, str]]:
    """Get recommended patterns for this query, ordered by expected value.

    Returns: [(pattern_id, gold_speedup, reason)]
    """
    recs = []
    for pat in qs.untried_patterns:
        if pat in qs.transforms_failed:
            continue  # Skip known failures
        gold_speed = GOLD_SPEEDUPS.get(pat, 1.0)
        reason = f"Untried, gold {gold_speed:.2f}x"
        recs.append((pat, gold_speed, reason))

    # Sort by gold speedup descending
    recs.sort(key=lambda x: x[1], reverse=True)
    return recs[:5]


def compute_transform_stats(all_states: Dict[int, QueryState]) -> Dict[str, dict]:
    """Compute per-transform success rates across all queries."""
    stats = {}
    for qs in all_states.values():
        for a in qs.attempts:
            for t in a.transforms:
                if not t or t in ("unknown", ""):
                    continue
                if t not in stats:
                    stats[t] = {"total": 0, "wins": 0, "improved": 0, "neutral": 0, "regression": 0, "error": 0, "speedups": []}
                stats[t]["total"] += 1
                if a.status == "error":
                    stats[t]["error"] += 1
                elif a.speedup >= 1.1:
                    stats[t]["wins"] += 1
                    stats[t]["speedups"].append(a.speedup)
                elif a.speedup >= 0.95:
                    stats[t]["neutral"] += 1
                    stats[t]["speedups"].append(a.speedup)
                else:
                    stats[t]["regression"] += 1
                    stats[t]["speedups"].append(a.speedup)
    return stats


def main():
    print("Loading data sources...", file=sys.stderr)
    csv_data = load_master_csv()
    yaml_data = load_state_yamls()

    # Build unified states
    all_states: Dict[int, QueryState] = {}
    for q in range(1, 100):
        yaml_states = yaml_data.get(q, [])
        csv_row = csv_data.get(q, {})
        all_states[q] = build_query_state(q, yaml_states, csv_row)

    # Compute transform statistics
    t_stats = compute_transform_stats(all_states)

    # Sort queries into tiers
    tier1 = []  # HIGH VALUE: long runtime + room for improvement
    tier2 = []  # MEDIUM VALUE: already improved but could go higher
    tier3 = []  # LOW VALUE: already winning or fast queries

    for q, qs in sorted(all_states.items()):
        cat = classify(qs.best_speedup)
        if cat in ("NEUTRAL", "REGRESSION") or (cat == "IMPROVED" and qs.baseline_ms > 100):
            tier1.append(qs)
        elif cat == "IMPROVED" or (cat == "WIN" and qs.baseline_ms > 500):
            tier2.append(qs)
        else:
            tier3.append(qs)

    # Sort tiers by baseline runtime (longest first = highest value)
    tier1.sort(key=lambda qs: qs.baseline_ms, reverse=True)
    tier2.sort(key=lambda qs: qs.baseline_ms, reverse=True)
    tier3.sort(key=lambda qs: qs.baseline_ms, reverse=True)

    # Count DSR1 classifications
    dsr1_cats = {"WIN": 0, "IMPROVED": 0, "NEUTRAL": 0, "REGRESSION": 0, "ERROR": 0}
    for qs in all_states.values():
        dsr1 = [a for a in qs.attempts if a.state_id == "dsr1"]
        if dsr1:
            a = dsr1[0]
            if a.status == "error":
                dsr1_cats["ERROR"] += 1
            else:
                dsr1_cats[classify(a.speedup)] += 1

    # Generate report
    lines = []
    lines.append("# DSR2 Recommendations Report")
    lines.append(f"\nGenerated for state_1 prompt generation.\n")

    # Executive summary
    lines.append("## Executive Summary\n")
    lines.append("### Current Portfolio (Best Across All Attempts)")
    cats = {"WIN": 0, "IMPROVED": 0, "NEUTRAL": 0, "REGRESSION": 0}
    for qs in all_states.values():
        cats[classify(qs.best_speedup)] += 1
    lines.append(f"- **WIN** (>=1.5x): {cats['WIN']} queries")
    lines.append(f"- **IMPROVED** (1.1-1.5x): {cats['IMPROVED']} queries")
    lines.append(f"- **NEUTRAL** (0.95-1.1x): {cats['NEUTRAL']} queries")
    lines.append(f"- **REGRESSION** (<0.95x): {cats['REGRESSION']} queries")
    lines.append("")

    lines.append("### DSR1 Round Results (DeepSeek Reasoner)")
    for cat in ["WIN", "IMPROVED", "NEUTRAL", "REGRESSION", "ERROR"]:
        lines.append(f"- {cat}: {dsr1_cats[cat]}")
    lines.append("")

    lines.append("### DSR2 Opportunity")
    lines.append(f"- **Tier 1 (High Value)**: {len(tier1)} queries - NEUTRAL/REGRESSION with room to improve")
    lines.append(f"- **Tier 2 (Medium Value)**: {len(tier2)} queries - IMPROVED, could push to WIN")
    lines.append(f"- **Tier 3 (Already Winning)**: {len(tier3)} queries - WIN or fast, lower priority")
    lines.append("")

    # Transform effectiveness matrix
    lines.append("## Transform Effectiveness (Across All Attempts)\n")
    lines.append("| Transform | Attempts | Win% | Avg Speedup | Regressions | Errors |")
    lines.append("|-----------|----------|------|-------------|-------------|--------|")
    for t_name, st in sorted(t_stats.items(), key=lambda x: x[1]["wins"] / max(x[1]["total"], 1), reverse=True):
        total = st["total"]
        win_pct = (st["wins"] / total * 100) if total > 0 else 0
        avg_sp = sum(st["speedups"]) / len(st["speedups"]) if st["speedups"] else 0
        lines.append(f"| {t_name} | {total} | {win_pct:.0f}% | {avg_sp:.2f}x | {st['regression']} | {st['error']} |")
    lines.append("")

    # DSR2 changes
    lines.append("## What's New in DSR2 Prompts\n")
    lines.append("### New Constraints (6 added, 8 total)")
    lines.append("- **CTE_COLUMN_COMPLETENESS** [CRITICAL]: CTE SELECT must include all downstream columns")
    lines.append("- **NO_MATERIALIZE_EXISTS** [CRITICAL]: Never convert EXISTS to materialized CTE")
    lines.append("- **MIN_BASELINE_THRESHOLD** [HIGH]: Skip CTE transforms on <100ms queries")
    lines.append("- **NO_UNFILTERED_DIMENSION_CTE** [HIGH]: Every CTE must have a filtering WHERE")
    lines.append("- **NO_UNION_SAME_COLUMN_OR** [HIGH]: Don't split same-column OR into UNION")
    lines.append("- **REMOVE_REPLACED_CTES** [HIGH]: Remove original CTEs after replacement")
    lines.append("")
    lines.append("### New Gold Examples (2 added, 15 total)")
    lines.append("- **composite_decorrelate_union** (Q35, 2.42x): Decorrelate EXISTS + OR-to-UNION composite")
    lines.append("- **shared_dimension_multi_channel** (Q80, 1.30x): Shared dim CTEs across channels")
    lines.append("")
    lines.append("### Counter-Examples Added (when_not_to_use)")
    lines.append("- **date_cte_isolate**: Don't use when optimizer already pushes predicates (Q31: 0.49x)")
    lines.append("- **prefetch_fact_join**: Don't use on <50ms queries or window-dominated (Q25: 0.50x)")
    lines.append("- **materialize_cte**: NEVER for EXISTS (Q16: 0.14x = 7x slowdown)")
    lines.append("- **multi_dimension_prefetch**: No unfiltered dim CTEs (Q67: 0.85x)")
    lines.append("- **or_to_union**: Don't split same-column OR (Q90: 0.59x)")
    lines.append("")

    # Tier 1: High-value targets
    lines.append("---\n")
    lines.append("## TIER 1: High-Value Targets\n")
    lines.append(f"*{len(tier1)} queries - NEUTRAL/REGRESSION with significant runtime. Highest ROI.*\n")

    for qs in tier1:
        cat = classify(qs.best_speedup)
        dsr1 = [a for a in qs.attempts if a.state_id == "dsr1"]
        dsr1_info = dsr1[0] if dsr1 else None

        lines.append(f"### Q{qs.query_num} — {cat} (best: {qs.best_speedup:.2f}x) — baseline: {qs.baseline_ms:.0f}ms")
        lines.append("")

        # Optimization chain
        lines.append(f"**Chain**: {format_attempt_chain(qs)}")
        lines.append("")

        # DSR1 result
        if dsr1_info:
            dsr1_t = "+".join(dsr1_info.transforms) if dsr1_info.transforms else "unknown"
            lines.append(f"**DSR1**: {dsr1_info.speedup:.2f}x using `{dsr1_t}` ({dsr1_info.status})")
        lines.append("")

        # Transforms tried
        if qs.transforms_tried:
            tried_parts = []
            for t in sorted(qs.transforms_tried):
                if t in qs.transforms_succeeded:
                    tried_parts.append(f"  + `{t}` (succeeded)")
                elif t in qs.transforms_failed:
                    tried_parts.append(f"  - `{t}` (failed/regression)")
                else:
                    tried_parts.append(f"  = `{t}` (neutral)")
            lines.append("**Transforms tried**:")
            lines.extend(tried_parts)
            lines.append("")

        # Untried patterns
        if qs.untried_patterns:
            lines.append(f"**Untried applicable patterns**: {', '.join(qs.untried_patterns)}")
        else:
            lines.append("**Untried applicable patterns**: None — all structural matches tried")
        lines.append("")

        # Recommendations
        recs = get_recommendations(qs)
        if recs:
            lines.append("**Recommendations**:")
            for i, (pat, gold_sp, reason) in enumerate(recs, 1):
                t_stat = t_stats.get(pat, {})
                total = t_stat.get("total", 0)
                wins = t_stat.get("wins", 0)
                rate = f"{wins}/{total}" if total > 0 else "no data"
                lines.append(f"  {i}. **{pat}** — gold {gold_sp:.2f}x, success rate: {rate}")
        else:
            lines.append("**Recommendations**: All applicable patterns exhausted. Needs novel approach or composite strategy.")
        lines.append("")

    # Tier 2: Medium value
    lines.append("---\n")
    lines.append("## TIER 2: Medium-Value Targets\n")
    lines.append(f"*{len(tier2)} queries - IMPROVED, could push to WIN threshold (1.5x).*\n")

    for qs in tier2:
        cat = classify(qs.best_speedup)
        dsr1 = [a for a in qs.attempts if a.state_id == "dsr1"]
        dsr1_info = dsr1[0] if dsr1 else None

        lines.append(f"### Q{qs.query_num} — {cat} (best: {qs.best_speedup:.2f}x) — baseline: {qs.baseline_ms:.0f}ms")
        lines.append("")
        lines.append(f"**Chain**: {format_attempt_chain(qs)}")
        if dsr1_info:
            dsr1_t = "+".join(dsr1_info.transforms) if dsr1_info.transforms else "unknown"
            lines.append(f"**DSR1**: {dsr1_info.speedup:.2f}x using `{dsr1_t}` ({dsr1_info.status})")

        if qs.untried_patterns:
            lines.append(f"**Untried**: {', '.join(qs.untried_patterns)}")

        recs = get_recommendations(qs)
        if recs:
            rec_str = ", ".join(f"`{r[0]}` ({r[1]:.1f}x)" for r in recs[:3])
            lines.append(f"**Top recs**: {rec_str}")
        lines.append("")

    # Tier 3: Already winning
    lines.append("---\n")
    lines.append("## TIER 3: Already Winning / Low Runtime\n")
    lines.append(f"*{len(tier3)} queries - WIN or very fast baseline. Lower priority.*\n")

    lines.append("| Query | Best | Baseline | Best Attempt | Untried |")
    lines.append("|-------|------|----------|--------------|---------|")
    for qs in tier3:
        untried_str = ", ".join(qs.untried_patterns[:3]) if qs.untried_patterns else "all tried"
        lines.append(f"| Q{qs.query_num} | {qs.best_speedup:.2f}x | {qs.baseline_ms:.0f}ms | {qs.best_attempt} | {untried_str} |")
    lines.append("")

    # Appendix: Full query matrix
    lines.append("---\n")
    lines.append("## Appendix: Complete Query Matrix\n")
    lines.append("| Q# | Baseline(ms) | Best | Best From | DSR1 | #Tried | #Untried | Top Untried |")
    lines.append("|----|-------------|------|-----------|------|--------|----------|-------------|")
    for q in range(1, 100):
        qs = all_states[q]
        dsr1 = [a for a in qs.attempts if a.state_id == "dsr1"]
        dsr1_sp = f"{dsr1[0].speedup:.2f}x" if dsr1 else "—"
        top_untried = qs.untried_patterns[0] if qs.untried_patterns else "—"
        lines.append(f"| Q{q} | {qs.baseline_ms:.0f} | {qs.best_speedup:.2f}x | {qs.best_attempt} | {dsr1_sp} | {len(qs.transforms_tried)} | {len(qs.untried_patterns)} | {top_untried} |")
    lines.append("")

    # Write output
    report = "\n".join(lines)
    OUTPUT.parent.mkdir(parents=True, exist_ok=True)
    OUTPUT.write_text(report)
    print(f"Written {len(lines)} lines to {OUTPUT}", file=sys.stderr)
    print(f"Tier 1: {len(tier1)}, Tier 2: {len(tier2)}, Tier 3: {len(tier3)}", file=sys.stderr)


if __name__ == "__main__":
    main()
