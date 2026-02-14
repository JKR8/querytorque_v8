"""Layer 1: Populate scanner blackboard from explore + scan + stacking data.

Reads existing JSON files from plan_explore/, plan_scanner/, and stacking
results, then writes scanner_blackboard.jsonl. Fully deterministic and
idempotent — re-running always rebuilds from scratch.

Merge key: (query_id, frozenset(flags.items())) — NOT combo_name strings.

Usage:
  python -m qt_sql.scanner_knowledge.blackboard benchmarks/postgres_dsb_76/
"""

from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from ..plan_scanner import PLAN_SPACE_COMBOS
from .schemas import ScannerObservation, derive_category, derive_combo_name

logger = logging.getLogger(__name__)


# ── Helpers ──────────────────────────────────────────────────────────────

def _flags_for_combo_name(combo_name: str) -> Optional[Dict[str, str]]:
    """Resolve a combo name (possibly a pair like 'a+b') to its flags dict."""
    # Direct lookup
    if combo_name in PLAN_SPACE_COMBOS:
        return dict(PLAN_SPACE_COMBOS[combo_name])

    # Pair combo: "no_nestloop+force_merge" → merge both flag dicts
    if "+" in combo_name:
        parts = combo_name.split("+")
        merged = {}
        for part in parts:
            if part not in PLAN_SPACE_COMBOS:
                return None
            merged.update(PLAN_SPACE_COMBOS[part])
        return merged

    return None


def _fmt_speedup(s: float) -> str:
    """Format speedup as human-readable string."""
    if s >= 1.0:
        return f"{s:.2f}x faster"
    return f"{1/s:.1f}x regression ({s:.2f}x)"


def _fmt_cost_ratio(r: float) -> str:
    """Format cost ratio as human-readable string."""
    if r > 1.0:
        return f"{r:.2f}x cheaper (better)"
    if r < 1.0:
        return f"{1/r:.1f}x more expensive"
    return "same cost"


# ── Explore data processing ─────────────────────────────────────────────

def _process_explore_files(
    explore_dir: Path,
) -> Dict[Tuple, ScannerObservation]:
    """Process all explore JSON files, return dict keyed by merge_key."""
    observations: Dict[Tuple, ScannerObservation] = {}

    if not explore_dir.exists():
        logger.warning(f"No explore directory: {explore_dir}")
        return observations

    explore_files = sorted(explore_dir.glob("*.json"))
    if not explore_files:
        logger.warning(f"No explore files in {explore_dir}")
        return observations

    for fpath in explore_files:
        if fpath.name == "summary.json":
            continue

        try:
            data = json.loads(fpath.read_text())
        except Exception as e:
            logger.warning(f"Failed to parse {fpath}: {e}")
            continue

        query_id = data.get("query_id", fpath.stem)
        baseline_cost = data.get("baseline_cost", 0)
        n_plan_changers = data.get("n_plan_changers", 0)
        n_distinct_plans = data.get("n_distinct_plans", 0)

        # Build vulnerability lookup: combo_name → list of vuln types
        vuln_by_combo: Dict[str, List[str]] = {}
        for vuln in data.get("vulnerabilities", []):
            vtype = vuln["type"]
            for combo in vuln.get("combos", []):
                vuln_by_combo.setdefault(combo, []).append(vtype)

        # Emit one observation per distinct non-baseline plan.
        # Only use single-flag combos as representatives — pairwise
        # combos are redundant (a winner excludes the alternatives).
        for plan_entry in data.get("distinct_plans", []):
            is_baseline = plan_entry.get("is_baseline", False)
            if is_baseline:
                continue

            cost_ratio = plan_entry.get("cost_ratio", 1.0)
            plan_combos = [c for c in plan_entry.get("combos", []) if c != "baseline"]
            if not plan_combos:
                continue

            # Pick the simplest single-flag combo as representative
            best_combo = None
            best_flags = None
            for combo_name in plan_combos:
                if "+" in combo_name:
                    continue  # Skip pairwise combos
                flags = _flags_for_combo_name(combo_name)
                if flags is None:
                    continue
                if best_flags is None or len(flags) < len(best_flags):
                    best_combo = combo_name
                    best_flags = flags

            if best_flags is None:
                # Plan only reachable via pairwise combo — skip it.
                # These interaction effects aren't useful for findings.
                continue

            category = derive_category(best_flags)
            canonical_name = derive_combo_name(best_flags)

            # Collect vulnerability types for all combos in this plan
            vtypes = set()
            for cn in plan_combos:
                for vt in vuln_by_combo.get(cn, []):
                    vtypes.add(vt)

            # Generate summary
            flag_desc = ", ".join(f"{k}={v}" for k, v in sorted(best_flags.items()))
            summary = (
                f"Setting {flag_desc}: plan changed "
                f"(cost ratio {_fmt_cost_ratio(cost_ratio)}). "
                f"Plan diversity: {n_distinct_plans} distinct plans from "
                f"{n_plan_changers} plan changers."
            )

            obs = ScannerObservation(
                query_id=query_id,
                flags=best_flags,
                source="explore",
                category=category,
                combo_name=canonical_name,
                summary=summary,
                plan_changed=True,
                cost_ratio=round(cost_ratio, 4) if cost_ratio else None,
                vulnerability_types=sorted(vtypes),
                n_plan_changers=n_plan_changers,
                n_distinct_plans=n_distinct_plans,
            )

            key = obs.merge_key()
            observations[key] = obs

    return observations


# ── Scan data processing ────────────────────────────────────────────────

def _process_scan_files(
    scan_dir: Path,
    existing: Dict[Tuple, ScannerObservation],
) -> Dict[Tuple, ScannerObservation]:
    """Process scan JSON files, merging into existing explore observations."""
    if not scan_dir.exists():
        logger.warning(f"No scan directory: {scan_dir}")
        return existing

    scan_files = sorted(scan_dir.glob("*.json"))
    if not scan_files:
        logger.warning(f"No scan files in {scan_dir}")
        return existing

    for fpath in scan_files:
        if fpath.name == "summary.json":
            continue

        try:
            data = json.loads(fpath.read_text())
        except Exception as e:
            logger.warning(f"Failed to parse {fpath}: {e}")
            continue

        query_id = data.get("query_id", fpath.stem)
        baseline_ms = data.get("baseline_ms", 0)

        for combo_data in data.get("combos", []):
            combo_name = combo_data.get("combo_name", "")
            config = combo_data.get("config", {})
            if not config:
                # Fallback: resolve from combo name
                resolved = _flags_for_combo_name(combo_name)
                if resolved:
                    config = resolved
                else:
                    continue

            flags = dict(config)
            category = derive_category(flags)
            canonical_name = derive_combo_name(flags)

            combo_ms = combo_data.get("time_ms", 0)
            speedup = combo_data.get("speedup", 1.0)
            rows_match = combo_data.get("rows_match", True)
            error = combo_data.get("error")

            if error:
                continue  # Skip errored combos

            # Only include winners (>1.05x) and regressions (<0.90x)
            if 0.90 <= speedup <= 1.05:
                continue  # Neutral — not interesting

            key = (query_id, frozenset(flags.items()))

            if key in existing:
                # Merge: add wall-clock data to existing explore record
                obs = existing[key]
                obs.wall_speedup = speedup
                obs.baseline_ms = baseline_ms
                obs.combo_ms = combo_ms
                obs.rows_match = rows_match
                obs.source = "explore+scan"

                # Enrich summary with wall-clock data
                flag_desc = ", ".join(f"{k}={v}" for k, v in sorted(flags.items()))
                parts = [f"Setting {flag_desc}:"]
                if speedup < 1.0:
                    parts.append(
                        f"{1/speedup:.1f}x regression "
                        f"({baseline_ms:.0f}ms→{combo_ms:.0f}ms)."
                    )
                elif speedup > 1.05:
                    parts.append(
                        f"{speedup:.2f}x faster "
                        f"({baseline_ms:.0f}ms→{combo_ms:.0f}ms)."
                    )
                else:
                    parts.append(f"neutral ({baseline_ms:.0f}ms→{combo_ms:.0f}ms).")
                if not rows_match:
                    parts.append("WARNING: row count mismatch.")
                if obs.plan_changed:
                    parts.append(f"Plan changed (cost ratio {_fmt_cost_ratio(obs.cost_ratio or 1.0)}).")
                parts.append(
                    f"Plan diversity: {obs.n_distinct_plans or '?'} distinct plans."
                )
                obs.summary = " ".join(parts)
            else:
                # New scan-only record
                parts = []
                flag_desc = ", ".join(f"{k}={v}" for k, v in sorted(flags.items()))
                parts.append(f"Setting {flag_desc}:")
                if speedup < 1.0:
                    parts.append(
                        f"{1/speedup:.1f}x regression "
                        f"({baseline_ms:.0f}ms→{combo_ms:.0f}ms)."
                    )
                elif speedup > 1.05:
                    parts.append(
                        f"{speedup:.2f}x faster "
                        f"({baseline_ms:.0f}ms→{combo_ms:.0f}ms)."
                    )
                else:
                    parts.append(f"neutral ({baseline_ms:.0f}ms→{combo_ms:.0f}ms).")
                if not rows_match:
                    parts.append("WARNING: row count mismatch.")

                obs = ScannerObservation(
                    query_id=query_id,
                    flags=flags,
                    source="scan",
                    category=category,
                    combo_name=canonical_name,
                    summary=" ".join(parts),
                    wall_speedup=speedup,
                    baseline_ms=baseline_ms,
                    combo_ms=combo_ms,
                    rows_match=rows_match,
                )
                existing[key] = obs

    return existing


# ── Stacking data processing ────────────────────────────────────────────

def _process_stacking_data(
    stacking_path: Path,
    existing: Dict[Tuple, ScannerObservation],
) -> Dict[Tuple, ScannerObservation]:
    """Process config-rewrite stacking experiment results."""
    if not stacking_path.exists():
        logger.info(f"No stacking data at {stacking_path}")
        return existing

    try:
        data = json.loads(stacking_path.read_text())
    except Exception as e:
        logger.warning(f"Failed to parse stacking data: {e}")
        return existing

    for query_id, query_data in data.items():
        variants = query_data.get("variants", [])
        if len(variants) < 4:
            continue

        # Extract the four key variants
        orig_ms = variants[0].get("avg_ms", 0)  # V1: Original SQL
        config_ms = variants[1].get("avg_ms", 0)  # Variant 2: Original + config
        rewrite_ms = variants[2].get("avg_ms", 0)  # V3: Rewrite only
        combined_ms = variants[3].get("avg_ms", 0)  # V4: Rewrite + config

        if orig_ms <= 0:
            continue

        config_speedup = orig_ms / config_ms if config_ms > 0 else 1.0
        rewrite_speedup = orig_ms / rewrite_ms if rewrite_ms > 0 else 1.0
        combined_speedup = orig_ms / combined_ms if combined_ms > 0 else 1.0

        # Classify interaction type
        if combined_speedup > max(config_speedup, rewrite_speedup) * 1.2:
            interaction = "SYNERGY"
        elif rewrite_speedup < 0.9 and combined_speedup > 1.0:
            interaction = "RESCUE"
        else:
            interaction = "REDUNDANT"

        # Extract config flags from variant name
        variant_name = variants[1].get("name", "")
        # Parse "Variant 2: Original + no_hashjoin+max_parallel" → combo names
        config_part = variant_name.split(" + ", 1)[-1] if " + " in variant_name else ""
        combo_names = [c.strip() for c in config_part.split("+") if c.strip()]

        flags: Dict[str, str] = {}
        for cn in combo_names:
            resolved = _flags_for_combo_name(cn)
            if resolved:
                flags.update(resolved)

        if not flags:
            # Can't resolve flags — use a synthetic key
            flags = {"stacking_experiment": query_id}

        summary = (
            f"{interaction}: Config alone {config_speedup:.2f}x, "
            f"rewrite alone {rewrite_speedup:.2f}x, "
            f"combined {combined_speedup:.2f}x. "
        )
        if interaction == "SYNERGY":
            summary += "Config enables rewrite gains."
        elif interaction == "RESCUE":
            summary += "Config rescues a regressing rewrite."
        else:
            summary += "Config and rewrite target the same bottleneck."

        obs = ScannerObservation(
            query_id=query_id,
            flags=flags,
            source="stacking",
            category="interaction",
            combo_name=derive_combo_name(flags),
            summary=summary,
            wall_speedup=combined_speedup,
            baseline_ms=orig_ms,
            combo_ms=combined_ms,
        )

        key = obs.merge_key()
        existing[key] = obs

    return existing


# ── Main entry point ────────────────────────────────────────────────────

def populate_blackboard(benchmark_dir: Path) -> Path:
    """Build scanner_blackboard.jsonl from explore + scan + stacking data.

    Returns path to the written JSONL file.
    """
    benchmark_dir = Path(benchmark_dir)
    explore_dir = benchmark_dir / "plan_explore"
    scan_dir = benchmark_dir / "plan_scanner"

    # Stacking data lives in research/scripts/ relative to project root
    project_root = benchmark_dir
    for _ in range(10):
        if (project_root / "research").exists():
            break
        project_root = project_root.parent
    stacking_path = project_root / "research" / "scripts" / "config_rewrite_stacking_results.json"

    # Layer 1: Process explore data
    observations = _process_explore_files(explore_dir)
    n_explore = len(observations)

    # Layer 2: Merge scan data
    observations = _process_scan_files(scan_dir, observations)
    n_scan_merged = sum(
        1 for o in observations.values() if o.source == "explore+scan"
    )
    n_scan_only = sum(
        1 for o in observations.values() if o.source == "scan"
    )

    # Layer 3: Add stacking data
    n_before_stacking = len(observations)
    observations = _process_stacking_data(stacking_path, observations)
    n_stacking = len(observations) - n_before_stacking

    # Write JSONL (sorted for deterministic output)
    output_path = benchmark_dir / "scanner_blackboard.jsonl"
    sorted_obs = sorted(
        observations.values(),
        key=lambda o: (o.query_id, o.combo_name),
    )

    with open(output_path, "w") as f:
        for obs in sorted_obs:
            f.write(obs.to_jsonl() + "\n")

    print(f"  Blackboard written: {output_path}")
    print(f"  Total observations: {len(sorted_obs)}")
    print(f"    From explore only: {n_explore - n_scan_merged}")
    print(f"    Merged (explore+scan): {n_scan_merged}")
    print(f"    From scan only: {n_scan_only}")
    print(f"    From stacking: {n_stacking}")

    # Category breakdown
    cat_counts: Dict[str, int] = {}
    for obs in sorted_obs:
        cat_counts[obs.category] = cat_counts.get(obs.category, 0) + 1
    print(f"  Categories:")
    for cat, count in sorted(cat_counts.items(), key=lambda x: -x[1]):
        print(f"    {cat:20s}: {count}")

    return output_path


# ── CLI entry point ─────────────────────────────────────────────────────

if __name__ == "__main__":
    import argparse
    import sys

    parser = argparse.ArgumentParser(
        description="Populate scanner blackboard from explore+scan data"
    )
    parser.add_argument(
        "benchmark_dir",
        type=Path,
        help="Path to benchmark directory",
    )
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    populate_blackboard(args.benchmark_dir)
