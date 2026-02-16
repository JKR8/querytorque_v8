"""Auto-generate knowledge/{dialect}.md playbook.

Two generation modes:
  1. **Gold examples** (preferred): Reads only curated gold examples from
     qt_sql/examples/{dialect}/ — the canonical knowledge base for beam pipeline.
  2. **Blackboard** (legacy): Reads blackboard JSON for historical research.

Gold example mode usage:
    qt playbook duckdb --output knowledge/duckdb_DRAFT.md

Blackboard mode usage (legacy):
    PYTHONPATH=packages/qt-shared:packages/qt-sql:. python3 -m qt_sql.knowledge.gen_playbook \\
        --blackboard packages/qt-sql/qt_sql/knowledge/duckdb_tpcds.json \\
        --engine-profile packages/qt-sql/qt_sql/constraints/engine_profile_duckdb.json \\
        --output packages/qt-sql/qt_sql/knowledge/duckdb_DRAFT.md
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
from collections import defaultdict
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
logger = logging.getLogger(__name__)

# ─────────────────────────────────────────────────────────────────────────────
# Constants
# ─────────────────────────────────────────────────────────────────────────────

# Where files live relative to this script
_HERE = Path(__file__).parent
_CONSTRAINTS_DIR = _HERE.parent / "constraints"
_BENCHMARKS_DIR = _HERE.parent / "benchmarks"

# Dialect → default blackboard path
_DEFAULT_BLACKBOARDS = {
    "duckdb": _HERE / "duckdb_tpcds.json",
    "postgresql": _BENCHMARKS_DIR / "postgres_dsb" / "knowledge" / "postgres_dsb.json",
}

# Severity thresholds for regression classification
_SEVERITY = [
    (0.20, "CATASTROPHIC"),
    (0.40, "SEVERE"),
    (0.60, "MAJOR"),
    (0.80, "MODERATE"),
    (1.00, "MINOR"),
]

# Gap ID → human-readable pathology name
_GAP_NAMES = {
    "CROSS_CTE_PREDICATE_BLINDNESS": "Predicate Chain Pushback",
    "REDUNDANT_SCAN_ELIMINATION": "Repeated Scans of Same Table",
    "CORRELATED_SUBQUERY_PARALYSIS": "Correlated Subquery Nested Loop",
    "CROSS_COLUMN_OR_DECOMPOSITION": "Cross-Column OR Forcing Full Scan",
    "LEFT_JOIN_FILTER_ORDER_RIGIDITY": "LEFT JOIN + NULL-Eliminating WHERE",
    "UNION_CTE_SELF_JOIN_DECOMPOSITION": "Self-Joined CTE Materialized for All Values",
    "AGGREGATE_BELOW_JOIN_BLINDNESS": "Aggregation After Join",
    "INTERSECT_MATERIALIZE_BOTH": "INTERSECT Materializing Both Sides",
    "COMMA_JOIN_WEAKNESS": "Comma Join Confusing Cardinality Estimation",
    "NON_EQUI_JOIN_INPUT_BLINDNESS": "Non-Equi Join Without Prefiltering",
    "CTE_MATERIALIZATION_FENCE": "CTE Materialization Blocking Parallelism",
    "PREDICATE_TRANSITIVITY_FAILURE": "Predicate Transitivity Not Propagated",
    "WINDOW_BEFORE_JOIN": "Window Functions in CTEs Before Join",
    "SHARED_SUBEXPRESSION": "Shared Subexpression Executed Multiple Times",
}

# Gap ID → short rewrite principle
_GAP_PRINCIPLES = {
    "CROSS_CTE_PREDICATE_BLINDNESS": "SMALLEST SET FIRST",
    "REDUNDANT_SCAN_ELIMINATION": "DON'T REPEAT WORK",
    "CORRELATED_SUBQUERY_PARALYSIS": "SETS OVER LOOPS",
    "CROSS_COLUMN_OR_DECOMPOSITION": "MINIMIZE ROWS TOUCHED",
    "LEFT_JOIN_FILTER_ORDER_RIGIDITY": "ARM THE OPTIMIZER",
    "UNION_CTE_SELF_JOIN_DECOMPOSITION": "SMALLEST SET FIRST",
    "AGGREGATE_BELOW_JOIN_BLINDNESS": "MINIMIZE ROWS TOUCHED",
    "INTERSECT_MATERIALIZE_BOTH": "SETS OVER LOOPS",
    "COMMA_JOIN_WEAKNESS": "ARM THE OPTIMIZER",
    "NON_EQUI_JOIN_INPUT_BLINDNESS": "MINIMIZE ROWS TOUCHED",
    "CTE_MATERIALIZATION_FENCE": "ARM THE OPTIMIZER",
    "PREDICATE_TRANSITIVITY_FAILURE": "ARM THE OPTIMIZER",
    "WINDOW_BEFORE_JOIN": "MINIMIZE ROWS TOUCHED",
    "SHARED_SUBEXPRESSION": "DON'T REPEAT WORK",
}


# ─────────────────────────────────────────────────────────────────────────────
# Data classes
# ─────────────────────────────────────────────────────────────────────────────


@dataclass
class AttemptRecord:
    """One optimization attempt from the blackboard."""

    query_id: str
    transform: str
    speedup: float
    status: str  # pass/success/fail/error
    rows_match: bool
    worker_id: Optional[int] = None
    strategy: str = ""
    source: str = ""
    gap: Optional[str] = None  # populated from transforms.json


@dataclass
class TreatmentStats:
    """Aggregated statistics for one transform within a pathology."""

    transform_id: str
    wins: int = 0
    total_attempts: int = 0
    speedups: List[float] = field(default_factory=list)
    regressions: List[Tuple[float, str]] = field(default_factory=list)  # (speedup, query_id)

    @property
    def avg_speedup(self) -> float:
        return sum(self.speedups) / len(self.speedups) if self.speedups else 0.0

    @property
    def worst_regression(self) -> Optional[Tuple[float, str]]:
        return min(self.regressions, key=lambda x: x[0]) if self.regressions else None

    @property
    def has_regressions(self) -> bool:
        return len(self.regressions) > 0


@dataclass
class PathologyDraft:
    """Auto-generated pathology entry."""

    gap_id: str
    name: str
    principle: str
    detect: str
    gates: str
    treatments: List[TreatmentStats]
    what_worked: List[str]
    what_didnt_work: List[str]
    field_notes: List[str]
    zero_regressions: bool = False

    @property
    def total_wins(self) -> int:
        return sum(t.wins for t in self.treatments)

    @property
    def best_avg_speedup(self) -> float:
        avgs = [t.avg_speedup for t in self.treatments if t.wins > 0]
        return max(avgs) if avgs else 0.0


# ─────────────────────────────────────────────────────────────────────────────
# Loaders
# ─────────────────────────────────────────────────────────────────────────────


def load_blackboard(path: Path) -> Dict[str, Any]:
    """Load blackboard JSON."""
    with open(path) as f:
        return json.load(f)


def load_engine_profile(path: Path) -> Dict[str, Any]:
    """Load engine profile JSON."""
    with open(path) as f:
        return json.load(f)


def load_transforms_catalog() -> Dict[str, Dict[str, Any]]:
    """Load transforms.json → dict keyed by transform ID."""
    tf_path = _HERE / "transforms.json"
    if not tf_path.exists():
        logger.warning("transforms.json not found at %s", tf_path)
        return {}
    with open(tf_path) as f:
        transforms = json.load(f)
    return {t["id"]: t for t in transforms}


# ─────────────────────────────────────────────────────────────────────────────
# Analysis
# ─────────────────────────────────────────────────────────────────────────────


def extract_attempts(blackboard: Dict[str, Any]) -> List[AttemptRecord]:
    """Extract all individual attempts from blackboard queries."""
    attempts = []
    queries = blackboard.get("queries", {})

    for qid, qdata in queries.items():
        for att in qdata.get("all_attempts", []):
            speedup = att.get("speedup", 0.0)
            if speedup <= 0:
                continue  # skip broken entries

            transforms = att.get("transforms", [])
            status = att.get("status", "")
            rows_match = att.get("rows_match", False)

            # Create one record per transform in this attempt
            if not transforms:
                transforms = ["unknown"]

            for tf in transforms:
                attempts.append(AttemptRecord(
                    query_id=qid,
                    transform=tf,
                    speedup=speedup,
                    status=status,
                    rows_match=rows_match,
                    worker_id=att.get("worker_id"),
                    strategy=att.get("strategy", ""),
                    source=att.get("source", ""),
                ))

    return attempts


def map_transforms_to_gaps(
    attempts: List[AttemptRecord],
    catalog: Dict[str, Dict[str, Any]],
) -> None:
    """Populate gap field on each attempt from transforms catalog."""
    for att in attempts:
        tf_entry = catalog.get(att.transform)
        if tf_entry and tf_entry.get("gap"):
            att.gap = tf_entry["gap"]


def compute_treatment_stats(
    attempts: List[AttemptRecord],
) -> Dict[str, Dict[str, TreatmentStats]]:
    """Group attempts by gap → transform → stats.

    Returns: {gap_id: {transform_id: TreatmentStats}}
    """
    grouped: Dict[str, Dict[str, TreatmentStats]] = defaultdict(
        lambda: defaultdict(lambda: TreatmentStats(transform_id=""))
    )

    for att in attempts:
        if not att.gap:
            continue

        gap = att.gap
        tf = att.transform
        stats = grouped[gap][tf]
        stats.transform_id = tf
        stats.total_attempts += 1

        if att.speedup >= 1.10 and att.rows_match:
            stats.wins += 1
            stats.speedups.append(att.speedup)
        elif att.speedup < 0.90:
            stats.regressions.append((att.speedup, att.query_id))

    return dict(grouped)


def classify_severity(speedup: float) -> str:
    """Classify regression severity based on speedup ratio."""
    for threshold, label in _SEVERITY:
        if speedup < threshold:
            return label
    return "MINOR"


# ─────────────────────────────────────────────────────────────────────────────
# Playbook generation
# ─────────────────────────────────────────────────────────────────────────────


def build_pathologies(
    gap_stats: Dict[str, Dict[str, TreatmentStats]],
    engine_profile: Dict[str, Any],
) -> List[PathologyDraft]:
    """Build PathologyDraft entries from gap statistics + engine profile."""
    # Index engine profile gaps
    profile_gaps = {g["id"]: g for g in engine_profile.get("gaps", [])}

    pathologies = []
    for gap_id, treatments_map in gap_stats.items():
        treatments = sorted(
            treatments_map.values(),
            key=lambda t: t.wins,
            reverse=True,
        )

        # Skip gaps with zero wins
        total_wins = sum(t.wins for t in treatments)
        if total_wins == 0:
            continue

        # Pull detect/gates from engine profile
        profile = profile_gaps.get(gap_id, {})
        detect = profile.get("detect", "[AUTO-GEN: Add EXPLAIN detection signals]")
        gates = profile.get("gates", "[AUTO-GEN: Add decision gates]")
        what_worked = profile.get("what_worked", [])
        what_didnt_work = profile.get("what_didnt_work", [])
        field_notes = profile.get("field_notes", [])

        # Check if any treatment has regressions
        has_any_regression = any(t.has_regressions for t in treatments)

        name = _GAP_NAMES.get(gap_id, gap_id.replace("_", " ").title())
        principle = _GAP_PRINCIPLES.get(gap_id, "MINIMIZE ROWS TOUCHED")

        pathologies.append(PathologyDraft(
            gap_id=gap_id,
            name=name,
            principle=principle,
            detect=detect,
            gates=gates,
            treatments=treatments,
            what_worked=what_worked,
            what_didnt_work=what_didnt_work,
            field_notes=field_notes,
            zero_regressions=not has_any_regression,
        ))

    # Sort: zero-regression pathologies first, then by total wins descending
    pathologies.sort(key=lambda p: (not p.zero_regressions, -p.total_wins))

    return pathologies


def collect_all_regressions(
    attempts: List[AttemptRecord],
) -> List[Dict[str, Any]]:
    """Collect all regression attempts for the registry."""
    regressions = []
    seen = set()

    for att in attempts:
        if att.speedup < 0.90 and att.rows_match:
            key = (att.transform, att.query_id, round(att.speedup, 2))
            if key in seen:
                continue
            seen.add(key)
            regressions.append({
                "transform": att.transform,
                "speedup": att.speedup,
                "query_id": att.query_id,
                "severity": classify_severity(att.speedup),
                "strategy": att.strategy,
            })

    # Sort by severity (worst first)
    regressions.sort(key=lambda r: r["speedup"])
    return regressions


# ─────────────────────────────────────────────────────────────────────────────
# Markdown rendering
# ─────────────────────────────────────────────────────────────────────────────


def render_playbook(
    dialect: str,
    dataset_name: str,
    engine_profile: Dict[str, Any],
    pathologies: List[PathologyDraft],
    regressions: List[Dict[str, Any]],
    blackboard_summary: Dict[str, Any],
) -> str:
    """Render the full Stage 4 playbook markdown."""
    lines: List[str] = []

    # Header
    lines.append(f"# {dialect.title()} Rewrite Playbook")
    lines.append(f"# {dataset_name} field intelligence")
    lines.append("")

    # Summary comment
    total = blackboard_summary.get("total", 0)
    wins = blackboard_summary.get("wins", 0)
    avg_spd = blackboard_summary.get("avg_speedup", 0)
    lines.append(f"<!-- Auto-generated from blackboard ({total} queries, "
                 f"{wins} wins, {avg_spd:.2f}x avg). Review and polish before use. -->")
    lines.append("")

    # Section 1: Engine Strengths
    lines.append("## ENGINE STRENGTHS — do NOT rewrite")
    lines.append("")
    for i, strength in enumerate(engine_profile.get("strengths", []), 1):
        sid = strength.get("id", "")
        summary = strength.get("summary", "")
        implication = strength.get("implication", "")
        lines.append(f"{i}. **{sid.replace('_', ' ').title()}**: {summary} {implication}")
    lines.append("")

    # Section 2: Global Guards
    lines.append("## GLOBAL GUARDS")
    lines.append("")
    _render_global_guards(lines, engine_profile, regressions)
    lines.append("")

    # Section 3: Documented Cases
    lines.append("---")
    lines.append("")
    lines.append("## DOCUMENTED CASES")
    lines.append("")
    lines.append("Cases ordered by safety (zero-regression cases first, then by decreasing risk).")
    lines.append("")

    for i, path in enumerate(pathologies):
        _render_pathology(lines, i, path)

    # Section 4: Pruning Guide
    lines.append("---")
    lines.append("")
    _render_pruning_guide(lines, pathologies)

    # Section 5: Regression Registry
    _render_regression_registry(lines, regressions)

    return "\n".join(lines)


def _render_global_guards(
    lines: List[str],
    engine_profile: Dict[str, Any],
    regressions: List[Dict[str, Any]],
) -> None:
    """Generate global guards from strengths (DO NOT fight) and worst regressions."""
    guard_num = 1

    # Guards from strengths (things the engine already does well)
    for strength in engine_profile.get("strengths", []):
        impl = strength.get("implication", "")
        if impl and ("never" in impl.lower() or "do not" in impl.lower()):
            lines.append(f"{guard_num}. {impl}")
            guard_num += 1

    # Guards from catastrophic/severe regressions
    seen_transforms = set()
    for reg in regressions:
        if reg["severity"] in ("CATASTROPHIC", "SEVERE") and reg["transform"] not in seen_transforms:
            seen_transforms.add(reg["transform"])
            spd = reg["speedup"]
            tf = reg["transform"]
            lines.append(
                f"{guard_num}. {tf} caused {spd:.2f}x regression — "
                f"review gates before applying"
            )
            guard_num += 1

    # Standard guards
    lines.append(f"{guard_num}. Baseline < 100ms → skip CTE-based rewrites (overhead exceeds savings)")
    guard_num += 1
    lines.append(f"{guard_num}. Convert comma joins to explicit JOIN...ON")
    guard_num += 1
    lines.append(f"{guard_num}. Every CTE MUST have a WHERE clause")


def _render_pathology(lines: List[str], index: int, path: PathologyDraft) -> None:
    """Render one pathology section."""
    safety_tag = " — ZERO REGRESSIONS" if path.zero_regressions else ""

    # Estimate win percentage
    win_pct = ""
    total_wins = path.total_wins
    if total_wins >= 5:
        win_pct = f" — ~{total_wins} wins"

    lines.append(
        f"**P{index}: {path.name}** ({path.principle}){safety_tag}{win_pct}"
    )
    lines.append("")
    lines.append("| Aspect | Detail |")
    lines.append("|---|---|")

    # Detect
    lines.append(f"| Detect | {path.detect} |")

    # Gates
    lines.append(f"| Gates | {path.gates} |")

    # Treatments
    treatment_parts = []
    for t in path.treatments:
        if t.wins > 0:
            avg = t.avg_speedup
            treatment_parts.append(f"{t.transform_id} ({t.wins} wins, {avg:.2f}x avg)")
    treatments_str = ", ".join(treatment_parts) if treatment_parts else "No verified wins yet"
    lines.append(f"| Treatments | {treatments_str} |")

    # Failures
    failure_parts = []
    for t in path.treatments:
        if t.has_regressions:
            worst = t.worst_regression
            if worst:
                failure_parts.append(f"{worst[0]:.2f}x ({t.transform_id} on {worst[1]})")
    if not failure_parts:
        failures_str = "None observed."
    else:
        failures_str = ", ".join(failure_parts)
    lines.append(f"| Failures | {failures_str} |")

    lines.append("")


def _render_pruning_guide(lines: List[str], pathologies: List[PathologyDraft]) -> None:
    """Render the pruning guide from pathology detection signals."""
    lines.append("## PRUNING GUIDE")
    lines.append("")
    lines.append("| Plan shows | Skip |")
    lines.append("|---|---|")

    # Generate pruning rules based on what each pathology detects
    pruning_rules = {
        "CROSS_CTE_PREDICATE_BLINDNESS": ("Row counts monotonically decreasing", "P{i} (predicate pushback)"),
        "REDUNDANT_SCAN_ELIMINATION": ("Each table appears once", "P{i} (repeated scans)"),
        "CORRELATED_SUBQUERY_PARALYSIS": ("No nested loops", "P{i} (decorrelation)"),
        "CROSS_COLUMN_OR_DECOMPOSITION": ("No OR predicates", "P{i} (OR decomposition)"),
        "LEFT_JOIN_FILTER_ORDER_RIGIDITY": ("No LEFT JOIN", "P{i} (INNER conversion)"),
        "UNION_CTE_SELF_JOIN_DECOMPOSITION": ("No self-joined CTEs", "P{i} (self-join decomp)"),
        "AGGREGATE_BELOW_JOIN_BLINDNESS": ("No GROUP BY", "P{i} (aggregate pushdown)"),
        "INTERSECT_MATERIALIZE_BOTH": ("No INTERSECT/EXCEPT", "P{i} (set rewrite)"),
        "COMMA_JOIN_WEAKNESS": ("No comma joins (all explicit JOINs)", "P{i} (comma join fix)"),
        "NON_EQUI_JOIN_INPUT_BLINDNESS": ("No non-equi joins (BETWEEN, <, >)", "P{i} (non-equi prefilter)"),
        "WINDOW_BEFORE_JOIN": ("No WINDOW/OVER", "P{i} (deferred window)"),
        "SHARED_SUBEXPRESSION": ("No repeated subexpressions", "P{i} (materialize CTE)"),
    }

    for i, path in enumerate(pathologies):
        rule = pruning_rules.get(path.gap_id)
        if rule:
            plan_shows, skip_template = rule
            skip = skip_template.format(i=i)
            lines.append(f"| {plan_shows} | {skip} |")

    # Always add baseline guard
    lines.append("| Baseline < 50ms | ALL CTE-based transforms |")
    lines.append("")


def _render_regression_registry(
    lines: List[str],
    regressions: List[Dict[str, Any]],
) -> None:
    """Render the regression registry table."""
    lines.append("## REGRESSION REGISTRY")
    lines.append("")
    lines.append("| Severity | Transform | Result | Query | Strategy |")
    lines.append("|----------|-----------|--------|-------|----------|")

    for reg in regressions:
        severity = reg["severity"]
        transform = reg["transform"]
        result = f"{reg['speedup']:.2f}x"
        query = reg["query_id"]
        strategy = reg.get("strategy", "")
        lines.append(f"| {severity} | {transform} | {result} | {query} | {strategy} |")

    lines.append("")


# ─────────────────────────────────────────────────────────────────────────────
# Gold-examples-only pipeline (preferred for beam)
# ─────────────────────────────────────────────────────────────────────────────

_EXAMPLES_DIR = _HERE.parent / "examples"

# Engine name → examples subdirectory
_ENGINE_TO_EXAMPLE_DIR = {
    "duckdb": "duckdb",
    "postgresql": "postgres",
    "postgres": "postgres",
    "snowflake": "snowflake",
}


def _load_gold_examples(examples_dir: Path) -> List[Dict[str, Any]]:
    """Load all gold example JSON files from a dialect directory."""
    examples = []
    if not examples_dir.exists():
        logger.warning("Examples directory not found: %s", examples_dir)
        return examples

    for json_file in sorted(examples_dir.glob("*.json")):
        try:
            data = json.loads(json_file.read_text(encoding="utf-8"))
            data["_source_file"] = json_file.name
            examples.append(data)
        except (json.JSONDecodeError, OSError) as e:
            logger.warning("Failed to load %s: %s", json_file, e)

    # Also load regressions subdirectory
    regressions_dir = examples_dir / "regressions"
    if regressions_dir.exists():
        for json_file in sorted(regressions_dir.glob("*.json")):
            try:
                data = json.loads(json_file.read_text(encoding="utf-8"))
                data["_source_file"] = json_file.name
                if "type" not in data:
                    data["type"] = "regression"
                examples.append(data)
            except (json.JSONDecodeError, OSError) as e:
                logger.warning("Failed to load %s: %s", json_file, e)

    return examples


def _group_examples_by_gap(
    examples: List[Dict[str, Any]],
    catalog: Dict[str, Dict[str, Any]],
) -> Dict[str, List[Dict[str, Any]]]:
    """Group gold examples by engine gap using the transforms catalog."""
    by_gap: Dict[str, List[Dict[str, Any]]] = defaultdict(list)

    for ex in examples:
        # Try to find gap from the example's transform ID
        ex_id = ex.get("id", "")
        transforms = ex.get("transforms", [ex_id])

        gap_found = False
        for tf_name in transforms:
            tf_entry = catalog.get(tf_name)
            if tf_entry and tf_entry.get("gap"):
                by_gap[tf_entry["gap"]].append(ex)
                gap_found = True
                break

        if not gap_found:
            # Try family-based grouping as fallback
            family = ex.get("family", "")
            family_gap_map = {
                "A": "CROSS_CTE_PREDICATE_BLINDNESS",
                "B": "CORRELATED_SUBQUERY_PARALYSIS",
                "C": "AGGREGATE_BELOW_JOIN_BLINDNESS",
                "D": "INTERSECT_MATERIALIZE_BOTH",
                "E": "CTE_MATERIALIZATION_FENCE",
                "F": "COMMA_JOIN_WEAKNESS",
            }
            fallback_gap = family_gap_map.get(family, "UNKNOWN")
            by_gap[fallback_gap].append(ex)

    return dict(by_gap)


def _build_treatments_from_gold(
    examples: List[Dict[str, Any]],
) -> List[TreatmentStats]:
    """Build TreatmentStats from gold examples (not blackboard attempts)."""
    by_transform: Dict[str, TreatmentStats] = defaultdict(
        lambda: TreatmentStats(transform_id="")
    )

    for ex in examples:
        ex_id = ex.get("id", "unknown")
        speedup_str = ex.get("verified_speedup", "1.0x")
        try:
            speedup = float(speedup_str.rstrip("x"))
        except (ValueError, AttributeError):
            speedup = 1.0

        stats = by_transform[ex_id]
        stats.transform_id = ex_id
        stats.total_attempts += 1

        ex_type = ex.get("type", "gold")
        if ex_type in ("gold", "win") and speedup >= 1.10:
            stats.wins += 1
            stats.speedups.append(speedup)
        elif ex_type == "regression" or speedup < 0.90:
            queries = ex.get("benchmark_queries", ["?"])
            stats.regressions.append((speedup, queries[0] if queries else "?"))

    return sorted(by_transform.values(), key=lambda t: t.wins, reverse=True)


def generate_playbook_from_gold_examples(
    examples_dir: Path,
    engine_profile_path: Path,
    dialect: str,
) -> str:
    """Generate playbook markdown from gold examples only.

    No blackboard, no trial JSON, no learning records.
    ONLY curated gold examples + engine profile + transforms catalog.

    Args:
        examples_dir: Path to qt_sql/examples/{dialect}/ directory.
        engine_profile_path: Path to engine_profile_{dialect}.json.
        dialect: Engine dialect name (duckdb, postgresql, snowflake).

    Returns:
        Complete playbook markdown string.
    """
    # 1. Load gold examples
    examples = _load_gold_examples(examples_dir)
    logger.info("Loaded %d gold examples from %s", len(examples), examples_dir)

    if not examples:
        logger.warning("No gold examples found — generating minimal playbook")

    # 2. Load engine profile
    logger.info("Loading engine profile: %s", engine_profile_path)
    engine_profile = load_engine_profile(engine_profile_path)

    # 3. Load transforms catalog
    logger.info("Loading transforms catalog")
    catalog = load_transforms_catalog()

    # 4. Group examples by engine gap
    by_gap = _group_examples_by_gap(examples, catalog)
    logger.info("Grouped into %d engine gaps", len(by_gap))

    # 5. Build pathology drafts from gold examples
    profile_gaps = {g["id"]: g for g in engine_profile.get("gaps", [])}
    pathologies = []

    for gap_id, gap_examples in by_gap.items():
        wins = [e for e in gap_examples if e.get("type", "gold") in ("gold", "win")]
        regressions = [e for e in gap_examples if e.get("type") == "regression"]

        treatments = _build_treatments_from_gold(gap_examples)
        total_wins = sum(t.wins for t in treatments)
        if total_wins == 0 and not regressions:
            continue

        profile = profile_gaps.get(gap_id, {})
        name = _GAP_NAMES.get(gap_id, gap_id.replace("_", " ").title())
        principle = _GAP_PRINCIPLES.get(gap_id, "MINIMIZE ROWS TOUCHED")

        # Extract what_worked from gold example key_insights
        what_worked = []
        for ex in wins:
            ki = ex.get("example", {}).get("key_insight", "")
            if ki:
                what_worked.append(ki[:200])

        # Extract what_didnt_work from regression examples
        what_didnt_work = []
        for ex in regressions:
            mechanism = ex.get("regression_mechanism", "")
            if not mechanism:
                mechanism = ex.get("example", {}).get("when_not_to_use", "")
            if mechanism:
                what_didnt_work.append(mechanism[:200])

        pathology = PathologyDraft(
            gap_id=gap_id,
            name=name,
            principle=principle,
            detect=profile.get("detect", "[Add EXPLAIN detection signals]"),
            gates=profile.get("gates", "[Add decision gates]"),
            treatments=treatments,
            what_worked=what_worked + profile.get("what_worked", []),
            what_didnt_work=what_didnt_work + profile.get("what_didnt_work", []),
            field_notes=profile.get("field_notes", []),
            zero_regressions=(len(regressions) == 0),
        )
        pathologies.append(pathology)

    # Sort: zero-regression first, then by total wins
    pathologies.sort(key=lambda p: (not p.zero_regressions, -p.total_wins))
    logger.info("Generated %d pathology entries", len(pathologies))

    # 6. Collect regression registry from gold regression examples
    all_regressions = []
    for ex in examples:
        if ex.get("type") == "regression":
            speedup_str = ex.get("verified_speedup", "1.0x")
            try:
                speedup = float(speedup_str.rstrip("x"))
            except (ValueError, AttributeError):
                continue
            if speedup < 0.90:
                queries = ex.get("benchmark_queries", ["?"])
                all_regressions.append({
                    "transform": ex.get("id", "unknown"),
                    "speedup": speedup,
                    "query_id": queries[0] if queries else "?",
                    "severity": classify_severity(speedup),
                    "strategy": ex.get("regression_mechanism", ""),
                })

    all_regressions.sort(key=lambda r: r["speedup"])
    logger.info("Found %d regression entries", len(all_regressions))

    # 7. Render markdown
    win_examples = [e for e in examples if e.get("type", "gold") in ("gold", "win")]
    reg_examples = [e for e in examples if e.get("type") == "regression"]
    summary = {
        "total": len(examples),
        "wins": len(win_examples),
        "avg_speedup": (
            sum(
                float(e.get("verified_speedup", "1.0x").rstrip("x"))
                for e in win_examples
            )
            / max(len(win_examples), 1)
        ),
    }

    playbook = render_playbook(
        dialect=dialect,
        dataset_name=f"Gold Examples ({len(examples)} curated)",
        engine_profile=engine_profile,
        pathologies=pathologies,
        regressions=all_regressions,
        blackboard_summary=summary,
    )

    return playbook


# ─────────────────────────────────────────────────────────────────────────────
# Blackboard pipeline (legacy — kept for historical research)
# ─────────────────────────────────────────────────────────────────────────────


def generate_playbook(
    blackboard_path: Path,
    engine_profile_path: Path,
    dialect: str,
) -> str:
    """Full pipeline: load data → analyze → render markdown."""

    # 1. Load inputs
    logger.info("Loading blackboard: %s", blackboard_path)
    blackboard = load_blackboard(blackboard_path)

    logger.info("Loading engine profile: %s", engine_profile_path)
    engine_profile = load_engine_profile(engine_profile_path)

    logger.info("Loading transforms catalog")
    catalog = load_transforms_catalog()

    # 2. Extract all attempts
    attempts = extract_attempts(blackboard)
    logger.info("Extracted %d attempt records", len(attempts))

    # 3. Map transforms → gaps
    map_transforms_to_gaps(attempts, catalog)
    mapped = sum(1 for a in attempts if a.gap)
    logger.info("Mapped %d/%d attempts to engine gaps", mapped, len(attempts))

    # 4. Compute per-gap per-treatment statistics
    gap_stats = compute_treatment_stats(attempts)
    logger.info("Found %d active gaps", len(gap_stats))

    # 5. Build pathology drafts
    pathologies = build_pathologies(gap_stats, engine_profile)
    logger.info("Generated %d pathology entries", len(pathologies))

    # 6. Collect regression registry
    regressions = collect_all_regressions(attempts)
    logger.info("Found %d unique regressions", len(regressions))

    # 7. Render markdown
    dataset_name = blackboard.get("dataset", dialect)
    summary = blackboard.get("summary", {})
    playbook = render_playbook(
        dialect=dialect,
        dataset_name=dataset_name,
        engine_profile=engine_profile,
        pathologies=pathologies,
        regressions=regressions,
        blackboard_summary=summary,
    )

    return playbook


# ─────────────────────────────────────────────────────────────────────────────
# CLI
# ─────────────────────────────────────────────────────────────────────────────


def _resolve_paths(dialect: str) -> Tuple[Path, Path]:
    """Resolve blackboard and engine profile paths for a dialect."""
    bb_path = _DEFAULT_BLACKBOARDS.get(dialect)
    if not bb_path or not bb_path.exists():
        raise FileNotFoundError(
            f"No default blackboard for dialect '{dialect}'. "
            f"Use --blackboard to specify path."
        )

    ep_path = _CONSTRAINTS_DIR / f"engine_profile_{dialect}.json"
    if not ep_path.exists():
        raise FileNotFoundError(
            f"Engine profile not found: {ep_path}"
        )

    return bb_path, ep_path


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Auto-generate knowledge/{dialect}.md playbook from blackboard data"
    )
    parser.add_argument(
        "--dialect", "-d",
        help="Engine dialect (duckdb, postgresql, snowflake). "
             "Auto-resolves blackboard and profile paths.",
    )
    parser.add_argument(
        "--blackboard", "-b",
        type=Path,
        help="Path to blackboard JSON file",
    )
    parser.add_argument(
        "--engine-profile", "-e",
        type=Path,
        help="Path to engine profile JSON file",
    )
    parser.add_argument(
        "--output", "-o",
        type=Path,
        help="Output path (default: knowledge/{dialect}_DRAFT.md)",
    )
    parser.add_argument(
        "--overwrite",
        action="store_true",
        help="Overwrite output if it exists",
    )

    args = parser.parse_args()

    # Resolve paths
    if args.dialect:
        bb_path, ep_path = _resolve_paths(args.dialect)
        dialect = args.dialect
    elif args.blackboard and args.engine_profile:
        bb_path = args.blackboard
        ep_path = args.engine_profile
        # Infer dialect from engine profile filename
        dialect = ep_path.stem.replace("engine_profile_", "")
    else:
        parser.error("Specify --dialect or both --blackboard and --engine-profile")
        return

    if args.blackboard:
        bb_path = args.blackboard
    if args.engine_profile:
        ep_path = args.engine_profile

    # Output path
    out_path = args.output or (_HERE / f"{dialect}_DRAFT.md")

    if out_path.exists() and not args.overwrite:
        logger.error("Output exists: %s (use --overwrite to replace)", out_path)
        sys.exit(1)

    # Generate
    playbook = generate_playbook(bb_path, ep_path, dialect)

    # Write
    out_path.write_text(playbook)
    logger.info("Playbook written to %s (%d lines, %d chars)",
                out_path, playbook.count("\n"), len(playbook))

    # Summary
    print(f"\n{'='*60}")
    print(f"  Generated: {out_path.name}")
    print(f"  Lines:     {playbook.count(chr(10))}")
    print(f"  Chars:     {len(playbook)}")
    print(f"{'='*60}")
    print(f"\nReview the DRAFT and compare with the existing {dialect}.md.")
    print("Look for:")
    print("  - Pathology names that need human polish")
    print("  - Detection signals that need EXPLAIN-specific detail")
    print("  - Gates that need safety calibration from domain knowledge")
    print("  - Missing root cause annotations on regressions")


if __name__ == "__main__":
    main()
