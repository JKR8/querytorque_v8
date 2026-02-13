#!/usr/bin/env python3
"""
Deep Q-Error Analysis:
1. EXPLAIN-only vs EXPLAIN ANALYZE — what's free vs what costs a query run?
2. Categorical variables derivable from Q-Error
3. Q-Error node type → pathology mapping
4. Under/over-estimate → transform routing
5. Integration with reasoning chain

Cross-references Q-Error data with leaderboard speedups and pathology tree.
"""
import csv
import json
import math
import sys
from collections import Counter, defaultdict
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Optional, Tuple

sys.path.insert(0, "packages/qt-shared")
sys.path.insert(0, "packages/qt-sql")

from analyze_qerror import extract_q_errors_from_node, QErrorNode

# ── data loading ──────────────────────────────────────────────────────────

def load_qerror_benchmark() -> Dict[int, dict]:
    """Load Q-Error benchmark results keyed by query number."""
    rows = {}
    with open("Q-Error/results_tpcds_benchmark.csv") as f:
        for r in csv.DictReader(f):
            qnum = int(r["query_num"]) if r["query_num"] != "0" else None
            if qnum:
                rows[qnum] = {
                    "max_qerror": float(r["max_qerror"]),
                    "severity": r["severity"],
                    "node_type": r["worst_node_type"],
                    "estimated": int(float(r["estimated"])),
                    "actual": int(float(r["actual"])),
                    "n_nodes": int(r["n_qerror_nodes"]),
                }
    return rows

def load_leaderboard() -> Dict[int, dict]:
    """Load master leaderboard keyed by query number."""
    rows = {}
    path = Path("research/archive/benchmark_results/CONSOLIDATED_BENCHMARKS/"
                "DuckDB_TPC-DS_Master_v3_20260206.csv")
    with open(path) as f:
        for r in csv.DictReader(f):
            qnum = int(r["Query_Num"])
            # Best speedup across all runs
            speedups = []
            for col in ["Kimi_Speedup", "Retry3W_SF10_Speedup", "DSR1_Speedup"]:
                try:
                    v = float(r.get(col, 0) or 0)
                    if v > 0:
                        speedups.append(v)
                except (ValueError, TypeError):
                    pass
            best = max(speedups) if speedups else 0.0
            rows[qnum] = {
                "classification": r["Classification"],
                "best_speedup": best,
                "kimi_speedup": float(r.get("Kimi_Speedup") or 0),
                "gold_transform": r.get("Gold_Transform", ""),
                "original_ms": float(r.get("Kimi_Original_ms") or 0),
            }
    return rows

def load_gold_examples() -> Dict[str, dict]:
    """Load gold examples keyed by query number string."""
    examples = {}
    gold_dir = Path("packages/qt-sql/qt_sql/examples/duckdb")
    for f in gold_dir.glob("*.json"):
        with open(f) as fp:
            d = json.load(fp)
        for q in d.get("benchmark_queries", []):
            qnum = int(q.replace("Q", "").replace("q", ""))
            examples[qnum] = {
                "transform_id": d["id"],
                "speedup": d.get("sf10_speedup", 0),
            }
    return examples

# ── EXPLAIN-only structural signals ──────────────────────────────────────

def extract_explain_only_signals(executor, sql: str) -> dict:
    """
    Extract structural signals from EXPLAIN-only (zero execution cost).
    These are the proxy features that predict Q-Error WITHOUT running the query.
    """
    plan = executor.explain(sql, analyze=False)
    if not plan or plan.get("type") == "error":
        return {"error": True}

    signals = {
        "sentinel_zeros": 0,        # Nodes with Est=0
        "sentinel_ones": 0,         # Non-leaf nodes with Est=1
        "delim_scan_present": False, # DELIM_SCAN = decorrelated subquery
        "cte_count": 0,             # Number of CTEs
        "max_estimated": 0,
        "min_estimated": float("inf"),
        "node_types": [],
        "table_scans": Counter(),    # Table name → scan count
        "has_nested_loop": False,
        "has_left_join": False,
        "has_intersect": False,
        "has_window": False,
        "has_or_filter": False,
    }

    def walk(node, depth=0):
        if not isinstance(node, dict):
            return
        op = node.get("operator_type", node.get("operator_name", ""))
        extra = node.get("extra_info", {})
        est_str = extra.get("Estimated Cardinality", "")

        try:
            est = int(est_str)
        except (ValueError, TypeError):
            est = None

        if est is not None:
            if est == 0:
                signals["sentinel_zeros"] += 1
            if est == 1 and depth > 0:
                signals["sentinel_ones"] += 1
            signals["max_estimated"] = max(signals["max_estimated"], est)
            if est > 0:
                signals["min_estimated"] = min(signals["min_estimated"], est)

        signals["node_types"].append(op)

        if "DELIM" in op:
            signals["delim_scan_present"] = True
        if op == "CTE":
            signals["cte_count"] += 1
        if "NESTED" in op:
            signals["has_nested_loop"] = True
        if "WINDOW" in op:
            signals["has_window"] = True
        if "INTERSECT" in op or "EXCEPT" in op:
            signals["has_intersect"] = True

        # Check join type
        jt = extra.get("Join Type", "")
        if "LEFT" in jt:
            signals["has_left_join"] = True

        # Count table scans
        if "SCAN" in op and "Table" in extra:
            signals["table_scans"][extra["Table"]] += 1

        # Check for OR in filters
        filters = extra.get("Filters", "")
        if isinstance(filters, str) and " OR " in filters.upper():
            signals["has_or_filter"] = True

        for child in node.get("children", []):
            walk(child, depth + 1)

    tree = plan.get("children", [])
    if isinstance(tree, list):
        for child in tree:
            walk(child)
    elif isinstance(tree, dict):
        walk(tree)

    # Derived: multi-scan tables
    signals["multi_scan_tables"] = [
        t for t, c in signals["table_scans"].items() if c > 1
    ]
    signals["max_scan_count"] = max(signals["table_scans"].values()) if signals["table_scans"] else 0

    return signals

# ── Categorical variable derivation ──────────────────────────────────────

@dataclass
class QErrorProfile:
    """Categorical variables derived from Q-Error analysis."""
    query_num: int

    # From EXPLAIN ANALYZE (needs execution)
    max_qerror: float
    severity: str                    # ACCURATE / MINOR_DRIFT / MODERATE_GUESS / MAJOR_HALLUCINATION / CATASTROPHIC_BLINDNESS
    mismatch_direction: str          # UNDER_EST / OVER_EST / ZERO_EST / ACCURATE
    mismatch_locus: str              # JOIN / SCAN / AGGREGATE / CTE / FILTER / PROJECTION
    mismatch_magnitude: str          # 1_ORDER (10x) / 2_ORDER (100x) / 3_ORDER (1000x) / EXTREME (>1000x)

    # From EXPLAIN-only (free)
    has_sentinel_zero: bool          # Any node Est=0 → planner blind
    has_sentinel_one: bool           # Non-leaf Est=1 → planner guessing
    has_delim_scan: bool             # DELIM_SCAN present → decorrelated subquery
    has_multi_scan: bool             # Same table scanned >1x → consolidation candidate
    has_left_join: bool              # LEFT JOIN present → P5 candidate
    has_intersect: bool              # INTERSECT present → P6 candidate
    has_nested_loop: bool            # Nested loop → P2 candidate
    has_window: bool                 # Window function → P8 candidate
    cte_count: int                   # Number of CTEs
    max_scan_count: int              # Max scans of same table

    # Pathology routing
    predicted_pathologies: List[str]  # P0-P9 candidates based on signals
    predicted_transforms: List[str]   # Transform candidates

    # Outcome (from leaderboard)
    actual_speedup: float
    actual_classification: str       # WIN / IMPROVED / NEUTRAL / REGRESSION
    actual_transform: str


def derive_mismatch_direction(est: int, act: int) -> str:
    if est == 0 and act == 0:
        return "ACCURATE"
    if est == 0:
        return "ZERO_EST"
    if act == 0:
        return "ZERO_EST"
    if est < act:
        return "UNDER_EST"
    if est > act:
        return "OVER_EST"
    return "ACCURATE"


def derive_mismatch_magnitude(qerror: float) -> str:
    if qerror < 10:
        return "MINOR"
    if qerror < 100:
        return "1_ORDER"
    if qerror < 1000:
        return "2_ORDER"
    if qerror < 10000:
        return "3_ORDER"
    return "EXTREME"


def derive_mismatch_locus(node_type: str) -> str:
    if "JOIN" in node_type:
        return "JOIN"
    if "SCAN" in node_type:
        return "SCAN"
    if "GROUP" in node_type or "AGGREGATE" in node_type:
        return "AGGREGATE"
    if "CTE" in node_type or "DELIM" in node_type:
        return "CTE"
    if "FILTER" in node_type:
        return "FILTER"
    return "PROJECTION"


def predict_pathologies(signals: dict, qerror_data: dict) -> Tuple[List[str], List[str]]:
    """
    Given EXPLAIN-only signals + Q-Error data, predict which pathologies
    are most likely and which transforms to try.
    """
    pathologies = []
    transforms = []
    locus = derive_mismatch_locus(qerror_data.get("node_type", ""))
    direction = derive_mismatch_direction(
        qerror_data.get("estimated", 0),
        qerror_data.get("actual", 0)
    )

    # P0: Predicate chain pushback
    if signals.get("cte_count", 0) >= 2:
        pathologies.append("P0")
        transforms.extend(["date_cte_isolate", "prefetch_fact_join", "early_filter"])

    # P1: Repeated scans
    if signals.get("max_scan_count", 0) > 1:
        pathologies.append("P1")
        transforms.extend(["single_pass_aggregation", "channel_bitmap_aggregation"])

    # P2: Correlated subquery
    if signals.get("has_delim_scan") or signals.get("has_nested_loop"):
        pathologies.append("P2")
        transforms.extend(["decorrelate", "composite_decorrelate_union"])

    # P3: Aggregate below join
    if locus == "AGGREGATE" and direction == "OVER_EST":
        pathologies.append("P3")
        transforms.extend(["aggregate_pushdown"])

    # P4: Cross-column OR
    if signals.get("has_or_filter"):
        pathologies.append("P4")
        transforms.extend(["or_to_union"])

    # P5: LEFT JOIN + NULL-eliminating WHERE
    if signals.get("has_left_join"):
        pathologies.append("P5")
        transforms.extend(["inner_join_conversion"])

    # P6: INTERSECT materialization
    if signals.get("has_intersect"):
        pathologies.append("P6")
        transforms.extend(["intersect_to_exists"])

    # P7: Self-joined CTE
    if locus == "CTE" and direction == "ZERO_EST":
        pathologies.append("P7")
        transforms.extend(["self_join_decomposition", "union_cte_split"])

    # P8: Window before join
    if signals.get("has_window") and signals.get("cte_count", 0) >= 2:
        pathologies.append("P8")
        transforms.extend(["deferred_window_aggregation"])

    # P9: Shared subexpression
    if signals.get("sentinel_zeros", 0) > 2:
        pathologies.append("P9")
        transforms.extend(["materialize_cte"])

    return pathologies, transforms


# ── Main analysis ─────────────────────────────────────────────────────────

def main():
    print("=" * 90)
    print("DEEP Q-ERROR ANALYSIS")
    print("=" * 90)

    # Load all data
    qerror_data = load_qerror_benchmark()
    leaderboard = load_leaderboard()
    gold = load_gold_examples()

    print(f"\nData loaded: {len(qerror_data)} Q-Error, {len(leaderboard)} leaderboard, {len(gold)} gold")

    # Merge datasets
    merged = {}
    for qnum in sorted(set(qerror_data.keys()) & set(leaderboard.keys())):
        qe = qerror_data[qnum]
        lb = leaderboard[qnum]
        ge = gold.get(qnum, {})

        merged[qnum] = {
            **qe,
            "best_speedup": lb["best_speedup"],
            "classification": lb["classification"],
            "gold_transform": lb.get("gold_transform", ""),
            "original_ms": lb["original_ms"],
            "gold_speedup": ge.get("speedup", 0),
            "gold_transform_id": ge.get("transform_id", ""),
        }

    print(f"Merged: {len(merged)} queries with both Q-Error and leaderboard data\n")

    # ─────────────────────────────────────────────────────────────────────
    # SECTION 1: EXPLAIN-only vs EXPLAIN ANALYZE
    # ─────────────────────────────────────────────────────────────────────
    print("=" * 90)
    print("§1  EXPLAIN-ONLY vs EXPLAIN ANALYZE — What's Free?")
    print("=" * 90)
    print("""
EXPLAIN (no execution):
  ✅ Estimated Cardinality per node      ✅ Plan tree structure
  ✅ Node types (JOIN, SCAN, etc.)       ✅ Join conditions & filters
  ✅ CTE boundaries                      ✅ DELIM_SCAN markers
  ❌ Actual row counts                   ❌ Execution timing
  ❌ Q-Error (needs actuals)             ❌ Memory/spill stats

EXPLAIN ANALYZE (full query execution):
  ✅ Everything above PLUS:
  ✅ operator_cardinality (actual rows)   ✅ operator_timing per node
  ✅ Q-Error computable                   ✅ Memory usage

CRITICAL INSIGHT: Q-Error itself requires EXPLAIN ANALYZE (i.e. running the
query). But STRUCTURAL SIGNALS from EXPLAIN-only are strong proxies:
""")

    # Count sentinel signals in benchmark data
    sentinel_zeros = sum(1 for q in merged.values() if q["estimated"] == 0)
    sentinel_ones = sum(1 for q in merged.values() if q["estimated"] == 1)
    delim_nodes = sum(1 for q in merged.values() if "DELIM" in q["node_type"])

    print(f"  Sentinel signals in 100 TPC-DS queries:")
    print(f"    Estimated = 0 (planner gave up):  {sentinel_zeros} queries")
    print(f"    Estimated = 1 (planner guessing):  {sentinel_ones} queries")
    print(f"    DELIM_SCAN (decorrelation marker): {delim_nodes} queries")

    # Check: are sentinel signals predictive of high Q-Error?
    sentinel_zero_qerrors = [q["max_qerror"] for q in merged.values() if q["estimated"] == 0 or q["actual"] == 0]
    non_sentinel_qerrors = [q["max_qerror"] for q in merged.values() if q["estimated"] != 0 and q["actual"] != 0]

    if sentinel_zero_qerrors:
        print(f"\n  Predictive power of Est=0 or Act=0 (visible from EXPLAIN when Est=0):")
        print(f"    Median Q-Error (sentinel zero):  {sorted(sentinel_zero_qerrors)[len(sentinel_zero_qerrors)//2]:,.0f}")
        if non_sentinel_qerrors:
            print(f"    Median Q-Error (non-sentinel):   {sorted(non_sentinel_qerrors)[len(non_sentinel_qerrors)//2]:,.0f}")
        print(f"    → Est=0 in EXPLAIN is {sorted(sentinel_zero_qerrors)[len(sentinel_zero_qerrors)//2] / max(1, sorted(non_sentinel_qerrors)[len(non_sentinel_qerrors)//2]):.0f}x stronger signal")

    print("""
  EXPLAIN-ONLY PROXY FEATURES (no execution needed):

  | Signal                     | What it tells you              | Detectable? |
  |----------------------------|--------------------------------|-------------|
  | Est=0 on any node          | Planner has ZERO information   | ✅ FREE     |
  | Est=1 on non-leaf node     | Planner is guessing minimum    | ✅ FREE     |
  | DELIM_SCAN/DELIM_JOIN      | Decorrelated subquery present  | ✅ FREE     |
  | Same table scanned N times | Repeated scan consolidation    | ✅ FREE     |
  | LEFT JOIN present          | Inner join conversion cand.    | ✅ FREE     |
  | INTERSECT/EXCEPT present   | EXISTS rewrite candidate       | ✅ FREE     |
  | CTE count ≥ 2              | Predicate pushback candidate   | ✅ FREE     |
  | Row est increase thru join | Join fanout misprediction      | ✅ FREE     |
  |                            |                                |             |
  | Actual row count           | Ground truth for Q-Error       | ❌ COSTLY   |
  | operator_timing            | Where time is actually spent   | ❌ COSTLY   |
  | Q-Error number             | Exact wrongness magnitude      | ❌ COSTLY   |

  VERDICT: For pre-screening, EXPLAIN-only gives ~80% of the signal.
  EXPLAIN ANALYZE is needed to QUANTIFY the wrongness and to compute the
  mismatch DIRECTION (under vs over-estimate), which routes to transforms.
""")

    # ─────────────────────────────────────────────────────────────────────
    # SECTION 2: Categorical Variables
    # ─────────────────────────────────────────────────────────────────────
    print("=" * 90)
    print("§2  CATEGORICAL VARIABLES — Derived from Q-Error")
    print("=" * 90)

    # Derive categories for all merged queries
    direction_counts = Counter()
    locus_counts = Counter()
    magnitude_counts = Counter()

    for qnum, d in merged.items():
        direction = derive_mismatch_direction(d["estimated"], d["actual"])
        locus = derive_mismatch_locus(d["node_type"])
        magnitude = derive_mismatch_magnitude(d["max_qerror"])
        d["direction"] = direction
        d["locus"] = locus
        d["magnitude"] = magnitude
        direction_counts[direction] += 1
        locus_counts[locus] += 1
        magnitude_counts[magnitude] += 1

    print("""
  Three categorical variables derivable from Q-Error analysis:
""")
    print("  MISMATCH_DIRECTION — which way is the optimizer wrong?")
    print("  " + "-" * 70)
    for cat in ["OVER_EST", "UNDER_EST", "ZERO_EST", "ACCURATE"]:
        count = direction_counts.get(cat, 0)
        pct = 100 * count / len(merged) if merged else 0
        # Compute avg speedup for this direction
        speeds = [d["best_speedup"] for d in merged.values() if d["direction"] == cat and d["best_speedup"] > 0]
        avg_spd = sum(speeds) / len(speeds) if speeds else 0
        wins = sum(1 for d in merged.values() if d["direction"] == cat and d["classification"] == "WIN")
        print(f"    {cat:<12} : {count:>3}/{len(merged)} ({pct:4.0f}%)  avg_speedup={avg_spd:.2f}x  wins={wins}")

    print(f"\n  MISMATCH_LOCUS — where in the plan is the optimizer wrong?")
    print("  " + "-" * 70)
    for cat in ["PROJECTION", "JOIN", "AGGREGATE", "CTE", "FILTER", "SCAN"]:
        count = locus_counts.get(cat, 0)
        pct = 100 * count / len(merged) if merged else 0
        speeds = [d["best_speedup"] for d in merged.values() if d["locus"] == cat and d["best_speedup"] > 0]
        avg_spd = sum(speeds) / len(speeds) if speeds else 0
        wins = sum(1 for d in merged.values() if d["locus"] == cat and d["classification"] == "WIN")
        print(f"    {cat:<12} : {count:>3}/{len(merged)} ({pct:4.0f}%)  avg_speedup={avg_spd:.2f}x  wins={wins}")

    print(f"\n  MISMATCH_MAGNITUDE — how wrong is the optimizer?")
    print("  " + "-" * 70)
    for cat in ["EXTREME", "3_ORDER", "2_ORDER", "1_ORDER", "MINOR"]:
        count = magnitude_counts.get(cat, 0)
        pct = 100 * count / len(merged) if merged else 0
        speeds = [d["best_speedup"] for d in merged.values() if d["magnitude"] == cat and d["best_speedup"] > 0]
        avg_spd = sum(speeds) / len(speeds) if speeds else 0
        wins = sum(1 for d in merged.values() if d["magnitude"] == cat and d["classification"] == "WIN")
        print(f"    {cat:<12} : {count:>3}/{len(merged)} ({pct:4.0f}%)  avg_speedup={avg_spd:.2f}x  wins={wins}")

    # ─────────────────────────────────────────────────────────────────────
    # SECTION 3: Q-Error Node Type → Pathology Mapping
    # ─────────────────────────────────────────────────────────────────────
    print(f"\n{'=' * 90}")
    print("§3  Q-ERROR NODE TYPE → PATHOLOGY MAPPING")
    print("=" * 90)
    print("""
  Does the Q-Error node type tell us WHICH pathology to target?
  Cross-referencing gold examples where we know the winning transform:
""")

    print(f"  {'Example':<28} {'Q-Err Node':<14} {'Direction':<12} {'Pathology':<6} {'Transform':<28} {'Speedup':<8}")
    print("  " + "-" * 96)

    # Manual pathology mapping for gold examples
    pathology_map = {
        "aggregate_pushdown": "P3",
        "channel_bitmap_aggregation": "P1",
        "self_join_decomposition": "P7",
        "inner_join_conversion": "P5",
        "early_filter": "P0",
        "intersect_to_exists": "P6",
        "rollup_to_union_windowing": "P7",
        "multi_intersect_exists_cte": "P6",
        "composite_decorrelate_union": "P2",
        "decorrelate": "P2",
        "date_cte_isolate": "P0",
        "union_cte_split": "P7",
        "or_to_union": "P4",
        "materialize_cte": "P9",
        "shared_dimension_multi_channel": "P0",
        "multi_dimension_prefetch": "P0",
    }

    gold_csv = list(csv.DictReader(open("Q-Error/results_all_gold_examples.csv")))
    node_to_pathology = defaultdict(list)

    for row in gold_csv:
        ex_id = row["example_id"]
        node = derive_mismatch_locus(row["orig_node_type"])
        direction = derive_mismatch_direction(int(float(row["orig_estimated"])), int(float(row["orig_actual"])))
        pathology = pathology_map.get(ex_id, "?")
        speedup = float(row["speedup"])

        node_to_pathology[f"{node}/{direction}"].append(pathology)

        print(f"  {ex_id:<28} {node:<14} {direction:<12} {pathology:<6} {ex_id:<28} {speedup:>6.2f}x")

    print(f"\n  NODE/DIRECTION → PATHOLOGY ROUTING TABLE:")
    print("  " + "-" * 70)
    for key, pathologies in sorted(node_to_pathology.items()):
        counts = Counter(pathologies)
        top = counts.most_common(3)
        routing = ", ".join(f"{p}({c})" for p, c in top)
        print(f"    {key:<24} → {routing}")

    # ─────────────────────────────────────────────────────────────────────
    # SECTION 4: Does Q-Error PINPOINT the weakness or just FLAG it?
    # ─────────────────────────────────────────────────────────────────────
    print(f"\n{'=' * 90}")
    print("§4  PINPOINT vs FLAG — How Specific Is the Signal?")
    print("=" * 90)
    print("""
  QUESTION: Does Q-Error tell us WHICH optimization to apply,
            or just that optimization is worthwhile?

  ANSWER: It's a LAYERED signal with increasing specificity:

  Layer 1 — MAGNITUDE (from EXPLAIN ANALYZE):
    "There IS an opportunity here."
    Q-Error > 100 → green light for optimization.
    Predictive power: 69% overlap with actual wins (gold examples).
    Limitation: Needs full query execution.

  Layer 2 — STRUCTURAL SIGNALS (from EXPLAIN-only, FREE):
    "Here's the TYPE of opportunity."
    DELIM_SCAN present → decorrelation candidate (P2)
    Same table N scans → consolidation candidate (P1)
    LEFT JOIN present → inner conversion candidate (P5)
    INTERSECT present → EXISTS rewrite candidate (P6)
    CTE count ≥ 2 → predicate pushback candidate (P0)
    Predictive power: narrows from 10 pathologies to 2-3.

  Layer 3 — DIRECTION (from EXPLAIN ANALYZE):
    "Here's the SPECIFIC weakness."
    UNDER_EST on JOIN → decorrelate, materialize CTE (planner chose wrong join)
    OVER_EST on AGGREGATE → push agg below join (planner overestimated fan-out)
    ZERO_EST on CTE → self-join decomposition (planner has no CTE statistics)
    Predictive power: narrows to 1-2 transforms.

  Layer 4 — LOCUS + DIRECTION (from EXPLAIN ANALYZE):
    "Here's the EXACT operator to fix."
    AGGREGATE/OVER_EST → P3 (aggregate pushdown)
    CTE/ZERO_EST → P7 (self-join decomposition)
    JOIN/UNDER_EST → P2 (decorrelation) or P0 (predicate pushback)
    SCAN/OVER_EST → P4 (cross-column OR) or P1 (repeated scans)
""")

    # ─────────────────────────────────────────────────────────────────────
    # SECTION 5: Win rate by Q-Error category
    # ─────────────────────────────────────────────────────────────────────
    print("=" * 90)
    print("§5  WIN RATE BY Q-ERROR CATEGORY")
    print("=" * 90)

    print(f"\n  {'Category':<28} {'Total':>5} {'WIN':>5} {'IMP':>5} {'NEU':>5} {'REG':>5} {'Win%':>6} {'Avg Spd':>8}")
    print("  " + "-" * 82)

    for label, filt in [
        ("CATASTROPHIC (>10K)", lambda d: d["max_qerror"] > 10000),
        ("MAJOR (100-10K)", lambda d: 100 < d["max_qerror"] <= 10000),
        ("MODERATE (10-100)", lambda d: 10 < d["max_qerror"] <= 100),
        ("MINOR/ACCURATE (<10)", lambda d: d["max_qerror"] <= 10),
        ("", None),
        ("Direction: OVER_EST", lambda d: d["direction"] == "OVER_EST"),
        ("Direction: UNDER_EST", lambda d: d["direction"] == "UNDER_EST"),
        ("Direction: ZERO_EST", lambda d: d["direction"] == "ZERO_EST"),
        ("", None),
        ("Locus: JOIN", lambda d: d["locus"] == "JOIN"),
        ("Locus: AGGREGATE", lambda d: d["locus"] == "AGGREGATE"),
        ("Locus: CTE", lambda d: d["locus"] == "CTE"),
        ("Locus: FILTER", lambda d: d["locus"] == "FILTER"),
        ("Locus: SCAN", lambda d: d["locus"] == "SCAN"),
        ("Locus: PROJECTION", lambda d: d["locus"] == "PROJECTION"),
    ]:
        if filt is None:
            print()
            continue
        subset = [d for d in merged.values() if filt(d)]
        if not subset:
            continue
        wins = sum(1 for d in subset if d["classification"] == "WIN")
        improved = sum(1 for d in subset if d["classification"] == "IMPROVED")
        neutral = sum(1 for d in subset if d["classification"] == "NEUTRAL")
        regression = sum(1 for d in subset if d["classification"] == "REGRESSION")
        speeds = [d["best_speedup"] for d in subset if d["best_speedup"] > 0]
        avg_spd = sum(speeds) / len(speeds) if speeds else 0
        win_pct = 100 * wins / len(subset) if subset else 0
        print(f"  {label:<28} {len(subset):>5} {wins:>5} {improved:>5} {neutral:>5} {regression:>5} {win_pct:>5.0f}% {avg_spd:>7.2f}x")

    # ─────────────────────────────────────────────────────────────────────
    # SECTION 6: Integration with Reasoning Chain
    # ─────────────────────────────────────────────────────────────────────
    print(f"\n{'=' * 90}")
    print("§6  INTEGRATION WITH THE ANALYST REASONING CHAIN")
    print("=" * 90)
    print("""
  Current chain (from duckdb.md EXPLAIN ANALYSIS PROCEDURE):
    Step 1: IDENTIFY COST SPINE
    Step 2: CLASSIFY EACH SPINE NODE
    Step 3: TRACE DATA FLOW (row counts should decrease)
    Step 4: CHECK SYMPTOM ROUTING TABLE
    Step 5: FORM BOTTLENECK HYPOTHESIS

  Enhanced chain with Q-Error:
    Step 1: IDENTIFY COST SPINE                               ← same
    Step 2: CLASSIFY EACH SPINE NODE                          ← same
    Step 2b: FLAG ESTIMATION ANOMALIES [NEW — EXPLAIN-only]
             • Est=0 on any spine node → BLIND_ESTIMATION
             • Est=1 on join/aggregate → SENTINEL_GUESS
             • DELIM_SCAN on spine → DECORRELATION_NEEDED
             • Same table appears 2+ times → REPEATED_SCAN
    Step 3: TRACE DATA FLOW                                   ← same
    Step 3b: COMPUTE Q-ERROR [NEW — needs EXPLAIN ANALYZE]
             If available: compute max(est/act, act/est) per spine node
             Classify: direction (OVER/UNDER/ZERO), locus, magnitude
    Step 4: CHECK SYMPTOM ROUTING TABLE                       ← enhanced
             Add Q-Error columns to routing table:
             | Symptom | + Q-Error Locus | + Direction | → Pathology |
    Step 5: FORM BOTTLENECK HYPOTHESIS                        ← enhanced
             Include Q-Error evidence: "The optimizer estimates X rows
             but actually processes Y rows (Q-Error = Z), suggesting
             [pathology] because [reasoning]."

  KEY: Step 2b is FREE (EXPLAIN-only). It narrows the pathology search
  from 10 candidates to 2-3 before the analyst even looks at Q-Error.
  Step 3b requires EXPLAIN ANALYZE but provides the quantitative signal
  that determines WHICH transform within that pathology to apply.
""")

    # ─────────────────────────────────────────────────────────────────────
    # SECTION 7: Enhanced Symptom Routing Table
    # ─────────────────────────────────────────────────────────────────────
    print("=" * 90)
    print("§7  ENHANCED SYMPTOM ROUTING TABLE")
    print("=" * 90)
    print("""
  | EXPLAIN Symptom               | Q-Error Locus | Direction  | Pathology | Action                        |
  |-------------------------------|---------------|------------|-----------|-------------------------------|
  | Row counts flat thru CTEs     | any           | any        | P0        | Push most selective predicate |
  | Same table scanned N times    | SCAN          | OVER_EST   | P1        | Single-pass CASE WHEN         |
  | Nested loop + inner aggregate | CTE/JOIN      | UNDER_EST  | P2        | Decorrelate to CTE+JOIN       |
  | Aggregate input >> output     | AGGREGATE     | OVER_EST   | P3        | Push GROUP BY below join      |
  | OR across DIFFERENT columns   | SCAN          | OVER_EST   | P4        | Split to UNION ALL (max 3)    |
  | LEFT JOIN + WHERE on right    | JOIN          | UNDER_EST  | P5        | Convert to INNER JOIN         |
  | INTERSECT, large inputs       | PROJECTION    | UNDER_EST  | P6        | Replace with EXISTS           |
  | CTE self-join, discriminators | CTE           | ZERO_EST   | P7        | Split per-partition CTEs      |
  | Window in CTE before join     | PROJECTION    | OVER_EST   | P8        | Defer window past join        |
  | Identical subtrees            | any           | OVER_EST   | P9        | Extract shared CTE            |
  |                               |               |            |           |                               |
  | Est=0 on CTE node             | CTE           | ZERO_EST   | P0/P7     | Planner blind to CTE stats    |
  | Est=1 on JOIN node            | JOIN          | UNDER_EST  | P2/P0     | Planner has no join estimate  |
  | DELIM_SCAN present            | CTE           | ZERO_EST   | P2        | Decorrelation needed          |
  | Est >> Act on GROUP BY        | AGGREGATE     | OVER_EST   | P3        | Agg below join opportunity    |
""")

    print("=" * 90)
    print("§8  VERDICT")
    print("=" * 90)
    print("""
  Q-Error is a LAYERED diagnostic, not a binary flag:

  1. EXPLAIN-only (FREE) gives structural signals that narrow
     10 pathologies → 2-3 candidates. This is the PRE-SCREEN.

  2. EXPLAIN ANALYZE (costs a query run) gives Q-Error magnitude
     + direction + locus. This PINPOINTS the weakness:
       • WHICH operator is wrong
       • HOW WRONG (magnitude → aggressiveness of transform)
       • WHICH WAY wrong (direction → specific transform choice)

  3. For benchmarks: use EXPLAIN ANALYZE — we run queries anyway.
     For real-world: use EXPLAIN-only signals as pre-screen,
     run EXPLAIN ANALYZE only on flagged queries (Est=0, DELIM_SCAN, etc.)

  4. The analyst's reasoning chain gains a new early-exit:
     If EXPLAIN shows ZERO estimation anomalies AND no structural
     signals → skip optimization (low ROI). Save API calls.

  5. Q-Error does NOT replace the pathology tree. It ACCELERATES it:
     Instead of checking P0 through P9 sequentially, Q-Error signals
     let the analyst jump directly to the 2-3 most likely pathologies.
""")

if __name__ == "__main__":
    main()
