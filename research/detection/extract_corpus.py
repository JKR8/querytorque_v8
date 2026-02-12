#!/usr/bin/env python3
"""Extract dossier data into corpus_schema_v2 format (transforms.json + trials.jsonl).

Sources:
  - packages/qt-sql/qt_sql/knowledge/duckdb_dossier.json
  - packages/qt-sql/qt_sql/knowledge/postgresql_dossier.json
  - Precondition features from detectability.jsx (hardcoded below)
  - Contraindication data from distilled_algorithm.md (hardcoded below)

Output:
  - research/detection/transforms.json   — cold table (~20 rows)
  - research/detection/trials.jsonl      — hot table (25+ rows)
"""

import json
import re
import sys
from pathlib import Path

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------
ROOT = Path(__file__).resolve().parent.parent.parent  # QueryTorque_V8/
DUCKDB_DOSSIER = ROOT / "packages/qt-sql/qt_sql/knowledge/duckdb_dossier.json"
PG_DOSSIER = ROOT / "packages/qt-sql/qt_sql/knowledge/postgresql_dossier.json"
OUT_DIR = Path(__file__).resolve().parent  # research/detection/
TRANSFORMS_OUT = OUT_DIR / "transforms.json"
TRIALS_OUT = OUT_DIR / "trials.jsonl"

# ---------------------------------------------------------------------------
# Precondition features — from detectability.jsx TRANSFORMS array
# ---------------------------------------------------------------------------
PRECONDITION_FEATURES: dict[str, list[str]] = {
    "date_cte_isolate": ["GROUP_BY", "HAVING", "AGG_AVG", "AGG_COUNT", "DATE_DIM", "MULTI_TABLE_5+", "SCALAR_SUB_2+"],
    "prefetch_fact_join": ["AGG_AVG", "AGG_SUM", "CASE_EXPR", "DATE_DIM", "GROUP_BY", "WINDOW_FUNC"],
    "early_filter": ["AGG_SUM", "CASE_EXPR", "GROUP_BY", "LEFT_JOIN"],
    "pushdown": ["AGG_AVG", "AGG_COUNT", "BETWEEN", "CASE_EXPR", "SCALAR_SUB_5+", "TABLE_REPEAT_8+"],
    "dimension_cte_isolate": ["AGG_AVG", "DATE_DIM", "GROUP_BY", "OR_BRANCH"],
    "multi_date_range_cte": ["AGG_AVG", "BETWEEN", "DATE_DIM", "GROUP_BY", "MULTI_TABLE_5+", "TABLE_REPEAT_3+"],
    "multi_dimension_prefetch": ["AGG_SUM", "CASE_EXPR", "DATE_DIM", "GROUP_BY"],
    "shared_dimension_multi_channel": ["AGG_SUM", "BETWEEN", "CTE", "DATE_DIM", "GROUP_BY", "LEFT_JOIN", "MULTI_TABLE_5+", "ROLLUP", "SCALAR_SUB_2+", "TABLE_REPEAT_3+", "UNION"],
    "single_pass_aggregation": ["AGG_AVG", "AGG_COUNT", "BETWEEN", "CASE_EXPR", "SCALAR_SUB_5+", "TABLE_REPEAT_8+"],
    "channel_bitmap_aggregation": ["AGG_COUNT", "SCALAR_SUB_5+", "TABLE_REPEAT_8+"],
    "decorrelate": ["AGG_AVG", "AGG_SUM", "CTE", "DATE_DIM", "GROUP_BY", "SCALAR_SUB_2+"],
    "composite_decorrelate_union": ["AGG_COUNT", "AGG_SUM", "DATE_DIM", "EXISTS", "GROUP_BY", "MULTI_TABLE_5+", "OR_BRANCH", "SCALAR_SUB_2+", "TABLE_REPEAT_3+"],
    "or_to_union": ["AGG_SUM", "DATE_DIM", "GROUP_BY", "OR_BRANCH"],
    "union_cte_split": ["CASE_EXPR", "CTE", "DATE_DIM", "GROUP_BY", "UNION"],
    "rollup_to_union_windowing": ["AGG_SUM", "CASE_EXPR", "DATE_DIM", "GROUP_BY", "ROLLUP", "WINDOW_FUNC"],
}

# needsExplain flags — from detectability.jsx
NEEDS_EXPLAIN: dict[str, bool] = {
    "date_cte_isolate": False,
    "prefetch_fact_join": True,
    "early_filter": True,
    "pushdown": True,
    "dimension_cte_isolate": False,
    "multi_date_range_cte": False,
    "multi_dimension_prefetch": True,
    "shared_dimension_multi_channel": False,
    "single_pass_aggregation": True,
    "channel_bitmap_aggregation": False,
    "decorrelate": True,
    "composite_decorrelate_union": False,
    "or_to_union": False,
    "union_cte_split": False,
    "rollup_to_union_windowing": False,
}

# ---------------------------------------------------------------------------
# Contraindications — from distilled_algorithm.md + dossier what_didnt_work
# ---------------------------------------------------------------------------
CONTRAINDICATIONS: dict[str, list[dict]] = {
    "date_cte_isolate": [
        {"id": "LOW_BASELINE", "instruction": "Skip if baseline <100ms — CTE overhead exceeds filter savings", "severity": "MEDIUM", "worst_ratio": 0.50},
        {"id": "ROLLUP_PRESENT", "instruction": "CTE may prevent optimizer from pushing ROLLUP/window down through join tree", "severity": "LOW", "worst_ratio": 0.85},
    ],
    "dimension_cte_isolate": [
        {"id": "CROSS_JOIN_3_DIMS", "instruction": "NEVER cross-join 3+ dimension CTEs — Cartesian explosion", "severity": "CRITICAL", "worst_ratio": 0.0076},
        {"id": "UNFILTERED_CTE", "instruction": "Every CTE must have a WHERE clause — unfiltered CTE = pure overhead", "severity": "HIGH", "worst_ratio": None},
    ],
    "or_to_union": [
        {"id": "MAX_3_BRANCHES", "instruction": "Max 3 UNION branches — 6+ is lethal (9 branches = 9x fact scans)", "severity": "CRITICAL", "worst_ratio": 0.23},
        {"id": "SAME_COL_OR", "instruction": "NEVER split same-column ORs — engine handles natively", "severity": "HIGH", "worst_ratio": 0.59},
        {"id": "SELF_JOIN_PRESENT", "instruction": "NEVER if self-join present — each branch re-does the self-join", "severity": "HIGH", "worst_ratio": 0.51},
    ],
    "decorrelate": [
        {"id": "MISSING_FILTER", "instruction": "Preserve ALL WHERE filters from original subquery — missing filter = cross-product", "severity": "CRITICAL", "worst_ratio": 0.34},
        {"id": "ALREADY_DECORRELATED", "instruction": "Check EXPLAIN — if hash join (not nested loop), optimizer already decorrelated", "severity": "MEDIUM", "worst_ratio": None},
    ],
    "union_cte_split": [
        {"id": "ORPHANED_UNION", "instruction": "Original UNION must be eliminated — keeping both = double materialization", "severity": "HIGH", "worst_ratio": 0.49},
    ],
    "prefetch_fact_join": [
        {"id": "MAX_2_CHAINS", "instruction": "Max 2 cascading fact-table CTE chains — 3rd causes excessive materialization", "severity": "MEDIUM", "worst_ratio": 0.78},
    ],
}

# min_baseline_ms guards (from distilled_algorithm.md)
MIN_BASELINE_MS: dict[str, float] = {
    "date_cte_isolate": 100.0,
}


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def load_json(path: Path) -> dict:
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def parse_speedup(speedup_str: str) -> float | None:
    """Parse '4.00x' → 4.0, 'timeout_rescue' → None."""
    if not speedup_str or speedup_str == "timeout_rescue":
        return None
    m = re.match(r"([\d.]+)x", speedup_str)
    return float(m.group(1)) if m else None


def get_timing_ms(example: dict, engine: str) -> tuple[float | None, float | None]:
    """Extract (original_ms, rewritten_ms) from gold example timing fields."""
    ps = example.get("plan_signature", {})

    if engine == "postgresql":
        # PG plan_signature uses _ms keys
        orig = ps.get("original_time_ms")
        opt = ps.get("optimized_time_ms")
        if orig is not None and opt is not None:
            return orig, opt

    if engine == "duckdb":
        # DuckDB plan_signature uses _s keys
        orig = ps.get("original_time_s")
        opt = ps.get("optimized_time_s")
        if orig is not None and opt is not None:
            return orig * 1000.0, opt * 1000.0

    # Fallback: explain_timing (always in seconds)
    et = example.get("explain_timing", {})
    orig = et.get("original_s")
    opt = et.get("optimized_s")
    if orig is not None and opt is not None:
        return orig * 1000.0, opt * 1000.0

    return None, None


def compute_outcome(ratio: float | None) -> str:
    if ratio is None:
        return "UNKNOWN"
    if ratio > 1.05:
        return "WIN"
    if ratio < 0.95:
        return "LOSS"
    return "DRAW"


def extract_ratio_from_text(text: str) -> float | None:
    """Extract ratio like '0.50x' or '0.23x' from what_didnt_work text."""
    m = re.search(r"([\d.]+)x", text)
    return float(m.group(1)) if m else None


def extract_query_from_text(text: str) -> str | None:
    """Extract query ID like 'Q25' or 'Q013' from what_didnt_work text."""
    m = re.match(r"(Q\d+)", text)
    return m.group(1) if m else None


# ---------------------------------------------------------------------------
# Step 1: Build transforms
# ---------------------------------------------------------------------------
def build_transforms(duckdb: dict, pg: dict) -> list[dict]:
    """Build unique transform records from both dossiers."""
    seen: dict[str, dict] = {}

    def process_example(ex: dict, gap_id: str | None, engine: str):
        tid = ex["id"]
        if tid in seen:
            # Merge: add engine info if new
            existing = seen[tid]
            if engine not in existing.get("_engines", []):
                existing["_engines"].append(engine)
            return

        # Build notes from key_insight + when_not_to_use
        notes_parts = []
        if ex.get("key_insight"):
            notes_parts.append(ex["key_insight"])
        if ex.get("when_not_to_use"):
            notes_parts.append(ex["when_not_to_use"])
        notes = " | ".join(notes_parts) if notes_parts else None

        seen[tid] = {
            "id": tid,
            "principle": ex.get("principle", ""),
            "precondition_features": PRECONDITION_FEATURES.get(tid, _derive_features_from_sql(ex)),
            "contraindications": CONTRAINDICATIONS.get(tid, []),
            "gap": gap_id,
            "notes": notes,
            "min_baseline_ms": MIN_BASELINE_MS.get(tid),
            "confirm_with_explain": NEEDS_EXPLAIN.get(tid, False),
            "_engines": [engine],
        }

    # DuckDB gaps
    for gap in duckdb.get("gaps", []):
        gap_id = gap["id"]
        for ex in gap.get("gold_examples", []):
            process_example(ex, gap_id, "duckdb")

    # DuckDB standalone examples
    for ex in duckdb.get("standalone_examples", []):
        process_example(ex, None, "duckdb")

    # PG gaps
    for gap in pg.get("gaps", []):
        gap_id = gap["id"]
        for ex in gap.get("gold_examples", []):
            process_example(ex, gap_id, "postgresql")

    # Finalize: remove internal _engines field, sort by id
    transforms = []
    for t in sorted(seen.values(), key=lambda x: x["id"]):
        engines = t.pop("_engines")
        t["engines"] = engines
        transforms.append(t)

    return transforms


def _derive_features_from_sql(ex: dict) -> list[str]:
    """For transforms not in detectability.jsx, derive basic features from SQL."""
    sql = (ex.get("original_sql") or "").upper()
    features = []
    if "GROUP BY" in sql:
        features.append("GROUP_BY")
    if "HAVING" in sql:
        features.append("HAVING")
    if "AVG(" in sql:
        features.append("AGG_AVG")
    if "SUM(" in sql:
        features.append("AGG_SUM")
    if "COUNT(" in sql:
        features.append("AGG_COUNT")
    if "CASE " in sql or "CASE\n" in sql:
        features.append("CASE_EXPR")
    if "BETWEEN" in sql:
        features.append("BETWEEN")
    if "DATE_DIM" in sql:
        features.append("DATE_DIM")
    if "LEFT JOIN" in sql or "LEFT OUTER JOIN" in sql:
        features.append("LEFT_JOIN")
    if "UNION" in sql:
        features.append("UNION")
    if "ROLLUP" in sql:
        features.append("ROLLUP")
    if "INTERSECT" in sql:
        features.append("INTERSECT")
    if "EXISTS" in sql:
        features.append("EXISTS")
    if "OVER (" in sql or "OVER(" in sql:
        features.append("WINDOW_FUNC")
    if " OR " in sql:
        features.append("OR_BRANCH")
    if "WITH " in sql:
        features.append("CTE")
    return sorted(set(features))


# ---------------------------------------------------------------------------
# Step 2: Build trials (gold examples)
# ---------------------------------------------------------------------------
def build_gold_trials(duckdb: dict, pg: dict) -> list[dict]:
    """Build trial records from gold examples in both dossiers."""
    trials = []
    trial_id = 1

    def process_example(ex: dict, gap_id: str | None, engine: str, engine_version: str):
        nonlocal trial_id

        orig_ms, opt_ms = get_timing_ms(ex, engine)
        declared_speedup = parse_speedup(ex.get("speedup", ""))

        # Use declared speedup as authoritative ratio (benchmarked, multi-run).
        # EXPLAIN ANALYZE single-run timing is captured in orig_ms/opt_ms but
        # often disagrees with the benchmarked speedup.
        if declared_speedup is not None:
            ratio = declared_speedup
        elif orig_ms and opt_ms and opt_ms > 0:
            ratio = round(orig_ms / opt_ms, 2)
        else:
            ratio = None

        outcome = compute_outcome(ratio)

        # Special case: timeout_rescue
        if ex.get("speedup") == "timeout_rescue":
            outcome = "WIN"
            # Use explain_timing for ms
            et = ex.get("explain_timing", {})
            orig_ms = et.get("original_s", 120.0) * 1000.0
            opt_ms = et.get("optimized_s", 0.26) * 1000.0
            ratio = round(orig_ms / opt_ms, 2) if opt_ms and opt_ms > 0 else None

        trial = {
            "id": trial_id,
            "query_sql": ex.get("original_sql"),
            "rewritten_sql": ex.get("optimized_sql"),
            "transform": ex["id"],
            "original_ms": round(orig_ms, 3) if orig_ms is not None else None,
            "rewritten_ms": round(opt_ms, 3) if opt_ms is not None else None,
            "ratio": ratio,
            "outcome": outcome,
            "declared_speedup": ex.get("speedup"),
            "sf10_speedup": ex.get("sf10_speedup"),
            "query_hash": None,
            "engine": engine,
            "engine_version": engine_version,
            "scale_factor": "SF1" if engine == "duckdb" else "SF10",
            "explain_original": ex.get("original_explain"),
            "explain_rewritten": ex.get("optimized_explain"),
            "gap": gap_id,
            "queries": ex.get("queries", []),
        }
        trials.append(trial)
        trial_id += 1

    # DuckDB gaps
    for gap in duckdb.get("gaps", []):
        gap_id = gap["id"]
        for ex in gap.get("gold_examples", []):
            process_example(ex, gap_id, "duckdb", duckdb.get("version_tested", "1.1+"))

    # DuckDB standalone examples
    for ex in duckdb.get("standalone_examples", []):
        process_example(ex, None, "duckdb", duckdb.get("version_tested", "1.1+"))

    # PG gaps
    for gap in pg.get("gaps", []):
        gap_id = gap["id"]
        for ex in gap.get("gold_examples", []):
            process_example(ex, gap_id, "postgresql", pg.get("version_tested", "14.3+"))

    return trials


# ---------------------------------------------------------------------------
# Step 3: Build regression trials from what_didnt_work + standalone_regressions
# ---------------------------------------------------------------------------
def build_regression_trials(duckdb: dict, pg: dict, start_id: int) -> list[dict]:
    """Build LOSS trial records from what_didnt_work entries and standalone regressions."""
    trials = []
    trial_id = start_id

    def process_what_didnt_work(entries: list, gap_id: str, engine: str, engine_version: str):
        nonlocal trial_id
        for entry in entries:
            ratio = extract_ratio_from_text(entry)
            query = extract_query_from_text(entry)
            outcome = compute_outcome(ratio) if ratio else "LOSS"

            trial = {
                "id": trial_id,
                "query_sql": None,
                "rewritten_sql": None,
                "transform": None,  # Unknown — the gap tells us the category
                "original_ms": None,
                "rewritten_ms": None,
                "ratio": ratio,
                "outcome": outcome,
                "query_hash": None,
                "engine": engine,
                "engine_version": engine_version,
                "scale_factor": "SF1" if engine == "duckdb" else "SF10",
                "explain_original": None,
                "explain_rewritten": None,
                "gap": gap_id,
                "queries": [query] if query else [],
                "regression_mechanism": entry,
            }
            trials.append(trial)
            trial_id += 1

    # DuckDB gaps what_didnt_work
    for gap in duckdb.get("gaps", []):
        gap_id = gap["id"]
        wdw = gap.get("what_didnt_work", [])
        if wdw:
            process_what_didnt_work(wdw, gap_id, "duckdb", duckdb.get("version_tested", "1.1+"))

    # PG gaps what_didnt_work
    for gap in pg.get("gaps", []):
        gap_id = gap["id"]
        wdw = gap.get("what_didnt_work", [])
        if wdw:
            process_what_didnt_work(wdw, gap_id, "postgresql", pg.get("version_tested", "14.3+"))

    # DuckDB standalone regressions (have SQL)
    for reg in duckdb.get("standalone_regressions", []):
        ratio = extract_ratio_from_text(reg.get("ratio", ""))
        trial = {
            "id": trial_id,
            "query_sql": reg.get("original_sql"),
            "rewritten_sql": reg.get("failed_sql"),
            "transform": reg.get("transform_attempted"),
            "original_ms": None,
            "rewritten_ms": None,
            "ratio": ratio,
            "outcome": compute_outcome(ratio) if ratio else "LOSS",
            "query_hash": None,
            "engine": "duckdb",
            "engine_version": duckdb.get("version_tested", "1.1+"),
            "scale_factor": "SF1",
            "explain_original": None,
            "explain_rewritten": None,
            "gap": None,
            "queries": [reg.get("query", "")],
            "regression_mechanism": reg.get("regression_mechanism"),
        }
        trials.append(trial)
        trial_id += 1

    return trials


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main():
    print("Loading dossiers...")
    duckdb = load_json(DUCKDB_DOSSIER)
    pg = load_json(PG_DOSSIER)

    # Step 1: transforms.json
    print("Building transforms...")
    transforms = build_transforms(duckdb, pg)
    with open(TRANSFORMS_OUT, "w", encoding="utf-8") as f:
        json.dump(transforms, f, indent=2, ensure_ascii=False)
    print(f"  -> {TRANSFORMS_OUT}: {len(transforms)} transforms")

    # Step 2: gold trials
    print("Building gold trials...")
    gold_trials = build_gold_trials(duckdb, pg)
    print(f"  -> {len(gold_trials)} gold trials")

    # Step 3: regression trials
    print("Building regression trials...")
    regression_trials = build_regression_trials(duckdb, pg, start_id=len(gold_trials) + 1)
    print(f"  -> {len(regression_trials)} regression trials")

    # Combine and write trials.jsonl
    all_trials = gold_trials + regression_trials
    with open(TRIALS_OUT, "w", encoding="utf-8") as f:
        for trial in all_trials:
            f.write(json.dumps(trial, ensure_ascii=False) + "\n")
    print(f"  -> {TRIALS_OUT}: {len(all_trials)} total trials")

    # Summary
    print("\n--- Summary ---")
    print(f"Transforms: {len(transforms)}")
    for t in transforms:
        features_count = len(t["precondition_features"])
        contra_count = len(t["contraindications"])
        engines = ", ".join(t["engines"])
        print(f"  {t['id']:40s} gap={t['gap'] or 'standalone':45s} features={features_count:2d} contras={contra_count} engines={engines}")

    print(f"\nGold trials: {len(gold_trials)}")
    for t in gold_trials:
        print(f"  #{t['id']:2d} {t['transform']:40s} ratio={t['ratio']!s:8s} outcome={t['outcome']:7s} engine={t['engine']}")

    print(f"\nRegression trials: {len(regression_trials)}")
    for t in regression_trials:
        mechanism = (t.get("regression_mechanism") or "")[:60]
        print(f"  #{t['id']:2d} ratio={t['ratio']!s:8s} outcome={t['outcome']:7s} {mechanism}")

    # Verification
    print("\n--- Verification ---")
    gold_with_sql = sum(1 for t in gold_trials if t["query_sql"])
    gold_with_explain = sum(1 for t in gold_trials if t["explain_original"])
    gold_with_timing = sum(1 for t in gold_trials if t["original_ms"] is not None)
    print(f"Gold trials with SQL:     {gold_with_sql}/{len(gold_trials)}")
    print(f"Gold trials with EXPLAIN: {gold_with_explain}/{len(gold_trials)}")
    print(f"Gold trials with timing:  {gold_with_timing}/{len(gold_trials)}")

    transforms_with_features = sum(1 for t in transforms if t["precondition_features"])
    transforms_with_contras = sum(1 for t in transforms if t["contraindications"])
    print(f"Transforms with features:        {transforms_with_features}/{len(transforms)}")
    print(f"Transforms with contraindications: {transforms_with_contras}/{len(transforms)}")


if __name__ == "__main__":
    main()
