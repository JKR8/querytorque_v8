"""
Build consolidated engine dossiers from scattered source files.

Reads engine profiles, gold examples, regression examples, and constraints,
then assembles two comprehensive dossier JSON files.

Usage:
    PYTHONPATH=packages/qt-shared:packages/qt-sql:. python3 -m qt_sql.knowledge.build_dossiers
"""
import json
import os
from pathlib import Path
from typing import Any

BASE = Path(__file__).resolve().parent.parent  # qt_sql/
CONSTRAINTS = BASE / "constraints"
EXAMPLES_DUCK = BASE / "examples" / "duckdb"
EXAMPLES_PG = BASE / "examples" / "postgres"
REGRESSIONS_DUCK = EXAMPLES_DUCK / "regressions"
KNOWLEDGE = BASE / "knowledge"


def _load(path: Path) -> dict:
    with open(path) as f:
        return json.load(f)


def _save(path: Path, data: dict):
    with open(path, "w") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)
    print(f"  Wrote {path.name} ({path.stat().st_size // 1024}KB)")


# ---------------------------------------------------------------------------
# Helpers to extract gold_example / regression / guard_rail entries
# ---------------------------------------------------------------------------

def _gold_entry(ex: dict) -> dict:
    """Extract a gold_example entry from an example JSON file."""
    entry: dict[str, Any] = {
        "id": ex["id"],
        "queries": ex.get("benchmark_queries", []),
        "speedup": ex.get("verified_speedup", ""),
    }
    if "sf10_speedup" in ex:
        entry["sf10_speedup"] = ex["sf10_speedup"]
    if "sf10_baseline_ms" in ex:
        entry["sf10_baseline_ms"] = ex["sf10_baseline_ms"]
    # principle
    entry["principle"] = ex.get("principle", ex.get("description", ""))
    # SQL
    entry["original_sql"] = ex.get("original_sql", "")
    entry["optimized_sql"] = ex.get("optimized_sql", "")
    # Insights
    if ex.get("example", {}).get("key_insight"):
        entry["key_insight"] = ex["example"]["key_insight"]
    elif ex.get("key_insight"):
        entry["key_insight"] = ex["key_insight"]
    if ex.get("example", {}).get("when_not_to_use"):
        entry["when_not_to_use"] = ex["example"]["when_not_to_use"]
    return entry


def _regression_entry(reg: dict) -> dict:
    """Extract a regression entry from a regression JSON file."""
    entry: dict[str, Any] = {
        "id": reg["id"],
        "query": reg.get("query_id", ""),
        "ratio": reg.get("verified_speedup", ""),
        "transform_attempted": reg.get("transform_attempted", ""),
        "regression_mechanism": reg.get("regression_mechanism", reg.get("description", "")),
    }
    entry["original_sql"] = reg.get("original_sql", reg.get("example", {}).get("before_sql", ""))
    entry["failed_sql"] = reg.get("example", {}).get("after_sql", "")
    return entry


def _guard_rail(c: dict) -> dict:
    """Extract a guard_rail entry from a constraint JSON file."""
    entry: dict[str, Any] = {
        "id": c["id"],
        "severity": c.get("severity", "MEDIUM"),
    }
    # Primary instruction
    entry["instruction"] = c.get("prompt_instruction", c.get("description", ""))
    # Evidence from failures
    failures = c.get("observed_failures", [])
    if failures:
        f = failures[0]
        q = f.get("query", "")
        reg = f.get("regression", f.get("speedup", ""))
        prob = f.get("problem", f.get("broken_rewrite", ""))
        entry["evidence"] = f"{q} {reg} — {prob}" if q else prob
    elif c.get("failure_rate"):
        entry["evidence"] = c["failure_rate"]
    return entry


# ---------------------------------------------------------------------------
# DuckDB Dossier
# ---------------------------------------------------------------------------

# Gap → gold example mapping
DUCK_GAP_GOLD = {
    "CROSS_CTE_PREDICATE_BLINDNESS": [
        "date_cte_isolate", "prefetch_fact_join", "early_filter", "pushdown",
        "dimension_cte_isolate", "multi_date_range_cte", "multi_dimension_prefetch",
        "shared_dimension_multi_channel",
    ],
    "REDUNDANT_SCAN_ELIMINATION": [
        "single_pass_aggregation", "channel_bitmap_aggregation",
    ],
    "CORRELATED_SUBQUERY_PARALYSIS": [
        "decorrelate", "composite_decorrelate_union",
    ],
    "CROSS_COLUMN_OR_DECOMPOSITION": [
        "or_to_union",
    ],
    "LEFT_JOIN_FILTER_ORDER_RIGIDITY": [],  # covered by prefetch examples
    "UNION_CTE_SELF_JOIN_DECOMPOSITION": [
        "union_cte_split", "rollup_to_union_windowing",
    ],
}

# Gap → regression example mapping
DUCK_GAP_REG = {
    "CROSS_CTE_PREDICATE_BLINDNESS": [
        "regression_q25_date_cte_isolate", "regression_q31_pushdown",
        "regression_q51_date_cte_isolate", "regression_q67_date_cte_isolate",
        "regression_q74_pushdown",
    ],
    "REDUNDANT_SCAN_ELIMINATION": [],
    "CORRELATED_SUBQUERY_PARALYSIS": [
        "regression_q1_decorrelate", "regression_q93_decorrelate",
    ],
    "CROSS_COLUMN_OR_DECOMPOSITION": [
        "regression_q90_materialize_cte",
    ],
    "LEFT_JOIN_FILTER_ORDER_RIGIDITY": [],
    "UNION_CTE_SELF_JOIN_DECOMPOSITION": [],
}

# Gap → constraint mapping (DuckDB-specific)
DUCK_GAP_CONSTRAINTS = {
    "CROSS_CTE_PREDICATE_BLINDNESS": [
        "no_cross_join_dimensions", "dimension_cte_same_column_or",
        "early_filter_cte_before_chain", "no_unfiltered_dimension_cte",
        "prefetch_multi_fact_chain",
    ],
    "REDUNDANT_SCAN_ELIMINATION": [
        "single_pass_aggregation_limit",
    ],
    "CORRELATED_SUBQUERY_PARALYSIS": [
        "decorrelate_must_filter_first",
    ],
    "CROSS_COLUMN_OR_DECOMPOSITION": [
        "or_to_union_guard", "or_to_union_self_join",
    ],
    "LEFT_JOIN_FILTER_ORDER_RIGIDITY": [],
    "UNION_CTE_SELF_JOIN_DECOMPOSITION": [
        "union_cte_split_must_replace",
    ],
}

# Orphan examples (not tied to a gap)
DUCK_ORPHANS = [
    "intersect_to_exists", "materialize_cte", "deferred_window_aggregation",
    "multi_intersect_exists_cte",
]

DUCK_ORPHAN_REG = [
    "regression_q16_semantic_rewrite", "regression_q95_semantic_rewrite",
]

# Global constraints
GLOBAL_CONSTRAINT_IDS = [
    "semantic_equivalence", "literal_preservation", "cte_column_completeness",
    "complete_output", "explicit_joins", "keep_exists_as_exists",
    "no_materialize_exists", "min_baseline_threshold", "remove_replaced_ctes",
    "decorrelate_must_filter_first",
]

# DuckDB transform catalog from research data
DUCK_TRANSFORM_CATALOG = {
    "date_cte_isolate": {"wins": 12, "avg_speedup": 1.34, "reliability": "HIGH"},
    "single_pass_aggregation": {"wins": 2, "max_speedup": 6.28, "reliability": "MEDIUM"},
    "channel_bitmap_aggregation": {"wins": 1, "max_speedup": 6.24, "reliability": "MEDIUM"},
    "decorrelate": {"wins": 3, "avg_speedup": 2.45, "reliability": "HIGH"},
    "or_to_union": {"wins": 5, "avg_speedup": 2.35, "reliability": "LOW",
                     "note": "High variance: 6.28x best, 0.23x worst"},
    "pushdown": {"wins": 4, "avg_speedup": 1.52, "reliability": "MEDIUM"},
    "early_filter": {"wins": 6, "avg_speedup": 1.67, "reliability": "HIGH"},
    "dimension_cte_isolate": {"wins": 5, "avg_speedup": 1.48, "reliability": "HIGH"},
    "prefetch_fact_join": {"wins": 4, "avg_speedup": 1.89, "reliability": "HIGH"},
    "multi_date_range_cte": {"wins": 3, "avg_speedup": 1.42, "reliability": "HIGH"},
    "multi_dimension_prefetch": {"wins": 3, "avg_speedup": 1.55, "reliability": "MEDIUM"},
    "union_cte_split": {"wins": 2, "avg_speedup": 1.72, "reliability": "MEDIUM"},
    "intersect_to_exists": {"wins": 2, "avg_speedup": 2.11, "reliability": "MEDIUM"},
    "composite_decorrelate_union": {"wins": 1, "max_speedup": 2.42, "reliability": "MEDIUM"},
    "rollup_to_union_windowing": {"wins": 1, "max_speedup": 2.47, "reliability": "LOW"},
    "materialize_cte": {"wins": 1, "max_speedup": 1.27, "reliability": "LOW"},
    "deferred_window_aggregation": {"wins": 1, "max_speedup": 1.36, "reliability": "LOW"},
    "shared_dimension_multi_channel": {"wins": 1, "max_speedup": 1.40, "reliability": "MEDIUM"},
}


def build_duckdb_dossier():
    print("Building DuckDB dossier...")
    profile = _load(CONSTRAINTS / "engine_profile_duckdb.json")

    # Load all gold examples
    gold_examples = {}
    for p in sorted(EXAMPLES_DUCK.glob("*.json")):
        ex = _load(p)
        gold_examples[ex["id"]] = ex

    # Load all regressions
    regressions = {}
    for p in sorted(REGRESSIONS_DUCK.glob("*.json")):
        reg = _load(p)
        regressions[reg["id"]] = reg

    # Load all constraints
    constraints = {}
    for p in sorted(CONSTRAINTS.glob("*.json")):
        c = _load(p)
        cid = c.get("id", p.stem)
        if c.get("profile_type") == "engine_profile":
            continue
        constraints[cid.lower()] = c

    # Enrich each gap
    for gap in profile["gaps"]:
        gap_id = gap["id"]

        # Gold examples
        gap["gold_examples"] = []
        for ex_id in DUCK_GAP_GOLD.get(gap_id, []):
            if ex_id in gold_examples:
                gap["gold_examples"].append(_gold_entry(gold_examples[ex_id]))

        # Regressions
        gap["regressions"] = []
        for reg_id in DUCK_GAP_REG.get(gap_id, []):
            if reg_id in regressions:
                gap["regressions"].append(_regression_entry(regressions[reg_id]))

        # Guard rails
        gap["guard_rails"] = []
        for c_name in DUCK_GAP_CONSTRAINTS.get(gap_id, []):
            c_key = c_name.lower()
            if c_key in constraints:
                gap["guard_rails"].append(_guard_rail(constraints[c_key]))

    # Standalone examples (orphans)
    standalone = []
    for ex_id in DUCK_ORPHANS:
        if ex_id in gold_examples:
            standalone.append(_gold_entry(gold_examples[ex_id]))
    orphan_regressions = []
    for reg_id in DUCK_ORPHAN_REG:
        if reg_id in regressions:
            orphan_regressions.append(_regression_entry(regressions[reg_id]))

    # Global guard rails
    global_rails = []
    for c_id in GLOBAL_CONSTRAINT_IDS:
        c_key = c_id.lower()
        if c_key in constraints:
            global_rails.append(_guard_rail(constraints[c_key]))

    # Assemble dossier
    dossier = {
        "schema_version": "3.0",
        "engine": profile["engine"],
        "version_tested": profile["version_tested"],
        "profile_type": "engine_dossier",
        "briefing_note": profile["briefing_note"],
        "strengths": profile["strengths"],
        "gaps": profile["gaps"],
        "standalone_examples": standalone,
        "standalone_regressions": orphan_regressions,
        "global_guard_rails": global_rails,
        "transform_catalog": DUCK_TRANSFORM_CATALOG,
    }

    _save(KNOWLEDGE / "duckdb_dossier.json", dossier)
    return dossier


# ---------------------------------------------------------------------------
# PostgreSQL Dossier
# ---------------------------------------------------------------------------

PG_GAP_GOLD = {
    "COMMA_JOIN_WEAKNESS": [
        "pg_dimension_prefetch_star",
    ],
    "CORRELATED_SUBQUERY_PARALYSIS": [
        "inline_decorrelate_materialized",
    ],
    "NON_EQUI_JOIN_INPUT_BLINDNESS": [
        "materialized_dimension_fact_prefilter",
    ],
    "CTE_MATERIALIZATION_FENCE": [],  # evidence in what_worked only
    "CROSS_CTE_PREDICATE_BLINDNESS": [
        "pg_self_join_decomposition",
    ],
}

# Map example file IDs to the IDs used in the JSON files (some have pg_ prefix)
PG_EXAMPLE_FILE_TO_ID = {
    "date_cte_explicit_join": "date_cte_explicit_join",
    "early_filter_decorrelate": "early_filter_decorrelate",
    "materialized_dimension_fact_prefilter": "pg_materialized_dimension_fact_prefilter",
    "self_join_decomposition": "pg_self_join_decomposition",
    "inline_decorrelate_materialized": "inline_decorrelate_materialized",
    "dimension_prefetch_star": "pg_dimension_prefetch_star",
}

# Also need: date_cte_explicit_join → COMMA_JOIN_WEAKNESS,
#             early_filter_decorrelate → CORRELATED_SUBQUERY_PARALYSIS
# Fix the mapping to be more accurate
PG_GAP_GOLD = {
    "COMMA_JOIN_WEAKNESS": [
        "pg_date_cte_explicit_join", "pg_dimension_prefetch_star",
    ],
    "CORRELATED_SUBQUERY_PARALYSIS": [
        "early_filter_decorrelate", "inline_decorrelate_materialized",
    ],
    "NON_EQUI_JOIN_INPUT_BLINDNESS": [
        "pg_materialized_dimension_fact_prefilter",
    ],
    "CTE_MATERIALIZATION_FENCE": [],
    "CROSS_CTE_PREDICATE_BLINDNESS": [
        "pg_self_join_decomposition",
    ],
}

PG_GAP_CONSTRAINTS = {
    "COMMA_JOIN_WEAKNESS": [],
    "CORRELATED_SUBQUERY_PARALYSIS": [],
    "NON_EQUI_JOIN_INPUT_BLINDNESS": [
        "pg_loose_prefilter_block",
    ],
    "CTE_MATERIALIZATION_FENCE": [
        "pg_cte_duplication_block", "no_materialized_keyword_pg",
    ],
    "CROSS_CTE_PREDICATE_BLINDNESS": [
        "pg_date_cte_caution",
    ],
}

# PG strength-level guard rails (attached to strengths, not gaps)
PG_STRENGTH_GUARDS = {
    "BITMAP_OR_SCAN": "pg_or_to_union_block",
    "SEMI_JOIN_EXISTS": "pg_exists_to_in_block",
}

PG_TRANSFORM_CATALOG = {
    "date_cte_isolate": {"wins": 4, "avg_speedup": 2.24, "reliability": "MEDIUM",
                          "note": "Must combine with explicit JOIN conversion"},
    "early_filter": {"wins": 3, "avg_speedup": 2.05, "reliability": "HIGH"},
    "decorrelate": {"wins": 2, "max_speedup": 4428, "reliability": "HIGH",
                     "note": "Timeout rescues on correlated scalar subqueries"},
    "materialize_cte": {"wins": 2, "avg_speedup": 2.94, "reliability": "MEDIUM",
                         "note": "PG auto-materializes; use for self-join dedup"},
    "explicit_join_conversion": {"wins": 3, "avg_speedup": 2.25, "reliability": "HIGH"},
    "dimension_prefetch": {"wins": 2, "avg_speedup": 2.80, "reliability": "HIGH"},
}


def build_pg_dossier():
    print("Building PostgreSQL dossier...")
    profile = _load(CONSTRAINTS / "engine_profile_postgresql.json")

    # Load PG gold examples
    gold_examples = {}
    for p in sorted(EXAMPLES_PG.glob("*.json")):
        ex = _load(p)
        gold_examples[ex["id"]] = ex

    # Load constraints
    constraints = {}
    for p in sorted(CONSTRAINTS.glob("*.json")):
        c = _load(p)
        cid = c.get("id", p.stem)
        if c.get("profile_type") == "engine_profile":
            continue
        constraints[cid.lower()] = c

    # Enrich gaps
    for gap in profile["gaps"]:
        gap_id = gap["id"]

        # Gold examples
        gap["gold_examples"] = []
        for ex_id in PG_GAP_GOLD.get(gap_id, []):
            if ex_id in gold_examples:
                gap["gold_examples"].append(_gold_entry(gold_examples[ex_id]))

        # No regression files for PG yet
        gap["regressions"] = []

        # Guard rails
        gap["guard_rails"] = []
        for c_name in PG_GAP_CONSTRAINTS.get(gap_id, []):
            c_key = c_name.lower()
            if c_key in constraints:
                gap["guard_rails"].append(_guard_rail(constraints[c_key]))

    # Enrich strengths with guard rails
    for strength in profile["strengths"]:
        s_id = strength["id"]
        if s_id in PG_STRENGTH_GUARDS:
            c_key = PG_STRENGTH_GUARDS[s_id].lower()
            if c_key in constraints:
                strength["guard_rail"] = _guard_rail(constraints[c_key])

    # Global guard rails
    global_rails = []
    for c_id in GLOBAL_CONSTRAINT_IDS:
        c_key = c_id.lower()
        if c_key in constraints:
            global_rails.append(_guard_rail(constraints[c_key]))

    # Assemble dossier
    dossier = {
        "schema_version": "3.0",
        "engine": profile["engine"],
        "version_tested": profile["version_tested"],
        "profile_type": "engine_dossier",
        "briefing_note": profile["briefing_note"],
        "strengths": profile["strengths"],
        "gaps": profile["gaps"],
        "set_local_config_intel": profile.get("set_local_config_intel", {}),
        "scale_sensitivity_warning": profile.get("scale_sensitivity_warning", ""),
        "global_guard_rails": global_rails,
        "transform_catalog": PG_TRANSFORM_CATALOG,
    }

    _save(KNOWLEDGE / "postgresql_dossier.json", dossier)
    return dossier


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    os.makedirs(KNOWLEDGE, exist_ok=True)
    duck = build_duckdb_dossier()
    pg = build_pg_dossier()

    # Verification summary
    print("\n=== Verification ===")

    # Count embedded items
    duck_gold_count = sum(len(g.get("gold_examples", [])) for g in duck["gaps"])
    duck_gold_count += len(duck.get("standalone_examples", []))
    duck_reg_count = sum(len(g.get("regressions", [])) for g in duck["gaps"])
    duck_reg_count += len(duck.get("standalone_regressions", []))
    duck_rail_count = sum(len(g.get("guard_rails", [])) for g in duck["gaps"])
    duck_rail_count += len(duck.get("global_guard_rails", []))

    pg_gold_count = sum(len(g.get("gold_examples", [])) for g in pg["gaps"])
    pg_rail_count = sum(len(g.get("guard_rails", [])) for g in pg["gaps"])
    pg_rail_count += len(pg.get("global_guard_rails", []))

    print(f"DuckDB:  {duck_gold_count} gold, {duck_reg_count} regressions, {duck_rail_count} guard rails")
    print(f"PG:      {pg_gold_count} gold, {pg_rail_count} guard rails")

    # Check against source file counts
    src_duck_gold = len(list(EXAMPLES_DUCK.glob("*.json")))
    src_duck_reg = len(list(REGRESSIONS_DUCK.glob("*.json")))
    src_pg_gold = len(list(EXAMPLES_PG.glob("*.json")))

    print(f"\nSource:  DuckDB {src_duck_gold} gold, {src_duck_reg} regressions | PG {src_pg_gold} gold")
    print(f"Embedded: DuckDB {duck_gold_count} gold, {duck_reg_count} regressions | PG {pg_gold_count} gold")

    if duck_gold_count < src_duck_gold:
        missing = set(p.stem for p in EXAMPLES_DUCK.glob("*.json")) - \
                  set(e["id"] for g in duck["gaps"] for e in g.get("gold_examples", [])) - \
                  set(e["id"] for e in duck.get("standalone_examples", []))
        print(f"  WARNING: Missing DuckDB gold: {missing}")
    if duck_reg_count < src_duck_reg:
        missing = set(p.stem for p in REGRESSIONS_DUCK.glob("*.json")) - \
                  set(e["id"] for g in duck["gaps"] for e in g.get("regressions", [])) - \
                  set(e["id"] for e in duck.get("standalone_regressions", []))
        print(f"  WARNING: Missing DuckDB regressions: {missing}")
    if pg_gold_count < src_pg_gold:
        all_pg = set(e["id"] for g in pg["gaps"] for e in g.get("gold_examples", []))
        src_pg_ids = set(_load(p)["id"] for p in EXAMPLES_PG.glob("*.json"))
        missing = src_pg_ids - all_pg
        print(f"  WARNING: Missing PG gold: {missing}")

    print("\nDone!")


if __name__ == "__main__":
    main()
