#!/usr/bin/env python3
"""Backfill gold examples with complete original/optimized SQL pairs.

Reads original SQL from pipeline queries, finds the best optimized SQL
from Kimi/DSR1/V2 sources, and updates each gold example JSON with:
  - original_sql: full runnable original query
  - optimized_sql: full runnable optimized query
  - sf10_speedup: fresh speedup from SF10 benchmark (if available)

Usage:
    python3 backfill_gold_pairs.py [--dry-run]
"""
import json
import sys
from pathlib import Path

RESEARCH = Path(__file__).parent.parent
EXAMPLES_DIR = RESEARCH.parent / "packages" / "qt-sql" / "qt_sql" / "optimization" / "examples"
QUERIES_DIR = RESEARCH / "pipeline" / "state_0" / "queries"
BENCH_JSON = RESEARCH / "tpcds_benchmark" / "results_sf10_20260206.json"

# Source paths for optimized SQL
def dsr1_path(q): return RESEARCH / "state" / "responses" / f"q{q}_optimized.sql"
def kimi_path(q):
    if q <= 30:
        return RESEARCH / "CONSOLIDATED_BENCHMARKS" / "kimi_q1-q30_optimization" / f"q{q}" / "output_optimized.sql"
    return RESEARCH / "CONSOLIDATED_BENCHMARKS" / "kimi_q31-q99_optimization" / f"q{q}" / "output_optimized.sql"
def kimi_ready_path(q):
    if q <= 30:
        return RESEARCH / "CONSOLIDATED_BENCHMARKS" / "kimi_q1-q30_optimization" / "benchmark_ready" / f"q{q}_optimized.sql"
    return RESEARCH / "CONSOLIDATED_BENCHMARKS" / "kimi_q31-q99_optimization" / "benchmark_ready" / f"q{q}_optimized.sql"
def v2_path(q): return RESEARCH / "CONSOLIDATED_BENCHMARKS" / "benchmark_output_v2" / f"q{q}" / "final_optimized.sql"


# ─── Mapping: gold example → query + preferred source ─────────────────────
# Source priority: pick the source whose optimized SQL matches the gold pattern.
# Some gold examples are based on Kimi rewrites, others on DSR1/Retry3W.

GOLD_MAP = {
    "decorrelate":                   {"query": 1,  "sources": ["kimi", "kimi_ready"]},
    "date_cte_isolate":              {"query": 6,  "sources": ["kimi", "kimi_ready", "dsr1"]},
    "early_filter":                  {"query": 93, "sources": ["kimi", "kimi_ready"]},
    "or_to_union":                   {"query": 15, "sources": ["kimi", "kimi_ready"]},
    "single_pass_aggregation":       {"query": 9,  "sources": ["kimi_ready", "kimi"]},
    "pushdown":                      {"query": 9,  "sources": ["dsr1", "kimi_ready"]},
    "prefetch_fact_join":            {"query": 63, "sources": ["dsr1", "kimi_ready"]},
    "multi_dimension_prefetch":      {"query": 43, "sources": ["dsr1", "kimi", "kimi_ready"]},
    "dimension_cte_isolate":         {"query": 26, "sources": ["dsr1", "kimi", "kimi_ready"]},
    "multi_date_range_cte":          {"query": 29, "sources": ["kimi", "kimi_ready", "dsr1"]},
    "intersect_to_exists":           {"query": 14, "sources": ["dsr1", "kimi"]},
    "materialize_cte":               {"query": 95, "sources": ["kimi", "kimi_ready"]},
    "union_cte_split":               {"query": 74, "sources": ["kimi", "kimi_ready"]},
    "composite_decorrelate_union":   {"query": 35, "sources": ["dsr1", "kimi"]},
    "shared_dimension_multi_channel":{"query": 80, "sources": ["dsr1", "kimi"]},
    "deferred_window_aggregation":   {"query": 51, "sources": ["dsr1", "kimi"]},
}


def get_original_sql(query_num: int) -> str | None:
    """Get the full original SQL for a query."""
    f = QUERIES_DIR / f"q{query_num}.sql"
    if not f.exists():
        return None
    text = f.read_text()
    # Strip comment lines but keep the SQL
    lines = []
    for line in text.split("\n"):
        if line.strip().startswith("--"):
            continue
        lines.append(line)
    sql = "\n".join(lines).strip()
    # For multi-statement files, take only the first statement
    parts = sql.split(";")
    stmts = [p.strip() for p in parts if p.strip() and len(p.strip()) > 20]
    return stmts[0] + ";" if stmts else None


def get_optimized_sql(query_num: int, sources: list[str]) -> tuple[str | None, str]:
    """Find the optimized SQL from the first available source.
    Returns (sql, source_name).
    """
    for src in sources:
        if src == "kimi":
            p = kimi_path(query_num)
        elif src == "kimi_ready":
            p = kimi_ready_path(query_num)
        elif src == "dsr1":
            p = dsr1_path(query_num)
        elif src == "v2":
            p = v2_path(query_num)
        else:
            continue

        if p.exists():
            text = p.read_text().strip()
            if text and len(text) > 20:
                # Clean up: strip comments, normalize
                lines = [l for l in text.split("\n") if not l.strip().startswith("--")]
                sql = "\n".join(lines).strip().rstrip(";") + ";"
                return sql, src

    return None, "none"


def main():
    dry_run = "--dry-run" in sys.argv

    # Load benchmark results
    bench = {}
    if BENCH_JSON.exists():
        with open(BENCH_JSON) as f:
            bench = {r["query"]: r for r in json.load(f)["results"]}

    updated = 0
    errors = 0

    for example_file in sorted(EXAMPLES_DIR.glob("*.json")):
        with open(example_file) as f:
            example = json.load(f)

        ex_id = example.get("id", example_file.stem)
        if ex_id not in GOLD_MAP:
            print(f"  SKIP {ex_id}: not in gold map")
            continue

        info = GOLD_MAP[ex_id]
        qnum = info["query"]
        sources = info["sources"]

        # Get original SQL
        orig_sql = get_original_sql(qnum)
        if not orig_sql:
            print(f"  ERROR {ex_id}: no original SQL for Q{qnum}")
            errors += 1
            continue

        # Get optimized SQL
        opt_sql, opt_source = get_optimized_sql(qnum, sources)
        if not opt_sql:
            print(f"  ERROR {ex_id}: no optimized SQL for Q{qnum} (tried: {sources})")
            errors += 1
            continue

        # Get fresh benchmark data
        b = bench.get(qnum, {})
        sf10_orig_ms = b.get("original_mean_ms")
        sf10_speedup = b.get("speedup")
        sf10_rows_match = b.get("rows_match")

        # Verify they're actually different
        orig_clean = " ".join(orig_sql.split()).lower()
        opt_clean = " ".join(opt_sql.split()).lower()
        if orig_clean == opt_clean:
            print(f"  WARN {ex_id}: original and optimized SQL are IDENTICAL for Q{qnum} ({opt_source})")
            # Try next source
            for alt_src in sources:
                if alt_src == opt_source:
                    continue
                alt_sql, alt_name = get_optimized_sql(qnum, [alt_src])
                if alt_sql:
                    alt_clean = " ".join(alt_sql.split()).lower()
                    if alt_clean != orig_clean:
                        opt_sql = alt_sql
                        opt_source = alt_name
                        print(f"         -> switched to {alt_name}")
                        break
            else:
                print(f"  ERROR {ex_id}: ALL sources identical to original for Q{qnum}")
                errors += 1
                continue

        # Update the example
        example["original_sql"] = orig_sql
        example["optimized_sql"] = opt_sql
        example["optimized_source"] = opt_source
        example["benchmark_query_num"] = qnum

        if sf10_orig_ms:
            example["sf10_baseline_ms"] = sf10_orig_ms
        if sf10_speedup and b.get("optimized_source") == opt_source.replace("_ready", ""):
            example["sf10_speedup"] = round(sf10_speedup, 2)
            example["sf10_rows_match"] = sf10_rows_match

        sp_str = f"{sf10_speedup:.2f}x" if sf10_speedup else "N/A"
        print(f"  OK   {ex_id:<35} Q{qnum:>2} src={opt_source:<12} "
              f"orig={len(orig_sql):>5}ch opt={len(opt_sql):>5}ch sf10={sp_str}")

        if not dry_run:
            with open(example_file, "w") as f:
                json.dump(example, f, indent=2)
                f.write("\n")

        updated += 1

    print(f"\n{'DRY RUN - ' if dry_run else ''}Updated: {updated}  Errors: {errors}")


if __name__ == "__main__":
    main()
