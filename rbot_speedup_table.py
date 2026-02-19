#!/usr/bin/env python3
"""
Compute R-Bot runtime speedups vs original baseline for PostgreSQL DSB-76 benchmark.

Data sources:
1. Beam result files -> original_ms (baseline runtime)
2. R-Bot race CSV -> rbot_median_ms

Output: table with original_ms, rbot_median_ms, rbot_speedup per query.
"""

import csv
import json
import math
from collections import defaultdict
from pathlib import Path

BEAM_DIR = Path(
    "packages/qt-sql/qt_sql/benchmarks/postgres_dsb_76/beam_sessions/"
    "run_beam_20260218_best"
)
RBOT_CSV = Path(
    "paper/rbot_dsb_full_run_20260213/QUERYTORQUE_vs_RBOT_RUNTIME_RACE.csv"
)

TIMEOUT_MS = 300_000  # 300s = timeout sentinel


def safe_float(val):
    """Convert to float, return None for empty/invalid."""
    if val is None or val == "":
        return None
    try:
        return float(val)
    except (ValueError, TypeError):
        return None


def load_beam_originals(beam_dir):
    """Load original_ms from beam result files.

    Returns dict: beam_query_id (e.g. 'query001_multi_i1') -> original_ms
    """
    originals = {}
    for session_dir in sorted(beam_dir.iterdir()):
        if not session_dir.is_dir():
            continue
        result_file = session_dir / "iter0_result.txt"
        if not result_file.exists():
            continue
        with open(result_file) as f:
            data = json.load(f)
        patches = data.get("patches", [])
        # Find first patch with non-null original_ms
        original_ms = None
        for patch in patches:
            if patch.get("original_ms") is not None:
                original_ms = patch["original_ms"]
                break
        if original_ms is not None:
            originals[session_dir.name] = original_ms
        else:
            print(f"  INFO: No original_ms in {session_dir.name} (all patches failed)")
    return originals


def load_rbot_csv(csv_path):
    """Load R-Bot race CSV rows."""
    with open(csv_path) as f:
        reader = csv.DictReader(f)
        return list(reader)


def build_mapping(rbot_rows, beam_originals):
    """Map CSV rows to beam query IDs and compute speedups.

    CSV instance 0 -> beam _i1, instance 1 -> beam _i2

    Special cases:
    - query039 has statement_idx 0 and 1 (multi-statement) -- sum them per instance
    - query072_spj_spj, query084_spj_spj, query101_spj_spj, query102_spj_spj
      in CSV map to _agg in beam
    """
    # CSV qt_query_id -> beam base name mapping for mismatches
    csv_to_beam_base = {
        "query072_spj_spj": "query072_agg",
        "query084_spj_spj": "query084_agg",
        "query101_spj_spj": "query101_agg",
        "query102_spj_spj": "query102_agg",
    }

    # Group rows by (qt_query_id, instance) for multi-statement queries (query039)
    grouped = defaultdict(list)
    for row in rbot_rows:
        key = (row["qt_query_id"], row["instance"])
        grouped[key].append(row)

    results = []
    for (qt_query_id, instance), rows in sorted(grouped.items()):
        # Sum rbot_median_ms across statements (handling empty values)
        rbot_parts = [safe_float(r["rbot_median_ms"]) for r in rows]
        qt_parts = [safe_float(r["qt_median_ms"]) for r in rows]

        # If any part is None, the total is None (can't sum partial)
        rbot_median_ms = sum(rbot_parts) if all(p is not None for p in rbot_parts) else None
        qt_median_ms = sum(qt_parts) if all(p is not None for p in qt_parts) else None

        # Map to beam query ID
        beam_base = csv_to_beam_base.get(qt_query_id, qt_query_id)
        instance_suffix = f"_i{int(instance) + 1}"
        beam_query_id = beam_base + instance_suffix

        original_ms = beam_originals.get(beam_query_id)

        winner = rows[0].get("winner", "")
        rbot_errors = int(rows[0].get("rbot_errors", 0) or 0)

        rbot_speedup = None
        qt_speedup = None
        if original_ms and rbot_median_ms and rbot_median_ms > 0:
            rbot_speedup = original_ms / rbot_median_ms
        if original_ms and qt_median_ms and qt_median_ms > 0:
            qt_speedup = original_ms / qt_median_ms

        results.append({
            "beam_query_id": beam_query_id,
            "csv_query_id": qt_query_id,
            "instance": instance,
            "original_ms": original_ms,
            "rbot_median_ms": rbot_median_ms,
            "qt_median_ms": qt_median_ms,
            "rbot_speedup": rbot_speedup,
            "qt_speedup": qt_speedup,
            "winner": winner,
            "rbot_errors": rbot_errors,
        })

    return results


def format_speedup(speedup):
    if speedup is None:
        return "N/A"
    if speedup >= 100:
        return f"{speedup:.0f}x"
    if speedup >= 10:
        return f"{speedup:.1f}x"
    return f"{speedup:.2f}x"


def format_ms(ms):
    if ms is None:
        return "N/A"
    if ms >= 10000:
        return f"{ms:,.0f}"
    return f"{ms:,.1f}"


def main():
    print("=" * 130)
    print("R-Bot vs QueryTorque Runtime Speedups -- PostgreSQL DSB-76 Benchmark")
    print("=" * 130)
    print()

    beam_originals = load_beam_originals(BEAM_DIR)
    print(f"Loaded {len(beam_originals)} beam original_ms values")

    rbot_rows = load_rbot_csv(RBOT_CSV)
    print(f"Loaded {len(rbot_rows)} R-Bot CSV rows")
    print()

    results = build_mapping(rbot_rows, beam_originals)

    # Check for unmatched
    unmatched = [r for r in results if r["original_ms"] is None]
    if unmatched:
        print(f"WARNING: {len(unmatched)} rows have no beam original_ms:")
        for r in unmatched:
            print(f"  {r['csv_query_id']} instance={r['instance']} -> {r['beam_query_id']}")
        print()

    rbot_failed = [r for r in results if r["rbot_median_ms"] is None]
    if rbot_failed:
        print(f"WARNING: {len(rbot_failed)} rows have no R-Bot median (R-Bot failed):")
        for r in rbot_failed:
            print(f"  {r['csv_query_id']} instance={r['instance']} (rbot_errors={r['rbot_errors']})")
        print()

    # Print table
    header = (
        f"{'Query ID':<28} "
        f"{'Original (ms)':>14} "
        f"{'R-Bot (ms)':>12} "
        f"{'R-Bot Spd':>10} "
        f"{'QT (ms)':>12} "
        f"{'QT Spd':>10} "
        f"{'Winner':>12} "
        f"{'Note':>12}"
    )
    print(header)
    print("-" * len(header))

    # Summary stats
    rbot_wins = 0
    rbot_regressions = 0
    rbot_neutrals = 0
    rbot_errors = 0
    qt_wins_count = 0
    total_matched = 0

    for r in results:
        original_ms = r["original_ms"]
        rbot_median_ms = r["rbot_median_ms"]
        rbot_speedup = r["rbot_speedup"]
        qt_speedup = r["qt_speedup"]

        note = ""
        if original_ms is not None and original_ms >= TIMEOUT_MS:
            note = "TIMEOUT"
        if rbot_median_ms is None:
            note = "RBOT_FAIL"
            rbot_errors += 1

        print(
            f"{r['beam_query_id']:<28} "
            f"{format_ms(original_ms):>14} "
            f"{format_ms(rbot_median_ms):>12} "
            f"{format_speedup(rbot_speedup):>10} "
            f"{format_ms(r['qt_median_ms']):>12} "
            f"{format_speedup(qt_speedup):>10} "
            f"{r['winner']:>12} "
            f"{note:>12}"
        )

        # Classify R-Bot result
        if rbot_speedup is not None:
            total_matched += 1
            if rbot_speedup >= 1.05:
                rbot_wins += 1
            elif rbot_speedup < 0.95:
                rbot_regressions += 1
            else:
                rbot_neutrals += 1

        if qt_speedup is not None and qt_speedup >= 1.05:
            qt_wins_count += 1

    print("-" * len(header))
    print()

    # Summary
    print("=" * 60)
    print("SUMMARY")
    print("=" * 60)
    print(f"Total query instances:         {len(results)}")
    print(f"Matched (have both baselines): {total_matched}")
    print(f"R-Bot errors (no result):      {rbot_errors}")
    print()
    print(f"R-Bot wins (>=1.05x):          {rbot_wins}")
    print(f"R-Bot neutral (0.95-1.05x):    {rbot_neutrals}")
    print(f"R-Bot regressions (<0.95x):    {rbot_regressions}")
    print(f"QT wins (>=1.05x):             {qt_wins_count}")
    print()

    # Top R-Bot wins
    sorted_by_rbot = sorted(
        [r for r in results if r["rbot_speedup"] is not None],
        key=lambda r: r["rbot_speedup"],
        reverse=True,
    )
    print("Top 10 R-Bot speedups:")
    for r in sorted_by_rbot[:10]:
        print(
            f"  {r['beam_query_id']:<28} "
            f"rbot={format_speedup(r['rbot_speedup']):>8} "
            f"qt={format_speedup(r['qt_speedup']):>8} "
            f"original={format_ms(r['original_ms'])}ms"
        )

    print()
    print("Worst 10 R-Bot results:")
    for r in sorted_by_rbot[-10:]:
        print(
            f"  {r['beam_query_id']:<28} "
            f"rbot={format_speedup(r['rbot_speedup']):>8} "
            f"qt={format_speedup(r['qt_speedup']):>8} "
            f"original={format_ms(r['original_ms'])}ms"
        )

    # Geometric and arithmetic means
    valid_rbot = [r["rbot_speedup"] for r in results if r["rbot_speedup"] is not None and r["rbot_speedup"] > 0]
    valid_qt = [r["qt_speedup"] for r in results if r["qt_speedup"] is not None and r["qt_speedup"] > 0]

    print()
    print("=" * 60)
    print("AGGREGATE STATISTICS")
    print("=" * 60)
    if valid_rbot:
        geo_rbot = math.exp(sum(math.log(s) for s in valid_rbot) / len(valid_rbot))
        arith_rbot = sum(valid_rbot) / len(valid_rbot)
        median_rbot = sorted(valid_rbot)[len(valid_rbot) // 2]
        print(f"R-Bot geometric mean speedup:  {format_speedup(geo_rbot)}  (n={len(valid_rbot)})")
        print(f"R-Bot arithmetic mean speedup: {format_speedup(arith_rbot)}")
        print(f"R-Bot median speedup:          {format_speedup(median_rbot)}")

    if valid_qt:
        geo_qt = math.exp(sum(math.log(s) for s in valid_qt) / len(valid_qt))
        arith_qt = sum(valid_qt) / len(valid_qt)
        median_qt = sorted(valid_qt)[len(valid_qt) // 2]
        print(f"QT geometric mean speedup:     {format_speedup(geo_qt)}  (n={len(valid_qt)})")
        print(f"QT arithmetic mean speedup:    {format_speedup(arith_qt)}")
        print(f"QT median speedup:             {format_speedup(median_qt)}")


if __name__ == "__main__":
    main()
