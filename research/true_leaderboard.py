"""
Build the TRUE PG DSB leaderboard from swarm benchmark_iter0.json files,
bypassing the broken best/ copy step.
"""
import json
from pathlib import Path

SWARM = Path("packages/qt-sql/qt_sql/benchmarks/postgres_dsb/swarm_batch_20260208_142643")
BEST_DIR = Path("packages/qt-sql/qt_sql/benchmarks/postgres_dsb/best")

results = []

for bfile in sorted(SWARM.glob("*/benchmark_iter0.json")):
    query_id = bfile.parent.name
    data = json.loads(bfile.read_text())

    baseline = data.get("baseline_trimmed_mean_ms", 0)
    if baseline == 0:
        continue

    # Find best passing worker
    best_w = None
    best_speedup = 0
    for w in data.get("workers", []):
        if w["status"] == "pass" and w["rows_match"] and w["speedup"] > best_speedup:
            best_speedup = w["speedup"]
            best_w = w

    # Check what's in best/ dir
    best_json = BEST_DIR / f"{query_id}.json"
    reported_speedup = None
    reported_source = None
    if best_json.exists():
        bj = json.loads(best_json.read_text())
        reported_speedup = bj.get("speedup")
        reported_source = bj.get("source", "?")

    results.append({
        "query": query_id,
        "baseline_ms": round(baseline, 1),
        "true_best_worker": best_w["worker_id"] if best_w else None,
        "true_speedup": round(best_speedup, 2) if best_w else 0,
        "true_opt_ms": round(best_w["trimmed_mean_ms"], 1) if best_w else None,
        "reported_speedup": reported_speedup,
        "reported_source": reported_source,
        "mismatch": best_w is not None and reported_speedup is not None and abs(best_speedup - reported_speedup) > 0.15,
    })

# Sort by true speedup descending
results.sort(key=lambda x: x["true_speedup"], reverse=True)

print(f"{'Query':<25} {'Orig ms':>10} {'True W':>6} {'True ms':>10} {'True Spd':>9} {'Reported':>9} {'Source':>10} {'MISMATCH':>9}")
print("-" * 100)

mismatches = 0
for r in results:
    flag = " <<<" if r["mismatch"] else ""
    tw = f"W{r['true_best_worker']}" if r["true_best_worker"] else "none"
    tm = f"{r['true_opt_ms']:.1f}" if r["true_opt_ms"] else "-"
    rep = f"{r['reported_speedup']:.2f}x" if r['reported_speedup'] else "-"
    print(f"{r['query']:<25} {r['baseline_ms']:>10.1f} {tw:>6} {tm:>10} {r['true_speedup']:>8.2f}x {rep:>9} {r['reported_source'] or '-':>10}{flag}")
    if r["mismatch"]:
        mismatches += 1

# Summary
wins = sum(1 for r in results if r["true_speedup"] >= 1.5)
improved = sum(1 for r in results if 1.1 <= r["true_speedup"] < 1.5)
neutral = sum(1 for r in results if 0.9 <= r["true_speedup"] < 1.1)
regression = sum(1 for r in results if r["true_speedup"] < 0.9 and r["true_speedup"] > 0)
no_pass = sum(1 for r in results if r["true_speedup"] == 0)

print(f"\n=== TRUE LEADERBOARD SUMMARY ===")
print(f"  WIN (>=1.5x):     {wins}")
print(f"  IMPROVED (1.1-1.5x): {improved}")
print(f"  NEUTRAL (0.9-1.1x): {neutral}")
print(f"  REGRESSION (<0.9x): {regression}")
print(f"  NO PASS:          {no_pass}")
print(f"  MISMATCHES:       {mismatches}")
