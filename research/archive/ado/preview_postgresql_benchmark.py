#!/usr/bin/env python3
"""
Preview script - shows what will be benchmarked without running it
"""

import json
from pathlib import Path

ROUND_DIR = Path("/mnt/c/Users/jakc9/Documents/QueryTorque_V8/research/ado/rounds/round_01")

print("=" * 80)
print("POSTGRESQL DSB BENCHMARK - PREVIEW")
print("=" * 80)
print()

# Discover queries
query_dirs = sorted([d for d in ROUND_DIR.iterdir() if d.is_dir()])

print(f"📊 Total Queries to Benchmark: {len(query_dirs)}")
print()
print("QUERY LIST WITH TRANSFORMS:")
print()

by_transform = {}

for query_dir in query_dirs:
    query_id = query_dir.name

    # Read metadata
    metadata_file = query_dir / "metadata.json"
    if metadata_file.exists():
        with open(metadata_file) as f:
            meta = json.load(f)

        transforms = ", ".join(meta.get("transforms", ["none"]))

        # Check SQL files exist
        orig_sql = query_dir / "original.sql"
        opt_sql = query_dir / "optimized.sql"

        has_files = "✅" if (orig_sql.exists() and opt_sql.exists()) else "❌"

        print(f"{has_files} {query_id:30} → {transforms}")

        # Track by transform
        key = transforms
        if key not in by_transform:
            by_transform[key] = []
        by_transform[key].append(query_id)

print()
print("=" * 80)
print("QUERIES BY TRANSFORM:")
print("=" * 80)
print()

for transform in sorted(by_transform.keys(), key=lambda x: len(by_transform[x]), reverse=True):
    count = len(by_transform[transform])
    pct = count * 100 // len(query_dirs)
    print(f"  {transform:45} → {count:2} queries ({pct:2}%)")
    if count <= 3:
        for q in by_transform[transform]:
            print(f"    • {q}")

print()
print("=" * 80)
print("BENCHMARK CONFIGURATION:")
print("=" * 80)
print()
print("Database:        postgres://jakc9@127.0.0.1:5433/dsb_sf10")
print("Validation:      3-run method")
print("  • Run 3 times per query")
print("  • Discard first run (warmup)")
print("  • Average last 2 runs")
print()
print("Timeout:         300 seconds per query")
print("Expected Time:   ~1 hour (53 queries × 6 runs)")
print()
print("Classification:")
print("  • WIN:        ≥1.3x speedup")
print("  • PASS:       0.95x - 1.3x (neutral)")
print("  • REGRESSION: <0.95x")
print()
print("=" * 80)
print("TO RUN BENCHMARK:")
print("=" * 80)
print()
print("  bash /mnt/c/Users/jakc9/Documents/QueryTorque_V8/research/ado/RUN_POSTGRESQL_BENCHMARK.sh")
print()
print("RESULTS WILL BE SAVED TO:")
print("  /mnt/c/Users/jakc9/Documents/QueryTorque_V8/research/ado/validation_results/postgresql_dsb_validation.json")
print()
