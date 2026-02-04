#!/usr/bin/env python3
"""
Test DSPy V5 optimizer on Q1
"""
import os
import sys
from pathlib import Path

# Add packages to path
ROOT = Path(__file__).resolve().parents[4]
sys.path.insert(0, str(ROOT / "packages" / "qt-shared"))
sys.path.insert(0, str(ROOT / "packages" / "qt-sql"))

from qt_sql.optimization.adaptive_rewriter_v5 import optimize_v5_dspy

# Load Q1
q1_path = ROOT / "research" / "archive" / "queries" / "q1.sql"
with open(q1_path) as f:
    sql = f.read()

print("=" * 80)
print("DSPy V5 Optimizer Test - Q1")
print("=" * 80)
print(f"\nOriginal Query:\n{sql}\n")

# Load .env file
from dotenv import load_dotenv
load_dotenv()

# Check API key (try both variable names)
api_key = os.environ.get("DEEPSEEK_API_KEY") or os.environ.get("QT_DEEPSEEK_API_KEY")
if not api_key:
    print("❌ DEEPSEEK_API_KEY or QT_DEEPSEEK_API_KEY not set")
    print("Set it with: export DEEPSEEK_API_KEY=your_key_here")
    sys.exit(1)

# Set for dspy_optimizer to find
os.environ["DEEPSEEK_API_KEY"] = api_key

print(f"✅ API Key configured (starts with: {api_key[:10]}...)")

# Check for databases
sample_db = "/mnt/d/TPC-DS/tpcds_sf1.duckdb"
full_db = "/mnt/d/TPC-DS/tpcds_sf100.duckdb"

# Check if databases exist
if not Path(sample_db).exists():
    print(f"⚠️  Sample DB not found at {sample_db}")
    print("Looking for alternative databases...")
    # Try to find any duckdb file
    import glob
    dbs = glob.glob("/mnt/*/TPC-DS/*.duckdb") + glob.glob("/mnt/c/Users/**/tpcds*.duckdb", recursive=True)
    if dbs:
        sample_db = dbs[0]
        print(f"Using: {sample_db}")
    else:
        print("❌ No database found. Please provide path to TPC-DS database.")
        sys.exit(1)
else:
    print(f"✅ Sample DB found: {sample_db}")

print("\n" + "=" * 80)
print("Running DSPy V5 Optimizer (5 parallel workers)...")
print("=" * 80)

try:
    # Run optimizer
    results = optimize_v5_dspy(
        sql=sql,
        sample_db=sample_db,
        max_workers=5,
        provider="deepseek"
    )

    print(f"\n✅ Optimization complete! Got {len(results)} valid candidates\n")

    # Display results
    for i, result in enumerate(results, 1):
        print("=" * 80)
        print(f"Candidate #{i} (Worker {result.worker_id})")
        print("=" * 80)
        print(f"Status: {result.status.value}")
        print(f"Speedup: {result.speedup:.2f}x")
        if result.error:
            print(f"Error: {result.error}")
        print(f"\nOptimized SQL:\n{result.optimized_sql}\n")
        print(f"Response:\n{result.response[:500]}...\n")

    # Show best
    if results:
        best = max(results, key=lambda r: r.speedup)
        print("=" * 80)
        print(f"🏆 BEST CANDIDATE: Worker {best.worker_id} with {best.speedup:.2f}x speedup")
        print("=" * 80)

except Exception as e:
    print(f"\n❌ Error during optimization: {e}")
    import traceback
    traceback.print_exc()
    sys.exit(1)
