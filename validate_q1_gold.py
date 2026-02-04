#!/usr/bin/env python3
"""Validate recorded Q1 gold optimization"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent / "packages" / "qt-shared"))
sys.path.insert(0, str(Path(__file__).parent / "packages" / "qt-sql"))

from qt_sql.validation.sql_validator import SQLValidator

# Load recorded gold optimization
gold_dir = Path("research/experiments/dspy_runs/all_20260201_205640/q1")
original_sql = (gold_dir / "original.sql").read_text()
optimized_sql = (gold_dir / "optimized.sql").read_text()

full_db = "/mnt/d/TPC-DS/tpcds_sf100.duckdb"

print("="*80)
print("Validating RECORDED Q1 Gold Optimization (Feb 1st)")
print("="*80)
print(f"Previous result: 1.35x speedup (0.0203s → 0.015s)")
print(f"\nRunning on: {full_db}\n")

validator = SQLValidator(database=full_db)
result = validator.validate(original_sql, optimized_sql)

print("="*80)
print("CURRENT VALIDATION")
print("="*80)
print(f"Status: {result.status.value}")
print(f"Speedup: {result.speedup:.2f}x")
print(f"Original time: {result.original_timing_ms/1000:.3f}s")
print(f"Optimized time: {result.optimized_timing_ms/1000:.3f}s")
print(f"Row counts match: {result.row_counts_match}")
print("="*80)
print(f"\nComparison:")
print(f"  Feb 1st:  1.35x (0.0203s → 0.015s)")
print(f"  Today:    {result.speedup:.2f}x ({result.original_timing_ms/1000:.3f}s → {result.optimized_timing_ms/1000:.3f}s)")
print("="*80)
