#!/usr/bin/env python3
"""Reorganize master board with best speedup first."""

import csv
from pathlib import Path
from datetime import datetime

PROJECT_ROOT = Path(__file__).parent.parent
INPUT = PROJECT_ROOT / "research/CONSOLIDATED_BENCHMARKS/DuckDB_TPC-DS_Master_v2_20260206.csv"

# Read input
rows = []
with open(INPUT) as f:
    reader = csv.DictReader(f)
    original_fields = reader.fieldnames
    for row in reader:
        rows.append(row)

# Process each row to find best speedup
output_rows = []
for row in rows:
    qnum = row['Query_Num']

    # Collect all speedups with their sources
    candidates = []

    # Kimi speedup (only if pass)
    if row.get('Kimi_Status') == 'pass':
        try:
            s = float(row.get('Kimi_Speedup', 0) or 0)
            if s > 0:
                candidates.append(('Kimi', s))
        except: pass

    # Evo speedup (only if success)
    if row.get('Evo_Status') == 'success':
        try:
            s = float(row.get('Evo_Best_Speedup', 0) or 0)
            if s > 0:
                candidates.append(('Evo', s))
        except: pass

    # Retry3W speedup (only if pass)
    if row.get('Retry3W_Status') == 'pass':
        try:
            s = float(row.get('Retry3W_SF10_Speedup', 0) or 0)
            if s > 0:
                candidates.append(('Retry3W', s))
        except: pass

    # Find best
    if candidates:
        best_source, best_speedup = max(candidates, key=lambda x: x[1])
    else:
        best_source, best_speedup = '', 0.0

    # Determine status
    if best_speedup >= 1.5:
        status = 'WIN'
    elif best_speedup >= 1.1:
        status = 'IMPROVED'
    elif best_speedup >= 0.95:
        status = 'NEUTRAL'
    elif best_speedup > 0:
        status = 'REGRESSION'
    else:
        status = 'NO_DATA'

    # Build output row with best first
    out = {
        'Query': f'Q{qnum}',
        'Best_Speedup': f'{best_speedup:.2f}x' if best_speedup > 0 else '',
        'Best_Source': best_source,
        'Status': status,
        # Individual sources
        'Kimi': f"{float(row.get('Kimi_Speedup', 0) or 0):.2f}x" if row.get('Kimi_Status') == 'pass' else '',
        'Evo': f"{float(row.get('Evo_Best_Speedup', 0) or 0):.2f}x" if row.get('Evo_Status') == 'success' else '',
        'Retry3W_SF10': f"{float(row.get('Retry3W_SF10_Speedup', 0) or 0):.2f}x" if row.get('Retry3W_Status') == 'pass' else '',
        'Retry3W_Worker': row.get('Retry3W_Best_Worker', ''),
        # Timing info from Kimi (most detailed)
        'Original_ms': row.get('Kimi_Original_ms', ''),
        'Optimized_ms': row.get('Kimi_Optimized_ms', ''),
        # Recommendations
        'Transform': row.get('Transform_Recommended', '') or row.get('Gold_Transform', ''),
        'Expected': row.get('Expected_Speedup', '') or row.get('Gold_Expected_Speedup', ''),
        # Detailed retry info
        'R3W_W1': row.get('Retry3W_W1', ''),
        'R3W_W2': row.get('Retry3W_W2', ''),
        'R3W_W3': row.get('Retry3W_W3', ''),
    }
    output_rows.append(out)

# Sort by best speedup descending
output_rows.sort(key=lambda x: float(x['Best_Speedup'].replace('x', '') or 0), reverse=True)

# Write output
output_file = PROJECT_ROOT / "research/CONSOLIDATED_BENCHMARKS/DuckDB_TPC-DS_Leaderboard_20260206.csv"
fieldnames = ['Query', 'Best_Speedup', 'Best_Source', 'Status', 'Kimi', 'Evo', 'Retry3W_SF10', 'Retry3W_Worker',
              'Original_ms', 'Optimized_ms', 'Transform', 'Expected', 'R3W_W1', 'R3W_W2', 'R3W_W3']

with open(output_file, 'w', newline='') as f:
    writer = csv.DictWriter(f, fieldnames=fieldnames)
    writer.writeheader()
    writer.writerows(output_rows)

print(f"Created: {output_file}")
print(f"\nTop 20 by speedup:")
print(f"{'Query':<6} {'Best':<8} {'Source':<8} {'Status':<12}")
print("-" * 40)
for row in output_rows[:20]:
    print(f"{row['Query']:<6} {row['Best_Speedup']:<8} {row['Best_Source']:<8} {row['Status']:<12}")

# Summary stats
wins = len([r for r in output_rows if r['Status'] == 'WIN'])
improved = len([r for r in output_rows if r['Status'] == 'IMPROVED'])
neutral = len([r for r in output_rows if r['Status'] == 'NEUTRAL'])
regression = len([r for r in output_rows if r['Status'] == 'REGRESSION'])
print(f"\nSummary: {wins} WIN, {improved} IMPROVED, {neutral} NEUTRAL, {regression} REGRESSION")
