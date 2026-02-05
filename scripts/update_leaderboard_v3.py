#!/usr/bin/env python3
"""Update leaderboard with 4-worker neutral results."""

import csv
from pathlib import Path
from datetime import datetime

PROJECT_ROOT = Path(__file__).parent.parent

# Load existing leaderboard
LEADERBOARD = PROJECT_ROOT / "research/CONSOLIDATED_BENCHMARKS/DuckDB_TPC-DS_Leaderboard_20260206.csv"

# Load new neutral results
NEUTRAL_RESULTS = PROJECT_ROOT / "retry_neutrals/validation_20260206_010443.csv"

# Read neutral results
neutral_data = {}
with open(NEUTRAL_RESULTS) as f:
    reader = csv.DictReader(f)
    for row in reader:
        qid = row['query_id'].upper()
        neutral_data[qid] = row

# Read and update leaderboard
rows = []
with open(LEADERBOARD) as f:
    reader = csv.DictReader(f)
    fieldnames = list(reader.fieldnames) + ['Retry4W_SF10', 'Retry4W_Worker', 'Retry4W_W1', 'Retry4W_W2', 'Retry4W_W3', 'Retry4W_W4']

    for row in reader:
        qid = row['Query']

        if qid in neutral_data:
            nr = neutral_data[qid]
            new_speedup = float(nr['best_speedup']) if nr['best_speedup'] else 0

            # Update best if neutral result is better
            current_best = float(row['Best_Speedup'].replace('x', '')) if row['Best_Speedup'] else 0

            if nr['best_status'] == 'pass' and new_speedup > current_best:
                row['Best_Speedup'] = f"{new_speedup:.2f}x"
                row['Best_Source'] = 'Retry4W'
                # Update status
                if new_speedup >= 1.5:
                    row['Status'] = 'WIN'
                elif new_speedup >= 1.1:
                    row['Status'] = 'IMPROVED'
                elif new_speedup >= 0.95:
                    row['Status'] = 'NEUTRAL'
                else:
                    row['Status'] = 'REGRESSION'

            # Add new columns
            row['Retry4W_SF10'] = f"{new_speedup:.2f}x" if nr['best_status'] == 'pass' else ''
            row['Retry4W_Worker'] = nr['best_worker'] if nr['best_status'] == 'pass' else ''
            row['Retry4W_W1'] = f"{float(nr['w1_speedup']):.2f}" if nr['w1_speedup'] and nr['w1_status'] == 'pass' else ''
            row['Retry4W_W2'] = f"{float(nr['w2_speedup']):.2f}" if nr['w2_speedup'] and nr['w2_status'] == 'pass' else ''
            row['Retry4W_W3'] = f"{float(nr['w3_speedup']):.2f}" if nr['w3_speedup'] and nr['w3_status'] == 'pass' else ''
            row['Retry4W_W4'] = f"{float(nr['w4_speedup']):.2f}" if nr['w4_speedup'] and nr['w4_status'] == 'pass' else ''
        else:
            row['Retry4W_SF10'] = ''
            row['Retry4W_Worker'] = ''
            row['Retry4W_W1'] = ''
            row['Retry4W_W2'] = ''
            row['Retry4W_W3'] = ''
            row['Retry4W_W4'] = ''

        rows.append(row)

# Sort by best speedup
rows.sort(key=lambda x: float(x['Best_Speedup'].replace('x', '') if x['Best_Speedup'] else '0'), reverse=True)

# Write updated leaderboard
output = PROJECT_ROOT / "research/CONSOLIDATED_BENCHMARKS/DuckDB_TPC-DS_Leaderboard_v3_20260206.csv"
with open(output, 'w', newline='') as f:
    writer = csv.DictWriter(f, fieldnames=fieldnames)
    writer.writeheader()
    writer.writerows(rows)

print(f"Updated leaderboard: {output}")

# Summary
print("\n" + "="*70)
print("UPDATED LEADERBOARD SUMMARY")
print("="*70)

wins = [r for r in rows if r['Status'] == 'WIN']
improved = [r for r in rows if r['Status'] == 'IMPROVED']
neutral = [r for r in rows if r['Status'] == 'NEUTRAL']
regression = [r for r in rows if r['Status'] == 'REGRESSION']

print(f"WIN (≥1.5x): {len(wins)}")
print(f"IMPROVED (1.1-1.5x): {len(improved)}")
print(f"NEUTRAL (0.95-1.1x): {len(neutral)}")
print(f"REGRESSION (<0.95x): {len(regression)}")

print("\nTop 25 by speedup:")
print(f"{'Query':<6} {'Best':<8} {'Source':<10} {'Status':<12}")
print("-" * 45)
for row in rows[:25]:
    print(f"{row['Query']:<6} {row['Best_Speedup']:<8} {row['Best_Source']:<10} {row['Status']:<12}")
