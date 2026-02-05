#!/usr/bin/env python3
"""Update the master benchmark board with 3-worker retry results."""

import csv
from pathlib import Path
from datetime import datetime

PROJECT_ROOT = Path(__file__).parent.parent
MASTER = PROJECT_ROOT / "research/CONSOLIDATED_BENCHMARKS/DuckDB_TPC-DS_Master_v1_20260205.csv"
SF5_RESULTS = PROJECT_ROOT / "retry_collect/validation_20260206_000844.csv"
SF10_RESULTS = PROJECT_ROOT / "retry_collect/validation_20260206_002648.csv"

# Load SF5 results
sf5_data = {}
with open(SF5_RESULTS) as f:
    reader = csv.DictReader(f)
    for row in reader:
        qnum = int(row['query_id'].replace('q', ''))
        sf5_data[qnum] = row

# Load SF10 results
sf10_data = {}
with open(SF10_RESULTS) as f:
    reader = csv.DictReader(f)
    for row in reader:
        qnum = int(row['query_id'].replace('q', ''))
        sf10_data[qnum] = row

# Load master board
master_rows = []
with open(MASTER) as f:
    reader = csv.DictReader(f)
    fieldnames = reader.fieldnames + [
        'Retry3W_SF5_Speedup', 'Retry3W_SF10_Speedup',
        'Retry3W_Best_Worker', 'Retry3W_Status',
        'Retry3W_W1', 'Retry3W_W2', 'Retry3W_W3'
    ]
    for row in reader:
        qnum = int(row['Query_Num'])

        if qnum in sf10_data:
            sf5 = sf5_data.get(qnum, {})
            sf10 = sf10_data[qnum]

            row['Retry3W_SF5_Speedup'] = f"{float(sf5.get('best_speedup', 0)):.2f}" if sf5 else ""
            row['Retry3W_SF10_Speedup'] = f"{float(sf10['best_speedup']):.2f}"
            row['Retry3W_Best_Worker'] = sf10['best_worker']
            row['Retry3W_Status'] = sf10['best_status']
            row['Retry3W_W1'] = f"{float(sf10['w1_speedup']):.2f}" if sf10['w1_speedup'] else ""
            row['Retry3W_W2'] = f"{float(sf10['w2_speedup']):.2f}" if sf10['w2_speedup'] else ""
            row['Retry3W_W3'] = f"{float(sf10['w3_speedup']):.2f}" if sf10['w3_speedup'] else ""
        else:
            row['Retry3W_SF5_Speedup'] = ""
            row['Retry3W_SF10_Speedup'] = ""
            row['Retry3W_Best_Worker'] = ""
            row['Retry3W_Status'] = ""
            row['Retry3W_W1'] = ""
            row['Retry3W_W2'] = ""
            row['Retry3W_W3'] = ""

        master_rows.append(row)

# Write updated master board
timestamp = datetime.now().strftime("%Y%m%d")
output_file = PROJECT_ROOT / f"research/CONSOLIDATED_BENCHMARKS/DuckDB_TPC-DS_Master_v2_{timestamp}.csv"

with open(output_file, 'w', newline='') as f:
    writer = csv.DictWriter(f, fieldnames=fieldnames)
    writer.writeheader()
    writer.writerows(master_rows)

print(f"Updated master board: {output_file}")
print(f"\n3-Worker Retry Summary (SF10):")

# Print summary
improved = []
for row in master_rows:
    if row['Retry3W_SF10_Speedup'] and row['Retry3W_Status'] == 'pass':
        speedup = float(row['Retry3W_SF10_Speedup'])
        if speedup > 1.0:
            improved.append((int(row['Query_Num']), speedup, row['Retry3W_Best_Worker']))

improved.sort(key=lambda x: -x[1])
print(f"Queries improved (>1.0x): {len(improved)}")
for qnum, speedup, worker in improved:
    print(f"  Q{qnum}: {speedup:.2f}x (W{worker})")
