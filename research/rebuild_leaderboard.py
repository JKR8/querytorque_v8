"""Rebuild DuckDB leaderboard v5 with corrected retry benchmarks."""
import csv
import json
import os

os.chdir("/mnt/c/Users/jakc9/Documents/QueryTorque_V8")

# Load corrected benchmark results
with open("research/retry_rebenchmark_results.json") as f:
    corrected = json.load(f)

# Build lookup: query -> corrected result
corrected_map = {}
for r in corrected:
    q = r["query"]  # e.g. "Q40"
    corrected_map[q] = r

# Read current leaderboard
with open("research/archive/benchmark_results/CONSOLIDATED_BENCHMARKS/DuckDB_TPC-DS_Leaderboard_v4_20260214.csv") as f:
    reader = csv.DictReader(f)
    rows = list(reader)
    fieldnames = reader.fieldnames

def parse_speedup(val):
    """Parse '2.16x' -> 2.16, or '' -> None."""
    if not val or val.strip() == '':
        return None
    return float(val.replace('x', ''))

def format_speedup(val):
    """Format 2.16 -> '2.16x'."""
    if val is None:
        return ''
    return f"{val:.2f}x"

def classify(speedup):
    if speedup is None:
        return "NO_DATA"
    if speedup >= 1.5:
        return "WIN"
    if speedup >= 1.05:
        return "IMPROVED"
    if speedup >= 0.95:
        return "NEUTRAL"
    return "REGRESSION"

updated_rows = []
changes = []

for row in rows:
    query = row['Query']
    old_status = row['Status']

    if old_status != 'PARAM_MISMATCH':
        updated_rows.append(row)
        continue

    # Check if we have corrected data
    corr = corrected_map.get(query)

    if corr is None:
        # Q74, Q76 - Kimi PARAM_MISMATCH, no retry data
        updated_rows.append(row)
        continue

    # Get corrected retry speedup
    if corr['status'] == 'OK':
        retry_speedup = corr['speedup']
    else:
        retry_speedup = None

    # Get Kimi speedup
    kimi_speedup = parse_speedup(row.get('Kimi', ''))

    # Get Evo speedup (leave as-is, note as unverified)
    evo_speedup = parse_speedup(row.get('Evo', ''))

    # Determine best from verified sources (Kimi + corrected retry)
    candidates = []
    if kimi_speedup is not None:
        candidates.append(('Kimi', kimi_speedup))
    if retry_speedup is not None:
        source_label = corr['source']
        candidates.append((source_label, retry_speedup))

    if not candidates:
        updated_rows.append(row)
        continue

    best_source, best_speedup = max(candidates, key=lambda x: x[1])
    new_status = classify(best_speedup)

    # Update the retry column with corrected value
    if corr['source'] == 'Retry3W':
        row['Retry3W_SF10'] = format_speedup(retry_speedup)
    elif corr['source'] == 'Retry4W':
        row['Retry4W_SF10'] = format_speedup(retry_speedup)

    # Update best
    old_best = row['Best_Speedup']
    row['Best_Speedup'] = format_speedup(best_speedup)
    row['Best_Source'] = best_source
    row['Status'] = new_status

    # Update timing columns based on best source
    if best_source == 'Kimi':
        # Keep existing Kimi timings (already in the row)
        pass
    elif retry_speedup is not None and best_source in ('Retry3W', 'Retry4W'):
        row['Original_ms'] = str(corr['orig_ms'])
        row['Optimized_ms'] = str(corr['opt_ms'])

    changes.append(f"  {query}: {old_best} {old_status} -> {format_speedup(best_speedup)} {new_status} (Kimi={format_speedup(kimi_speedup)}, Retry_corrected={format_speedup(retry_speedup)})")

    updated_rows.append(row)

# Sort by best speedup descending
def sort_key(row):
    s = parse_speedup(row['Best_Speedup'])
    if s is None:
        return -999
    return s

updated_rows.sort(key=sort_key, reverse=True)

# Write v5
output_path = "research/archive/benchmark_results/CONSOLIDATED_BENCHMARKS/DuckDB_TPC-DS_Leaderboard_v5_20260214.csv"
with open(output_path, 'w', newline='') as f:
    writer = csv.DictWriter(f, fieldnames=fieldnames)
    writer.writeheader()
    writer.writerows(updated_rows)

# Summary stats
statuses = {}
for row in updated_rows:
    s = row['Status']
    statuses[s] = statuses.get(s, 0) + 1

print("CHANGES:")
for c in changes:
    print(c)

print(f"\nLeaderboard v5 written to {output_path}")
print(f"\nSummary:")
for status in ['WIN', 'IMPROVED', 'NEUTRAL', 'REGRESSION', 'PARAM_MISMATCH', 'NO_DATA']:
    print(f"  {status}: {statuses.get(status, 0)}")

# Count total queries
total = len(updated_rows)
optimizable = statuses.get('WIN', 0) + statuses.get('IMPROVED', 0)
print(f"\nTotal queries: {total}")
print(f"Optimizable (WIN+IMPROVED): {optimizable}/{total - statuses.get('NO_DATA', 0)} ({100*optimizable/(total - statuses.get('NO_DATA', 0)):.1f}%)")
