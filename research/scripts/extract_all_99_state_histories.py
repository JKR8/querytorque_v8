#!/usr/bin/env python3
"""
Extract complete state histories for all 99 TPC-DS queries.
Sources:
- DuckDB_TPC-DS_Master_v2_20260206.csv (master leaderboard)
- Individual validation results (SF1, SF10)
- Retry results (3-worker, 4-worker)
"""

import csv
import json
import yaml
from pathlib import Path
from collections import defaultdict

def parse_master_leaderboard():
    """Parse the master leaderboard CSV for all 99 queries."""

    master_file = Path('/mnt/c/Users/jakc9/Documents/QueryTorque_V8/research/CONSOLIDATED_BENCHMARKS/DuckDB_TPC-DS_Master_v2_20260206.csv')

    if not master_file.exists():
        print(f"❌ File not found: {master_file}")
        return None

    queries = {}

    with open(master_file) as f:
        reader = csv.DictReader(f)
        for row in reader:
            query_num = row['Query_Num']

            # Build state history for this query
            history = {
                'query_id': f'q{query_num}',
                'query_num': int(query_num),
                'baseline_speedup': 1.0,
                'classification': row.get('Classification', 'UNKNOWN'),
                'states': [
                    {
                        'state_id': 'baseline',
                        'speedup': 1.0,
                        'model': None,
                        'transforms': [],
                        'status': 'success',
                        'error': None,
                        'description': 'Original unoptimized query'
                    }
                ]
            }

            # Kimi attempt
            if row.get('Kimi_Status') == 'pass' and row.get('Kimi_Speedup'):
                try:
                    speedup = float(row['Kimi_Speedup'])
                    error = row.get('Kimi_Error')
                    history['states'].append({
                        'state_id': 'kimi',
                        'speedup': speedup,
                        'model': 'kimi_k2.5',
                        'transforms': [row.get('Gold_Transform', '?')],
                        'status': 'error' if error else ('success' if speedup >= 1.1 else 'neutral' if speedup >= 0.95 else 'regression'),
                        'error': {
                            'type': 'execution',
                            'message': error[:100] if error else None
                        } if error else None,
                        'description': f'Kimi K2.5 model'
                    })
                except (ValueError, TypeError):
                    pass

            # V2 attempt
            if row.get('V2_Status') == 'success' and row.get('V2_Syntax_Valid') == 'True':
                error = row.get('V2_Error')
                history['states'].append({
                    'state_id': 'v2_standard',
                    'speedup': 1.0,  # V2 data structure doesn't have speedup
                    'model': 'v2_standard',
                    'transforms': [row.get('Transform_Recommended', '?')],
                    'status': 'error' if error else 'success',
                    'error': {
                        'type': 'execution',
                        'message': error[:100] if error else None
                    } if error else None,
                    'syntax_valid': True,
                    'description': 'V2 Standard model'
                })

            # Retry3W results (best of 3 workers)
            if row.get('Retry3W_Status') == 'pass':
                try:
                    speedup = float(row.get('Retry3W_SF10_Speedup', 0))
                    if speedup > 0:
                        best_worker = row.get('Retry3W_Best_Worker', '?')
                        history['states'].append({
                            'state_id': f'retry3w_{best_worker}',
                            'speedup': speedup,
                            'model': 'v2_evolutionary',
                            'worker': best_worker,
                            'transforms': [],  # Would need to reconstruct from worker-specific data
                            'status': 'success' if speedup >= 1.1 else 'neutral' if speedup >= 0.95 else 'regression',
                            'error': None,
                            'description': f'3-Worker Retry (SF10) - best from {best_worker}'
                        })
                except (ValueError, TypeError):
                    pass

            # Track best speedup
            speedups = [s['speedup'] for s in history['states'][1:] if s['speedup'] > 0]
            if speedups:
                history['best_speedup'] = max(speedups)
            else:
                history['best_speedup'] = 1.0

            queries[f'q{query_num}'] = history

    return queries

def extract_all_99_histories():
    """Extract state histories for all 99 queries."""

    print("📊 Extracting complete state histories for all 99 TPC-DS queries...")
    print()

    # Parse master leaderboard
    queries = parse_master_leaderboard()
    if not queries:
        return

    # Create output directory
    output_dir = Path('/mnt/c/Users/jakc9/Documents/QueryTorque_V8/research/state_histories_all_99')
    output_dir.mkdir(exist_ok=True, parents=True)

    # Write individual YAML files
    for query_id in sorted(queries.keys(), key=lambda x: int(x[1:])):
        history = queries[query_id]
        yaml_file = output_dir / f"{query_id}_state_history.yaml"

        with open(yaml_file, 'w') as f:
            yaml.dump(history, f, default_flow_style=False, sort_keys=False)

    # Write master file
    master_file = output_dir / 'state_histories_all_99_master.yaml'
    with open(master_file, 'w') as f:
        yaml.dump(queries, f, default_flow_style=False, sort_keys=False)

    # Summary statistics
    print("✅ Complete state histories extracted!")
    print(f"📁 Location: {output_dir}")
    print(f"📄 Files created:")
    print(f"   - Individual YAML: 99 files (q##_state_history.yaml)")
    print(f"   - Master YAML: state_histories_all_99_master.yaml")

    # Analysis
    print(f"\n📈 Analysis across all 99 queries:")

    classifications = defaultdict(int)
    models_used = defaultdict(int)
    speedup_ranges = {'>= 1.5x': 0, '1.1-1.5x': 0, '0.95-1.1x': 0, '< 0.95x': 0}

    for query_id, history in queries.items():
        classifications[history['classification']] += 1

        # Count models
        for state in history['states'][1:]:
            if state.get('model'):
                models_used[state['model']] += 1

        # Count speedup ranges
        best = history.get('best_speedup', 1.0)
        if best >= 1.5:
            speedup_ranges['>= 1.5x'] += 1
        elif best >= 1.1:
            speedup_ranges['1.1-1.5x'] += 1
        elif best >= 0.95:
            speedup_ranges['0.95-1.1x'] += 1
        else:
            speedup_ranges['< 0.95x'] += 1

    print(f"\nClassifications:")
    for cls in sorted(classifications.keys()):
        count = classifications[cls]
        print(f"  {cls:30s}: {count:2d}")

    print(f"\nModels used:")
    for model in sorted(models_used.keys()):
        count = models_used[model]
        print(f"  {model:30s}: {count:2d} attempts")

    print(f"\nSpeedup ranges:")
    for range_name, count in speedup_ranges.items():
        print(f"  {range_name:15s}: {count:2d} queries")

    print(f"\n🎯 Next steps:")
    print(f"  1. Load individual YAML files for query analysis")
    print(f"  2. Identify B→C transitions (missing intermediate states)")
    print(f"  3. Run full validation on all 99 to fill gaps")
    print(f"  4. Integrate error messages from validation logs")

if __name__ == '__main__':
    extract_all_99_histories()
