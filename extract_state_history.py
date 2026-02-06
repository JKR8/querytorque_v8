#!/usr/bin/env python3
"""
Extract state history from validation records.
Creates YAML files for each query showing:
- Baseline state
- Progressive states from each worker/model
- Transforms applied and speedup achieved
"""

import json
import yaml
from pathlib import Path
from collections import defaultdict

def extract_state_history():
    """Extract state history from validation records."""

    # Load validation data
    validation_file = Path('/mnt/d/validation_output/validation_record_sf10_complete.json')

    if not validation_file.exists():
        print(f"❌ File not found: {validation_file}")
        return

    with open(validation_file) as f:
        data = json.load(f)

    # Create output directory
    output_dir = Path('/mnt/c/Users/jakc9/Documents/QueryTorque_V8/research/state_histories')
    output_dir.mkdir(exist_ok=True, parents=True)

    queries = data.get('queries', {})
    print(f"📊 Processing {len(queries)} queries...")

    all_histories = {}

    # Process each query
    for query_id, query_data in sorted(queries.items()):
        history = {
            'query_id': query_id,
            'baseline_speedup': 1.0,
            'best_speedup': query_data.get('best_speedup', 0),
            'best_worker': query_data.get('best_worker', 'none'),
            'category': query_data.get('category', 'UNKNOWN'),
            'timestamp': data['metadata']['timestamp'],
            'scale': data['metadata']['validation_scale'],
            'states': []
        }

        # Add baseline state
        history['states'].append({
            'state_id': 'baseline',
            'speedup': 1.0,
            'transforms': [],
            'worker': None,
            'model': None,
            'prompt': None,
            'description': 'Original unoptimized query'
        })

        # Add states for each worker
        speedups = query_data.get('speedups', {})
        patterns = query_data.get('patterns_by_worker', {})

        for worker_id in sorted(['w1', 'w2', 'w3', 'w4']):
            speedup = speedups.get(worker_id)
            worker_patterns = patterns.get(worker_id, [])

            # Skip if no speedup (error or not run)
            if speedup is None or speedup == 0.0:
                continue

            history['states'].append({
                'state_id': worker_id,
                'speedup': speedup,
                'transforms': worker_patterns,
                'worker': worker_id,
                'model': 'unknown',  # Will be filled in when we have this data
                'prompt': 'unknown',  # Will be filled in when we have this data
                'description': f'After applying {len(worker_patterns)} transform(s)'
            })

        all_histories[query_id] = history

        # Write individual YAML file
        yaml_file = output_dir / f"{query_id}_state_history.yaml"
        with open(yaml_file, 'w') as f:
            yaml.dump(history, f, default_flow_style=False, sort_keys=False)

        print(f"  ✓ {query_id}: {history['best_speedup']:.2f}x ({history['category']}) - {len(history['states'])-1} worker attempts")

    # Write master state history file
    master_file = output_dir / 'state_histories_master.yaml'
    with open(master_file, 'w') as f:
        yaml.dump(all_histories, f, default_flow_style=False, sort_keys=False)

    print(f"\n✅ State histories extracted!")
    print(f"📁 Location: {output_dir}")
    print(f"📄 Files created:")
    print(f"   - Individual YAML: {len(all_histories)} files (q##_state_history.yaml)")
    print(f"   - Master YAML: state_histories_master.yaml")

    # Summary statistics
    print(f"\n📈 Summary:")
    wins = sum(1 for h in all_histories.values() if h['category'] == 'WIN')
    passes = sum(1 for h in all_histories.values() if h['category'] == 'NEUTRAL')
    failures = sum(1 for h in all_histories.values() if h['category'] == 'REGRESSION')
    print(f"   Wins: {wins} | Passes: {passes} | Failures: {failures}")

    # Identify queries with limited intermediate states (baseline + 1 worker only)
    print(f"\n🔍 Queries with missing intermediate states (B→C transitions):")
    limited = []
    for qid, hist in sorted(all_histories.items()):
        num_states = len(hist['states'])
        if num_states == 2:  # Only baseline + 1 worker
            limited.append((qid, hist['best_speedup'], num_states))

    if limited:
        print(f"   {len(limited)} queries have only baseline→optimization (missing B→C):")
        for qid, speedup, num_states in limited[:5]:
            print(f"      {qid}: {speedup:.2f}x ({num_states} states)")
        if len(limited) > 5:
            print(f"      ... and {len(limited)-5} more")
    else:
        print(f"   ✓ All queries have multiple worker attempts")

if __name__ == '__main__':
    extract_state_history()
