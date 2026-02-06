#!/usr/bin/env python3
"""
Enhanced state history extraction with error tracking.
Categorizes failures and creates structure for error information.
"""

import json
import yaml
from pathlib import Path
from collections import defaultdict

def categorize_failure(speedup, transforms):
    """Categorize type of failure based on speedup and transforms."""
    if speedup == 0.0:
        return {
            'type': 'ERROR',
            'category': 'execution',
            'description': 'Query execution failed or timed out',
            'likely_causes': ['syntax_error', 'semantic_error', 'timeout', 'memory_limit']
        }
    elif speedup < 0.5:
        return {
            'type': 'SEVERE_REGRESSION',
            'category': 'performance',
            'description': f'Query became {speedup:.2f}x slower - severe regression',
            'likely_causes': ['cartesian_product', 'unnecessary_materialization', 'bad_join_order']
        }
    elif speedup < 0.95:
        return {
            'type': 'REGRESSION',
            'category': 'performance',
            'description': f'Query became {speedup:.2f}x slower',
            'likely_causes': ['suboptimal_join_order', 'excessive_filtering', 'inefficient_grouping']
        }
    return None

def check_known_issues(query_id, transforms, speedup):
    """Check for known issues with transforms."""
    issues = []

    # OR_to_UNION limit violation
    if 'or_to_union' in transforms:
        # Queries known to have issues with or_to_union
        or_to_union_risky = ['q13', 'q25', 'q48', 'q49']
        if query_id in or_to_union_risky:
            issues.append({
                'type': 'CONSTRAINT_VIOLATION',
                'transform': 'or_to_union',
                'issue': 'OR→UNION may exceed 3-branch limit',
                'reference': 'constraints/or_to_union_limit.json'
            })

    return issues

def extract_enhanced_state_history():
    """Extract state history with enhanced error tracking."""

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
    print(f"📊 Processing {len(queries)} queries with enhanced error tracking...\n")

    all_histories = {}
    failure_patterns = defaultdict(lambda: {
        'queries': [],
        'count': 0,
        'avg_speedup': 0,
        'error_types': defaultdict(int)
    })

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
            'status': 'success',
            'error': None,
            'description': 'Original unoptimized query'
        })

        # Add states for each worker
        speedups = query_data.get('speedups', {})
        patterns = query_data.get('patterns_by_worker', {})

        for worker_id in sorted(['w1', 'w2', 'w3', 'w4']):
            speedup = speedups.get(worker_id)
            worker_patterns = patterns.get(worker_id, [])

            # Skip if not run
            if speedup is None:
                continue

            # Determine status
            if speedup == 0.0:
                status = 'error'
                failure_info = categorize_failure(speedup, worker_patterns)
                error_obj = {
                    'type': failure_info['category'],
                    'message': failure_info['description'],
                    'likely_causes': failure_info['likely_causes']
                }
            elif speedup < 0.95:
                status = 'regression'
                failure_info = categorize_failure(speedup, worker_patterns)
                error_obj = {
                    'type': failure_info['category'],
                    'message': failure_info['description'],
                    'likely_causes': failure_info['likely_causes']
                }
            else:
                status = 'success'
                error_obj = None

            # Check for known issues
            known_issues = check_known_issues(query_id, worker_patterns, speedup)

            state = {
                'state_id': worker_id,
                'speedup': speedup,
                'transforms': worker_patterns,
                'worker': worker_id,
                'model': 'unknown',  # Placeholder
                'prompt': 'unknown',  # Placeholder
                'status': status,
                'error': error_obj,
                'known_issues': known_issues if known_issues else None,
                'description': f'After applying {len(worker_patterns)} transform(s)'
            }

            history['states'].append(state)

            # Track failure patterns
            if status != 'success':
                for pattern in worker_patterns:
                    failure_patterns[pattern]['queries'].append(query_id)
                    failure_patterns[pattern]['count'] += 1
                    failure_patterns[pattern]['error_types'][status] += 1

        all_histories[query_id] = history

        # Write individual YAML file
        yaml_file = output_dir / f"{query_id}_state_history.yaml"
        with open(yaml_file, 'w') as f:
            yaml.dump(history, f, default_flow_style=False, sort_keys=False)

        # Print summary
        failures = sum(1 for s in history['states'][1:] if s.get('status') != 'success')
        print(f"  {query_id:4s} | {history['best_speedup']:6.2f}x | {history['category']:12s} | {failures} failures")

    # Write master state history file
    master_file = output_dir / 'state_histories_master.yaml'
    with open(master_file, 'w') as f:
        yaml.dump(all_histories, f, default_flow_style=False, sort_keys=False)

    # Analyze failure patterns by transform
    print(f"\n{'='*70}")
    print(f"FAILURE PATTERN ANALYSIS BY TRANSFORM")
    print(f"{'='*70}\n")

    for transform in sorted(failure_patterns.keys()):
        pattern = failure_patterns[transform]
        errors = dict(pattern['error_types'])
        print(f"Transform: {transform}")
        print(f"  Failure count: {pattern['count']}")
        print(f"  Error types: {errors}")
        print(f"  Queries affected: {', '.join(sorted(set(pattern['queries'])))}")
        print()

    # Write failure analysis
    failure_analysis = {
        'summary': {
            'total_transforms_analyzed': len(failure_patterns),
            'total_failures': sum(p['count'] for p in failure_patterns.values()),
            'timestamp': data['metadata']['timestamp']
        },
        'by_transform': {
            t: {
                'count': p['count'],
                'error_types': dict(p['error_types']),
                'affected_queries': sorted(set(p['queries']))
            }
            for t, p in failure_patterns.items()
        }
    }

    analysis_file = output_dir / 'failure_analysis.yaml'
    with open(analysis_file, 'w') as f:
        yaml.dump(failure_analysis, f, default_flow_style=False, sort_keys=False)

    print(f"{'='*70}")
    print(f"✅ Enhanced state histories extracted!")
    print(f"📁 Location: {output_dir}")
    print(f"📄 Files created:")
    print(f"   - Individual YAML: {len(all_histories)} files (q##_state_history.yaml)")
    print(f"   - Master YAML: state_histories_master.yaml")
    print(f"   - Failure Analysis: failure_analysis.yaml")

if __name__ == '__main__':
    extract_enhanced_state_history()
